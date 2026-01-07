"""Tests for schema-to-domain converters and validation wrappers."""

import datetime
import pytest
from peeps_scheduler.constants import DEFAULT_EVENT_DURATION, TIMEZONE
from peeps_scheduler.models import Peep, Role, SwitchPreference
from peeps_scheduler.validation.converters import (
    convert_to_peeps,
    event_spec_to_event,
    member_to_peep,
)
from peeps_scheduler.validation.fields import ValidationContext
from peeps_scheduler.validation.file_schemas.cancellations_json import CancellationsJsonSchema
from peeps_scheduler.validation.file_schemas.members_csv import (
    MembersCsvFileSchema,
)
from peeps_scheduler.validation.file_schemas.partnerships_json import PartnershipsJsonSchema
from peeps_scheduler.validation.file_schemas.responses_csv import ResponsesCsvFileSchema
from peeps_scheduler.validation.parsers import EventSpec
from tests.validation.fixtures import (
    cancellations_data,
    member_data,
    partnerships_json_data,
    response_data,
)


@pytest.mark.contract
class TestMemberToPeep:
    """Tests for member_to_peep factory function."""

    def test_maps_member_fields_without_response(self, ctx):
        """Happy path: Maps all member schema fields to Peep correctly."""
        member_schemas = MembersCsvFileSchema.model_validate(
            [
                member_data(
                    {
                        "id": "42",
                        "Name": "Bob Builder",
                        "Display Name": "Bobby",
                        "Email Address": "bob@example.com",
                        "Role": "Follower",
                        "Index": "3",
                        "Priority": "8",
                        "Total Attended": "15",
                        "Active": "FALSE",
                        "Date Joined": "3/15/2021",
                    }
                )
            ]
        ).root
        member_schema = member_schemas[0]

        peep = member_to_peep(member_schema)

        assert peep.id == 42
        assert peep.full_name == "Bob Builder"
        assert peep.display_name == "Bobby"
        assert peep.email == "bob@example.com"
        assert peep.role == Role.FOLLOWER
        assert peep.index == 3
        assert peep.priority == 8
        assert peep.total_attended == 15
        assert not peep.active
        assert peep.date_joined == datetime.date(2021, 3, 15)
        assert peep.responded is False

    def test_member_with_response_overrides_role(self, ctx):
        """Edge case: Response primary_role overrides member role."""
        member_schemas = MembersCsvFileSchema.model_validate(
            [member_data({"Role": "Leader"})]
        ).root
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        response_schema = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [response_data({"Primary Role": "Follower"})],
                "event_rows": None,
            },
            context={"ctx": response_ctx},
        )

        peep = member_to_peep(member_schema, response_schema)

        # Response role should override member role
        assert peep.role == Role.FOLLOWER

    def test_member_with_response_adds_availability(self, ctx):
        """Edge case: Response availability is added to peep."""
        member_schemas = MembersCsvFileSchema.model_validate(
            [member_data()]
        ).root
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        response_schema = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [
                    response_data(
                        {"Availability": "Saturday January 4 - 1pm, Sunday January 5 - 2pm"}
                    )
                ],
                "event_rows": None,
            },
            context={"ctx": response_ctx},
        )

        peep = member_to_peep(member_schema, response_schema)

        assert len(peep.availability) == 2
        assert all(isinstance(event_date, datetime.datetime) for event_date in peep.availability)

    def test_member_with_response_sets_switch_preference(self, ctx):
        """Edge case: Response secondary_role becomes switch_pref."""
        member_schemas = MembersCsvFileSchema.model_validate(
            [member_data()]
        ).root
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        response_schema = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [
                    response_data(
                        {
                            "Secondary Role": "I'm willing to dance my secondary role only if it's needed to enable filling a session"
                        }
                    )
                ],
                "event_rows": None,
            },
            context={"ctx": response_ctx},
        )

        peep = member_to_peep(member_schema, response_schema)

        assert peep.switch_pref == SwitchPreference.SWITCH_IF_NEEDED

    def test_member_with_response_sets_event_limit(self, ctx):
        """Edge case: Response max_sessions becomes event_limit."""
        member_schemas = MembersCsvFileSchema.model_validate(
            [member_data()]
        ).root
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        response_schema = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [response_data({"Max Sessions": "4"})],
                "event_rows": None,
            },
            context={"ctx": response_ctx},
        )

        peep = member_to_peep(member_schema, response_schema)

        assert peep.event_limit == 4

    def test_member_with_response_sets_min_interval_days(self, ctx):
        """Edge case: Response min_interval_days is set correctly."""
        member_schemas = MembersCsvFileSchema.model_validate(
            [member_data()]
        ).root
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        response_schema = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [response_data({"Min Interval Days": "7"})],
                "event_rows": None,
            },
            context={"ctx": response_ctx},
        )

        peep = member_to_peep(member_schema, response_schema)

        assert peep.min_interval_days == 7

    def test_member_with_response_marks_responded(self, ctx):
        """Edge case: responded flag is True when response provided."""
        member_schemas = MembersCsvFileSchema.model_validate(
            [member_data()]
        ).root
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        response_schema = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [response_data()],
                "event_rows": None,
            },
            context={"ctx": response_ctx},
        )

        peep = member_to_peep(member_schema, response_schema)

        assert peep.responded is True

    def test_member_with_none_email_address(self, ctx):
        """Edge case: Member with None email_address (inactive)."""
        member_schemas = MembersCsvFileSchema.model_validate(
            [
                member_data(
                    {
                        "Email Address": "",
                        "Active": "FALSE",
                    }
                )
            ]
        ).root
        member_schema = member_schemas[0]

        peep = member_to_peep(member_schema)

        # Email should be empty or None, not raise error
        assert peep.email == "" or peep.email is None


@pytest.mark.unit
class TestEventSpecToEvent:
    """Tests for event_spec_to_event factory function."""

    def test_maps_event_spec_to_event(self, ctx):
        """Happy path: directly maps EventSpec to Event."""
        spec = EventSpec(
            start=datetime.datetime(2020, 1, 4, 13, 0),
            duration_minutes=90,
            raw="Saturday January 4 - 1pm",
        )
        event = event_spec_to_event(spec)

        assert event.date == spec.start
        assert event.duration_minutes == spec.duration_minutes

    def test_event_duration_none_gets_default_duration(self, ctx):
        """EventSpecs with no duration generate Events with default duration"""
        spec = EventSpec(
            start=datetime.datetime(2020, 1, 4, 13, 0),
            duration_minutes=None,
            raw="Saturday January 4 - 1pm",
        )
        event = event_spec_to_event(spec)

        assert event.duration_minutes == DEFAULT_EVENT_DURATION


@pytest.mark.contract
class TestConvertToPeeps:
    """Tests for convert_to_peeps extraction function."""

    def test_converts_members_and_responses_to_peeps(self, ctx):
        """Happy path: Converts member and response dicts to Peep objects."""
        from peeps_scheduler.validation.converters import convert_to_peeps

        validated_members = MembersCsvFileSchema.model_validate([member_data()]).root
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        validated_responses = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [response_data()],
                "event_rows": None,
            },
            context={"ctx": response_ctx},
        )

        peeps = convert_to_peeps(validated_members, validated_responses)

        assert len(peeps) == 1
        assert isinstance(peeps[0], Peep)
        assert peeps[0].full_name == "Alice Alpha"
        assert peeps[0].responded is True

    def test_handles_members_without_responses(self, ctx):
        """Edge case: Members without responses are still converted to Peeps."""
        from peeps_scheduler.validation.converters import convert_to_peeps

        validated_members = MembersCsvFileSchema.model_validate([member_data()]).root

        peeps = convert_to_peeps(validated_members, {})

        assert len(peeps) == 1
        assert peeps[0].responded is False

    def test_convert_to_peeps_matches_normalized_gmail_addresses(self, ctx):
        """
        Test that convert_to_peeps() correctly matches members and responses
        when Gmail addresses use different dot patterns.
        """
        validated_members = MembersCsvFileSchema.model_validate(
            [
                member_data(
                    {
                        "Name": "John Smith",
                        "Email Address": "john.smith@gmail.com",  # Has dots
                    }
                )
            ]
        ).root
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        validated_responses = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [
                    response_data(
                        {
                            "Name": "John Smith",
                            "Email Address": "johnsmith@gmail.com",  # No dots
                        }
                    )
                ],
                "event_rows": None,
            },
            context={"ctx": response_ctx},
        )

        # Convert - should match despite different email formats
        peeps = convert_to_peeps(validated_members, validated_responses)

        # Verify match was successful
        assert len(peeps) == 1
        john = peeps[0]

        # Should have response data (proving match worked)
        assert john.responded is True  # Response was matched


@pytest.mark.contract
class TestExtractCancellations:
    """Tests for extract_cancellations extraction function."""

    def test_extracts_cancellations_from_validated_data(self, ctx):
        """Happy path: Extracts cancelled event IDs and availability."""
        from peeps_scheduler.validation.converters import extract_cancellations

        raw = cancellations_data()
        validated = CancellationsJsonSchema.model_validate(raw, context={"ctx": ctx})
        cancelled_event_ids, cancelled_availability = extract_cancellations(validated)

        assert isinstance(cancelled_event_ids, set)
        assert isinstance(cancelled_availability, dict)
        assert len(cancelled_event_ids) > 0

    def test_returns_empty_for_none_input(self):
        """Edge case: Returns empty collections if input is None."""
        from peeps_scheduler.validation.converters import extract_cancellations

        cancelled_event_ids, cancelled_availability = extract_cancellations(None)

        assert cancelled_event_ids == set()
        assert cancelled_availability == {}


@pytest.mark.contract
class TestExtractPartnerships:
    """Tests for extract_partnerships extraction function."""

    def test_extracts_partnerships_from_validated_data(self):
        """Happy path: Extracts partnership mappings."""
        from peeps_scheduler.validation.converters import extract_partnerships

        raw = partnerships_json_data()
        validated = PartnershipsJsonSchema.model_validate(raw)
        partnerships = extract_partnerships(validated)

        assert isinstance(partnerships, dict)
        assert len(partnerships) > 0
        # Check structure: requester_id -> set of partner_ids
        for requester_id, partner_ids in partnerships.items():
            assert isinstance(requester_id, int)
            assert isinstance(partner_ids, set)

    def test_returns_empty_for_none_input(self):
        """Edge case: Returns empty dict if input is None."""
        from peeps_scheduler.validation.converters import extract_partnerships

        partnerships = extract_partnerships(None)

        assert partnerships == {}


