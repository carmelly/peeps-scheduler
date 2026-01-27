"""Tests for schema-to-domain converters and validation wrappers."""

import datetime
import pytest
from peeps_scheduler.constants import DEFAULT_EVENT_DURATION, DEFAULT_TIMEZONE
from peeps_scheduler.models import (
    CancelledMemberAvailability,
    Event,
    PartnershipRequest,
    Peep,
    Role,
    SwitchPreference,
)
from peeps_scheduler.validation.builders import (
    _event_spec_to_event,
    _member_to_peep,
    build_cancelled_availability,
    build_cancelled_events,
    build_partnerships,
    build_peeps,
)
from peeps_scheduler.validation.fields import ValidationContext
from peeps_scheduler.validation.file_schemas.members_csv import (
    MembersCsvFileSchema,
)
from peeps_scheduler.validation.file_schemas.period import (
    CancelledAvailabilityJsonSchema,
    PartnershipRequestJsonSchema,
)
from peeps_scheduler.validation.file_schemas.responses_csv import ResponsesCsvFileSchema
from peeps_scheduler.validation.parsers import EventSpec
from tests.validation.fixtures import (
    member_data,
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

        peep = _member_to_peep(member_schema)

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
        member_schemas = MembersCsvFileSchema.model_validate([member_data({"Role": "Leader"})]).root
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=DEFAULT_TIMEZONE)
        response_schema = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [response_data({"Primary Role": "Follower"})],
                "event_rows": None,
            },
            context={"ctx": response_ctx},
        )

        peep = _member_to_peep(member_schema, response_schema)

        # Response role should override member role
        assert peep.role == Role.FOLLOWER

    def test_member_with_response_adds_availability(self, ctx):
        """Edge case: Response availability is added to peep."""
        member_schemas = MembersCsvFileSchema.model_validate([member_data()]).root
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=DEFAULT_TIMEZONE)
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

        peep = _member_to_peep(member_schema, response_schema)

        assert len(peep.availability) == 2
        assert all(isinstance(event_date, datetime.datetime) for event_date in peep.availability)

    def test_member_with_response_sets_switch_preference(self, ctx):
        """Edge case: Response secondary_role becomes switch_pref."""
        member_schemas = MembersCsvFileSchema.model_validate([member_data()]).root
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=DEFAULT_TIMEZONE)
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

        peep = _member_to_peep(member_schema, response_schema)

        assert peep.switch_pref == SwitchPreference.SWITCH_IF_NEEDED

    def test_member_with_response_sets_event_limit(self, ctx):
        """Edge case: Response max_sessions becomes event_limit."""
        member_schemas = MembersCsvFileSchema.model_validate([member_data()]).root
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=DEFAULT_TIMEZONE)
        response_schema = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [response_data({"Max Sessions": "4"})],
                "event_rows": None,
            },
            context={"ctx": response_ctx},
        )

        peep = _member_to_peep(member_schema, response_schema)

        assert peep.event_limit == 4

    def test_member_with_response_sets_min_interval_days(self, ctx):
        """Edge case: Response min_interval_days is set correctly."""
        member_schemas = MembersCsvFileSchema.model_validate([member_data()]).root
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=DEFAULT_TIMEZONE)
        response_schema = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [response_data({"Min Interval Days": "7"})],
                "event_rows": None,
            },
            context={"ctx": response_ctx},
        )

        peep = _member_to_peep(member_schema, response_schema)

        assert peep.min_interval_days == 7

    def test_member_with_response_marks_responded(self, ctx):
        """Edge case: responded flag is True when response provided."""
        member_schemas = MembersCsvFileSchema.model_validate([member_data()]).root
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=DEFAULT_TIMEZONE)
        response_schema = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [response_data()],
                "event_rows": None,
            },
            context={"ctx": response_ctx},
        )

        peep = _member_to_peep(member_schema, response_schema)

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

        peep = _member_to_peep(member_schema)

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
        event = _event_spec_to_event(spec)

        assert event.date == spec.start
        assert event.duration_minutes == spec.duration_minutes

    def test_event_duration_none_gets_default_duration(self, ctx):
        """EventSpecs with no duration generate Events with default duration"""
        spec = EventSpec(
            start=datetime.datetime(2020, 1, 4, 13, 0),
            duration_minutes=None,
            raw="Saturday January 4 - 1pm",
        )
        event = _event_spec_to_event(spec)

        assert event.duration_minutes == DEFAULT_EVENT_DURATION


@pytest.mark.contract
class TestBuildPeeps:
    """Tests for build_peeps extraction function."""

    def test_builds_members_and_responses_to_peeps(self, ctx):
        """Happy path: Converts member and response dicts to Peep objects."""

        validated_members = MembersCsvFileSchema.model_validate([member_data()]).root
        response_ctx = ValidationContext(year=2020, tz=DEFAULT_TIMEZONE)
        validated_responses = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [response_data()],
                "event_rows": None,
            },
            context={"ctx": response_ctx},
        )

        peeps = build_peeps(validated_members, validated_responses)

        assert len(peeps) == 1
        assert isinstance(peeps[0], Peep)
        assert peeps[0].full_name == "Alice Alpha"
        assert peeps[0].responded is True

    def test_secondary_role_none_coerced_to_primary_only(self, ctx):
        """Edge case: Secondary role of None becomes SwitchPreference.PRIMARY_ONLY."""

        validated_members = MembersCsvFileSchema.model_validate([member_data()]).root
        response_ctx = ValidationContext(year=2020, tz=DEFAULT_TIMEZONE)
        validated_responses = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [
                    response_data(
                        {
                            "Secondary Role": None,
                        }
                    )
                ],
                "event_rows": None,
            },
            context={"ctx": response_ctx},
        )

        peeps = build_peeps(validated_members, validated_responses)

        assert len(peeps) == 1
        assert peeps[0].switch_pref == SwitchPreference.PRIMARY_ONLY

    def test_handles_members_without_responses(self, ctx):
        """Edge case: Members without responses are still converted to Peeps."""

        validated_members = MembersCsvFileSchema.model_validate([member_data()]).root

        peeps = build_peeps(validated_members, {})

        assert len(peeps) == 1
        assert peeps[0].responded is False

    def test_build_peeps_matches_normalized_gmail_addresses(self, ctx):
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
        response_ctx = ValidationContext(year=2020, tz=DEFAULT_TIMEZONE)
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
        peeps = build_peeps(validated_members, validated_responses)

        # Verify match was successful
        assert len(peeps) == 1
        john = peeps[0]

        # Should have response data (proving match worked)
        assert john.responded is True  # Response was matched


@pytest.mark.contract
class TestBuildCancelledEvents:
    """Tests for build_cancelled_events function."""

    def test_builds_cancelled_events_set(self, event_factory, ctx):
        """Happy path: Builds correct set of cancelled event IDs."""
        events = [
            event_factory(id=1, date=datetime.datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)),
            event_factory(id=2, date=datetime.datetime(2020, 1, 5, 15, 0, tzinfo=ctx.tz)),
        ]
        cancelled_event_specs = [
            EventSpec(
                start=datetime.datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz),
                duration_minutes=90,
                raw="Saturday January 4 - 1pm",
            )
        ]
        cancelled_events = build_cancelled_events(
            cancelled_event_specs=cancelled_event_specs, events=events
        )
        assert isinstance(cancelled_events, list)
        assert all(isinstance(event, Event) for event in cancelled_events)
        assert len(cancelled_events) == 1
        assert cancelled_events[0] == events[0]

    def test_builds_empty_set_for_no_cancellations(self, event_factory, ctx):
        """Edge case: Returns empty set when no cancellations provided."""
        events = [
            event_factory(id=1, date=datetime.datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)),
            event_factory(id=2, date=datetime.datetime(2020, 1, 5, 15, 0, tzinfo=ctx.tz)),
        ]

        cancelled_events = build_cancelled_events(
            cancelled_event_specs=[],
            events=events,
        )
        assert cancelled_events == []


class TestBuildCancelledAvailability:
    """Tests for build_cancelled_availability function."""

    def test_builds_cancelled_availability_mapping(self, peep_factory, event_factory, ctx):
        """Happy path: Builds correct mapping from CancellationsJsonSchema list."""
        peeps = [
            peep_factory(id=1, email="alice@example.com"),
            peep_factory(id=2, email="bob@example.com"),
        ]

        events = [
            event_factory(id=1, date=datetime.datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)),
            event_factory(id=2, date=datetime.datetime(2020, 1, 5, 15, 0, tzinfo=ctx.tz)),
        ]
        cancelled_availability = [
            CancelledAvailabilityJsonSchema.model_validate(
                {"member_email": "alice@example.com", "events": ["Saturday January 4 - 1pm"]},
                context={"ctx": ctx},
            ),
            CancelledAvailabilityJsonSchema.model_validate(
                {
                    "member_email": "bob@example.com",
                    "events": ["Sunday January 5 - 3pm"],
                },
                context={"ctx": ctx},
            ),
        ]
        cancelled_availability_list = build_cancelled_availability(
            schemas=cancelled_availability, peeps=peeps, events=events
        )
        assert isinstance(cancelled_availability_list, list)
        assert all(
            isinstance(ca, CancelledMemberAvailability) for ca in cancelled_availability_list
        )
        assert len(cancelled_availability_list) == 2
        assert cancelled_availability_list[0].peep == peeps[0]  # Alice
        assert cancelled_availability_list[0].events == [events[0]]  # Event 1
        assert cancelled_availability_list[1].peep == peeps[1]  # Bob
        assert cancelled_availability_list[1].events == [events[1]]  # Event 2


@pytest.mark.contract
class TestBuildPartnerships:
    """Tests for build_partnerships function."""

    def test_builds_partnerships_mapping(self, peep_factory, ctx):
        """Happy path: Builds correct mapping from PartnershipRequest list."""

        requests = [
            PartnershipRequestJsonSchema.model_validate(
                {"requester_email": "alice@example.com", "target_emails": ["bob@example.com"]},
                context={"ctx": ctx},
            ),
            PartnershipRequestJsonSchema.model_validate(
                {
                    "requester_email": "carol@example.com",
                    "target_emails": ["dave@example.com", "eve@example.com"],
                },
                context={"ctx": ctx},
            ),
        ]
        peeps = [
            peep_factory(id=1, email="alice@example.com"),
            peep_factory(id=2, email="bob@example.com"),
            peep_factory(id=3, email="carol@example.com"),
            peep_factory(id=4, email="dave@example.com"),
            peep_factory(id=5, email="eve@example.com"),
        ]
        partnerships = build_partnerships(schemas=requests, peeps=peeps)
        assert isinstance(partnerships, list)
        assert all(isinstance(p, PartnershipRequest) for p in partnerships)
        assert len(partnerships) == 2
        assert partnerships[0].requester == peeps[0]  # Alice
        assert partnerships[0].target_peeps == [peeps[1]]  # Bob
        assert partnerships[1].requester == peeps[2]  # Carol
        assert partnerships[1].target_peeps == [peeps[3], peeps[4]]  # Dave, Eve

    def test_builds_empty_partnerships_for_no_requests(self, peep_factory, ctx):
        """Edge case: Returns empty list when no partnership requests provided."""
        from peeps_scheduler.validation.builders import build_partnerships

        requests = []
        peeps = [peep_factory(id=1, email="alice@example.com")]
        partnerships = build_partnerships(schemas=requests, peeps=peeps)
        assert partnerships == []
