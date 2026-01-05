"""Tests for schema-to-domain converters and validation wrappers."""

import datetime
import pytest
from peeps_scheduler.constants import DEFAULT_EVENT_DURATION, TIMEZONE
from peeps_scheduler.models import Event, Peep, Role, SwitchPreference
from peeps_scheduler.validation.converters import (
    convert_to_events,
    convert_to_peeps,
    event_spec_to_event,
    member_to_peep,
    validate_members,
    validate_responses,
)
from peeps_scheduler.validation.errors import FileValidationError
from peeps_scheduler.validation.fields import ValidationContext
from peeps_scheduler.validation.file_schemas.attendance_json import (
    ActualAttendanceJsonSchema,
    AttendanceEventJsonSchema,
)
from peeps_scheduler.validation.file_schemas.cancellations_json import CancellationsJsonSchema
from peeps_scheduler.validation.file_schemas.members_csv import MemberCsvRowSchema
from peeps_scheduler.validation.file_schemas.partnerships_json import PartnershipsJsonSchema
from peeps_scheduler.validation.file_schemas.responses_csv import ResponsesCsvFileSchema
from peeps_scheduler.validation.file_schemas.results_json import (
    ResultEventJsonSchema,
    ResultsJsonSchema,
)
from peeps_scheduler.validation.parsers import EventSpec
from tests.validation.fixtures import (
    attendance_data,
    cancellations_data,
    member_data,
    partnerships_json_data,
    response_data,
    results_data,
)


@pytest.mark.contract
class TestMemberToPeep:
    """Tests for member_to_peep factory function."""

    def test_maps_member_fields_without_response(self, ctx):
        """Happy path: Maps all member schema fields to Peep correctly."""
        member_schemas = validate_members(
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
            ],
            "members.csv",
            context={"ctx": ctx},
        )
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
        member_schemas = validate_members(
            [member_data({"Role": "Leader"})],
            "members.csv",
            context={"ctx": ctx},
        )
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        response_schema = validate_responses(
            {
                "responses": [response_data({"Primary Role": "Follower"})],
                "event_rows": None,
            },
            response_ctx,
            "responses.csv",
        )

        peep = member_to_peep(member_schema, response_schema)

        # Response role should override member role
        assert peep.role == Role.FOLLOWER

    def test_member_with_response_adds_availability(self, ctx):
        """Edge case: Response availability is added to peep."""
        member_schemas = validate_members(
            [member_data()],
            "members.csv",
            context={"ctx": ctx},
        )
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        response_schema = validate_responses(
            {
                "responses": [
                    response_data(
                        {"Availability": "Saturday January 4 - 1pm, Sunday January 5 - 2pm"}
                    )
                ],
                "event_rows": None,
            },
            response_ctx,
            "responses.csv",
        )

        peep = member_to_peep(member_schema, response_schema)

        assert len(peep.availability) == 2
        assert all(isinstance(event_date, datetime.datetime) for event_date in peep.availability)

    def test_member_with_response_sets_switch_preference(self, ctx):
        """Edge case: Response secondary_role becomes switch_pref."""
        member_schemas = validate_members(
            [member_data()],
            "members.csv",
            context={"ctx": ctx},
        )
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        response_schema = validate_responses(
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
            response_ctx,
            "responses.csv",
        )

        peep = member_to_peep(member_schema, response_schema)

        assert peep.switch_pref == SwitchPreference.SWITCH_IF_NEEDED

    def test_member_with_response_sets_event_limit(self, ctx):
        """Edge case: Response max_sessions becomes event_limit."""
        member_schemas = validate_members(
            [member_data()],
            "members.csv",
            context={"ctx": ctx},
        )
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        response_schema = validate_responses(
            {
                "responses": [response_data({"Max Sessions": "4"})],
                "event_rows": None,
            },
            response_ctx,
            "responses.csv",
        )

        peep = member_to_peep(member_schema, response_schema)

        assert peep.event_limit == 4

    def test_member_with_response_sets_min_interval_days(self, ctx):
        """Edge case: Response min_interval_days is set correctly."""
        member_schemas = validate_members(
            [member_data()],
            "members.csv",
            context={"ctx": ctx},
        )
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        response_schema = validate_responses(
            {
                "responses": [response_data({"Min Interval Days": "7"})],
                "event_rows": None,
            },
            response_ctx,
            "responses.csv",
        )

        peep = member_to_peep(member_schema, response_schema)

        assert peep.min_interval_days == 7

    def test_member_with_response_marks_responded(self, ctx):
        """Edge case: responded flag is True when response provided."""
        member_schemas = validate_members(
            [member_data()],
            "members.csv",
            context={"ctx": ctx},
        )
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        response_schema = validate_responses(
            {
                "responses": [response_data()],
                "event_rows": None,
            },
            response_ctx,
            "responses.csv",
        )

        peep = member_to_peep(member_schema, response_schema)

        assert peep.responded is True

    def test_member_with_none_email_address(self, ctx):
        """Edge case: Member with None email_address (inactive)."""
        member_schemas = validate_members(
            [
                member_data(
                    {
                        "Email Address": "",
                        "Active": "FALSE",
                    }
                )
            ],
            "members.csv",
            context={"ctx": ctx},
        )
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

        validated_members = validate_members([member_data()], "members.csv", context={"ctx": ctx})
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        validated_responses = validate_responses(
            {
                "responses": [response_data()],
                "event_rows": None,
            },
            response_ctx,
            "responses.csv",
        )

        peeps = convert_to_peeps(validated_members, validated_responses)

        assert len(peeps) == 1
        assert isinstance(peeps[0], Peep)
        assert peeps[0].full_name == "Alice Alpha"
        assert peeps[0].responded is True

    def test_handles_members_without_responses(self, ctx):
        """Edge case: Members without responses are still converted to Peeps."""
        from peeps_scheduler.validation.converters import convert_to_peeps

        validated_members = validate_members([member_data()], "members.csv", context={"ctx": ctx})

        peeps = convert_to_peeps(validated_members, {})

        assert len(peeps) == 1
        assert peeps[0].responded is False

    def test_convert_to_peeps_matches_normalized_gmail_addresses(self, ctx):
        """
        Test that convert_to_peeps() correctly matches members and responses
        when Gmail addresses use different dot patterns.
        """
        validated_members = validate_members(
            [
                member_data(
                    {
                        "Name": "John Smith",
                        "Email Address": "john.smith@gmail.com",  # Has dots
                    }
                )
            ],
            "members.csv",
            context={"ctx": ctx},
        )
        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        validated_responses = validate_responses(
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
            response_ctx,
            "responses.csv",
        )

        # Convert - should match despite different email formats
        peeps = convert_to_peeps(validated_members, validated_responses)

        # Verify match was successful
        assert len(peeps) == 1
        john = peeps[0]

        # Should have response data (proving match worked)
        assert john.responded is True  # Response was matched


@pytest.mark.contract
class TestConvertToEvents:
    """Tests for convert_to_events extraction function."""

    def test_converts_response_availability_to_events(self, ctx):
        """Happy path: When no event rows, converts response availability dict to Event objects."""

        response_dict = validate_responses(
            {
                "responses": [
                    response_data(
                        {"Availability": "Saturday January 4 - 1pm, Sunday January 5 - 2pm"}
                    )
                ],
                "event_rows": None,
            },
            ctx,
            "responses.csv",
        )

        events = convert_to_events(response_dict)

        assert len(events) == 2
        assert all(isinstance(e, Event) for e in events)
        assert events[0].date == datetime.datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)
        assert events[0].duration_minutes == DEFAULT_EVENT_DURATION
        assert events[1].date == datetime.datetime(2020, 1, 5, 14, 0, tzinfo=ctx.tz)
        assert events[1].duration_minutes == DEFAULT_EVENT_DURATION

    def test_converts_event_rows_to_events(self, ctx):
        """Happy path: When event rows exist, converts event rows to Event objects.
        Includes event from event row that did not appear in responses"""
        response_dict = validate_responses(
            {
                "responses": [
                    response_data(
                        {"Availability": "Saturday January 4 - 1pm, Sunday January 5 - 2pm"}
                    )
                ],
                "event_rows": [
                    {"Name": "Saturday January 4 - 1pm", "Event Duration": 120},
                    {"Name": "Sunday January 5 - 2pm", "Event Duration": 60},
                    {"Name": "Friday January 10 - 6:30pm", "Event Duration": 90},
                ],
            },
            ctx,
            "responses.csv",
        )

        events = convert_to_events(response_dict)

        assert len(events) == 3
        assert all(isinstance(e, Event) for e in events)
        assert events[0].date == datetime.datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)
        assert events[0].duration_minutes == 120
        assert events[1].date == datetime.datetime(2020, 1, 5, 14, 0, tzinfo=ctx.tz)
        assert events[1].duration_minutes == 60
        assert events[2].date == datetime.datetime(2020, 1, 10, 18, 30, tzinfo=ctx.tz)
        assert events[2].duration_minutes == 90

    def test_deduplicates_events_by_start_datetime(self, ctx):
        """Edge case: Events deduplicated when multiple responses share availability."""
        from peeps_scheduler.validation.converters import convert_to_events

        response_ctx = ValidationContext(year=2020, tz=TIMEZONE)
        response_dict = validate_responses(
            {
                "responses": [
                    response_data({"Name": "Alice", "Availability": "Saturday January 4 - 1pm"}),
                    response_data(
                        {
                            "Name": "Bob",
                            "Email Address": "bob@test.com",
                            "Availability": "Saturday January 4 - 1pm",
                        }
                    ),
                ],
                "event_rows": None,
            },
            response_ctx,
            "responses.csv",
        )

        events = convert_to_events(response_dict)

        # Should have 1 event, not 2 (deduped)
        assert len(events) == 1


@pytest.mark.contract
class TestExtractCancellations:
    """Tests for extract_cancellations extraction function."""

    def test_extracts_cancellations_from_validated_data(self, ctx):
        """Happy path: Extracts cancelled event IDs and availability."""
        from peeps_scheduler.validation.converters import (
            extract_cancellations,
            validate_cancellations,
        )

        raw = cancellations_data()
        validated = validate_cancellations(raw, ctx, "cancellations.json")
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
        from peeps_scheduler.validation.converters import (
            extract_partnerships,
            validate_partnerships,
        )

        raw = partnerships_json_data()
        validated = validate_partnerships(raw, "partnerships.json")
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


@pytest.mark.unit
class TestValidateMembers:
    """Tests for validate_members wrapper function."""

    def test_validate_members_with_valid_data(self, ctx):
        """Happy path: Valid member data returns list of MemberCsvRowSchema objects."""
        from peeps_scheduler.validation.converters import validate_members

        raw_rows = [
            member_data(),
            member_data(
                {
                    "id": "2",
                    "Name": "Bob Beta",
                    "Display Name": "Bob",
                    "Email Address": "bob@test.com",
                    "Index": "1",
                }
            ),
        ]

        result = validate_members(raw_rows, "members.csv", context={"ctx": ctx})

        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(row, MemberCsvRowSchema) for row in result)
        assert result[0].id == 1
        assert result[1].id == 2

    def test_validate_members_with_invalid_data(self, ctx):
        """Error path: Invalid member data raises FileValidationError."""
        from peeps_scheduler.validation.converters import validate_members

        raw_rows = [member_data({"Email Address": ""})]  # Active without email

        with pytest.raises(FileValidationError) as exc_info:
            validate_members(raw_rows, "members.csv", context={"ctx": ctx})

        assert "members.csv" in str(exc_info.value)


@pytest.mark.unit
class TestValidateResponses:
    """Tests for validate_responses wrapper function."""

    def test_validate_responses_with_valid_data(self, ctx):
        """Happy path: Valid response data returns ResponsesCsvFileSchema object."""
        from peeps_scheduler.validation.converters import validate_responses

        raw_data = {
            "responses": [response_data()],
            "event_rows": [],
        }

        result = validate_responses(raw_data, ctx, "responses.csv")

        assert isinstance(result, ResponsesCsvFileSchema)
        assert hasattr(result, "responses")

    def test_validate_responses_with_invalid_data(self, ctx):
        """Error path: Invalid response data raises FileValidationError."""
        from peeps_scheduler.validation.converters import validate_responses

        raw_data = {
            "responses": [response_data({"Timestamp": "invalid"})],
            "event_rows": [],
        }

        with pytest.raises(FileValidationError) as exc_info:
            validate_responses(raw_data, ctx, "responses.csv")

        assert "responses.csv" in str(exc_info.value)


@pytest.mark.unit
class TestValidateCancellations:
    """Tests for validate_cancellations wrapper function."""

    def test_validate_cancellations_with_valid_data(self, ctx):
        """Happy path: Valid cancellations data returns CancellationsJsonSchema object."""
        from peeps_scheduler.validation.converters import validate_cancellations

        result = validate_cancellations(cancellations_data(), ctx, "cancellations.json")

        assert isinstance(result, CancellationsJsonSchema)

    def test_validate_cancellations_with_invalid_data(self, ctx):
        """Error path: Invalid cancellations data raises FileValidationError."""
        from peeps_scheduler.validation.converters import validate_cancellations

        raw_data = {
            "cancelled_events": [{"start": "not a datetime"}],
            "cancelled_availability": [],
        }

        with pytest.raises(FileValidationError) as exc_info:
            validate_cancellations(raw_data, ctx, "cancellations.json")

        assert "cancellations.json" in str(exc_info.value)


@pytest.mark.unit
class TestValidatePartnerships:
    """Tests for validate_partnerships wrapper function."""

    def test_validate_partnerships_with_valid_data(self):
        """Happy path: Valid partnerships data returns PartnershipsJsonSchema object."""
        from peeps_scheduler.validation.converters import validate_partnerships

        result = validate_partnerships(partnerships_json_data(), "partnerships.json")

        assert isinstance(result, PartnershipsJsonSchema)
        assert hasattr(result, "partnerships")
        assert isinstance(result.partnerships, list)
        assert len(result.partnerships) == 3

    def test_validate_partnerships_with_invalid_data(self):
        """Error path: Invalid partnerships data raises FileValidationError."""
        from peeps_scheduler.validation.converters import validate_partnerships

        raw_data = {
            "abc": [1, 2],  # Invalid requester ID (not convertible to int)
        }

        with pytest.raises(FileValidationError) as exc_info:
            validate_partnerships(raw_data, "partnerships.json")

        assert "partnerships.json" in str(exc_info.value)


@pytest.mark.unit
class TestValidateAttendance:
    """Tests for validate_attendance wrapper function."""

    def test_validate_attendance_with_valid_data(self, ctx):
        """Happy path: Valid attendance data returns ActualAttendanceJsonSchema object with valid_events attribute."""
        from peeps_scheduler.validation.converters import validate_attendance

        result = validate_attendance(attendance_data(), ctx, "attendance.json")

        assert isinstance(result, ActualAttendanceJsonSchema)
        assert isinstance(result.valid_events, list)
        assert len(result.valid_events) == 1

        event = result.valid_events[0]
        assert isinstance(event, AttendanceEventJsonSchema)
        assert event.start_dt == datetime.datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)
        assert event.duration_minutes == 120
        assert isinstance(event.attendees, list)
        assert len(event.attendees) == 2
        assert event.attendees[0].id == 38
        assert event.attendees[0].name == "Alice"
        assert event.attendees[0].role == Role.LEADER
        assert event.attendees[1].id == 25
        assert event.attendees[1].name == "Bob"
        assert event.attendees[1].role == Role.FOLLOWER

    def test_validate_attendance_with_invalid_data(self, ctx):
        """Error path: Invalid attendance data raises FileValidationError."""
        from peeps_scheduler.validation.converters import validate_attendance

        raw_data = attendance_data(
            {
                "valid_events": [{"id": "not a number", "date": "2020-01-04 13:00"}],
            }
        )

        with pytest.raises(FileValidationError, match=r"attendance.json") as e:
            validate_attendance(raw_data, ctx, "attendance.json")

        errors = e.value.errors()
        assert len(errors) == 3
        assert errors[0]["loc"] == ("valid_events", 0, "id")
        assert errors[1]["loc"] == ("valid_events", 0, "duration_minutes")
        assert errors[2]["loc"] == ("valid_events", 0, "attendees")


@pytest.mark.unit
class TestValidateResults:
    """Tests for validate_results wrapper function."""

    def test_validate_results_with_valid_data(self, ctx):
        """Happy path: Valid results data returns ResultsJsonSchema object with correct attributes."""
        from peeps_scheduler.validation.converters import validate_results

        result = validate_results(results_data(), ctx, "results.json")

        assert isinstance(result, ResultsJsonSchema)
        assert isinstance(result.valid_events, list)

        assert len(result.valid_events) == 1

        event = result.valid_events[0]
        assert isinstance(event, ResultEventJsonSchema)
        assert event.start_dt == datetime.datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)
        assert event.duration_minutes == 120
        assert isinstance(event.attendees, list)
        assert len(event.attendees) == 2
        assert isinstance(event.alternates, list)
        assert len(event.alternates) == 2
        assert event.alternates[0].id == 41
        assert event.alternates[0].name == "Dave"
        assert event.alternates[0].role == Role.LEADER
        assert event.alternates[1].id == 27
        assert event.alternates[1].name == "Eve"
        assert event.alternates[1].role == Role.FOLLOWER

    def test_validate_results_with_invalid_data(self, ctx):
        """Error path: Invalid results data raises FileValidationError."""
        from peeps_scheduler.validation.converters import validate_results

        raw_data = results_data(
            {
                "valid_events": [{"id": "not a number", "date": "2020-01-04 13:00"}],
            }
        )

        with pytest.raises(FileValidationError, match=r"results.json") as e:
            validate_results(raw_data, ctx, "results.json")

        errors = e.value.errors()
        assert len(errors) == 4
        assert errors[0]["loc"] == ("valid_events", 0, "id")
        assert errors[1]["loc"] == ("valid_events", 0, "duration_minutes")
        assert errors[2]["loc"] == ("valid_events", 0, "attendees")
        assert errors[3]["loc"] == ("valid_events", 0, "alternates")
