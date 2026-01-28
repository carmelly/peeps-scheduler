"""Tests for schema-to-domain converters and validation wrappers."""

import datetime
import pytest
from peeps_scheduler.constants import DEFAULT_TIMEZONE
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
    build_attendance_events,
    build_cancelled_availability,
    build_cancelled_events,
    build_events,
    build_partnerships,
    build_peeps,
    build_results_events,
)
from peeps_scheduler.validation.fields import ValidationContext
from peeps_scheduler.validation.file_schemas.attendance_json import ActualAttendanceJsonSchema
from peeps_scheduler.validation.file_schemas.members_csv import (
    MembersCsvFileSchema,
)
from peeps_scheduler.validation.file_schemas.period import (
    CancelledAvailabilityJsonSchema,
    PartnershipRequestJsonSchema,
)
from peeps_scheduler.validation.file_schemas.responses_csv import ResponsesCsvFileSchema
from peeps_scheduler.validation.file_schemas.results_json import ResultsJsonSchema
from peeps_scheduler.validation.parsers import EventSpec
from tests.validation.fixtures import (
    attendance_data,
    attendance_event_data,
    member_data,
    response_data,
    result_event_data,
    results_data,
)


def _events_by_datetime(response_schema: ResponsesCsvFileSchema) -> dict:
    events = [
        Event(id=index, date=spec.start, duration_minutes=spec.duration_minutes)
        for index, spec in enumerate(response_schema.events)
    ]
    return {event.date: event for event in events}


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

        events_by_datetime = _events_by_datetime(response_schema)
        peep = _member_to_peep(member_schema, response_schema, events_by_datetime)

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

        events_by_datetime = _events_by_datetime(response_schema)
        peep = _member_to_peep(member_schema, response_schema, events_by_datetime)

        assert len(peep.availability) == 2
        assert all(isinstance(event, Event) for event in peep.availability)
        assert all(event.date.tzinfo == ctx.tz for event in peep.availability)
        # Verify the correct events are in availability
        expected_dates = [
            datetime.datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz),
            datetime.datetime(2020, 1, 5, 14, 0, tzinfo=ctx.tz),
        ]
        assert sorted([event.date for event in peep.availability]) == sorted(expected_dates)

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

        events_by_datetime = _events_by_datetime(response_schema)
        peep = _member_to_peep(member_schema, response_schema, events_by_datetime)

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

        events_by_datetime = _events_by_datetime(response_schema)
        peep = _member_to_peep(member_schema, response_schema, events_by_datetime)

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

        events_by_datetime = _events_by_datetime(response_schema)
        peep = _member_to_peep(member_schema, response_schema, events_by_datetime)

        assert peep.min_interval_days == 7

    def test_member_with_response_sets_topic_votes(self, ctx):
        """Edge case: Response deep dive topics become peep topic votes."""
        member_schemas = MembersCsvFileSchema.model_validate([member_data()]).root
        member_schema = member_schemas[0]
        response_ctx = ValidationContext(year=2020, tz=DEFAULT_TIMEZONE)
        response_schema = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [
                    response_data(
                        {
                            "Deep Dive Topics": (
                                "Balance for Spins and Turns, Angles for Shaping & Slotting"
                            )
                        }
                    )
                ],
                "event_rows": None,
            },
            context={"ctx": response_ctx},
        )

        events_by_datetime = _events_by_datetime(response_schema)
        peep = _member_to_peep(member_schema, response_schema, events_by_datetime)

        assert peep.topic_votes == [
            "Balance for Spins and Turns",
            "Angles for Shaping & Slotting",
        ]

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

        events_by_datetime = _events_by_datetime(response_schema)
        peep = _member_to_peep(member_schema, response_schema, events_by_datetime)

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
        event = _event_spec_to_event(1, spec)

        assert event.id == 1
        assert event.date == spec.start
        assert event.duration_minutes == spec.duration_minutes


@pytest.mark.unit
class TestBuildEvents:
    """Tests for build_events factory function."""

    def test_build_events_preserves_order_when_event_rows_exist(self):
        """Happy path: preserves event order and assigns sequential IDs."""
        specs = [
            EventSpec(
                start=datetime.datetime(2020, 1, 5, 13, 0),
                duration_minutes=90,
                raw="Sunday January 5 - 1pm",
            ),
            EventSpec(
                start=datetime.datetime(2020, 1, 4, 13, 0),
                duration_minutes=90,
                raw="Saturday January 4 - 1pm",
            ),
        ]

        events = build_events(specs, preserve_order=True)

        assert [event.id for event in events] == [0, 1]
        assert [event.date for event in events] == [specs[0].start, specs[1].start]

    def test_build_events_sorts_by_start_without_event_rows(self):
        """Happy path: assigns IDs in chronological order when no event rows exist."""
        specs = [
            EventSpec(
                start=datetime.datetime(2020, 1, 5, 13, 0),
                duration_minutes=90,
                raw="Sunday January 5 - 1pm",
            ),
            EventSpec(
                start=datetime.datetime(2020, 1, 4, 13, 0),
                duration_minutes=90,
                raw="Saturday January 4 - 1pm",
            ),
        ]

        events = build_events(specs, preserve_order=False)

        assert [event.id for event in events] == [0, 1]
        assert [event.date for event in events] == [specs[1].start, specs[0].start]


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
        events = build_events(validated_responses.events, preserve_order=False)

        peeps = build_peeps(validated_members, validated_responses, events)

        assert len(peeps) == 1
        assert isinstance(peeps[0], Peep)
        assert peeps[0].full_name == "Alice Alpha"
        assert peeps[0].responded is True
        assert peeps[0].availability == events

    def test_handles_members_without_responses(self, ctx):
        """Edge case: Members without responses are still converted to Peeps."""

        validated_members = MembersCsvFileSchema.model_validate([member_data()]).root

        peeps = build_peeps(validated_members, {}, [])

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
        events = build_events(validated_responses.events, preserve_order=False)

        # Convert - should match despite different email formats
        peeps = build_peeps(validated_members, validated_responses, events)

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


class TestBuildAttendanceEvents:
    """Tests for build_attendance_events function."""

    def test_builds_attendance_events(self, peep_factory, ctx):
        """Happy path: Builds Event objects with attendee assignments."""
        peeps = [
            peep_factory(id=1, role=Role.LEADER),
            peep_factory(id=2, role=Role.FOLLOWER),
        ]
        attendance_payload = attendance_data(
            {
                "valid_events": [
                    attendance_event_data(
                        {
                            "id": 1,
                            "date": "2020-01-04 13:00",
                            "duration_minutes": 90,
                            "attendees": [
                                {"id": 1, "name": "Leader One", "role": "leader"},
                                {"id": 2, "name": "Follower Two", "role": "follower"},
                            ],
                        }
                    )
                ]
            }
        )
        schema = ActualAttendanceJsonSchema.model_validate(attendance_payload, context={"ctx": ctx})

        events = build_attendance_events(schema, peeps)

        assert len(events) == 1
        event = events[0]
        assert event.id == 1
        assert peeps[0] in event.leaders
        assert peeps[1] in event.followers


class TestBuildResultsEvents:
    """Tests for build_results_events function."""

    def test_builds_results_events_with_alternates(self, peep_factory, ctx):
        """Happy path: Builds Event objects with attendees and alternates."""
        peeps = [
            peep_factory(id=1, role=Role.LEADER),
            peep_factory(id=2, role=Role.FOLLOWER),
            peep_factory(id=3, role=Role.LEADER),
            peep_factory(id=4, role=Role.FOLLOWER),
        ]
        results_payload = results_data(
            {
                "valid_events": [
                    result_event_data(
                        {
                            "id": 2,
                            "date": "2020-01-04 13:00",
                            "duration_minutes": 90,
                            "attendees": [
                                {"id": 1, "name": "Leader One", "role": "leader"},
                                {"id": 2, "name": "Follower Two", "role": "follower"},
                            ],
                            "alternates": [
                                {"id": 3, "name": "Alt Leader", "role": "leader"},
                                {"id": 4, "name": "Alt Follower", "role": "follower"},
                            ],
                        }
                    )
                ]
            }
        )
        schema = ResultsJsonSchema.model_validate(results_payload, context={"ctx": ctx})

        events = build_results_events(schema, peeps)

        assert len(events) == 1
        event = events[0]
        assert event.id == 2
        assert peeps[0] in event.leaders
        assert peeps[1] in event.followers
        assert peeps[2] in event.alt_leaders
        assert peeps[3] in event.alt_followers
