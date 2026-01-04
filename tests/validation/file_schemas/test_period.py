import pytest
from pydantic import ValidationError
from peeps_scheduler.validation.file_schemas.attendance_json import RosterEntryJsonSchema
from peeps_scheduler.validation.file_schemas.cancellations_json import CancelledEventJsonSchema
from peeps_scheduler.validation.file_schemas.members_csv import MemberCsvRowSchema
from peeps_scheduler.validation.file_schemas.partnerships_json import PartnershipRequestJsonSchema
from peeps_scheduler.validation.file_schemas.period import (
    PeriodFileSchema,
    validate_cancellations,
    validate_partnerships,
    validate_response_emails,
    validate_roster_entries,
)
from peeps_scheduler.validation.parsers import parse_event_name
from tests.validation.conftest import assert_error_for_model

pytestmark = pytest.mark.unit


def response_data(overrides: dict | None = None) -> dict:
    defaults = {
        "Timestamp": "1/1/2020 12:00:00",
        "Name": "Alice Alpha",
        "Display Name": "Alice",
        "Email Address": "alice@test.com",
        "Primary Role": "Leader",
        "Secondary Role": "I only want to be scheduled in my primary role",
        "Max Sessions": "2",
        "Availability": "Saturday January 4 - 1pm",
        "Min Interval Days": "0",
    }
    return {**defaults, **(overrides or {})}


def event_row_data(overrides: dict | None = None) -> dict:
    defaults = {
        "Name": "Saturday January 4 - 1pm",
        "Event Duration": "90",
    }
    return {**defaults, **(overrides or {})}


def member_data(overrides: dict | None = None) -> dict:
    defaults = {
        "id": "1",
        "Name": "Alice Alpha",
        "Display Name": "Alice",
        "Email Address": "alice@test.com",
        "Role": "Leader",
        "Index": "0",
        "Priority": "1",
        "Total Attended": "0",
        "Active": "TRUE",
        "Date Joined": "1/1/2020",
    }
    return {**defaults, **(overrides or {})}


def period_data(overrides: dict | None = None) -> dict:
    defaults = {
        "members": [
            member_data(
                {
                    "id": "1",
                    "Index": "0",
                    "Name": "Alice Alpha",
                    "Display Name": "Alice",
                    "Email Address": "alice@test.com",
                }
            ),
            member_data(
                {
                    "id": "2",
                    "Index": "1",
                    "Name": "Bob Beta",
                    "Display Name": "Bob",
                    "Email Address": "bob@test.com",
                }
            ),
        ],
        "responses": {
            "responses": [
                response_data({"Name": "Alice Alpha", "Email Address": "alice@test.com"}),
                response_data({"Name": "Bob Beta", "Email Address": "bob@test.com"}),
            ],
        },
    }
    return {**defaults, **(overrides or {})}


def attendance_event_data(overrides: dict | None = None) -> dict:
    defaults = {
        "id": 4,
        "date": "2020-01-04 13:00",
        "duration_minutes": 120,
        "attendees": [
            {"id": 38, "name": "Alice", "role": "leader"},
            {"id": 25, "name": "Bob", "role": "follower"},
        ],
    }
    return {**defaults, **(overrides or {})}


def result_event_data(overrides: dict | None = None) -> dict:
    defaults = {
        **attendance_event_data(),
        "alternates": [
            {"id": 41, "name": "Dave", "role": "leader"},
            {"id": 27, "name": "Eve", "role": "follower"},
        ],
    }
    return {**defaults, **(overrides or {})}


class TestPeriodFileSchema:
    def test_valid_minimal(self, ctx):
        schema = PeriodFileSchema.model_validate(period_data(), context={"ctx": ctx})

        assert len(schema.members.root) == 2
        assert len(schema.responses.responses) == 2
        assert schema.responses.event_rows is None
        assert schema.results is None
        assert schema.attendance is None
        assert schema.cancelled_events is None
        assert schema.cancelled_availability is None
        assert schema.partnerships is None

    def test_response_emails_must_exist_in_members(self, ctx):
        data = period_data(
            {
                "responses": {
                    "responses": [
                        response_data({"Email Address": "missing@test.com", "Name": "Zoe Zeta"})
                    ]
                }
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "response email")

    def test_roster_ids_must_exist(self, ctx):
        data = period_data(
            {
                "results": {
                    "valid_events": [
                        result_event_data(
                            {
                                "attendees": [{"id": 99, "name": "Alice", "role": "leader"}],
                                "alternates": [],
                            }
                        )
                    ]
                }
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "roster id")

    def test_roster_name_must_match_display_name(self, ctx):
        data = period_data(
            {
                "results": {
                    "valid_events": [
                        result_event_data(
                            {
                                "attendees": [{"id": 1, "name": "Alice Alpha", "role": "leader"}],
                                "alternates": [],
                            }
                        )
                    ]
                }
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "display name")

    def test_roster_name_falls_back_to_full_name(self, ctx):
        data = period_data(
            {
                "members": [
                    member_data(
                        {
                            "id": "1",
                            "Index": "0",
                            "Name": "Alice Alpha",
                            "Display Name": "",
                            "Email Address": "alice@test.com",
                        }
                    )
                ],
                "responses": {"responses": [response_data({"Email Address": "alice@test.com"})]},
                "results": {
                    "valid_events": [
                        result_event_data(
                            {
                                "attendees": [{"id": 1, "name": "Alice Alpha", "role": "leader"}],
                                "alternates": [],
                            }
                        )
                    ]
                },
            }
        )

        schema = PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert schema.results is not None

    def test_roster_name_mismatch_without_display_name_raises(self, ctx):
        data = period_data(
            {
                "members": [
                    member_data(
                        {
                            "id": "1",
                            "Index": "0",
                            "Name": "Alice Alpha",
                            "Display Name": "",
                            "Email Address": "alice@test.com",
                        }
                    )
                ],
                "responses": {"responses": [response_data({"Email Address": "alice@test.com"})]},
                "results": {
                    "valid_events": [
                        result_event_data(
                            {
                                "attendees": [{"id": 1, "name": "Alice", "role": "leader"}],
                                "alternates": [],
                            }
                        )
                    ]
                },
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "display name")

    def test_attendance_roster_ids_must_exist(self, ctx):
        data = period_data(
            {
                "attendance": {
                    "valid_events": [
                        attendance_event_data(
                            {"attendees": [{"id": 99, "name": "Alice", "role": "leader"}]}
                        )
                    ]
                }
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "roster id")

    def test_partnership_target_ids_must_exist(self, ctx):
        data = period_data(
            {
                "partnerships": [{"1": [2, 99]}],
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "partner id")

    def test_partnership_requester_id_must_exist(self, ctx):
        data = period_data(
            {
                "partnerships": [{"99": [1]}],
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "requester id")

    def test_cancelled_event_ids_must_exist(self, ctx):
        data = period_data(
            {
                "responses": {
                    "responses": [response_data()],
                    "event_rows": [
                        event_row_data(
                            {"Name": "Saturday January 4 - 1pm", "Event Duration": "90"}
                        )
                    ],
                },
                "cancelled_events": {
                    "cancelled_events": ["Sunday January 5 - 2pm"],
                },
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "cancelled event")

    def test_cancelled_availability_email_must_exist(self, ctx):
        data = period_data(
            {
                "responses": {
                    "responses": [response_data()],
                    "event_rows": [
                        event_row_data(
                            {"Name": "Saturday January 4 - 1pm", "Event Duration": "90"}
                        )
                    ],
                },
                "cancelled_availability": [
                    {
                        "email": "missing@test.com",
                        "events": ["Saturday January 4 - 1pm"],
                    }
                ],
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "cancelled availability email")

    def test_cancelled_availability_events_must_exist(self, ctx):
        data = period_data(
            {
                "responses": {
                    "responses": [response_data()],
                    "event_rows": [
                        event_row_data(
                            {"Name": "Saturday January 4 - 1pm", "Event Duration": "90"}
                        )
                    ],
                },
                "cancelled_availability": [
                    {
                        "email": "alice@test.com",
                        "events": ["Sunday January 5 - 2pm"],
                    }
                ],
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "cancelled availability event")


class TestPeriodValidators:
    def test_missing_response_emails_raise(self):
        with pytest.raises(ValueError) as e:
            validate_response_emails({"alice@test.com"}, ["alice@test.com", "missing@test.com"])

        assert "response email" in str(e.value)

    def test_missing_roster_id_raises(self):
        member = MemberCsvRowSchema.model_validate(member_data())
        roster = RosterEntryJsonSchema.model_validate(
            {"id": 99, "name": "Alice", "role": "leader"}
        )

        with pytest.raises(ValueError) as e:
            validate_roster_entries({member.id: member}, [roster])

        assert "roster id" in str(e.value)

    def test_display_name_mismatch_raises(self):
        member = MemberCsvRowSchema.model_validate(member_data())
        roster = RosterEntryJsonSchema.model_validate(
            {"id": member.id, "name": "Bob", "role": "leader"}
        )

        with pytest.raises(ValueError) as e:
            validate_roster_entries({member.id: member}, [roster])

        assert "display name mismatch" in str(e.value)

    def test_requester_id_missing_raises(self):
        request = PartnershipRequestJsonSchema.model_validate({"99": [1]})

        with pytest.raises(ValueError) as e:
            validate_partnerships({1}, [request])

        assert "requester id" in str(e.value)

    def test_missing_cancelled_event_raises(self, ctx):
        event_starts = {parse_event_name("Saturday January 4 - 1pm", ctx.year, ctx.tz).start}
        cancelled_events = CancelledEventJsonSchema.model_validate(
            {"cancelled_events": ["Sunday January 5 - 2pm"]},
            context={"ctx": ctx},
        )

        with pytest.raises(ValueError) as e:
            validate_cancellations(
                event_starts,
                {"alice@test.com"},
                cancelled_events,
                None,
            )

        assert "cancelled event not found" in str(e.value)
