from datetime import datetime
import pytest
from pydantic import ValidationError
from peeps_scheduler.models import Role
from peeps_scheduler.validation.file_schemas.attendance_json import (
    ActualAttendanceJsonSchema,
    AttendanceEventJsonSchema,
    RosterEntryJsonSchema,
)
from tests.validation.conftest import assert_error_for_model


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


def attendance_data(overrides: dict | None = None) -> dict:
    defaults = {
        "valid_events": [attendance_event_data()],
    }
    return {**defaults, **(overrides or {})}


@pytest.mark.unit
class TestAttendanceEventJsonSchema:
    def test_valid_defaults(self, ctx):
        event = AttendanceEventJsonSchema.model_validate(
            attendance_event_data(), context={"ctx": ctx}
        )

        assert isinstance(event.legacy_id, int)
        assert isinstance(event.start_dt, datetime)
        assert isinstance(event.duration_minutes, int)
        assert isinstance(event.attendees, list)
        assert all(isinstance(att, RosterEntryJsonSchema) for att in event.attendees)

        assert event.legacy_id == 4
        assert event.start_dt == datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)
        assert event.duration_minutes == 120
        assert [att.id for att in event.attendees] == [38, 25]
        assert [att.role for att in event.attendees] == [Role.LEADER, Role.FOLLOWER]
        assert [att.index_order for att in event.attendees] == [0, 1]
        assert all(isinstance(att.index_order, int) for att in event.attendees)

    def test_duplicate_attendee_ids_raise(self, ctx):
        data = attendance_event_data(
            {
                "attendees": [
                    {"id": 38, "name": "Alice", "role": "leader"},
                    {"id": 38, "name": "Alice", "role": "leader"},
                ]
            }
        )

        with pytest.raises(ValidationError) as e:
            AttendanceEventJsonSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "duplicate attendee id")

    def test_empty_attendees_raise(self, ctx):
        data = attendance_event_data({"attendees": []})

        with pytest.raises(ValidationError) as e:
            AttendanceEventJsonSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "must not be empty")


@pytest.mark.unit
class TestActualAttendanceJsonSchema:
    def test_valid_defaults(self, ctx):
        schema = ActualAttendanceJsonSchema.model_validate(
            attendance_data(), context={"ctx": ctx}
        )

        assert isinstance(schema.valid_events, list)
        assert len(schema.valid_events) == 1
        assert isinstance(schema.valid_events[0], AttendanceEventJsonSchema)

    def test_duplicate_start_dt_raise(self, ctx):
        data = attendance_data(
            {
                "valid_events": [
                    attendance_event_data(
                        {"id": 1, "date": "2020-01-04 13:00"}
                    ),
                    attendance_event_data(
                        {"id": 2, "date": "2020-01-04 13:00"}
                    ),
                ]
            }
        )

        with pytest.raises(ValidationError) as e:
            ActualAttendanceJsonSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "duplicate event start")

    def test_duplicate_legacy_id_raise(self, ctx):
        data = attendance_data(
            {
                "valid_events": [
                    attendance_event_data(
                        {"id": 1, "date": "2020-01-04 13:00"}
                    ),
                    attendance_event_data(
                        {"id": 1, "date": "2020-01-11 13:00"}
                    ),
                ]
            }
        )

        with pytest.raises(ValidationError) as e:
            ActualAttendanceJsonSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "duplicate legacy id")
