from datetime import datetime
import pytest
from pydantic import BaseModel, ValidationError
from peeps_scheduler.models import Role
from peeps_scheduler.validation.fields import (
    MAX_EMAIL_LENGTH,
    MAX_PERSON_NAME_LENGTH,
    EmailAddressStr,
    EventDateTime,
    EventDuration,
    EventNameOldFormatStr,
    EventSpecList,
    PersonNameStr,
    RoleEnum,
)
from peeps_scheduler.validation.parsers import EventSpec, parse_event_name
from tests.validation.conftest import assert_error_for_field

pytestmark = pytest.mark.unit


class TestPersonNameStr:
    class MockNameSchema(BaseModel):
        name: PersonNameStr

    def test_valid_name(self):
        schema = self.MockNameSchema.model_validate({"name": "Alice Alpha"})
        assert schema.name == "Alice Alpha"

    def test_valid_with_accents_and_period(self):
        schema = self.MockNameSchema.model_validate({"name": "Dr. Élodie-Marie"})
        assert schema.name == "Dr. Élodie-Marie"

    def test_max_length_valid(self):
        name = "A" * MAX_PERSON_NAME_LENGTH
        schema = self.MockNameSchema.model_validate({"name": name})
        assert schema.name == name

    def test_max_length_exceeded_raises(self):
        name = "A" * (MAX_PERSON_NAME_LENGTH + 1)
        with pytest.raises(ValidationError) as e:
            self.MockNameSchema.model_validate({"name": name})
        assert_error_for_field(e.value.errors(), "name", "at most")

    @pytest.mark.parametrize("v", ["", "   "])
    def test_empty_name_raises(self, v):
        with pytest.raises(ValidationError) as e:
            self.MockNameSchema.model_validate({"name": v})
        assert_error_for_field(e.value.errors(), "name", "must not be empty")

    @pytest.mark.parametrize(
        "v, msg",
        [
            ("Alice123", "must contain only letters"),
            ("Alice!", "must contain only letters"),
            ("@Bob", "must contain only letters"),
            (123, "should be a valid string"),
            (None, "should be a valid string"),
        ],
    )
    def test_invalid_name_raises(self, v, msg):
        with pytest.raises(ValidationError) as e:
            self.MockNameSchema.model_validate({"name": v})
        assert_error_for_field(e.value.errors(), "name", msg)


class TestRoleEnum:
    class MockRoleSchema(BaseModel):
        role: RoleEnum

    def test_valid_role(self):
        schema = self.MockRoleSchema.model_validate({"role": "leader"})
        assert schema.role == Role.LEADER

    @pytest.mark.parametrize("v", ["", "   "])
    def test_empty_role_raises(self, v):
        with pytest.raises(ValidationError) as e:
            self.MockRoleSchema.model_validate({"role": v})
        assert_error_for_field(e.value.errors(), "role", "must not be empty")


class TestEventSpecList:
    class MockEventSpecListSchema(BaseModel):
        events: EventSpecList

    def test_valid_defaults_list(self, ctx):
        schema = self.MockEventSpecListSchema.model_validate(
            {"events": ["Friday January 10th - 5:30pm to 7pm"]},
            context={"ctx": ctx},
        )

        assert isinstance(schema.events, list)
        assert all(isinstance(e, EventSpec) for e in schema.events)
        assert schema.events == [
            parse_event_name("Friday January 10th - 5:30pm to 7pm", ctx.year, ctx.tz),
        ]

    def test_valid_defaults_comma_string(self, ctx):
        schema = self.MockEventSpecListSchema.model_validate(
            {"events": "Saturday January 4 - 1pm, Friday January 10th - 5:30pm to 7pm"},
            context={"ctx": ctx},
        )

        assert isinstance(schema.events, list)
        assert all(isinstance(e, EventSpec) for e in schema.events)
        assert schema.events == [
            parse_event_name("Saturday January 4 - 1pm", ctx.year, ctx.tz),
            parse_event_name("Friday January 10th - 5:30pm to 7pm", ctx.year, ctx.tz),
        ]

    @pytest.mark.parametrize("v", [None, "", "   "])
    def test_valid_empty_values_become_empty_list(self, ctx, v):
        schema = self.MockEventSpecListSchema.model_validate(
            {"events": v},
            context={"ctx": ctx},
        )
        assert schema.events == []

    def test_duplicate_events_by_start_raise(self, ctx):
        with pytest.raises(ValidationError) as e:
            self.MockEventSpecListSchema.model_validate(
                {"events": ["Saturday January 4 - 1pm", "Saturday January 4th - 1pm to 3pm"]},
                context={"ctx": ctx},
            )

        assert_error_for_field(e.value.errors(), "events", "duplicate")

    def test_event_duration_not_in_class_config_raises(self, ctx):
        with pytest.raises(ValidationError) as e:
            self.MockEventSpecListSchema.model_validate(
                {"events": ["Saturday January 4 - 1pm to 3:37pm"]}, context={"ctx": ctx}
            )
        assert_error_for_field(e.value.errors(), "events", "unsupported event duration")

    @pytest.mark.parametrize(
        "v,msg",
        [
            (123, "must be a list of event names"),
            (["Saturday January 4 - 1pm", 123], "must be a list of event names"),
            (["invalid date"], "invalid"),
            ("invalid date", "invalid"),
            (", Saturday January 4 - 1pm", "invalid"),
        ],
    )
    def test_invalid_data_raises(self, ctx, v, msg):
        with pytest.raises(ValidationError) as e:
            self.MockEventSpecListSchema.model_validate({"events": v}, context={"ctx": ctx})

        assert_error_for_field(e.value.errors(), "events", msg)

    @pytest.mark.parametrize("bad_ctx", [None, "invalid"])
    def test_missing_or_invalid_context_raises(self, bad_ctx):
        with pytest.raises(ValidationError) as e:
            self.MockEventSpecListSchema.model_validate(
                {"events": ["Saturday January 4 - 1pm"]},
                context={"ctx": bad_ctx},
            )

        assert_error_for_field(e.value.errors(), "events", "validation context")


class TestEventNameOldFormatStr:
    class MockEventNameSchema(BaseModel):
        name: EventNameOldFormatStr

    def test_valid(self, ctx):
        schema = self.MockEventNameSchema.model_validate(
            {"name": "Saturday January 4 - 1pm"},
            context={"ctx": ctx},
        )
        assert schema.name == "Saturday January 4 - 1pm"

    def test_invalid_event_name_raises(self, ctx):
        with pytest.raises(ValidationError) as e:
            self.MockEventNameSchema.model_validate(
                {"name": "invalid event"},
                context={"ctx": ctx},
            )
        assert_error_for_field(e.value.errors(), "name", "invalid event name")

    def test_new_format_event_name_raises(self, ctx):
        with pytest.raises(ValidationError) as e:
            self.MockEventNameSchema.model_validate(
                {"name": "Saturday January 4 - 1pm to 3pm"},
                context={"ctx": ctx},
            )
        assert_error_for_field(e.value.errors(), "name", "invalid event name format")

    def test_event_prefix_rejected(self, ctx):
        with pytest.raises(ValidationError) as e:
            self.MockEventNameSchema.model_validate(
                {"name": "Event: Saturday January 4 - 1pm"},
                context={"ctx": ctx},
            )
        assert_error_for_field(e.value.errors(), "name", "invalid event name")

    def test_invalid_type_raises(self, ctx):
        with pytest.raises(ValidationError) as e:
            self.MockEventNameSchema.model_validate(
                {"name": 123},
                context={"ctx": ctx},
            )
        assert_error_for_field(e.value.errors(), "name", "should be a valid string")

    @pytest.mark.parametrize("bad_ctx", [None, "invalid"])
    def test_missing_or_invalid_context_raises(self, bad_ctx):
        with pytest.raises(ValidationError) as e:
            self.MockEventNameSchema.model_validate(
                {"name": "Saturday January 4 - 1pm"},
                context={"ctx": bad_ctx},
            )
        assert_error_for_field(e.value.errors(), "name", "validation context")


class TestEventDateTime:
    class MockEventDateSchema(BaseModel):
        date: EventDateTime

    @pytest.mark.parametrize(
        "date, expected",
        [
            ("2020-01-04 13:00", datetime(2020, 1, 4, 13, 0)),
            ("2020-1-4 13:00", datetime(2020, 1, 4, 13, 0)),
        ],
    )
    def test_valid_with_context(self, ctx, date, expected):
        schema = self.MockEventDateSchema.model_validate(
            {"date": date},
            context={"ctx": ctx},
        )
        expected = expected.replace(tzinfo=ctx.tz)
        assert schema.date == expected

    def test_valid_datetime_input(self, ctx):
        dt = datetime(2020, 1, 4, 13, 0)
        schema = self.MockEventDateSchema.model_validate(
            {"date": dt},
            context={"ctx": ctx},
        )
        assert schema.date == dt.replace(tzinfo=ctx.tz)

    @pytest.mark.parametrize("bad_ctx", [None, "invalid"])
    def test_missing_or_invalid_context_raises(self, bad_ctx):
        with pytest.raises(ValidationError) as e:
            self.MockEventDateSchema.model_validate(
                {"date": "2020-01-04 13:00"},
                context={"ctx": bad_ctx},
            )
        assert_error_for_field(e.value.errors(), "date", "validation context")

    @pytest.mark.parametrize(
        "date, msg",
        [
            ("2020-01-04", "invalid event datetime"),
            ("not a date", "invalid event datetime"),
            (123, "must be a string"),
            (None, "must be a string"),
        ],
    )
    def test_invalid_format(self, ctx, date, msg):
        with pytest.raises(ValidationError) as e:
            self.MockEventDateSchema.model_validate(
                {"date": date},
                context={"ctx": ctx},
            )
        assert_error_for_field(e.value.errors(), "date", msg)


class TestEventDuration:
    class MockDurationSchema(BaseModel):
        duration_minutes: EventDuration

    def test_valid(self):
        schema = self.MockDurationSchema.model_validate({"duration_minutes": 90})
        assert schema.duration_minutes == 90

    def test_invalid_duration_not_in_class_config_raises(self):
        with pytest.raises(ValidationError) as e:
            self.MockDurationSchema.model_validate({"duration_minutes": 37})
        assert_error_for_field(e.value.errors(), "duration_minutes", "unsupported event duration")


class TestEmailAddressStr:
    class MockEmailSchema(BaseModel):
        email: EmailAddressStr

    def test_max_length_valid(self):
        local = "l" * 64
        domain = self._build_domain(MAX_EMAIL_LENGTH - len(local) - 1)
        email = f"{local}@{domain}"
        assert len(email) == MAX_EMAIL_LENGTH

        schema = self.MockEmailSchema.model_validate({"email": email})
        assert schema.email == email

    def test_max_length_exceeded_raises(self):
        local = "l" * 64
        domain = self._build_domain(MAX_EMAIL_LENGTH - len(local) - 1 + 1)
        email = f"{local}@{domain}"
        assert len(email) == MAX_EMAIL_LENGTH + 1

        with pytest.raises(ValidationError) as e:
            self.MockEmailSchema.model_validate({"email": email})
        assert_error_for_field(e.value.errors(), "email", "too long")

    @staticmethod
    def _build_domain(total_len: int) -> str:
        labels = []
        remaining = total_len
        while remaining > 63:
            labels.append("d" * 63)
            remaining -= 64
        labels.append("d" * remaining)
        return ".".join(labels)
