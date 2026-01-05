from datetime import datetime
import pytest
from pydantic import ValidationError
from peeps_scheduler.validation.file_schemas.responses_csv import (
    EventRowCsvSchema,
    ResponseCsvRowSchema,
    ResponsesCsvFileSchema,
)
from peeps_scheduler.validation.parsers import EventSpec
from tests.validation.conftest import assert_error_for_field, assert_error_for_model
from tests.validation.fixtures import event_row_data, response_data

pytestmark = pytest.mark.unit


class TestResponseCsvRowSchema:
    def test_valid_defaults(self, ctx):
        from peeps_scheduler.models import Role, SwitchPreference

        schema = ResponseCsvRowSchema.model_validate(response_data(), context={"ctx": ctx})
        assert schema.timestamp == datetime(2020, 1, 1, 12, 0)
        assert schema.full_name == "Alice Alpha"
        assert schema.display_name == "Alice"
        assert schema.primary_role == Role.LEADER
        assert schema.secondary_role == SwitchPreference.PRIMARY_ONLY
        assert schema.max_sessions == 2
        assert isinstance(schema.availability[0], EventSpec)
        assert schema.min_interval_days == 0

    def test_valid_optional_fields_missing(self, ctx):
        row = response_data()
        del row["Display Name"]
        del row["Secondary Role"]
        schema = ResponseCsvRowSchema.model_validate(row, context={"ctx": ctx})
        assert schema.display_name is None
        assert schema.secondary_role is None
        row = response_data({"Display Name": "", "Secondary Role": ""})
        schema = ResponseCsvRowSchema.model_validate(row, context={"ctx": ctx})
        assert schema.display_name is None
        assert schema.secondary_role is None
        row = response_data({"Display Name": None})
        schema = ResponseCsvRowSchema.model_validate(row, context={"ctx": ctx})
        assert schema.display_name is None

    @pytest.mark.parametrize(
        "timestamp, msg",
        [
            ("2021-01-0", "format not recognized"),
            ("not a timestamp", "format not recognized"),
            (None, "must be a string"),
        ],
    )
    def test_invalid_timestamp_raises(self, ctx, timestamp, msg):
        row = response_data({"Timestamp": timestamp})
        with pytest.raises(ValidationError) as e:
            ResponseCsvRowSchema.model_validate(row, context={"ctx": ctx})
        assert_error_for_field(e.value.errors(), "Timestamp", msg)

    def test_invalid_name_raises(self, ctx):
        row = response_data({"Name": "Alice123"})
        with pytest.raises(ValidationError) as e:
            ResponseCsvRowSchema.model_validate(row, context={"ctx": ctx})
        assert_error_for_field(e.value.errors(), "Name", " must contain only letters")

    def test_invalid_empty_name_raises(self, ctx):
        row = response_data({"Name": ""})
        with pytest.raises(ValidationError) as e:
            ResponseCsvRowSchema.model_validate(row, context={"ctx": ctx})
        assert_error_for_field(e.value.errors(), "Name", "must not be empty")

    @pytest.mark.parametrize("primary_role", ["invalid role", ""])
    def test_invalid_primary_role_raises(self, ctx, primary_role):
        row = response_data({"Primary Role": primary_role})
        with pytest.raises(ValidationError) as e:
            ResponseCsvRowSchema.model_validate(row, context={"ctx": ctx})
        assert_error_for_field(e.value.errors(), "Primary Role")

    @pytest.mark.parametrize("secondary_role", ["invalid secondary role", 123])
    def test_invalid_secondary_role_raises(self, ctx, secondary_role):
        row = response_data({"Secondary Role": secondary_role})
        with pytest.raises(ValidationError) as e:
            ResponseCsvRowSchema.model_validate(row, context={"ctx": ctx})
        assert_error_for_field(e.value.errors(), "Secondary Role")

    @pytest.mark.parametrize(
        "availability_str",
        [
            "Saturday January 4 - 1pm, Sunday January 5 - 2pm to 4pm",
            "Sunday January 5 - 2pm to 4pm, Saturday January 4 - 1pm",
        ],
    )
    def test_inconsistent_availability_format_raises(self, ctx, availability_str):
        row = response_data({"Availability": availability_str})
        with pytest.raises(ValidationError) as e:
            ResponseCsvRowSchema.model_validate(row, context={"ctx": ctx})
        assert_error_for_field(e.value.errors(), "Availability", "format must match")


class TestEventRowCsvSchema:
    def test_valid_defaults(self, ctx):
        row = event_row_data()
        schema = EventRowCsvSchema.model_validate(row, context={"ctx": ctx})
        assert schema.name == "Saturday January 4 - 1pm"
        assert schema.start_dt == datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)
        assert schema.duration_minutes == 90


class TestResponsesCsvFileSchema:
    def test_valid_defaults(self, ctx):
        schema = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [
                    response_data({"Email Address": "alice@test.com"}),
                    response_data({"Name": "Bob Beta", "Email Address": "bob@test.com"}),
                ]
            },
            context={"ctx": ctx},
        )

        assert len(schema.responses) == 2
        assert all(isinstance(row, ResponseCsvRowSchema) for row in schema.responses)

    def test_duplicate_email_raises(self, ctx):
        with pytest.raises(ValidationError) as e:
            ResponsesCsvFileSchema.model_validate(
                {
                    "responses": [
                        response_data({"Email Address": "AliCe@TEST.com"}),
                        response_data({"Email Address": "alice@test.com"}),
                    ]
                },
                context={"ctx": ctx},
            )

        assert_error_for_model(e.value.errors(), "duplicate email")

    def test_duplicate_start_dt_raises(self, ctx):
        with pytest.raises(ValidationError) as e:
            ResponsesCsvFileSchema.model_validate(
                {
                    "responses": [response_data()],
                    "event_rows": [
                        event_row_data({"Name": "Saturday January 4 - 1pm"}),
                        event_row_data(
                            {"Name": "Saturday January 4 - 1pm", "Event Duration": "60"}
                        ),
                    ],
                },
                context={"ctx": ctx},
            )

        assert_error_for_model(e.value.errors(), "duplicate event start")

    def test_event_rows_require_old_format_availability(self, ctx):
        with pytest.raises(ValidationError) as e:
            ResponsesCsvFileSchema.model_validate(
                {
                    "responses": [
                        response_data({"Availability": "Saturday January 4 - 1pm to 3pm"})
                    ],
                    "event_rows": [
                        event_row_data({"Name": "Saturday January 4 - 1pm", "Event Duration": "90"})
                    ],
                },
                context={"ctx": ctx},
            )

        assert_error_for_model(e.value.errors(), "old format")

    def test_availability_must_match_event_rows(self, ctx):
        with pytest.raises(ValidationError) as e:
            ResponsesCsvFileSchema.model_validate(
                {
                    "responses": [response_data({"Availability": "Sunday January 5 - 2pm"})],
                    "event_rows": [
                        event_row_data({"Name": "Saturday January 4 - 1pm", "Event Duration": "90"})
                    ],
                },
                context={"ctx": ctx},
            )

        assert_error_for_model(e.value.errors(), "event rows")
