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
        assert schema.deep_dive_topics == []

    def test_deep_dive_topics_parses_list(self, ctx):
        row = response_data(
            {
                "Deep Dive Topics": (
                    "Balance for Spins and Turns, Angles for Shaping & Slotting"
                )
            }
        )
        schema = ResponseCsvRowSchema.model_validate(row, context={"ctx": ctx})
        assert schema.deep_dive_topics == [
            "Balance for Spins and Turns",
            "Angles for Shaping & Slotting",
        ]

    def test_deep_dive_topics_strips_parenthetical_commas(self, ctx):
        row = response_data(
            {
                "Deep Dive Topics": (
                    "Rhythm & Blues (swung timing, swung body action, rhythmic footwork)"
                )
            }
        )
        schema = ResponseCsvRowSchema.model_validate(row, context={"ctx": ctx})
        assert schema.deep_dive_topics == ["Rhythm & Blues"]

    def test_deep_dive_topics_blank(self, ctx):
        row = response_data({"Deep Dive Topics": ""})
        schema = ResponseCsvRowSchema.model_validate(row, context={"ctx": ctx})
        assert schema.deep_dive_topics == []

    def test_deep_dive_topics_invalid_type_raises(self, ctx):
        row = response_data({"Deep Dive Topics": 123})
        with pytest.raises(ValidationError) as e:
            ResponseCsvRowSchema.model_validate(row, context={"ctx": ctx})
        assert_error_for_field(e.value.errors(), "Deep Dive Topics")

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

    def test_events_saved_from_availability(self, ctx):
        """Test ResponsesCsvFileSchema.events saved from response availability."""
        schema = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [
                    response_data(
                        {"Availability": "Saturday January 4 - 1pm, Sunday January 5 - 2pm"}
                    )
                ],
            },
            context={"ctx": ctx},
        )

        assert hasattr(schema, "events")
        assert len(schema.events) == 2
        assert all(isinstance(e, EventSpec) for e in schema.events)
        assert schema.events[0].start == datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)
        assert schema.events[0].duration_minutes is None
        assert schema.events[1].start == datetime(2020, 1, 5, 14, 0, tzinfo=ctx.tz)

    def test_events_deduplicated_by_datetime(self, ctx):
        """Edge case: Events deduplicated when multiple responses share same availability."""
        schema = ResponsesCsvFileSchema.model_validate(
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
            },
            context={"ctx": ctx},
        )

        # Should have 1 event, not 2 (deduped)
        assert len(schema.events) == 1
        assert schema.events[0].start == datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)

    def test_event_rows_take_precedence_over_availability(self, ctx):
        """Test event_rows used for events when they exist (availability not used)."""
        schema = ResponsesCsvFileSchema.model_validate(
            {
                "responses": [
                    response_data({"Availability": "Saturday January 4 - 1pm"})
                ],
                "event_rows": [
                    event_row_data({"Name": "Saturday January 4 - 1pm", "Event Duration": "120"}),
                    event_row_data(
                        {"Name": "Friday January 10 - 6:30pm", "Event Duration": "90"}
                    ),
                ],
            },
            context={"ctx": ctx},
        )

        assert len(schema.events) == 2
        assert all(isinstance(e, EventSpec) for e in schema.events)
        # First event from event_rows
        assert schema.events[0].start == datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)
        assert schema.events[0].duration_minutes == 120
        # Second event from event_rows (not in availability)
        assert schema.events[1].start == datetime(2020, 1, 10, 18, 30, tzinfo=ctx.tz)
        assert schema.events[1].duration_minutes == 90
