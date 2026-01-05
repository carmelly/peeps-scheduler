import pytest
from peeps_scheduler.validation.file_schemas.cancellations_json import (
    CancelledAvailabilityJsonSchema,
    CancelledEventJsonSchema,
)
from peeps_scheduler.validation.parsers import EventSpec, parse_event_name
from tests.validation.fixtures import cancellations_data


@pytest.mark.unit
class TestCancellationJsonSchema:
    def test_event_cancellations_valid_defaults(self, ctx):
        data = {"cancelled_events": cancellations_data()["cancelled_events"]}
        schema = CancelledEventJsonSchema.model_validate(data, context={"ctx": ctx})

        assert isinstance(schema.cancelled_events, list)
        assert all(isinstance(e, EventSpec) for e in schema.cancelled_events)
        assert schema.cancelled_events == [
            parse_event_name("Friday January 10th - 5:30pm to 7pm", ctx.year, ctx.tz),
        ]

    def test_cancelled_availability_valid_defaults(self, ctx):
        data = cancellations_data().get("cancelled_availability")[0]
        schema = CancelledAvailabilityJsonSchema.model_validate(data, context={"ctx": ctx})

        assert isinstance(schema.email, str)
        assert schema.email == "alice@test.com"

        assert isinstance(schema.events, list)
        assert schema.events == [
            parse_event_name("Saturday January 4 - 1pm", ctx.year, ctx.tz),
            parse_event_name("Friday January 10th - 3pm", ctx.year, ctx.tz),
        ]
        assert all(isinstance(e, EventSpec) for e in schema.events)
