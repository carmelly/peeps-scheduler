import pytest
from pydantic import ValidationError
from peeps_scheduler.models import Role
from peeps_scheduler.validation.file_schemas.attendance_json import RosterEntryJsonSchema
from peeps_scheduler.validation.file_schemas.results_json import (
    ResultEventJsonSchema,
    ResultsJsonSchema,
)
from tests.validation.conftest import assert_error_for_model
from tests.validation.fixtures import result_event_data, results_data


@pytest.mark.unit
class TestResultEventJsonSchema:
    def test_alternates_valid_defaults(self, ctx):
        event = ResultEventJsonSchema.model_validate(result_event_data(), context={"ctx": ctx})

        assert isinstance(event.alternates, list)
        assert all(isinstance(alt, RosterEntryJsonSchema) for alt in event.alternates)
        assert [alt.id for alt in event.alternates] == [41, 27]
        assert [alt.role for alt in event.alternates] == [Role.LEADER, Role.FOLLOWER]
        assert [alt.index_order for alt in event.alternates] == [0, 1]
        assert all(isinstance(alt.index_order, int) for alt in event.alternates)

    def test_duplicate_alternate_ids_raise(self, ctx):
        data = result_event_data(
            {
                "alternates": [
                    {"id": 11, "name": "Carol", "role": "leader"},
                    {"id": 11, "name": "Dave", "role": "follower"},
                ]
            }
        )

        with pytest.raises(ValidationError) as e:
            ResultEventJsonSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "duplicate alternate id")

    def test_empty_alternates_allowed(self, ctx):
        data = result_event_data({"alternates": []})
        event = ResultEventJsonSchema.model_validate(data, context={"ctx": ctx})
        assert event.alternates == []


@pytest.mark.unit
class TestResultsJsonSchema:
    def test_attendees_and_alternates_no_overlap(self, ctx):
        data = results_data(
            {
                "valid_events": [
                    result_event_data(
                        {
                            "attendees": [{"id": 10, "name": "Alice", "role": "leader"}],
                            "alternates": [{"id": 10, "name": "Bob", "role": "follower"}],
                        }
                    )
                ]
            }
        )

        with pytest.raises(ValidationError) as e:
            ResultsJsonSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "overlap")

    def test_duplicate_start_dt_raise(self, ctx):
        data = results_data(
            {
                "valid_events": [
                    result_event_data({"id": 1, "date": "2020-01-04 13:00"}),
                    result_event_data({"id": 2, "date": "2020-01-04 13:00"}),
                ]
            }
        )

        with pytest.raises(ValidationError) as e:
            ResultsJsonSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "duplicate event start")

    def test_duplicate_legacy_id_raise(self, ctx):
        data = results_data(
            {
                "valid_events": [
                    result_event_data({"id": 1, "date": "2020-01-04 13:00"}),
                    result_event_data({"id": 1, "date": "2020-01-11 13:00"}),
                ]
            }
        )

        with pytest.raises(ValidationError) as e:
            ResultsJsonSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "duplicate legacy id")
