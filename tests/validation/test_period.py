"""Tests for period loading and orchestration."""

import json
import tempfile
from pathlib import Path
import pytest
from peeps_scheduler.models import Event, Peep, Role
from peeps_scheduler.validation.file_schemas.period import PeriodFileSchema
from peeps_scheduler.validation.period import (
    PeriodData,
    load_and_validate_period,
    to_period_data,
)
from tests.validation.file_schemas.test_period import period_data
from tests.validation.fixtures import event_row_data, response_data

pytestmark = pytest.mark.integration


@pytest.fixture(scope="function")
def temp_period_dir(ctx):
    """Create temporary period directory with valid test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Write members.csv
        members_csv = tmpdir_path / "members.csv"
        members_csv.write_text(
            "id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined\n"
            "1,Alice Alpha,Alice,alice@test.com,Leader,0,5,10,TRUE,1/1/2020\n"
        )

        # Write responses.csv
        responses_csv = tmpdir_path / "responses.csv"
        responses_csv.write_text(
            "Timestamp,Name,Display Name,Email Address,Primary Role,Secondary Role,Max Sessions,Availability,Min Interval Days\n"
            "1/1/2020 12:00:00,Alice Alpha,Alice,alice@test.com,Follower,I only want to be scheduled in my primary role,2,Saturday January 4 - 1pm,0\n"
        )

        # Write cancellations.json
        cancellations_json = tmpdir_path / "cancellations.json"
        cancellations_json.write_text(
            json.dumps(
                {
                    "cancelled_events": [],
                    "cancelled_availability": [],
                }
            )
        )

        # Write partnerships.json
        partnerships_json = tmpdir_path / "partnerships.json"
        partnerships_json.write_text(
            json.dumps(
                {
                    "1": [],
                }
            )
        )

        yield tmpdir_path


@pytest.mark.integration
class TestPeriodSchemaIntegration:
    """Integration tests for complete PeriodFileSchema workflow."""

    def test_load_and_validate_period_uses_period_file_schema(self, ctx, temp_period_dir):
        """Integration: load_and_validate_period() uses PeriodFileSchema.model_validate()."""
        period_data_obj = load_and_validate_period(str(temp_period_dir), 2020)

        assert isinstance(period_data_obj, PeriodData)
        assert len(period_data_obj.peeps) >= 1
        assert len(period_data_obj.events) >= 1

    def test_period_file_schema_validates_all_cross_file_constraints(self, ctx, temp_period_dir):
        """Integration: PeriodFileSchema enforces all cross-file validation rules."""
        # This test verifies the complete validation works end-to-end
        period_data_obj = load_and_validate_period(str(temp_period_dir), 2020)

        # Should successfully validate all components
        assert isinstance(period_data_obj.peeps, list)
        assert isinstance(period_data_obj.events, list)
        assert isinstance(period_data_obj.cancelled_event_ids, set)
        assert isinstance(period_data_obj.partnerships, dict)

    def test_to_period_data_converts_event_specs_to_events(self, ctx):
        """Contract: to_period_data() converts EventSpec to Event domain objects."""
        schema = PeriodFileSchema.model_validate(
            period_data(
                {
                    "responses": {
                        "responses": [response_data()],
                        "event_rows": [
                            event_row_data(
                                {"Name": "Saturday January 4 - 1pm", "Event Duration": "90"}
                            )
                        ],
                    }
                }
            ),
            context={"ctx": ctx},
        )

        result = to_period_data(schema, 2020)

        # Events should be Event domain objects, not EventSpecs
        assert len(result.events) >= 1
        assert all(isinstance(e, Event) for e in result.events)
        assert result.events[0].date is not None
        assert result.events[0].duration_minutes is not None


@pytest.mark.unit
class TestToPeriodData:
    """Tests for to_period_data() function with PeriodFileSchema."""

    def test_accepts_period_file_schema(self, ctx):
        """Contract: to_period_data() accepts PeriodFileSchema object."""
        schema = PeriodFileSchema.model_validate(period_data(), context={"ctx": ctx})

        result = to_period_data(schema, 2020)

        assert isinstance(result, PeriodData)
        assert isinstance(result.peeps, list)
        assert isinstance(result.events, list)
        assert isinstance(result.cancelled_event_ids, set)
        assert isinstance(result.cancelled_availability, dict)
        assert isinstance(result.partnerships, dict)

    def test_populates_peeps_from_schema(self, ctx):
        """Contract: Peeps populated correctly from schema members and responses."""
        schema = PeriodFileSchema.model_validate(period_data(), context={"ctx": ctx})

        result = to_period_data(schema, 2020)

        assert len(result.peeps) >= 2
        assert all(isinstance(p, Peep) for p in result.peeps)
        assert any(p.id == 1 for p in result.peeps)
        assert any(p.id == 2 for p in result.peeps)

    def test_populates_events_from_schema_responses_events(self, ctx):
        """Contract: Events created from schema.responses.events (EventSpecs)."""
        schema = PeriodFileSchema.model_validate(
            period_data(
                {
                    "responses": {
                        "responses": [response_data()],
                        "event_rows": [
                            event_row_data(
                                {"Name": "Saturday January 4 - 1pm", "Event Duration": "90"}
                            )
                        ],
                    }
                }
            ),
            context={"ctx": ctx},
        )

        result = to_period_data(schema, 2020)

        assert len(result.events) >= 1
        assert all(isinstance(e, Event) for e in result.events)
        # Events should be created from responses.events
        event = result.events[0]
        assert event.date is not None
        assert event.duration_minutes == 90  # From event_row

    def test_extracts_cancellations_from_schema(self, ctx):
        """Contract: Cancellations extracted correctly from schema."""
        schema = PeriodFileSchema.model_validate(
            period_data(
                {
                    "responses": {
                        "responses": [response_data()],
                        "event_rows": [
                            event_row_data(
                                {"Name": "Saturday January 4 - 1pm", "Event Duration": "90"}
                            )
                        ],
                    },
                    "cancelled_events": {"cancelled_events": ["Saturday January 4 - 1pm"]},
                }
            ),
            context={"ctx": ctx},
        )

        result = to_period_data(schema, 2020)

        assert isinstance(result.cancelled_event_ids, set)
        assert len(result.cancelled_event_ids) >= 1


class TestLoadAndValidatePeriod:
    """Tests for load_and_validate_period orchestrator function."""

    def test_load_and_validate_period_valid_data(self, ctx, temp_period_dir):
        """Happy path: Valid period directory returns PeriodData with correct structure."""
        period_data = load_and_validate_period(str(temp_period_dir), 2020)

        assert isinstance(period_data, PeriodData)
        assert isinstance(period_data.peeps, list)
        assert isinstance(period_data.events, list)
        assert isinstance(period_data.cancelled_event_ids, set)
        assert isinstance(period_data.cancelled_availability, dict)
        assert isinstance(period_data.partnerships, dict)

    def test_load_and_validate_period_creates_peeps(self, ctx, temp_period_dir):
        """Field mapping: Peeps created from members and responses."""
        period_data = load_and_validate_period(str(temp_period_dir), 2020)

        assert len(period_data.peeps) == 1
        peep = period_data.peeps[0]
        assert isinstance(peep, Peep)
        assert peep.id == 1
        assert peep.full_name == "Alice Alpha"
        # Response should override role
        assert peep.role == Role.FOLLOWER

    def test_load_and_validate_period_creates_events(self, ctx, temp_period_dir):
        """Field mapping: Events created from response availability."""
        period_data = load_and_validate_period(str(temp_period_dir), 2020)

        assert len(period_data.events) > 0
        event = period_data.events[0]
        assert isinstance(event, Event)
        assert event.date is not None

    def test_load_and_validate_period_missing_members_file(self, ctx, temp_period_dir):
        """Error path: Missing members.csv raises FileNotFoundError."""
        members_file = temp_period_dir / "members.csv"
        members_file.unlink()

        with pytest.raises(FileNotFoundError):
            load_and_validate_period(str(temp_period_dir), 2020)

    def test_load_and_validate_period_missing_responses_file(self, ctx, temp_period_dir):
        """Error path: Missing responses.csv raises FileNotFoundError."""
        responses_file = temp_period_dir / "responses.csv"
        responses_file.unlink()

        with pytest.raises(FileNotFoundError):
            load_and_validate_period(str(temp_period_dir), 2020)

    def test_load_and_validate_period_missing_cancellations_file(self, ctx, temp_period_dir):
        """Edge case: Missing optional cancellations.json is handled gracefully."""
        cancellations_file = temp_period_dir / "cancellations.json"
        cancellations_file.unlink()

        period_data = load_and_validate_period(str(temp_period_dir), 2020)

        # Should return PeriodData with empty cancellations
        assert isinstance(period_data, PeriodData)
        assert period_data.cancelled_event_ids == set()
        assert period_data.cancelled_availability == {}

    def test_load_and_validate_period_missing_partnerships_file(self, ctx, temp_period_dir):
        """Edge case: Missing optional partnerships.json is handled gracefully."""
        partnerships_file = temp_period_dir / "partnerships.json"
        partnerships_file.unlink()

        period_data = load_and_validate_period(str(temp_period_dir), 2020)

        # Should return PeriodData with empty partnerships
        assert isinstance(period_data, PeriodData)
        assert period_data.partnerships == {}

    def test_load_and_validate_period_deduplicates_events(self, ctx, temp_period_dir):
        """Field mapping: Events deduplicated when multiple people share availability."""
        # Create 2 members with identical availability slot
        members_csv = temp_period_dir / "members.csv"
        members_csv.write_text(
            "id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined\n"
            "1,Alice Alpha,Alice,alice@test.com,Leader,0,5,10,TRUE,1/1/2020\n"
            "2,Bob Beta,Bob,bob@test.com,Follower,1,3,5,TRUE,2/1/2020\n"
        )

        # Create 2 responses with identical availability slot
        responses_csv = temp_period_dir / "responses.csv"
        responses_csv.write_text(
            "Timestamp,Name,Display Name,Email Address,Primary Role,Secondary Role,Max Sessions,Availability,Min Interval Days\n"
            "1/1/2020 12:00:00,Alice Alpha,Alice,alice@test.com,Leader,I only want to be scheduled in my primary role,2,Saturday January 4 - 1pm,0\n"
            "1/1/2020 12:15:00,Bob Beta,Bob,bob@test.com,Follower,I only want to be scheduled in my primary role,2,Saturday January 4 - 1pm,0\n"
        )

        period_data = load_and_validate_period(str(temp_period_dir), 2020)

        # Should have exactly 1 event, not 2
        assert len(period_data.events) == 1
        event = period_data.events[0]
        assert event.date.day == 4
        assert event.date.month == 1
        assert event.date.hour == 13  # 1pm
