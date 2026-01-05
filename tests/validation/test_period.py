"""Tests for period loading and orchestration."""

import json
import tempfile
from pathlib import Path
import pytest
from peeps_scheduler.models import Event, Peep, Role
from peeps_scheduler.validation.errors import FileValidationError
from peeps_scheduler.validation.period import (
    PeriodData,
    load_and_validate_period,
)

pytestmark = pytest.mark.integration


class TestLoadAndValidatePeriod:
    """Tests for load_and_validate_period orchestrator function."""

    @pytest.fixture
    def temp_period_dir(self, ctx):
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

    def test_validate_period_data_with_single_file_error(self, ctx):
        """Edge case: Single file error raises FileValidationError directly (not MultiFileValidationError)."""
        from peeps_scheduler.validation.errors import FileValidationError, MultiFileValidationError
        from peeps_scheduler.validation.period import validate_period_data
        from tests.validation.fixtures import member_data, response_data

        # Create raw_data with error in ONLY members file
        raw_data = {
            "members": [member_data({"Email Address": ""})],  # Active without email
            "responses": [response_data({"Email Address": "alice@test.com"})],  # Valid
            "cancellations": None,
            "partnerships": None,
        }

        # Should raise FileValidationError directly (not MultiFileValidationError)
        with pytest.raises(FileValidationError) as exc_info:
            validate_period_data(raw_data, 2020)

        # Should be FileValidationError (not MultiFileValidationError)
        assert isinstance(exc_info.value, FileValidationError)
        assert not isinstance(exc_info.value, MultiFileValidationError)
        # Should have exactly 1 error
        assert len(exc_info.value.errors()) == 1
        assert "members.csv" in str(exc_info.value)

    def test_validate_period_data_with_two_file_errors(self, ctx):
        """Edge case: Two file errors raises MultiFileValidationError with both files mentioned."""
        from peeps_scheduler.validation.errors import FileValidationError, MultiFileValidationError
        from peeps_scheduler.validation.period import validate_period_data
        from tests.validation.fixtures import member_data, response_data

        # Create raw_data with errors in members AND responses
        raw_data = {
            "members": [member_data({"Email Address": ""})],  # Active without email
            "responses": [
                response_data({"Email Address": "alice@test.com"}),
                response_data(
                    {"Name": "Bob", "Email Address": "alice@test.com"}
                ),  # Duplicate email
            ],
            "cancellations": None,
            "partnerships": None,
        }

        # Should raise MultiFileValidationError
        with pytest.raises(MultiFileValidationError) as exc_info:
            validate_period_data(raw_data, 2020)

        error_msg = str(exc_info.value)
        # Both files with errors should be mentioned
        assert "members.csv" in error_msg
        assert "responses.csv" in error_msg
        # Optional files without errors should NOT be mentioned
        assert "cancellations.json" not in error_msg
        assert "partnerships.json" not in error_msg
        # Should have exactly 2 file errors
        assert len(exc_info.value.file_errors) == 2

    def test_validate_period_data_collects_all_file_validation_errors(self, ctx):
        """Integration: validate_period_data collects errors from ALL files before raising.

        Validates all files and collects ALL errors, then raises with all filenames
        in the error message. This allows users to fix all errors at once instead of
        one-at-a-time fail-fast.
        """
        from peeps_scheduler.validation.errors import FileValidationError, MultiFileValidationError
        from peeps_scheduler.validation.period import validate_period_data
        from tests.validation.fixtures import member_data, response_data

        # Create raw_data with errors in ALL files
        raw_data = {
            "members": [member_data({"Email Address": ""})],  # Active without email
            "responses": [
                response_data({"Email Address": "alice@test.com"}),
                response_data({"Name": "Bob", "Email Address": "alice@test.com"}),  # Duplicate
            ],
            "cancellations": {"cancelled_events": "not a list"},  # Wrong type
            "partnerships": {"invalid": "format"},  # Wrong format
        }

        # Should raise MultiFileValidationError with ALL filenames mentioned
        with pytest.raises(MultiFileValidationError) as exc_info:
            validate_period_data(raw_data, 2020)

        error_msg = str(exc_info.value)
        # All 4 files should be mentioned in the combined error
        assert "members.csv" in error_msg
        assert "responses.csv" in error_msg
        assert "cancellations.json" in error_msg
        assert "partnerships.json" in error_msg
        # Should have exactly 4 file errors
        assert len(exc_info.value.file_errors) == 4

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
