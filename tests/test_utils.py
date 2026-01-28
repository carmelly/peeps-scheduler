"""
Test utils.py functions, particularly apply_event_results which handles result application.

Following testing philosophy:
- Test what could actually break in result processing
- Use fixtures for complex file-based scenarios
- Focus on individual behaviors with separate test methods
- Fail fast on missing required files
"""

import json
import tempfile
from pathlib import Path
import pytest
from peeps_scheduler import utils
from peeps_scheduler.validation.period import load_and_validate_period

# --- Fixtures ---


@pytest.fixture
def members_csv_content():
    """Standard members CSV content with current format."""
    return """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,John Doe,John D.,john@example.com,Leader,0,5,2,TRUE,1/1/2020
2,Jane Smith,Jane S.,jane@example.com,Follower,1,3,1,TRUE,1/2/2020
3,Bob Wilson,Bob W.,bob@example.com,Leader,2,4,0,TRUE,1/3/2020
4,Alice Brown,Alice B.,alice@example.com,Follower,3,2,1,TRUE,1/4/2020"""


@pytest.fixture
def responses_csv_content():
    """Standard responses CSV content - John and Bob responded."""
    return """Timestamp,Name,Display Name,Email Address,Primary Role,Secondary Role,Max Sessions,Availability,Min Interval Days
1/1/2020 12:00:00,John Doe,John D.,john@example.com,Leader,I only want to be scheduled in my primary role,2,"Saturday March 7 - 7pm, Saturday March 14 - 7pm",0
1/1/2020 12:05:00,Bob Wilson,Bob W.,bob@example.com,Leader,,1,Saturday March 7 - 7pm,0"""


@pytest.fixture
def actual_attendance_data():
    """Actual attendance JSON - John attended both events, Jane attended one event."""
    return {
        "valid_events": [
            {
                "id": 0,
                "date": "2020-03-07 19:00",
                "duration_minutes": 90,
                "attendees": [
                    {"id": 1, "name": "John D.", "role": "Leader"},  # John attended
                    {"id": 2, "name": "Jane S.", "role": "Follower"},  # Jane attended
                ],
            },
            {
                "id": 1,
                "date": "2020-03-14 19:00",
                "duration_minutes": 90,
                "attendees": [
                    {"id": 1, "name": "John D.", "role": "Leader"}  # John attended
                ],
            },
        ]
    }


@pytest.fixture
def temp_files(members_csv_content, responses_csv_content, actual_attendance_data):
    """Create temporary files for testing."""
    temp_dir = Path(tempfile.mkdtemp())

    # Create members.csv
    members_path = temp_dir / "members.csv"
    with members_path.open("w") as f:
        f.write(members_csv_content)

    # Create responses.csv
    responses_path = temp_dir / "responses.csv"
    with responses_path.open("w") as f:
        f.write(responses_csv_content)

    # Create actual_attendance.json
    attendance_path = temp_dir / "actual_attendance.json"
    with attendance_path.open("w") as f:
        json.dump(actual_attendance_data, f)

    yield {
        "temp_dir": temp_dir,
        "members": members_path,
        "responses": responses_path,
        "attendance": attendance_path,
    }

    # Cleanup
    import shutil

    shutil.rmtree(temp_dir, ignore_errors=True)


def _apply_results(temp_files):
    period_data = load_and_validate_period(
        str(temp_files["temp_dir"]),
        2020,
        allow_missing_responses=True,
        require_attendance=True,
    )
    return utils.apply_event_results(period_data)


class TestApplyEventResultsErrorHandling:
    """Test error handling for missing files."""

    def test_missing_members_file_raises_error(self, temp_files):
        """Test that missing members.csv raises an error."""
        temp_files["members"].unlink()

        with pytest.raises(FileNotFoundError):
            load_and_validate_period(
                str(temp_files["temp_dir"]),
                2020,
                allow_missing_responses=True,
                require_attendance=True,
            )

    def test_missing_attendance_file_raises_error(self, temp_files):
        """Test that missing actual_attendance.json raises an error."""
        temp_files["attendance"].unlink()

        with pytest.raises(FileNotFoundError):
            load_and_validate_period(
                str(temp_files["temp_dir"]),
                2020,
                allow_missing_responses=True,
                require_attendance=True,
            )

    def test_missing_responses_file_handles_gracefully(self, temp_files):
        """Test that missing responses.csv is handled gracefully (responses_csv is optional)."""
        temp_files["responses"].unlink()

        # Should not raise an error, just handle gracefully
        result_peeps = _apply_results(temp_files)

        # Should return peeps even without responses file
        assert len(result_peeps) > 0

        # All peeps should have responded=False since no responses file was processed
        for peep in result_peeps:
            assert not peep.responded


class TestRespondedFlagSetting:
    """Test that peep.responded is set correctly based on responses file."""

    def test_responded_flag_set_for_respondents(self, temp_files):
        """Test that peeps who responded are marked as responded."""
        result_peeps = _apply_results(temp_files)

        john = next(p for p in result_peeps if p.id == 1)
        bob = next(p for p in result_peeps if p.id == 3)

        assert john.responded  # John responded
        assert bob.responded  # Bob responded

    def test_responded_flag_not_set_for_non_respondents(self, temp_files):
        """Test that peeps who didn't respond are not marked as responded."""
        result_peeps = _apply_results(temp_files)

        jane = next(p for p in result_peeps if p.id == 2)
        alice = next(p for p in result_peeps if p.id == 4)

        assert not jane.responded  # Jane didn't respond
        assert not alice.responded  # Alice didn't respond

    def test_email_matching_case_insensitive(self, temp_files):
        """Test that email matching works regardless of case."""
        # Create responses with different case emails
        responses_content = """Timestamp,Name,Display Name,Email Address,Primary Role,Secondary Role,Max Sessions,Availability,Min Interval Days
1/1/2020 12:00:00,John Doe,John D.,JOHN@EXAMPLE.COM,Leader,I only want to be scheduled in my primary role,2,"Saturday March 7 - 7pm, Saturday March 14 - 7pm",0
1/1/2020 12:05:00,Bob Wilson,Bob W.,BOB@EXAMPLE.COM,Leader,,1,Saturday March 7 - 7pm,0"""

        with temp_files["responses"].open("w") as f:
            f.write(responses_content)

        result_peeps = _apply_results(temp_files)

        john = next(p for p in result_peeps if p.id == 1)
        bob = next(p for p in result_peeps if p.id == 3)

        assert john.responded
        assert bob.responded


class TestAttendanceIncrementing:
    """Test that total_attended is incremented correctly."""

    def test_total_attended_incremented_for_attendees(self, temp_files):
        """Test that total_attended is incremented for event attendees."""
        result_peeps = _apply_results(temp_files)

        john = next(p for p in result_peeps if p.id == 1)
        jane = next(p for p in result_peeps if p.id == 2)

        # John attended 2 events (originally had 2, should now have 4)
        assert john.total_attended == 4

        # Jane attended 1 event (originally had 1, should now have 2)
        assert jane.total_attended == 2

    def test_total_attended_unchanged_for_non_attendees(self, temp_files):
        """Test that total_attended is unchanged for non-attendees."""
        result_peeps = _apply_results(temp_files)

        bob = next(p for p in result_peeps if p.id == 3)
        alice = next(p for p in result_peeps if p.id == 4)

        # Bob didn't attend any events (originally had 0, should still have 0)
        assert bob.total_attended == 0

        # Alice didn't attend any events (originally had 1, should still have 1)
        assert alice.total_attended == 1


class TestPriorityReset:
    """Test that priority is reset for peeps who attended at least 1 event."""

    def test_priority_reset_for_attendees(self, temp_files):
        """Test that priority is reset to 0 for peeps who attended events."""
        result_peeps = _apply_results(temp_files)

        john = next(p for p in result_peeps if p.id == 1)
        jane = next(p for p in result_peeps if p.id == 2)

        # Both John and Jane attended events, so priority should be reset to 0
        assert john.priority == 0
        assert jane.priority == 0


class TestPriorityIncrease:
    """Test that priority increases for peeps who responded but didn't attend."""

    def test_priority_increased_for_respondents_who_didnt_attend(self, temp_files):
        """Test that priority increases for peeps who responded but didn't attend."""
        result_peeps = _apply_results(temp_files)

        bob = next(p for p in result_peeps if p.id == 3)

        # Bob responded but didn't attend, so priority should increase from 4 to 5
        assert bob.priority == 5


class TestPriorityUnchanged:
    """Test that priority remains the same for peeps who didn't respond and didn't attend."""

    def test_priority_unchanged_for_non_respondents_who_didnt_attend(self, temp_files):
        """Test that priority stays the same for peeps who didn't respond and didn't attend."""
        result_peeps = _apply_results(temp_files)

        alice = next(p for p in result_peeps if p.id == 4)

        # Alice didn't respond and didn't attend, so priority should stay at 2
        assert alice.priority == 2


class TestPeepIndexOrdering:
    """Test that peep index ordering is updated correctly after priority changes."""

    def test_index_ordering_updated_after_priority_changes(self, temp_files):
        """Test that peeps are reordered by priority after applying results."""
        result_peeps = _apply_results(temp_files)

        # After applying results:
        # Bob: priority 5, index should be 0 (highest priority)
        # Alice: priority 2, index should be 1
        # Jane: priority 0, index should be 2 (attended 1 event)
        # John: priority 0, index should be 3 (attended 2 events, pushed to back twice)

        bob = next(p for p in result_peeps if p.id == 3)
        alice = next(p for p in result_peeps if p.id == 4)
        john = next(p for p in result_peeps if p.id == 1)
        jane = next(p for p in result_peeps if p.id == 2)

        assert bob.index == 0  # Highest priority
        assert alice.index == 1
        assert jane.index == 2  # Attended 1 event
        assert john.index == 3  # Attended 2 events, most recent attendee
