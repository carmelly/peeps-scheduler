"""
Integration tests for the Novice Peeps scheduling system.

Following testing philosophy:
- Test end-to-end workflows that users actually encounter
- Focus on data pipeline integrity and real-world scenarios
- Use realistic (but small) datasets to verify system behavior
- Test cross-component integration, not individual unit behavior
"""

import json
import shutil
import tempfile
from pathlib import Path
import pytest
from peeps_scheduler.scheduler import Scheduler
from peeps_scheduler.validation import FileValidationError, load_and_validate_period


class TestEndToEndWorkflows:
    """Test complete user workflows from start to finish."""

    def test_full_pipeline_gracefully_handles_impossible_events(self):
        """Test complete end-to-end pipeline when no events can be scheduled."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data with insufficient peeps (60-min events need 2 per role, we have 1 per role)
            period_path = Path(temp_dir) / "test_period"
            period_path.mkdir()

            members_csv_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alice,Alice,alice@test.com,Leader,0,1,0,TRUE,1/1/2025
2,Bob,Bob,bob@test.com,Follower,1,1,0,TRUE,1/1/2025"""

            responses_csv_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Min Interval Days
2/1/2025 10:00:00,alice@test.com,Alice,Leader,I only want to be scheduled in my primary role,1,"Saturday March 15 - 7pm to 8pm",0
2/1/2025 10:00:00,bob@test.com,Bob,Follower,I only want to be scheduled in my primary role,1,"Saturday March 15 - 7pm to 8pm",0"""

            (period_path / "members.csv").write_text(members_csv_content)
            (period_path / "responses.csv").write_text(responses_csv_content)

            period_data = load_and_validate_period(str(period_path), 2025)
            scheduler = Scheduler(
                period_data=period_data,
                data_folder=str(period_path),
                max_events=1,
                interactive=False,
            )
            result = scheduler.run()

            # Verify scheduler handled impossible scenario gracefully
            results_json = period_path / "results.json"

            # With impossible constraints, scheduler.run() should:
            # 1. Return None (early return when no sequences found)
            # 2. NOT create results.json file (save_event_sequence never called)
            assert result is None, (
                f"Expected scheduler.run() to return None with impossible constraints, got {result}"
            )
            assert not results_json.exists(), (
                "Expected no results.json file created when no sequences can be scheduled"
            )

    def test_scheduler_handles_impossible_constraints(self):
        """Test complete end-to-end pipeline with extremely impossible attendance constraints."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create scenario with 120-min event (needs 6 per role) but only 1 of each role
            # Set up period directory structure
            period_path = Path(temp_dir) / "test_period"
            period_path.mkdir()

            members_csv_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,OnlyLeader,OnlyLeader,leader@test.com,Leader,0,1,0,TRUE,1/1/2025
2,OnlyFollower,OnlyFollower,follower@test.com,Follower,1,1,0,TRUE,1/1/2025"""

            responses_csv_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Min Interval Days
2/1/2025 10:00:00,leader@test.com,OnlyLeader,Leader,I only want to be scheduled in my primary role,1,"Saturday March 15 - 7pm to 9pm",0
2/1/2025 10:00:00,follower@test.com,OnlyFollower,Follower,I only want to be scheduled in my primary role,1,"Saturday March 15 - 7pm to 9pm",0"""

            (period_path / "members.csv").write_text(members_csv_content)
            (period_path / "responses.csv").write_text(responses_csv_content)

            # Run complete scheduler workflow
            period_data = load_and_validate_period(str(period_path), 2025)
            scheduler = Scheduler(
                period_data=period_data,
                data_folder=str(period_path),
                max_events=1,
                interactive=False,
            )
            result = scheduler.run()

            # Verify scheduler handled extremely impossible scenario gracefully
            results_json = period_path / "results.json"

            # With extremely impossible constraints (1 peep per role for 120-min event), scheduler.run() should:
            # 1. Return None (early return when no sequences found)
            # 2. NOT create results.json file (save_event_sequence never called)
            assert result is None, (
                f"Expected scheduler.run() to return None with impossible constraints, got {result}"
            )
            assert not results_json.exists(), (
                "Expected no results.json file created when constraints are impossible to meet"
            )

    def test_scheduler_run_golden_master(self):
        """Test complete CSV-to-JSON-to-scheduler pipeline with golden master data.

        This test uses 2025-09-sanitized data as the golden master, which reflects
        the current state of the scheduling algorithm with sanitized test data.

        This test validates the complete end-to-end workflow:
        1. Load CSV files (members.csv and responses.csv)
        2. Convert CSV to JSON (output.json)
        3. Run scheduler algorithm
        4. Generate results (results.json)
        5. Verify all generated files match golden master exactly
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            # Load expected results from 2025-09-sanitized data
            golden_master_dir = Path(__file__).parent / "golden_master_2025_09_sanitized"

            with (golden_master_dir / "results.json").open() as f:
                expected_results = json.load(f)

            # Set up period directory structure as scheduler expects
            period_path = Path(temp_dir) / "2025-test_period"
            period_path.mkdir()

            # Step 1: Copy CSV files from golden master for true end-to-end testing
            # This tests the complete CSV-to-JSON-to-scheduler pipeline
            responses_csv = period_path / "responses.csv"
            members_csv = period_path / "members.csv"
            shutil.copy(golden_master_dir / "responses.csv", responses_csv)
            shutil.copy(golden_master_dir / "members.csv", members_csv)

            period_data = load_and_validate_period(str(period_path), 2025)
            scheduler = Scheduler(
                period_data=period_data, data_folder=str(period_path), max_events=10, interactive=False
            )
            result = scheduler.run()

            # Verify scheduler succeeded (should not return None)
            assert result is not None, "Scheduler should succeed with valid historical data"

            result_json = period_path / "results.json"

            print(result_json)

            assert result_json.exists(), "results.json should be created for successful scheduling"

            with (golden_master_dir / "results.json").open() as f:
                expected_results = json.load(f)
            with result_json.open() as f:
                actual_results = json.load(f)

            assert actual_results == expected_results, (
                "Generated results.json should match golden master"
            )

            print(
                "Golden master integration test passed: validation -> Scheduler pipeline produces identical results"
            )


class TestCancellationsWorkflow:
    """Test cancellations.json integration with the scheduler.

    Cancelled events should be:
    - Preserved in output.json (to maintain peep availability data)
    - Filtered out from results.json (not scheduled)
    """

    def test_scheduler_raises_error_for_unknown_cancelled_event(self):
        """Test that validation raises error when period_config specifies non-existent event.

        Configuration error: user mistakenly specified an event that doesn't exist in responses.

        Scenario:
        - Create 2 events: "Saturday March 1 - 5pm" and "Sunday March 2 - 5pm"
        - Create period_config.json cancelling non-existent: "Friday March 7 - 5pm"
        - Validate period
        - Assert: Raises FileValidationError about cancelled event not found
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            period_path = Path(temp_dir) / "test_period"
            period_path.mkdir()

            # Members.csv: minimal (4 leaders + 4 followers for two 60-min events)
            members_csv_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alice Leader,Alice,alice@test.com,Leader,0,4,0,TRUE,1/1/2025
5,Eve Follower,Eve,eve@test.com,Follower,1,4,0,TRUE,1/1/2025
2,Bob Leader,Bob,bob@test.com,Leader,2,3,0,TRUE,1/1/2025
6,Fiona Follower,Fiona,fiona@test.com,Follower,3,3,0,TRUE,1/1/2025
3,Charlie Leader,Charlie,charlie@test.com,Leader,4,2,0,TRUE,1/1/2025
7,Grace Follower,Grace,grace@test.com,Follower,5,2,0,TRUE,1/1/2025
4,David Leader,David,david@test.com,Leader,6,1,0,TRUE,1/1/2025
8,Hannah Follower,Hannah,hannah@test.com,Follower,7,1,0,TRUE,1/1/2025"""

            members_path = period_path / "members.csv"
            members_path.write_text(members_csv_content)

            # Responses.csv: 2 valid events
            responses_csv_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days,Preferred gap between sessions?,Partnership Preference,Questions or Comments for Organizers,Questions or Comments for Leilani
,,Event: Saturday March 1 - 5pm,,,,,60,,,,,
,,Event: Sunday March 2 - 5pm,,,,,60,,,,,
2/1/2025 10:00:00,alice@test.com,Alice,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,bob@test.com,Bob,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,charlie@test.com,Charlie,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,david@test.com,David,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,eve@test.com,Eve,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,fiona@test.com,Fiona,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,grace@test.com,Grace,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,hannah@test.com,Hannah,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,"""

            responses_path = period_path / "responses.csv"
            responses_path.write_text(responses_csv_content)

            # Create period_config.json with a NON-EXISTENT event
            period_config_content = {
                "cancelled_events": [
                    "Friday March 7 - 5pm to 6pm"  # Doesn't exist in responses
                ],
                "cancelled_member_availability": [],
                "notes": "User mistakenly cancelled non-existent event",
            }
            period_config_path = period_path / "period_config.json"
            with period_config_path.open("w") as f:
                json.dump(period_config_content, f)

            with pytest.raises(
                FileValidationError, match=r"cancelled event.*not found|unknown.*cancelled"
            ):
                load_and_validate_period(str(period_path), 2025)

    def test_scheduler_skips_cancelled_events(self):
        """Test that cancelled events are filtered from results.

        Scenario:
        - Create 2 events (60-min each, require 2 leaders + 2 followers)
        - Cancel 1 event via cancellations.json
        - Run scheduler
        - Assert: results.json contains only 1 event (cancelled filtered)
        - Assert: No peeps scheduled for cancelled event in results.json
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            period_path = Path(temp_dir) / "test_period"
            period_path.mkdir()

            # Create minimal test data: 2 events, enough peeps for both
            # Event 1: Saturday March 1 - 5pm (60 min)
            # Event 2: Sunday March 2 - 5pm (60 min)

            # Members.csv: 4 leaders + 4 followers (enough for both events), sorted by priority descending
            members_csv_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alice Leader,Alice,alice@test.com,Leader,0,10,0,TRUE,1/1/2025
5,Eve Follower,Eve,eve@test.com,Follower,1,9,0,TRUE,1/1/2025
2,Bob Leader,Bob,bob@test.com,Leader,2,8,0,TRUE,1/1/2025
6,Fiona Follower,Fiona,fiona@test.com,Follower,3,7,0,TRUE,1/1/2025
3,Charlie Leader,Charlie,charlie@test.com,Leader,4,6,0,TRUE,1/1/2025
7,Grace Follower,Grace,grace@test.com,Follower,5,5,0,TRUE,1/1/2025
4,David Leader,David,david@test.com,Leader,6,4,0,TRUE,1/1/2025
8,Hannah Follower,Hannah,hannah@test.com,Follower,7,3,0,TRUE,1/1/2025"""

            members_path = period_path / "members.csv"
            members_path.write_text(members_csv_content)

            # Responses.csv: Event rows format with all required columns
            responses_csv_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days,Preferred gap between sessions?,Partnership Preference,Questions or Comments for Organizers,Questions or Comments for Leilani
,,Event: Saturday March 1 - 5pm,,,,,60,,,,,
,,Event: Sunday March 2 - 5pm,,,,,60,,,,,
2/1/2025 10:00:00,alice@test.com,Alice,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,bob@test.com,Bob,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,charlie@test.com,Charlie,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,david@test.com,David,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,eve@test.com,Eve,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,fiona@test.com,Fiona,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,grace@test.com,Grace,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,hannah@test.com,Hannah,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,"""

            responses_path = period_path / "responses.csv"
            responses_path.write_text(responses_csv_content)

            # Create period_config.json with one event cancelled
            period_config_content = {
                "cancelled_events": ["Sunday March 2 - 5pm"],
                "cancelled_member_availability": [],
                "notes": "Instructor unavailable - notified members on 2025-02-15",
            }
            period_config_path = period_path / "period_config.json"
            with period_config_path.open("w") as f:
                json.dump(period_config_content, f)

            period_data = load_and_validate_period(str(period_path), 2025)
            scheduler = Scheduler(
                period_data=period_data, data_folder=str(period_path), max_events=10, interactive=False
            )
            result = scheduler.run()

            # Verify scheduler succeeded
            assert result is not None, (
                "Scheduler should succeed with valid data and valid cancelled events"
            )

            # Verify results.json exists and contains ONLY 1 event (cancelled filtered)
            results_json = period_path / "results.json"
            assert results_json.exists(), "results.json should be created"

            with results_json.open() as f:
                results_data = json.load(f)

            results_events = results_data.get("valid_events", [])
            assert len(results_events) == 1, (
                f"results.json should have 1 event (cancelled filtered), got {len(results_events)}"
            )
            assert len(results_events[0]["attendees"]) == 6, (
                "Non-cancelled event should have 6 attendees"
            )

    def test_scheduler_works_without_cancellations_json(self):
        """Test scheduling when cancellations.json doesn't exist.

        Scenario:
        - Create 2 events
        - NO period_config.json file
        - Run scheduler
        - Assert: Scheduler succeeds (backward compatible)
        - Assert: Both events are scheduled normally
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            period_path = Path(temp_dir) / "test_period"
            period_path.mkdir()

            # Members.csv: 4 leaders + 4 followers, sorted by priority (highest to lowest)
            members_csv_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alice Leader,Alice,alice@test.com,Leader,0,4,0,TRUE,1/1/2025
5,Eve Follower,Eve,eve@test.com,Follower,1,4,0,TRUE,1/1/2025
2,Bob Leader,Bob,bob@test.com,Leader,2,3,0,TRUE,1/1/2025
6,Fiona Follower,Fiona,fiona@test.com,Follower,3,3,0,TRUE,1/1/2025
3,Charlie Leader,Charlie,charlie@test.com,Leader,4,2,0,TRUE,1/1/2025
7,Grace Follower,Grace,grace@test.com,Follower,5,2,0,TRUE,1/1/2025
4,David Leader,David,david@test.com,Leader,6,1,0,TRUE,1/1/2025
8,Hannah Follower,Hannah,hannah@test.com,Follower,7,1,0,TRUE,1/1/2025"""

            members_path = period_path / "members.csv"
            members_path.write_text(members_csv_content)

            # Responses.csv: Event rows format with all required columns
            responses_csv_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days,Preferred gap between sessions?,Partnership Preference,Questions or Comments for Organizers,Questions or Comments for Leilani
,,Event: Saturday March 1 - 5pm,,,,,60,,,,,
,,Event: Sunday March 2 - 5pm,,,,,60,,,,,
2/1/2025 10:00:00,alice@test.com,Alice,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,bob@test.com,Bob,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,charlie@test.com,Charlie,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,david@test.com,David,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,eve@test.com,Eve,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,fiona@test.com,Fiona,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,grace@test.com,Grace,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2/1/2025 10:00:00,hannah@test.com,Hannah,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,"""

            responses_path = period_path / "responses.csv"
            responses_path.write_text(responses_csv_content)

            # DO NOT create period_config.json

            period_data = load_and_validate_period(str(period_path), 2025)
            scheduler = Scheduler(
                period_data=period_data, data_folder=str(period_path), max_events=10, interactive=False
            )
            result = scheduler.run()

            # Verify scheduler succeeded
            assert result is not None, "Scheduler should succeed without cancellations.json"

            # Verify results.json exists and contains both events
            results_json = period_path / "results.json"
            assert results_json.exists(), "results.json should be created"

            with results_json.open() as f:
                results_data = json.load(f)

            results_events = results_data.get("valid_events", [])
            assert len(results_events) == 2, (
                f"Without cancellations.json, both events should be scheduled. Got {len(results_events)}"
            )

            # Just verify we have 2 events scheduled (no filtering without cancellations.json)
            assert len(results_events) == 2, (
                "Both events should be scheduled without cancellations.json"
            )

    def test_scheduler_skips_cancelled_availability(self):
        """Test that cancelled availability prevents scheduling for that event."""
        with tempfile.TemporaryDirectory() as temp_dir:
            period_path = Path(temp_dir) / "test_period"
            period_path.mkdir()

            # Members.csv: 5 leaders + 4 followers (enough to pass ABS_MIN_ROLE after cancellation)
            members_csv_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alex Leader,Alex,alex@test.com,Leader,0,10,0,TRUE,1/1/2025
2,Bob Leader,Bob,bob@test.com,Leader,1,9,0,TRUE,1/1/2025
3,Casey Leader,Casey,casey@test.com,Leader,2,8,0,TRUE,1/1/2025
4,Drew Leader,Drew,drew@test.com,Leader,3,7,0,TRUE,1/1/2025
5,Eli Leader,Eli,eli@test.com,Leader,4,6,0,TRUE,1/1/2025
6,Dana Follower,Dana,dana@test.com,Follower,5,5,0,TRUE,1/1/2025
7,Eve Follower,Eve,eve@test.com,Follower,6,4,0,TRUE,1/1/2025
8,Fran Follower,Fran,fran@test.com,Follower,7,3,0,TRUE,1/1/2025
9,Gia Follower,Gia,gia@test.com,Follower,8,2,0,TRUE,1/1/2025"""

            members_path = period_path / "members.csv"
            members_path.write_text(members_csv_content)

            # Responses.csv: one event, all members available
            responses_csv_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days,Preferred gap between sessions?,Partnership Preference,Questions or Comments for Organizers,Questions or Comments for Leilani
,,Event: Saturday March 1 - 5pm,,,,,60,,,,,
2/1/2025 10:00:00,alex@test.com,Alex,Leader,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2/1/2025 10:00:00,bob@test.com,Bob,Leader,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2/1/2025 10:00:00,casey@test.com,Casey,Leader,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2/1/2025 10:00:00,drew@test.com,Drew,Leader,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2/1/2025 10:00:00,eli@test.com,Eli,Leader,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2/1/2025 10:00:00,dana@test.com,Dana,Follower,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2/1/2025 10:00:00,eve@test.com,Eve,Follower,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2/1/2025 10:00:00,fran@test.com,Fran,Follower,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2/1/2025 10:00:00,gia@test.com,Gia,Follower,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,"""

            responses_path = period_path / "responses.csv"
            responses_path.write_text(responses_csv_content)

            period_config_content = {
                "cancelled_events": [],
                "cancelled_member_availability": [
                    {"member_email": "alex@test.com", "events": ["Saturday March 1 - 5pm"]}
                ],
                "notes": "Alex is no longer available",
            }
            period_config_path = period_path / "period_config.json"
            with period_config_path.open("w") as f:
                json.dump(period_config_content, f)

            period_data = load_and_validate_period(str(period_path), 2025)
            scheduler = Scheduler(
                period_data=period_data, data_folder=str(period_path), max_events=10, interactive=False
            )
            result = scheduler.run()

            assert result is not None, "Scheduler should succeed with cancelled availability"

            results_json = period_path / "results.json"
            with results_json.open() as f:
                results_data = json.load(f)

            attendees = results_data["valid_events"][0]["attendees"]
            alternates = results_data["valid_events"][0]["alternates"]
            assigned_ids = {a["id"] for a in attendees + alternates}
            assert 1 not in assigned_ids, "Cancelled leader should not be scheduled"

    def test_scheduler_raises_error_for_cancelled_availability_unknown_email(self):
        """Test that period_config.json fails for unknown email in cancelled availability."""
        with tempfile.TemporaryDirectory() as temp_dir:
            period_path = Path(temp_dir) / "test_period"
            period_path.mkdir()

            members_csv_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alice Leader,Alice,alice@test.com,Leader,0,4,0,TRUE,1/1/2025
2,Eve Follower,Eve,eve@test.com,Follower,1,4,0,TRUE,1/1/2025"""

            members_path = period_path / "members.csv"
            members_path.write_text(members_csv_content)

            responses_csv_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days,Preferred gap between sessions?,Partnership Preference,Questions or Comments for Organizers,Questions or Comments for Leilani
,,Event: Saturday March 1 - 5pm,,,,,60,,,,,
2/1/2025 10:00:00,alice@test.com,Alice,Leader,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2/1/2025 10:00:00,eve@test.com,Eve,Follower,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,"""

            responses_path = period_path / "responses.csv"
            responses_path.write_text(responses_csv_content)

            period_config_content = {
                "cancelled_events": [],
                "cancelled_member_availability": [
                    {"member_email": "unknown@test.com", "events": ["Saturday March 1 - 5pm"]}
                ],
            }
            period_config_path = period_path / "period_config.json"
            with period_config_path.open("w") as f:
                json.dump(period_config_content, f)

            with pytest.raises(FileValidationError, match=r"unknown email|cancelled availability"):
                load_and_validate_period(str(period_path), 2025)
