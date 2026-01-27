"""Tests for period loading and orchestration."""

import json
import tempfile
from datetime import datetime
from pathlib import Path
import pytest
from peeps_scheduler.models import (
    CancelledMemberAvailability,
    Event,
    PartnershipRequest,
    Peep,
    Role,
)
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
    """
    Create a temporary period directory with comprehensive, valid test files.

    Files created and their contents (readable summary):

    - members.csv
      Header: id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
      Rows:
        1, Alice Alpha   (email: alice@test.com)   Role: Leader
        2, Bob Beta      (email: bob@test.com)     Role: Follower
        3, Carol Clark   (email: carol@test.com)   Role: Leader

    - responses.csv
      Header: Timestamp,Name,Display Name,Email Address,Primary Role,Secondary Role,Max Sessions,Availability,Min Interval Days
      Rows:
        Alice (timestamp 1/1/2020 12:00:00)
          Primary Role: Follower (overrides members.csv role for scheduling)
          Availability: "Saturday January 4 - 1pm"
        Bob (timestamp 1/1/2020 12:15:00)
          Primary Role: Follower
          Availability: "Saturday January 4 - 1pm" (same slot as Alice — used to test deduplication)
        Carol (timestamp 1/1/2020 12:30:00)
          Primary Role: Leader
          Availability: "Saturday January 11 - 1pm"

      Notes on parsing:
        - "Saturday January 4 - 1pm" → datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)
        - "Saturday January 11 - 1pm" → datetime(2020, 1, 11, 13, 0, tzinfo=ctx.tz)

    - period_config.json
      {
        "cancelled_events": ["Saturday January 11 - 1pm"],
        "cancelled_member_availability": [
          {"member_id": 2, "availability": "Saturday January 4 - 1pm"}
        ],
        "partnership_requests": [
          {"requester_id": 1, "partner_id": 2}
        ]
      }

    Example assertions you can write after calling load_and_validate_period/ to_period_data:
      - `assert any(p.id == 1 for p in period_data.peeps)`
      - `assert any(p.id == 2 for p in period_data.peeps)`
      - `# Alice's effective role comes from her response`
        `alice = next(p for p in period_data.peeps if p.id == 1)`
        `assert alice.role == Role.FOLLOWER`
      - `# Two identical availabilities should produce one deduplicated Event`
        `assert sum(1 for e in period_data.events if e.date.day == 4 and e.date.month == 1) == 1`
      - `# Cancelled event removed / present in cancelled_events`
        `assert any(epec.name == "Saturday January 11 - 1pm" or e.date.day == 11 for e in period_data.cancelled_events)`

    Implementation notes:
      - Keeps `Carol` inactive to test handling of inactive members.
      - Adds a partnership request and a cancelled-member-availability entry for integration testing.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # members.csv (3 members: active and inactive)
        members_csv = tmpdir_path / "members.csv"
        members_csv.write_text(
            "id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined\n"
            "1,Alice Alpha,Alice,alice@test.com,follower,0,3,0,TRUE,1/1/2020\n"
            "2,Bob Beta,Bob,bob@test.com,follower,1,3,1,TRUE,1/2/2020\n"
            "3,Carol Clark,Carol,carol@test.com,leader,2,2,4,TRUE,1/3/2020\n"
        )

        # responses.csv with overlapping availability and a separate cancelled event slot
        responses_csv = tmpdir_path / "responses.csv"
        responses_csv.write_text(
            "Timestamp,Name,Display Name,Email Address,Primary Role,Secondary Role,Max Sessions,Availability,Min Interval Days\n"
            "1/1/2020 12:00:00,Alice Alpha,Alice,alice@test.com,Follower,I only want to be scheduled in my primary role,2,Saturday January 4 - 1pm,0\n"
            "1/1/2020 12:15:00,Bob Beta,Bob,bob@test.com,Follower,,1,Saturday January 4 - 1pm,0\n"
            "1/1/2020 12:30:00,Carol Clark,Carol,carol@test.com,Leader,,3,Saturday January 11 - 1pm,0\n"
        )

        # consolidated period_config.json with cancellations and partnership requests
        period_config_json = tmpdir_path / "period_config.json"
        period_config_json.write_text(
            json.dumps(
                {
                    "cancelled_events": ["Saturday January 11 - 1pm"],
                    "cancelled_member_availability": [
                        {
                            "member_email": "bob@test.com",
                            "events": ["Saturday January 4 - 1pm"],
                        }
                    ],
                    "partnership_requests": [
                        {
                            "requester_email": "alice@test.com",
                            "target_emails": ["bob@test.com", "carol@test.com"],
                        }
                    ],
                }
            )
        )

        yield tmpdir_path


@pytest.mark.integration
class TestPeriodSchemaIntegration:
    """Integration tests for complete PeriodFileSchema workflow."""

    def tests_load_and_validate_period_returns_period_data(self, ctx, temp_period_dir):
        """Integration: load_and_validate_period() uses PeriodFileSchema.model_validate()."""
        period_data = load_and_validate_period(str(temp_period_dir), 2020)

        # TODO: need better asserts
        assert isinstance(period_data, PeriodData)
        assert len(period_data.peeps) >= 1
        assert len(period_data.events) >= 1

    def test_period_file_schema_validates_all_cross_file_constraints(self, ctx, temp_period_dir):
        """Integration: PeriodFileSchema enforces all cross-file validation rules."""
        # This test verifies the complete validation works end-to-end
        period_data_obj = load_and_validate_period(str(temp_period_dir), 2020)

        # Should successfully validate all components
        assert isinstance(period_data_obj.peeps, list)
        assert isinstance(period_data_obj.events, list)
        assert isinstance(period_data_obj.cancelled_events, list)
        assert isinstance(period_data_obj.partnership_requests, list)

    def test_to_period_data_converts_event_specs_to_events(self, ctx):
        """Contract: to_period_data() converts EventSpec to Event domain objects."""
        schema = PeriodFileSchema.model_validate(
            period_data(
                {
                    "responses": {
                        "responses": [response_data()],
                        "event_rows": [
                            # Defaults:
                            # "Name": "Saturday January 4 - 1pm"
                            # "Event Duration": "90"
                            event_row_data()
                        ],
                    }
                }
            ),
            context={"ctx": ctx},
        )

        result = to_period_data(schema, 2020)

        # Events should be Event domain objects, not EventSpecs
        assert len(result.events) == 1
        assert isinstance(result.events[0], Event)
        assert result.events[0].date == datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)
        assert result.events[0].duration_minutes == 90

    def test_load_and_validate_period_happy_path_comprehensive(self, ctx, temp_period_dir):
        """Comprehensive happy-path: validate full PeriodData shapes and types."""
        period_data = load_and_validate_period(str(temp_period_dir), 2020)
        # Top-level object and field shapes
        assert isinstance(period_data, PeriodData)
        assert isinstance(period_data.cancelled_member_availability, list)
        assert isinstance(period_data.partnership_requests, list)

        # Simple indexed identifier checks (fixture order is deterministic)
        # First peep should be Alice
        alice = period_data.peeps[0]
        assert isinstance(alice, Peep)
        assert alice.id == 1
        assert alice.full_name == "Alice Alpha"

        # Second peep should be Bob
        bob = period_data.peeps[1]
        assert bob.id == 2

        # Third peep should be Carol
        carol = period_data.peeps[2]
        assert carol.id == 3

        # First event should be the Jan 4 availability (extracted from responses)
        assert isinstance(period_data.events[0], Event)
        jan4 = period_data.events[0]
        assert jan4.date == datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)

        # First cancelled event should be Jan 11 per period_config.json
        jan11 = period_data.cancelled_events[0]
        assert jan11.date == datetime(2020, 1, 11, 13, 0, tzinfo=ctx.tz)

        # Cancelled member availability should be Bob's Jan 4 slot
        c_avail = period_data.cancelled_member_availability[0]
        assert isinstance(c_avail, CancelledMemberAvailability)
        assert c_avail.peep == bob
        assert c_avail.events == [jan4]

        # Partnership request should be from Alice to Bob and Carol
        assert isinstance(period_data.partnership_requests[0], PartnershipRequest)
        assert period_data.partnership_requests[0].requester == alice
        assert period_data.partnership_requests[0].target_peeps == [bob, carol]


@pytest.mark.unit
class TestToPeriodData:
    """Tests for to_period_data() function with PeriodFileSchema."""

    def test_accepts_period_file_schema(self, ctx):
        """Contract: to_period_data() accepts PeriodFileSchema object."""
        schema = PeriodFileSchema.model_validate(period_data(), context={"ctx": ctx})

        result = to_period_data(schema, 2020)

        assert isinstance(result, PeriodData)
        assert hasattr(result, "peeps")
        assert hasattr(result, "events")
        assert hasattr(result, "cancelled_events")
        assert hasattr(result, "cancelled_member_availability")
        assert hasattr(result, "partnership_requests")

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
                    "cancelled_events": ["Saturday January 4 - 1pm"],
                }
            ),
            context={"ctx": ctx},
        )

        result = to_period_data(schema, 2020)

        assert isinstance(result.cancelled_events, list)
        assert len(result.cancelled_events) == 1


class TestLoadAndValidatePeriod:
    """Tests for load_and_validate_period orchestrator function."""

    def test_load_and_validate_period_valid_data(self, ctx, temp_period_dir):
        """Happy path: Valid period directory returns PeriodData with correct structure."""
        period_data = load_and_validate_period(str(temp_period_dir), 2020)

        assert isinstance(period_data, PeriodData)
        assert all(isinstance(peep, Peep) for peep in period_data.peeps)
        assert all(isinstance(event, Event) for event in period_data.events)
        assert all(isinstance(c_event, Event) for c_event in period_data.cancelled_events)
        assert all(
            isinstance(c_avail, CancelledMemberAvailability)
            for c_avail in period_data.cancelled_member_availability
        )
        assert all(
            isinstance(p_request, PartnershipRequest)
            for p_request in period_data.partnership_requests
        )

    def test_load_and_validate_period_creates_peeps(self, ctx, temp_period_dir):
        """Field mapping: Peeps created from members and responses."""
        period_data = load_and_validate_period(str(temp_period_dir), 2020)

        assert len(period_data.peeps) == 3
        assert all(isinstance(peep, Peep) for peep in period_data.peeps)

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

    def test_load_and_validate_period_missing_period_config_file(self, ctx, temp_period_dir):
        """Edge case: Missing optional period_config.json is handled gracefully."""
        period_config_file = temp_period_dir / "period_config.json"
        period_config_file.unlink()

        period_data = load_and_validate_period(str(temp_period_dir), 2020)

        # Should return PeriodData with empty cancellations/p       artnerships
        assert isinstance(period_data, PeriodData)
        assert period_data.cancelled_events == []
        assert period_data.cancelled_member_availability == []
        assert period_data.partnership_requests == []

    def test_load_and_validate_period_deduplicates_events(self, ctx, temp_period_dir):
        """Field mapping: Events deduplicated when multiple people share availability."""

        # Create 2 responses with identical availability slot
        responses_csv = temp_period_dir / "responses.csv"
        responses_csv.write_text(
            "Timestamp,Name,Display Name,Email Address,Primary Role,Secondary Role,Max Sessions,Availability,Min Interval Days\n"
            "1/1/2020 12:00:00,Alice Alpha,Alice,alice@test.com,Leader,I only want to be scheduled in my primary role,2,Saturday January 4 - 1pm,0\n"
            "1/1/2020 12:15:00,Bob Beta,Bob,bob@test.com,Follower,I only want to be scheduled in my primary role,2,Saturday January 4 - 1pm,0\n"
        )
        # remove period_config.json to avoid interference
        period_config_file = temp_period_dir / "period_config.json"
        period_config_file.unlink()

        period_data = load_and_validate_period(str(temp_period_dir), 2020)

        # Should have exactly 1 event, not 2
        assert len(period_data.events) == 1
        event = period_data.events[0]
        assert event.date == datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)
