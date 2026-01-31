import json
import pytest
from peeps_scheduler.adapters.file.saver import FilePeriodSaver, _sequence_to_dict
from peeps_scheduler.models import Event, EventSequence, Role


def _build_sequence(event_factory, sample_peeps) -> tuple[EventSequence, Event]:
    event = event_factory(id=1, duration_minutes=90)
    for peep in sample_peeps:
        role = Role.LEADER if peep.role == Role.LEADER else Role.FOLLOWER
        event.add_attendee(peep, role)
    event.topic = "Test Topic"

    sequence = EventSequence([event], list(sample_peeps))
    sequence.valid_events = [event]
    return sequence, event


def _get_saver(period_root, period_slug) -> FilePeriodSaver:
    period_path = period_root / period_slug
    period_path.mkdir()
    return FilePeriodSaver(period_root)


@pytest.mark.integration
class TestFilePeriodSaverIntegration:
    def test_save_results_writes_json(self, period_root, period_slug, event_factory, sample_peeps):
        saver = _get_saver(period_root, period_slug)

        sequence, event = _build_sequence(event_factory, sample_peeps)

        saver.save_results(period_slug, sequence)
        results_path = period_root / period_slug / "results.json"
        assert results_path.exists()

        data = json.loads(results_path.read_text())
        assert data["valid_events"][0]["id"] == event.id

    def test_save_members_writes_csv(self, period_root, period_slug, sample_peeps):
        saver = _get_saver(period_root, period_slug)
        saver.save_members(period_slug, sample_peeps)

        csv_path = period_root / period_slug / "members_updated.csv"
        assert csv_path.exists()

        contents = csv_path.read_text()
        assert "Alice Alpha" in contents
        assert "Bob Beta" in contents


@pytest.mark.unit
class TestSequenceToDict:
    """Tests for the _sequence_to_dict function."""

    def test_includes_essential_fields(self, event_factory, peep_factory):
        """Test that _sequence_to_dict includes all fields needed for serialization."""
        events = [event_factory(id=1)]
        peeps = [peep_factory(id=1), peep_factory(id=2)]

        sequence = EventSequence(events, peeps)
        sequence.num_unique_attendees = 2
        sequence.system_weight = 10
        sequence.partnerships_fulfilled = 3
        sequence.mutual_unique_fulfilled = 2
        sequence.mutual_repeat_fulfilled = 1
        sequence.one_sided_fulfilled = 1

        data = _sequence_to_dict(sequence)
        # Should include key serialization fields
        assert "valid_events" in data
        assert "num_unique_attendees" in data
        assert "system_weight" in data
        assert "partnerships_fulfilled" in data
        assert "mutual_unique_fulfilled" in data
        assert "mutual_repeat_fulfilled" in data
        assert "one_sided_fulfilled" in data

        assert data["num_unique_attendees"] == 2
        assert data["system_weight"] == 10
        assert data["partnerships_fulfilled"] == 3
        assert data["mutual_unique_fulfilled"] == 2
        assert data["mutual_repeat_fulfilled"] == 1
        assert data["one_sided_fulfilled"] == 1

    def test_serializes_valid_events_with_attendees(self, event_factory, peep_factory):
        """Test that _sequence_to_dict properly serializes valid events with attendee info."""
        event = event_factory(id=42)
        peep = peep_factory(id=1, display_name="TestPeep")

        event.add_attendee(peep, Role.LEADER)

        sequence = EventSequence([event], [peep])
        sequence.valid_events = [event]

        data = _sequence_to_dict(sequence)

        # Should have valid_events with attendee information
        assert len(data["valid_events"]) == 1
        event_data = data["valid_events"][0]

        assert event_data["id"] == 42
        assert "attendees" in event_data
        assert len(event_data["attendees"]) == 1
        assert event_data["attendees"][0]["name"] == "TestPeep"

    def test_handles_empty_sequence(self):
        """Test that _sequence_to_dict handles an empty EventSequence."""
        sequence = EventSequence([], [])

        data = _sequence_to_dict(sequence)
        assert data["valid_events"] == []
        assert data["num_unique_attendees"] == 0
        assert data["system_weight"] == 0
        assert data["partnerships_fulfilled"] == 0
        assert data["mutual_unique_fulfilled"] == 0
        assert data["mutual_repeat_fulfilled"] == 0
        assert data["one_sided_fulfilled"] == 0

    def test_uses_scheduled_role_not_primary_role(self, event_factory, peep_factory):
        """Test that _sequence_to_dict uses the role a peep is dancing, not their primary role."""
        event = event_factory(id=1)

        # Create follower scheduled as leader (non-primary role)
        follower = peep_factory(id=1, display_name="Follower", role=Role.FOLLOWER)
        event.add_attendee(follower, Role.LEADER)

        # Create leader scheduled as follower (non-primary role)
        leader = peep_factory(id=2, display_name="Leader", role=Role.LEADER)
        event.add_attendee(leader, Role.FOLLOWER)

        # Add a leader in primary role (control case)
        leader2 = peep_factory(id=3, display_name="Leader2", role=Role.LEADER)
        event.add_attendee(leader2, Role.LEADER)

        sequence = EventSequence([event], [follower, leader, leader2])
        sequence.valid_events = [event]

        data = _sequence_to_dict(sequence)
        attendees = data["valid_events"][0]["attendees"]

        # Find attendees by id
        follower_data = next(a for a in attendees if a["id"] == 1)
        leader_data = next(a for a in attendees if a["id"] == 2)
        leader2_data = next(a for a in attendees if a["id"] == 3)

        # Verify roles are correct (scheduled role, not primary role)
        assert follower_data["role"] == "leader"  # Scheduled as leader
        assert leader_data["role"] == "follower"  # Scheduled as follower
        assert leader2_data["role"] == "leader"  # Scheduled as leader (primary)
