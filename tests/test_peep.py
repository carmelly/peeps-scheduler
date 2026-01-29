"""
Test Peep class functionality with focus on constraint checking and data handling.

Following testing philosophy:
- Test what could actually break
- Use inline creation for simple constraint tests
- Use factories for complex multi-field scenarios
- Focus on individual Peep behavior, not scheduling logic
"""

import datetime
from peeps_scheduler.models import Event, Peep, Role


class TestPeepConstraints:
    """Test core constraint checking logic - the most critical functionality."""

    def test_can_attend_when_available(self):
        """Test that peep can attend event in their availability."""
        availability = [
            Event(id=1, date=datetime.datetime(2025, 1, 10, 10), duration_minutes=120),
            Event(id=2, date=datetime.datetime(2025, 1, 10, 14), duration_minutes=120),
            Event(id=3, date=datetime.datetime(2025, 1, 11, 10), duration_minutes=120),
        ]
        peep = Peep(id=1, role=Role.LEADER, availability=availability, event_limit=2)
        event = Event(id=2, date=datetime.datetime(2025, 1, 10, 14), duration_minutes=120)

        assert peep.can_attend(event)

    def test_cannot_attend_when_unavailable(self):
        """Test that peep cannot attend event not in availability."""
        availability = [
            Event(id=1, date=datetime.datetime(2025, 1, 10, 10), duration_minutes=120),
            Event(id=3, date=datetime.datetime(2025, 1, 11, 10), duration_minutes=120),
        ]
        peep = Peep(id=1, role=Role.LEADER, availability=availability, event_limit=2)  # No event 2
        event = Event(id=2, date=datetime.datetime(2025, 1, 10, 14), duration_minutes=120)

        assert not peep.can_attend(event)

    def test_cannot_attend_when_over_event_limit(self):
        """Test that peep cannot attend when at event limit."""
        availability = [
            Event(id=1, date=datetime.datetime(2025, 1, 10, 10), duration_minutes=120)
        ]
        peep = Peep(id=1, role=Role.LEADER, availability=availability, event_limit=1)
        peep.num_events = 1  # Already at limit
        event = Event(id=1, date=datetime.datetime(2025, 1, 10, 10), duration_minutes=120)

        assert not peep.can_attend(event)

    def test_can_attend_when_under_event_limit(self):
        """Test that peep can attend when under event limit."""
        availability = [
            Event(id=1, date=datetime.datetime(2025, 1, 10, 10), duration_minutes=120)
        ]
        peep = Peep(id=1, role=Role.LEADER, availability=availability, event_limit=2)
        peep.num_events = 1  # Under limit
        event = Event(id=1, date=datetime.datetime(2025, 1, 10, 10), duration_minutes=120)

        assert peep.can_attend(event)

    def test_cannot_attend_within_interval_days(self):
        """Test that peep cannot attend event within minimum interval."""
        availability = [
            Event(id=1, date=datetime.datetime(2025, 1, 10, 18), duration_minutes=120)
        ]
        peep = Peep(
            id=1, role=Role.LEADER, availability=availability, event_limit=2, min_interval_days=3
        )
        event = Event(id=1, date=datetime.datetime(2025, 1, 10), duration_minutes=120)

        # Add a previous event 2 days ago (within 3-day interval)
        previous_date = datetime.datetime(2025, 1, 8)
        peep.assigned_event_dates.append(previous_date)

        assert not peep.can_attend(event)

    def test_can_attend_exactly_at_interval_days(self):
        """Test that peep can attend event exactly at minimum interval."""
        availability = [
            Event(id=1, date=datetime.datetime(2025, 1, 10, 18), duration_minutes=120)
        ]
        peep = Peep(
            id=1, role=Role.LEADER, availability=availability, event_limit=2, min_interval_days=3
        )
        event = Event(id=1, date=datetime.datetime(2025, 1, 10), duration_minutes=120)

        # Add a previous event exactly 3 days ago (meets interval requirement)
        previous_date = datetime.datetime(2025, 1, 7)
        peep.assigned_event_dates.append(previous_date)

        assert peep.can_attend(event)

    def test_can_attend_with_zero_interval_days(self):
        """Test that peep with zero interval can attend multiple events same day."""
        availability = [
            Event(id=1, date=datetime.datetime(2025, 1, 10, 14), duration_minutes=120)
        ]
        peep = Peep(
            id=1, role=Role.LEADER, availability=availability, event_limit=2, min_interval_days=0
        )
        event = Event(id=1, date=datetime.datetime(2025, 1, 10, 14), duration_minutes=120)

        # Add a previous event same day
        same_day_earlier = datetime.datetime(2025, 1, 10, 10)
        peep.assigned_event_dates.append(same_day_earlier)

        assert peep.can_attend(event)

    def test_interval_calculation_works_both_directions(self):
        """Test that interval checking works for events before or after previous events."""
        availability = [
            Event(id=1, date=datetime.datetime(2025, 1, 10, 18), duration_minutes=120)
        ]
        peep = Peep(
            id=1, role=Role.LEADER, availability=availability, event_limit=2, min_interval_days=2
        )

        # Event is 2025-01-10
        event = Event(id=1, date=datetime.datetime(2025, 1, 10), duration_minutes=120)

        # Previous event 1 day after (2025-01-11) - should block
        future_date = datetime.datetime(2025, 1, 11)
        peep.assigned_event_dates.append(future_date)

        assert not peep.can_attend(event)


class TestDataConversion:
    """Test CSV/dict conversion for data pipeline."""

    def test_constructor_handles_missing_optional_fields(self):
        """Test that constructor gracefully handles missing optional fields."""
        # Test with minimal required fields
        peep = Peep(id=1, role=Role.LEADER)

        assert peep.id == 1
        assert peep.role == Role.LEADER
        assert peep.index == 0
        assert peep.priority == 0
        assert peep.total_attended == 0
        assert peep.availability == []
        assert peep.event_limit == 0
        assert peep.min_interval_days == 0
        assert peep.topic_votes == []

    def test_assigns_default_switch_preference_if_missing(self):
        """Test that constructor assigns default switch preference if not provided."""
        peep = Peep(id=1, role=Role.LEADER, switch_pref=None)

        assert peep.switch_pref == peep.switch_pref.PRIMARY_ONLY
