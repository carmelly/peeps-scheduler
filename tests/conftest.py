import datetime
import pytest
from peeps_scheduler.models import Event, Peep, Role, SwitchPreference


@pytest.fixture
def peep_factory():
    """Factory for creating test peeps with sensible defaults."""

    def _normalize_availability(values):
        if not values:
            return []
        normalized = []
        for value in values:
            if isinstance(value, Event):
                normalized.append(value)
            else:
                normalized.append(Event(id=value, date=datetime.datetime(2025, 1, 15, 18, 0)))
        return normalized

    def _create(id=1, role=Role.LEADER, **kwargs):
        availability = kwargs.pop("availability", None)
        availability = [] if availability is None else _normalize_availability(availability)
        defaults = {
            "full_name": f"TestPeep{id}",
            "display_name": f"TestPeep{id}",
            "email": f"peep{id}@test.com",
            "availability": availability,
            "event_limit": 2,
            "priority": 0,
            "responded": True,
            "switch_pref": SwitchPreference.PRIMARY_ONLY,
            "index": 0,
            "total_attended": 0,
            "min_interval_days": 0,
            "active": True,
            "date_joined": "2025-01-01",
        }
        defaults.update(kwargs)
        return Peep(id=id, role=role, **defaults)

    return _create


@pytest.fixture
def event_factory():
    """Factory for creating test events with sensible defaults."""

    def _create(id=1, duration_minutes=120, **kwargs):
        defaults = {"date": datetime.datetime(2025, 1, 15, 18, 0)}
        defaults.update(kwargs)
        return Event(id=id, duration_minutes=duration_minutes, **defaults)

    return _create
