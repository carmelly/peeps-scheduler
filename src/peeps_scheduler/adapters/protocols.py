from typing import Protocol
from peeps_scheduler.models import EventSequence, Peep, PeriodData


class PeriodLoader(Protocol):
    def load_period(
        self,
        period_slug: str,
    ) -> PeriodData:
        """Load and build domain data for a period."""


class PeriodSaver(Protocol):
    def save_results(self, period_slug: str, sequence: EventSequence) -> None:
        """Persist a scheduled sequence."""

    def save_members(self, period_slug: str, peeps: list[Peep]) -> None:
        """Persist updated members after applying attendance."""
