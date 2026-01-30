from peeps_scheduler.adapters.protocols import PeriodSaver
from peeps_scheduler.models import EventSequence, Peep


class FilePeriodSaver(PeriodSaver):
    def save_results(self, period_slug: str, sequence: EventSequence) -> None:
        raise NotImplementedError

    def save_members(self, period_slug: str, peeps: list[Peep]) -> None:
        raise NotImplementedError
