import csv
import datetime
import json
import logging
from pathlib import Path
from peeps_scheduler import constants
from peeps_scheduler.adapters.protocols import PeriodSaver
from peeps_scheduler.models import EventSequence, Role

PEEPS_CSV_FIELDS = [
    "id",
    "Name",
    "Display Name",
    "Email Address",
    "Role",
    "Index",
    "Priority",
    "Total Attended",
    "Active",
    "Date Joined",
]


def _sequence_to_dict(sequence: EventSequence) -> dict:
    """Convert EventSequence to a serializable dictionary."""
    return {
        "valid_events": [
            {
                "id": event.id,
                "date": event.date.strftime(constants.DATE_FORMAT),
                "duration_minutes": event.duration_minutes,
                "attendees": [
                    {
                        "id": peep.id,
                        "name": peep.name,
                        "role": (
                            Role.LEADER.value if peep in event._leaders else Role.FOLLOWER.value
                        ),
                    }
                    for peep in event.attendees
                ],
                "alternates": [
                    {"id": peep.id, "name": peep.name, "role": Role.LEADER.value}
                    for peep in event.alt_leaders
                ]
                + [
                    {"id": peep.id, "name": peep.name, "role": Role.FOLLOWER.value}
                    for peep in event.alt_followers
                ],
                "leaders_string": event.get_participants_str(Role.LEADER),
                "followers_string": event.get_participants_str(Role.FOLLOWER),
                **({"topic": event.topic} if event.topic is not None else {}),
            }
            for event in sequence.valid_events
        ],
        "num_unique_attendees": sequence.num_unique_attendees,
        "priority_fulfilled": sequence.priority_fulfilled,
        "partnerships_fulfilled": sequence.partnerships_fulfilled,
        "mutual_unique_fulfilled": sequence.mutual_unique_fulfilled,
        "mutual_repeat_fulfilled": sequence.mutual_repeat_fulfilled,
        "one_sided_fulfilled": sequence.one_sided_fulfilled,
        "system_weight": sequence.system_weight,
    }


class FilePeriodSaver(PeriodSaver):
    def __init__(
        self,
        base_path: Path,
    ) -> None:
        self.base_path = base_path

    def _custom_json_serializer(self, obj):
        if hasattr(obj, "value"):
            return obj.value
        if isinstance(obj, datetime.datetime):
            return obj.strftime(constants.DATE_FORMAT)
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        return str(obj)

    def _save_json(self, data, output_path: Path):
        with output_path.open("w") as f:
            json.dump(data, f, indent=4, default=self._custom_json_serializer)

    def save_results(self, period_slug: str, sequence: EventSequence):
        """Save sequence to results.json in the period folder."""
        filename = self.base_path / period_slug / "results.json"

        sequence_dict = _sequence_to_dict(sequence)
        self._save_json(sequence_dict, filename)

    def save_members(self, period_slug: str, peeps):
        """Save updated peeps to members_updated.csv in the period folder."""
        output_path = self.base_path / period_slug / "members_updated.csv"
        with output_path.open("w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=PEEPS_CSV_FIELDS)
            writer.writeheader()
            for peep in peeps:
                peep_data = {
                    "id": peep.id,
                    "Name": peep.full_name,
                    "Display Name": peep.display_name,
                    "Email Address": peep.email,
                    "Role": peep.role.value,
                    "Index": peep.index,
                    "Priority": peep.priority,
                    "Total Attended": peep.total_attended,
                    "Active": str(peep.active).upper(),
                    "Date Joined": peep.date_joined,
                }
                writer.writerow(peep_data)
        logging.info(f"Updated members saved to {output_path}")
