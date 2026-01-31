import logging
from pathlib import Path
from peeps_scheduler.models import Peep


def load_csv(filename: str) -> list[dict]:
    from peeps_scheduler.adapters.file.loader import _load_csv_rows

    return _load_csv_rows(Path(filename))


def save_peeps_csv(peeps: list[Peep], output_path: Path):
    """Save updated peeps to the provided output path."""
    from peeps_scheduler.adapters.file.saver import FilePeriodSaver

    base_path = output_path.parent.parent
    period_slug = output_path.parent.name
    saver = FilePeriodSaver(base_path)
    saver.save_members(period_slug, peeps)
    logging.info(f"Updated peeps saved to {output_path}")


def save_json(data, output_path: Path):
    """Save data to a JSON file, handling Enums and datetime."""
    from peeps_scheduler.adapters.file.saver import FilePeriodSaver

    saver = FilePeriodSaver(output_path.parent)
    saver._save_json(data, output_path)
    logging.info(f"Data saved to {output_path}")
