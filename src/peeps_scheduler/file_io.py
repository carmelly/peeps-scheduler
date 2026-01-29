import csv
import datetime
import json
import logging
from pathlib import Path
import peeps_scheduler.constants as constants
from peeps_scheduler.models import Peep

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


def load_csv(filename, required_columns=None):
    """Load CSV file and validate required columns, trimming whitespace from headers and values."""
    if required_columns is None:
        required_columns = []
    with Path(filename).open(newline="", encoding="utf-8") as csvfile:
        # Read the first line (fieldnames), trim whitespace
        reader = csv.reader(csvfile)
        try:
            raw_fieldnames = next(reader)
        except StopIteration:
            return []

        fieldnames = [name.strip() for name in raw_fieldnames]

        # Check required columns
        missing = set(required_columns) - set(fieldnames)
        if required_columns and missing:
            raise ValueError(f"missing required column(s): {missing}")

        # Rebuild DictReader with cleaned headers
        dict_reader = csv.DictReader(csvfile, fieldnames=fieldnames)
        rows = []

        def _normalize_text(s):
            # Replace smart quotes (\u2018, \u2019, \u201C, \u201D) with ASCII quotes
            s = (
                s.replace("\u2019", "'")
                .replace("\u2018", "'")
                .replace("\u201c", '"')
                .replace("\u201d", '"')
            )
            # Normalize multiple spaces to single space
            import re

            s = re.sub(r"\s+", " ", s)
            return s

        # Strip whitespace, normalize quotes and whitespace for every value
        for row in dict_reader:
            cleaned = {k: _normalize_text(v.strip()) if v else "" for k, v in row.items()}
            rows.append(cleaned)

        return rows


def save_peeps_csv(peeps: list[Peep], output_path: Path):
    """Save updated peeps to the provided output path."""
    output_path = Path(output_path)
    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=PEEPS_CSV_FIELDS)
        writer.writeheader()
        for peep in peeps:
            writer.writerow(peep.to_csv())
    logging.info(f"Updated peeps saved to {output_path}")


def save_json(data, filename):
    """Save data to a JSON file, handling Enums and datetime."""

    def custom_serializer(obj):
        if hasattr(obj, "value"):
            return obj.value
        if isinstance(obj, datetime.datetime):
            return obj.strftime(constants.DATE_FORMAT)
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        return str(obj)

    with Path(filename).open("w") as f:
        json.dump(data, f, indent=4, default=custom_serializer)
