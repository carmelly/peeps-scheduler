import csv
import json
import re
from datetime import date
from pathlib import Path
from peeps_scheduler.adapters.file.mappers import map_period
from peeps_scheduler.adapters.file.validation.period import validate_period_data
from peeps_scheduler.adapters.protocols import PeriodLoader
from peeps_scheduler.constants import PRIVATE_DATA_ROOT
from peeps_scheduler.models import PeriodData

DEFAULT_BASE_PATH = Path(PRIVATE_DATA_ROOT) / "original"
DEFAULT_SCHEDULER_YEAR = date.today().year

CsvRow = dict[str, str]  # Type alias for a CSV row represented as a dictionary


def _normalize_text(value: str | None) -> str:
    if not value:  # None or empty string
        return ""

    # Strip whitespace and replace smart quotes (\u2018, \u2019, \u201C, \u201D) with ASCII quotes
    value = (
        value.strip()
        .replace("\u2019", "'")
        .replace("\u2018", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
    )

    # Normalize multiple spaces to single space
    value = re.sub(r"\s+", " ", value)
    return value


def _split_response_rows(
    rows: list[CsvRow],
) -> tuple[list[CsvRow], list[CsvRow]]:
    """Split response rows into event rows and response data rows."""
    event_rows = []
    response_data_rows = []
    for row in rows:
        name_value = (row.get("Name") or "").strip()
        if name_value.startswith("Event:"):
            event_rows.append({**row, "Name": name_value.split("Event:", 1)[1].strip()})
        else:
            response_data_rows.append(row)
    return event_rows, response_data_rows


def _load_csv_file(path: Path, required: bool = False) -> list[CsvRow]:
    """Load CSV file and normalize text in headers and values."""
    if not path.is_file():
        if required:
            raise FileNotFoundError(f"Required file not found: {path}")
        else:
            return []

    with path.open(newline="", encoding="utf-8") as csvfile:
        # Read the first line (fieldnames), trim whitespace
        reader = csv.DictReader(csvfile)

        # Strip whitespace from row headers
        reader.fieldnames = [name.strip() for name in reader.fieldnames or []]

        # Rebuild DictReader with cleaned headers
        rows = []

        # Strip whitespace, normalize quotes and whitespace for every value
        for row in reader:
            cleaned = {k: _normalize_text(v) for k, v in row.items()}
            rows.append(cleaned)

        return rows


def _load_json_file(path: Path, required: bool = False) -> dict:
    """Load JSON file and return as dictionary."""
    if not path.is_file():
        if required:
            raise FileNotFoundError(f"Required file not found: {path}")
        else:
            return {}
    with path.open(encoding="utf-8") as f:
        return json.load(f)


class FilePeriodLoader(PeriodLoader):
    def __init__(
        self,
        base_path: Path = DEFAULT_BASE_PATH,
        year: int = DEFAULT_SCHEDULER_YEAR,
        require_responses: bool = True,
        require_attendance: bool = False,
    ) -> None:
        self.base_path = base_path
        self.year = year
        self.require_responses = require_responses
        self.require_attendance = require_attendance

    def list_periods(self) -> list[str]:
        """List all available periods in the base path."""
        periods = []
        for item in self.base_path.iterdir():
            if item.is_dir():
                periods.append(item.name)

        return sorted(periods)

    def load_period(self, period_slug: str) -> PeriodData:
        period_path = self.base_path / period_slug

        # Load raw period data from files
        raw_data = self.load_period_files(
            period_path,
            require_responses=self.require_responses,
            require_attendance=self.require_attendance,
        )

        # Validate and convert to PeriodData
        period_schema = validate_period_data(raw_data, self.year)
        period_data = map_period(period_schema)
        return period_data

    def load_period_files(
        self,
        period_path: Path,
        require_responses: bool = True,
        require_attendance: bool = False,
    ) -> dict:
        """
        Load raw CSV/JSON files from period directory.

        Returns dict formatted for PeriodFileSchema validation.

        Raises:
            FileNotFoundError: If required files (members.csv, responses.csv) missing
        """
        # Load members.csv -- always required
        member_rows = _load_csv_file(period_path / "members.csv", required=True)

        # Load responses.csv if required and split into event rows and response data rows
        response_rows = _load_csv_file(period_path / "responses.csv", required=require_responses)
        event_rows, response_data_rows = _split_response_rows(response_rows)

        # Load optional period_config.json (contains cancellations, partnerships, topics)
        period_config_data = _load_json_file(period_path / "period_config.json")
        # Load results file if exists
        results_data = _load_json_file(period_path / "results.json")

        # Load attendance file if exists
        attendance_data = _load_json_file(
            period_path / "actual_attendance.json", required=require_attendance
        )

        period_data = {
            "members": member_rows,
            "responses": {
                "responses": response_data_rows,
                "event_rows": event_rows or None,
            },
            "cancelled_events": period_config_data.get("cancelled_events", []),
            "cancelled_member_availability": period_config_data.get(
                "cancelled_member_availability", []
            ),
            "partnership_requests": period_config_data.get("partnership_requests", []),
            "topics": period_config_data.get("topics", []),
            "results": results_data or None,
            "attendance": attendance_data or None,
        }

        return period_data


def load_period_from_files(
    period_path: Path,
    year: int,
    require_responses: bool = True,
    require_attendance: bool = False,
) -> PeriodData:
    """Load and validate period data from files in the given period path."""
    loader = FilePeriodLoader(
        base_path=period_path.parent,
        year=year,
        require_responses=require_responses,
        require_attendance=require_attendance,
    )
    period_data = loader.load_period(period_path.name)
    return period_data
