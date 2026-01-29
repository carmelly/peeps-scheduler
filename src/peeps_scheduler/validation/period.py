"""
Period data loading and orchestration.

Provides composable functions for loading, validating, and converting period data.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from pydantic import ValidationError
from peeps_scheduler import file_io
from peeps_scheduler.constants import DEFAULT_TIMEZONE
from peeps_scheduler.models import CancelledMemberAvailability, Event, PartnershipRequest, Peep
from peeps_scheduler.validation.builders import (
    build_attendance_events,
    build_cancelled_availability,
    build_cancelled_events,
    build_events,
    build_partnerships,
    build_peeps,
    build_results_events,
)
from peeps_scheduler.validation.errors import FileValidationError
from peeps_scheduler.validation.fields import ValidationContext
from peeps_scheduler.validation.file_schemas.period import PeriodFileSchema


@dataclass(frozen=True)
class PeriodData:
    """Everything needed to run the scheduler."""

    peeps: list[Peep]
    events: list[Event]
    results_events: list[Event] = ()
    attendance_events: list[Event] = ()
    cancelled_events: list[Event] = ()
    cancelled_member_availability: list[CancelledMemberAvailability] = ()
    partnership_requests: list[PartnershipRequest] = ()
    topics: list[str] = ()


def load_and_validate_period(
    period_path: str,
    year: int,
    allow_missing_responses: bool = False,
    require_attendance: bool = False,
) -> PeriodData:
    """
    Load, validate, and convert period data to domain objects.

    Single entry point for scheduler workflow. Composes all period loading steps.

    Args:
        period_path: Path to period directory
        year: Year for validation context

    Returns:
        PeriodData with all validated and converted components

    Raises:
        FileNotFoundError: If required files missing
        FileValidationError: If validation fails
    """
    raw = load_period_files(
        period_path,
        allow_missing_responses=allow_missing_responses,
        require_attendance=require_attendance,
    )
    ctx = ValidationContext(year=year, tz=DEFAULT_TIMEZONE)
    try:
        period_schema = PeriodFileSchema.model_validate(raw, context={"ctx": ctx})
    except ValidationError as exc:
        file_path = _infer_validation_file(exc, Path(period_path))
        raise FileValidationError(str(file_path), exc) from exc
    return to_period_data(period_schema, year)


def _infer_validation_file(error: ValidationError, period_dir: Path) -> Path:
    fields = set()
    for err in error.errors():
        loc = err.get("loc") or ()
        fields.add(loc[0] if loc else None)
    if len(fields) == 1:
        field = next(iter(fields))
        if field == "members":
            return period_dir / "members.csv"
        if field == "responses":
            return period_dir / "responses.csv"
        if field in {
            "cancelled_events",
            "cancelled_member_availability",
            "partnership_requests",
            "topics",
        }:
            return period_dir / "period_config.json"
    return period_dir / "period_config.json"


def load_period_files(
    period_path: str,
    allow_missing_responses: bool = False,
    require_attendance: bool = False,
) -> dict:
    """
    Load raw CSV/JSON files from period directory.

    Returns dict formatted for PeriodFileSchema validation.

    Raises:
        FileNotFoundError: If required files (members.csv, responses.csv) missing
    """
    period_dir = Path(period_path)

    # Define file paths
    members_file = period_dir / "members.csv"
    responses_file = period_dir / "responses.csv"
    results_file = period_dir / "results.json"
    attendance_file = period_dir / "actual_attendance.json"
    period_config_file = period_dir / "period_config.json"

    # Check required files exist
    if not members_file.is_file():
        raise FileNotFoundError(f"Required file not found: {members_file}")
    if not responses_file.is_file() and not allow_missing_responses:
        raise FileNotFoundError(f"Required file not found: {responses_file}")
    if require_attendance and not attendance_file.is_file():
        raise FileNotFoundError(f"Required file not found: {attendance_file}")

    # Load required CSV files using file_io (gets normalization)
    member_rows = file_io.load_csv(str(members_file))
    response_rows = file_io.load_csv(str(responses_file)) if responses_file.is_file() else []

    event_rows = []
    response_data_rows = []
    for row in response_rows:
        name = (row.get("Name") or "").strip()
        if name.startswith("Event:"):
            event_rows.append({**row, "Name": name.split("Event:", 1)[1].strip()})
        else:
            response_data_rows.append(row)

    # Load optional period_config.json (contains cancellations, partnerships, topics)
    period_config_data = {}
    if period_config_file.is_file():
        with period_config_file.open() as f:
            period_config_data = json.load(f)

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
    }

    if results_file.is_file():
        with results_file.open() as f:
            period_data["results"] = json.load(f)

    if attendance_file.is_file():
        with attendance_file.open() as f:
            period_data["attendance"] = json.load(f)

    return period_data


def to_period_data(period_schema: PeriodFileSchema, year: int) -> PeriodData:
    """
    Convert PeriodFileSchema to PeriodData domain object.

    Args:
        period_schema: Validated PeriodFileSchema object
        year: Year for context

    Returns:
        PeriodData with all components assembled
    """
    preserve_order = bool(period_schema.responses.event_rows)
    events = build_events(period_schema.responses.events, preserve_order)
    peeps = build_peeps(period_schema.members.root, period_schema.responses, events)
    results_events = build_results_events(period_schema.results, peeps)
    attendance_events = build_attendance_events(period_schema.attendance, peeps)
    cancelled_events = build_cancelled_events(period_schema.cancelled_events, events)
    cancelled_availability = build_cancelled_availability(
        period_schema.cancelled_member_availability, peeps, events
    )
    partnership_requests = build_partnerships(period_schema.partnership_requests, peeps)

    return PeriodData(
        peeps=peeps,
        events=events,
        results_events=results_events,
        attendance_events=attendance_events,
        cancelled_events=cancelled_events,
        cancelled_member_availability=cancelled_availability,
        partnership_requests=partnership_requests,
        topics=period_schema.topics,
    )
