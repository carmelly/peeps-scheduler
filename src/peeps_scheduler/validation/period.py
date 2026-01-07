"""
Period data loading and orchestration.

Provides composable functions for loading, validating, and converting period data.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from peeps_scheduler import file_io
from peeps_scheduler.constants import DEFAULT_EVENT_DURATION, TIMEZONE
from peeps_scheduler.models import Event, Peep
from peeps_scheduler.validation.converters import (
    convert_to_peeps,
)
from peeps_scheduler.validation.fields import ValidationContext
from peeps_scheduler.validation.file_schemas.period import PeriodFileSchema


@dataclass(frozen=True)
class PeriodData:
    """Everything needed to run the scheduler."""

    peeps: list[Peep]
    events: list[Event]
    cancelled_event_ids: set[str]
    cancelled_availability: dict[str, set[str]]  # email → event_ids
    partnerships: dict[int, set[int]]  # requester_id → partner_ids


def load_and_validate_period(period_path: str, year: int) -> PeriodData:
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
    raw = load_period_files(period_path)
    ctx = ValidationContext(year=year, tz=TIMEZONE)
    period_schema = PeriodFileSchema.model_validate(raw, context={"ctx": ctx})
    return to_period_data(period_schema, year)


def load_period_files(period_path: str) -> dict:
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
    cancellations_file = period_dir / "cancellations.json"
    partnerships_file = period_dir / "partnerships.json"

    # Check required files exist
    if not members_file.is_file():
        raise FileNotFoundError(f"Required file not found: {members_file}")
    if not responses_file.is_file():
        raise FileNotFoundError(f"Required file not found: {responses_file}")

    # Load required CSV files using file_io (gets normalization)
    member_rows = file_io.load_csv(str(members_file))
    response_rows = file_io.load_csv(str(responses_file))

    # Load optional JSON files (return None if missing)
    cancellations_data = None
    if cancellations_file.is_file():
        with cancellations_file.open() as f:
            cancellations_data = json.load(f)

    partnerships_data = None
    if partnerships_file.is_file():
        with partnerships_file.open() as f:
            partnerships_dict = json.load(f)
            # Convert flat dict to list of single-entry dicts for schema validation
            # {"1": [2, 3]} -> [{"1": [2, 3]}]
            partnerships_data = [{requester_id: partner_ids} for requester_id, partner_ids in partnerships_dict.items()]

    return {
        "members": member_rows,
        "responses": {
            "responses": response_rows,
            "event_rows": None,
        },
        "cancelled_events": cancellations_data,
        "partnerships": partnerships_data,
    }


def to_period_data(period_schema: PeriodFileSchema, year: int) -> PeriodData:
    """
    Convert PeriodFileSchema to PeriodData domain object.

    Args:
        period_schema: Validated PeriodFileSchema object
        year: Year for context

    Returns:
        PeriodData with all components assembled
    """
    # Extract members from schema
    members = period_schema.members.root

    # Convert to peeps
    peeps = convert_to_peeps(members, period_schema.responses)

    # Convert events from schema.responses.events (EventSpecs)
    events = [
        Event(date=spec.start, duration_minutes=spec.duration_minutes or DEFAULT_EVENT_DURATION)
        for spec in period_schema.responses.events
    ]

    # Extract cancellations
    if period_schema.cancelled_events:
        # Convert CancelledEventJsonSchema to CancellationsJsonSchema format
        cancelled_event_ids = set()
        for event in period_schema.cancelled_events.cancelled_events:
            start = event.start
            if start:
                event_id = start.strftime("%Y-%m-%d %H:%M")
                cancelled_event_ids.add(event_id)
    else:
        cancelled_event_ids = set()

    # Handle cancelled availability
    cancelled_availability = {}
    if period_schema.cancelled_availability:
        for avail in period_schema.cancelled_availability:
            email = avail.email
            if not email:
                continue
            events_set = set()
            for event in avail.events:
                start = event.start
                if start:
                    event_id = start.strftime("%Y-%m-%d %H:%M")
                    events_set.add(event_id)
            if events_set:
                cancelled_availability[email] = events_set

    # Extract partnerships
    partnerships = {}
    if period_schema.partnerships:
        for partnership in period_schema.partnerships:
            requester_id = partnership.requester_id
            target_ids = partnership.target_ids
            if requester_id:
                partnerships[requester_id] = set(target_ids) if target_ids else set()

    return PeriodData(
        peeps=peeps,
        events=events,
        cancelled_event_ids=cancelled_event_ids,
        cancelled_availability=cancelled_availability,
        partnerships=partnerships,
    )
