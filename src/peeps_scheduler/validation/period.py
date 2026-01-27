"""
Period data loading and orchestration.

Provides composable functions for loading, validating, and converting period data.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from peeps_scheduler import file_io
from peeps_scheduler.constants import DEFAULT_TIMEZONE
from peeps_scheduler.models import CancelledMemberAvailability, Event, PartnershipRequest, Peep
from peeps_scheduler.validation.builders import (
    build_cancelled_availability,
    build_cancelled_events,
    build_events,
    build_partnerships,
    build_peeps,
)
from peeps_scheduler.validation.fields import ValidationContext
from peeps_scheduler.validation.file_schemas.period import (
    PeriodFileSchema,
)


@dataclass(frozen=True)
class PeriodData:
    """Everything needed to run the scheduler."""

    peeps: list[Peep]
    events: list[Event]
    cancelled_events: list[Event] = ()
    cancelled_member_availability: list[CancelledMemberAvailability] = ()
    partnership_requests: list[PartnershipRequest] = ()
    topics: list[str] = ()


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
    ctx = ValidationContext(year=year, tz=DEFAULT_TIMEZONE)
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
    period_config_file = period_dir / "period_config.json"

    # Check required files exist
    if not members_file.is_file():
        raise FileNotFoundError(f"Required file not found: {members_file}")
    if not responses_file.is_file():
        raise FileNotFoundError(f"Required file not found: {responses_file}")

    # Load required CSV files using file_io (gets normalization)
    member_rows = file_io.load_csv(str(members_file))
    response_rows = file_io.load_csv(str(responses_file))

    # Load optional period_config.json (contains cancellations, partnerships, topics)
    period_config_data = {}
    if period_config_file.is_file():
        with period_config_file.open() as f:
            period_config_data = json.load(f)

    return {
        "members": member_rows,
        "responses": {
            "responses": response_rows,
            "event_rows": None,
        },
        "cancelled_events": period_config_data.get("cancelled_events", []),
        "cancelled_member_availability": period_config_data.get(
            "cancelled_member_availability", []
        ),
        "partnership_requests": period_config_data.get("partnership_requests", []),
        "topics": period_config_data.get("topics", []),
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
    peeps = build_peeps(period_schema.members.root, period_schema.responses)
    events = build_events(period_schema.responses.events)
    cancelled_events = build_cancelled_events(period_schema.cancelled_events, events)
    cancelled_availability = build_cancelled_availability(
        period_schema.cancelled_member_availability, peeps, events
    )
    partnership_requests = build_partnerships(period_schema.partnership_requests, peeps)

    return PeriodData(
        peeps=peeps,
        events=events,
        cancelled_events=cancelled_events,
        cancelled_member_availability=cancelled_availability,
        partnership_requests=partnership_requests,
        topics=period_schema.topics,
    )
