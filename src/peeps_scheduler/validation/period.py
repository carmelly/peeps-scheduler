"""
Period data loading and orchestration.

Provides composable functions for loading, validating, and converting period data.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from peeps_scheduler import file_io
from peeps_scheduler.constants import TIMEZONE
from peeps_scheduler.models import Event, Peep
from peeps_scheduler.validation.converters import (
    convert_to_events,
    convert_to_peeps,
    extract_cancellations,
    extract_partnerships,
    validate_cancellations,
    validate_members,
    validate_partnerships,
)
from peeps_scheduler.validation.errors import FileValidationError, MultiFileValidationError
from peeps_scheduler.validation.fields import ValidationContext


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
    validated = validate_period_data(raw, year)
    return to_period_data(validated, year)


def load_period_files(period_path: str) -> dict:
    """
    Load raw CSV/JSON files from period directory.

    Returns dict with keys: 'members', 'responses', 'cancellations', 'partnerships'
    Missing optional files (cancellations, partnerships) have None values.

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
            partnerships_data = json.load(f)

    return {
        "members": member_rows,
        "responses": response_rows,
        "cancellations": cancellations_data,
        "partnerships": partnerships_data,
    }


def validate_period_data(raw_data: dict, year: int) -> dict:
    """
    Validate all period data using validation wrappers.

    Args:
        raw_data: Dict from load_period_files() with keys: members, responses, cancellations, partnerships
        year: Year for validation context

    Returns:
        Dict with validated data (same keys, validated structures)

    Raises:
        FileValidationError: If validation fails in single file
        MultiFileValidationError: If validation fails in multiple files
    """
    # Create validation context
    ctx = ValidationContext(year=year, tz=TIMEZONE)

    # Collect errors from all files
    file_errors = []

    # Validate members
    validated_members = None
    try:
        validated_members = validate_members(raw_data["members"], "members.csv", context={"ctx": ctx})
    except FileValidationError as e:
        file_errors.append(e)

    # Validate responses
    validated_responses = None
    responses_data = {
        "responses": raw_data["responses"],
        "event_rows": None,
    }
    try:
        from peeps_scheduler.validation.converters import validate_responses
        validated_responses = validate_responses(responses_data, ctx, "responses.csv")
    except FileValidationError as e:
        file_errors.append(e)

    # Validate optional files
    validated_cancellations = None
    if raw_data["cancellations"] is not None:
        try:
            validated_cancellations = validate_cancellations(
                raw_data["cancellations"], ctx, "cancellations.json"
            )
        except FileValidationError as e:
            file_errors.append(e)

    validated_partnerships = None
    if raw_data["partnerships"] is not None:
        try:
            validated_partnerships = validate_partnerships(
                raw_data["partnerships"], "partnerships.json"
            )
        except FileValidationError as e:
            file_errors.append(e)

    # Raise appropriate error based on collected errors
    if len(file_errors) == 1:
        raise file_errors[0]
    elif len(file_errors) > 1:
        raise MultiFileValidationError(file_errors)

    return {
        "validated_members": validated_members,
        "validated_responses": validated_responses,
        "validated_cancellations": validated_cancellations,
        "validated_partnerships": validated_partnerships,
    }


def to_period_data(validated: dict, year: int) -> PeriodData:
    """
    Convert validated data to PeriodData domain object.

    Args:
        validated: Dict from validate_period_data()
        year: Year for context

    Returns:
        PeriodData with all components assembled
    """
    validated_members = validated["validated_members"]
    validated_responses = validated["validated_responses"]
    validated_cancellations = validated["validated_cancellations"]
    validated_partnerships = validated["validated_partnerships"]

    # Convert to peeps
    peeps = convert_to_peeps(validated_members, validated_responses)

    # Convert to events
    events = convert_to_events(validated_responses)

    # Extract cancellations
    cancelled_event_ids, cancelled_availability = extract_cancellations(validated_cancellations)

    # Extract partnerships
    partnerships = extract_partnerships(validated_partnerships)

    return PeriodData(
        peeps=peeps,
        events=events,
        cancelled_event_ids=cancelled_event_ids,
        cancelled_availability=cancelled_availability,
        partnerships=partnerships,
    )
