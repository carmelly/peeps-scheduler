"""
Period data loading and orchestration.

Provides composable functions for loading, validating, and converting period data.
"""

from pathlib import Path
from pydantic import ValidationError
from peeps_scheduler.constants import DEFAULT_TIMEZONE
from peeps_scheduler.models import PeriodData
from .errors import FileValidationError
from .fields import ValidationContext
from .file_schemas.period import PeriodFileSchema


def _infer_validation_file(error: ValidationError) -> str:
    fields = set()
    for err in error.errors():
        loc = err.get("loc") or ()
        fields.add(loc[0] if loc else None)
    if len(fields) == 1:
        field = next(iter(fields))
        if field == "members":
            return "members.csv"
        if field == "responses":
            return "responses.csv"
        if field in {
            "cancelled_events",
            "cancelled_member_availability",
            "partnership_requests",
            "topics",
        }:
            return "period_config.json"


def validate_period_data(
    raw_data: dict,
    year: int,
) -> PeriodFileSchema:
    """
    Validate raw period data and return validated schema.

    Args:
        raw_data: Raw period data as dict
        year: Year for validation context
    Returns:
        Validated PeriodFileSchema
    """
    ctx = ValidationContext(year=year, tz=DEFAULT_TIMEZONE)
    try:
        period_schema = PeriodFileSchema.model_validate(raw_data, context={"ctx": ctx})
    except ValidationError as exc:
        filename = _infer_validation_file(exc)
        raise FileValidationError(filename, exc) from exc
    return period_schema


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
    from peeps_scheduler.adapters.file.loader import FilePeriodLoader

    loader = FilePeriodLoader(
        Path(period_path).parent, year, not allow_missing_responses, require_attendance
    )
    period_slug = Path(period_path).name
    return loader.load_period(period_slug)
