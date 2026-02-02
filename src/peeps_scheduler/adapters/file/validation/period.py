"""
Period data loading and orchestration.

Provides composable functions for loading, validating, and converting period data.
"""

from pydantic import ValidationError
from peeps_scheduler.constants import DEFAULT_TIMEZONE
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
