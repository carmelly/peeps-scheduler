import unicodedata
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated
from pydantic import AfterValidator, BeforeValidator, EmailStr, PositiveInt, StringConstraints
from peeps_scheduler.constants import CLASS_CONFIG
from peeps_scheduler.models import Role
from peeps_scheduler.validation.helpers import validate_unique
from peeps_scheduler.validation.parsers import (
    EventSpec,
    parse_event_datetime,
    parse_event_name,
)

MAX_PERSON_NAME_LENGTH = 100
MAX_EMAIL_LENGTH = 254


@dataclass(frozen=True)
class ValidationContext:
    """Data class representing the context for validation."""

    year: int
    tz: datetime.tzinfo


def require_context(v, info):
    ctx = (info.context or {}).get("ctx")
    if ctx is None:
        raise ValueError("validation context is required")
    if not isinstance(ctx, ValidationContext):
        raise ValueError("invalid validation context")
    return v


def validate_event_datetime(v, info):
    ctx = info.context["ctx"]
    return parse_event_datetime(v, ctx.tz)


def validate_event_name_old_format(v, info):
    ctx = info.context["ctx"]
    parsed = parse_event_name(v, ctx.year, ctx.tz)
    if parsed.duration_minutes is not None:
        raise ValueError("invalid event name format")
    return v


def validate_and_parse_events(v, info):
    """Coerce availability input and parse it into EventSpec entries."""

    def _coerce_event_input(v) -> list[str]:
        if v is None or (isinstance(v, str) and not v.strip()):
            return []

        if isinstance(v, list) and all(isinstance(x, str) for x in v):
            return v

        if isinstance(v, str):
            parts = [p.strip() for p in v.split(",")]
            return [p.strip() for p in parts]
        raise ValueError("must be a list of event names or a comma-separated string")

    def _parse_event_names(names: list[str], ctx) -> list[EventSpec]:
        # Parse strings -> EventSpec (raises on bad format)
        return [parse_event_name(s, ctx.year, ctx.tz) for s in names]

    ctx = info.context["ctx"]
    events_list = _coerce_event_input(v)
    parsed_events = _parse_event_names(events_list, ctx)
    starts = [event.start for event in parsed_events]
    validate_unique(starts, msg=f"duplicate events in {info.field_name}")
    return parsed_events


def validate_event_durations(v: list[EventSpec]):
    """Ensure parsed event durations align with CLASS_CONFIG."""
    for parsed_event in v:
        duration = parsed_event.duration_minutes
        if duration is not None and duration not in CLASS_CONFIG:
            raise ValueError(f"unsupported event duration: {duration!s}")
    return v


def validate_role(v):
    """Parse role value into Role enum, rejecting empty strings."""
    if isinstance(v, str) and not v.strip():
        raise ValueError("Role must not be empty")
    return Role.from_string(v)  # will raise ValueError if invalid


def validate_person_name(v):
    """Validate person name characters and non-empty input."""

    def _is_letter(char: str) -> bool:
        return unicodedata.category(char).startswith("L")

    if not v.strip():
        raise ValueError("must not be empty")

    for ch in v:
        if _is_letter(ch):
            continue
        if ch in {" ", "-", "'", "."}:
            continue
        raise ValueError("must contain only letters, spaces, hyphens, apostrophes, or periods")
    return v


def validate_duration_minutes(v: int) -> int:
    """Ensure duration minutes matches configured class durations."""
    if v not in CLASS_CONFIG:
        raise ValueError(f"unsupported event duration: {v!s}")
    return v


PersonNameStr = Annotated[
    str,
    StringConstraints(max_length=MAX_PERSON_NAME_LENGTH),
    AfterValidator(validate_person_name),
]
EmailAddressStr = Annotated[EmailStr, StringConstraints(max_length=MAX_EMAIL_LENGTH)]
EventNameOldFormatStr = Annotated[
    str,
    BeforeValidator(require_context),
    AfterValidator(validate_event_name_old_format),
]
EventSpecList = Annotated[
    list[EventSpec],
    BeforeValidator(validate_and_parse_events),
    BeforeValidator(require_context),
    AfterValidator(validate_event_durations),
]
RoleEnum = Annotated[Role, BeforeValidator(validate_role)]
EventDateTime = Annotated[
    datetime,
    BeforeValidator(validate_event_datetime),
    BeforeValidator(require_context),
]
EventDuration = Annotated[PositiveInt, AfterValidator(validate_duration_minutes)]
