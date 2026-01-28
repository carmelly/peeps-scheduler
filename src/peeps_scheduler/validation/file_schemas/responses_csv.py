import re
from datetime import datetime
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveInt,
    field_validator,
    model_validator,
)
from peeps_scheduler.models import SwitchPreference
from peeps_scheduler.validation.fields import (
    EmailAddressStr,
    EventNameOldFormatStr,
    EventSpecList,
    OptionalPersonNameStr,
    PersonNameStr,
    RoleEnum,
)
from peeps_scheduler.validation.helpers import normalize_email_for_match, validate_unique
from peeps_scheduler.validation.parsers import EventSpec, parse_event_name, parse_switch_preference


def _strip_parenthetical(value: str) -> str:
    if not value:
        return ""
    return re.sub(r"\([^)]*\)", "", value)


def _normalize_topic(value: str) -> str:
    return " ".join(_strip_parenthetical(value).split()).strip()


class ResponseCsvRowSchema(BaseModel):
    """Schema for validating response rows in responses.csv."""

    model_config = ConfigDict(str_strip_whitespace=True)

    # Required fields
    full_name: PersonNameStr = Field(alias="Name")
    timestamp: datetime = Field(alias="Timestamp")
    email_address: EmailAddressStr = Field(alias="Email Address")
    primary_role: RoleEnum = Field(alias="Primary Role")
    max_sessions: NonNegativeInt = Field(alias="Max Sessions")
    min_interval_days: NonNegativeInt = Field(alias="Min Interval Days")

    # Optional fields
    display_name: OptionalPersonNameStr = Field(alias="Display Name", default=None)
    secondary_role: SwitchPreference | None = Field(alias="Secondary Role", default=None)
    availability: EventSpecList = Field(alias="Availability")
    deep_dive_topics: list[str] = Field(alias="Deep Dive Topics", default_factory=list)

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v):
        """Validate Timestamp: must be valid datetime string."""
        if not isinstance(v, str):
            raise ValueError("Timestamp must be a string")
        try:
            return datetime.strptime(v, "%m/%d/%Y %H:%M:%S")
        except ValueError as e:
            raise ValueError(f"Timestamp format not recognized: {v}") from e

    @field_validator("secondary_role", mode="before")
    @classmethod
    def validate_secondary_role(cls, v):
        """Validate Secondary Role: optional field for role preferences."""
        if v is None or v == "":
            return None
        if not isinstance(v, str):
            raise ValueError("Secondary Role must be a string")
        return parse_switch_preference(v)

    @field_validator("deep_dive_topics", mode="before")
    @classmethod
    def validate_deep_dive_topics(cls, v):
        """Parse optional deep dive topics from a comma-separated string."""
        if v is None:
            return []
        if isinstance(v, list):
            normalized = [_normalize_topic(str(item)) for item in v]
            return [item for item in normalized if item]
        if isinstance(v, str):
            if v.strip() == "":
                return []
            cleaned = _strip_parenthetical(v)
            parts = [part.strip() for part in cleaned.split(",")]
            normalized = [_normalize_topic(part) for part in parts]
            return [part for part in normalized if part]
        raise ValueError("Deep Dive Topics must be a comma-separated string")

    @field_validator("availability", mode="after")
    @classmethod
    def validate_consistent_format(cls, v: EventSpecList):
        """Availability strings must all either include duration or not"""
        if not v:
            # Empty availability is valid (member not available this period)
            return v
        has_duration_list = [event.duration_minutes is not None for event in v]
        if len(set(has_duration_list)) != 1:
            raise ValueError("format must match in Availability: all events must use same format")
        return v


class EventRowCsvSchema(BaseModel):
    """Schema for validating event header rows in responses.csv"""

    model_config = ConfigDict(str_strip_whitespace=True)

    name: EventNameOldFormatStr = Field(alias="Name")
    duration_minutes: PositiveInt = Field(alias="Event Duration")

    start_dt: datetime | None = None

    @model_validator(mode="after")
    def populate_start_dt(self, info):
        ctx = info.context["ctx"]
        parsed_event = parse_event_name(self.name, ctx.year, ctx.tz)
        self.start_dt = parsed_event.start
        return self


class ResponsesCsvFileSchema(BaseModel):
    responses: list[ResponseCsvRowSchema]
    event_rows: list[EventRowCsvSchema] | None = None
    events: list[EventSpec] = Field(default_factory=list)

    @field_validator("responses", mode="after")
    @classmethod
    def validate_unique_emails(cls, v):
        """Ensure all email addresses in responses are unique."""
        emails = [normalize_email_for_match(row.email_address) for row in v if row.email_address]
        validate_unique(emails, msg="duplicate email")
        return v

    @field_validator("event_rows", mode="after")
    @classmethod
    def validate_unique_event_rows(cls, v):
        """If event_rows exist, ensure all event starts are unique."""
        if v:
            starts = [row.start_dt for row in v if row.start_dt]
            validate_unique(starts, msg="duplicate event start")
        return v

    @model_validator(mode="after")
    def validate_availability_format(self):
        """If event_rows exist, ensure availability uses old format (no duration)."""
        if self.event_rows:
            for response in self.responses:
                for parsed in response.availability:
                    if parsed.duration_minutes is not None:
                        raise ValueError("availability must use old format when event rows exist")
        return self

    @model_validator(mode="after")
    def validate_events_exist_in_event_rows(self):
        """If event_rows exist, ensure all events in responses exist in event_rows."""
        if self.event_rows:
            event_row_starts = {row.start_dt for row in self.event_rows if row.start_dt}
            unknown_availability = []
            for response in self.responses:
                for parsed in response.availability:
                    if parsed.start not in event_row_starts:
                        unknown_availability.append(parsed.raw)

            if unknown_availability:
                raise ValueError("availability includes event not in event rows")

        return self

    @model_validator(mode="after")
    def extract_and_deduplicate_events(self):
        """Extract events from event_rows or response availability, deduplicate by start datetime."""
        # If event_rows exist, use those for events
        if self.event_rows:
            self.events = [
                EventSpec(
                    start=row.start_dt,
                    duration_minutes=row.duration_minutes,
                    raw=row.name,
                )
                for row in self.event_rows
            ]
        else:
            # Otherwise, collect from response availability and deduplicate by start datetime
            unique_events_map = {}  # Key: start datetime, Value: EventSpec
            for response in self.responses:
                for event_spec in response.availability:
                    event_start = event_spec.start
                    if event_start not in unique_events_map:
                        unique_events_map[event_start] = event_spec

            self.events = list(unique_events_map.values())

        return self
