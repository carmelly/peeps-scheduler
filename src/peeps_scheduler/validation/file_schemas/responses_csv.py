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
    PersonNameStr,
    RoleEnum,
)
from peeps_scheduler.validation.helpers import normalize_email_for_match, validate_unique
from peeps_scheduler.validation.parsers import parse_event_name


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
    display_name: PersonNameStr | None = Field(alias="Display Name", default=None)
    secondary_role: SwitchPreference | None = Field(alias="Secondary Role", default=None)
    availability: EventSpecList = Field(alias="Availability")

    @field_validator("display_name", mode="before")
    @classmethod
    def coerce_empty_display_name_to_none(cls, v):
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return v

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
        return SwitchPreference.from_string(v)  # will raise ValueError if invalid

    @field_validator("availability", mode="after")
    @classmethod
    def validate_consistent_format(cls, v: EventSpecList):
        """Availability strings must all either include duration or not"""
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

    @model_validator(mode="after")
    def validate_consistency(self):
        emails = [
            normalize_email_for_match(row.email_address)
            for row in self.responses
            if row.email_address
        ]
        validate_unique(emails, msg="duplicate email")

        if self.event_rows:
            starts = [row.start_dt for row in self.event_rows if row.start_dt]
            validate_unique(starts, msg="duplicate event start")

            for response in self.responses:
                for parsed in response.availability:
                    if parsed.duration_minutes is not None:
                        raise ValueError(
                            "availability must use old format when event rows exist"
                        )

            event_row_starts = {row.start_dt for row in self.event_rows if row.start_dt}
            unknown_availability = []
            for response in self.responses:
                for parsed in response.availability:
                    if parsed.start not in event_row_starts:
                        unknown_availability.append(parsed.raw)

            if unknown_availability:
                raise ValueError("availability includes event not in event rows")

        return self
