from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveInt,
    field_validator,
    model_validator,
)
from peeps_scheduler.validation.fields import (
    EventDateTime,
    EventDuration,
    PersonNameStr,
    RoleEnum,
)
from peeps_scheduler.validation.helpers import validate_unique


class RosterEntryJsonSchema(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="ignore")

    id: PositiveInt = Field(alias="id")
    name: PersonNameStr = Field(alias="name")
    role: RoleEnum = Field(alias="role")
    index_order: NonNegativeInt = Field(default=0, alias="index_order")


class BaseEventJsonSchema(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="ignore")

    legacy_id: NonNegativeInt = Field(alias="id")
    start_dt: EventDateTime = Field(alias="date")
    duration_minutes: EventDuration = Field(alias="duration_minutes")
    attendees: list[RosterEntryJsonSchema] = Field(alias="attendees")

    @field_validator("attendees", mode="after")
    @classmethod
    def set_attendee_index_order(cls, v: list[RosterEntryJsonSchema]):
        if not v:
            raise ValueError("attendees must not be empty")
        attendee_ids = [entry.id for entry in v]
        validate_unique(attendee_ids, msg="duplicate attendee id")
        for idx, entry in enumerate(v):
            entry.index_order = idx
        return v


class AttendanceEventJsonSchema(BaseEventJsonSchema):
    pass


class ActualAttendanceJsonSchema(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="ignore")

    valid_events: list[AttendanceEventJsonSchema] = Field(alias="valid_events")

    @model_validator(mode="after")
    def validate_unique_events(self):
        starts = [event.start_dt for event in self.valid_events]
        validate_unique(starts, msg="duplicate event start")
        legacy_ids = [event.legacy_id for event in self.valid_events]
        validate_unique(legacy_ids, msg="duplicate legacy id")
        return self
