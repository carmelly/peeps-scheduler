from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from peeps_scheduler.validation.file_schemas.attendance_json import (
    BaseEventJsonSchema,
    RosterEntryJsonSchema,
)
from peeps_scheduler.validation.helpers import validate_unique


class ResultEventJsonSchema(BaseEventJsonSchema):
    alternates: list[RosterEntryJsonSchema] = Field(alias="alternates")

    @field_validator("alternates", mode="after")
    @classmethod
    def set_alternate_index_order(cls, v: list[RosterEntryJsonSchema]):
        alternate_ids = [entry.id for entry in v]
        validate_unique(alternate_ids, msg="duplicate alternate id")
        for idx, entry in enumerate(v):
            entry.index_order = idx
        return v

    @model_validator(mode="after")
    def validate_no_overlap(self):
        attendee_ids = {entry.id for entry in self.attendees}
        alternate_ids = {entry.id for entry in self.alternates}
        if attendee_ids & alternate_ids:
            raise ValueError("attendees and alternates overlap")
        return self


class ResultsJsonSchema(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="ignore")

    valid_events: list[ResultEventJsonSchema] = Field(alias="valid_events")

    @model_validator(mode="after")
    def validate_unique_events(self):
        starts = [event.start_dt for event in self.valid_events]
        validate_unique(starts, msg="duplicate event start")
        legacy_ids = [event.legacy_id for event in self.valid_events]
        validate_unique(legacy_ids, msg="duplicate legacy id")
        return self
