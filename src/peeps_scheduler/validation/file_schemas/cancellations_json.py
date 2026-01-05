from pydantic import BaseModel, ConfigDict
from peeps_scheduler.validation.fields import EmailAddressStr, EventSpecList


class CancelledEventJsonSchema(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    cancelled_events: EventSpecList


class CancelledAvailabilityJsonSchema(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    email: EmailAddressStr
    events: EventSpecList


class CancellationsJsonSchema(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    cancelled_events: EventSpecList
    cancelled_availability: list[CancelledAvailabilityJsonSchema]
