from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from peeps_scheduler.adapters.file.validation.fields import EmailAddressStr, EventSpecList
from peeps_scheduler.adapters.file.validation.helpers import normalize_topic


class CancelledAvailabilityJsonSchema(BaseModel):
    """Schema for member's cancelled availability (email-based)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    member_email: EmailAddressStr
    events: EventSpecList

    @model_validator(mode="before")
    @classmethod
    def check_required_fields(cls, data):
        """Check that required fields are present."""
        if not isinstance(data, dict):
            return data

        if "member_email" not in data:
            raise ValueError("member_email is required")
        if "events" not in data:
            raise ValueError("events is required")

        return data


class PartnershipRequestJsonSchema(BaseModel):
    """Schema for individual partnership request (email-based)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    requester_email: EmailAddressStr
    target_emails: list[EmailAddressStr]

    @field_validator("target_emails", mode="after")
    @classmethod
    def validate_no_self_partnership(cls, v, info):
        """Ensure requester_email is NOT in target_emails."""
        if info.data.get("requester_email") in v:
            raise ValueError("requester cannot be in target_emails")
        return v


class PeriodConfigJsonSchema(BaseModel):
    """Schema for period_config.json - top level container."""

    model_config = ConfigDict(extra="ignore")

    cancelled_events: EventSpecList = []
    cancelled_member_availability: list[CancelledAvailabilityJsonSchema] = []
    partnership_requests: list[PartnershipRequestJsonSchema] = []
    topics: list[str] = []

    @field_validator("topics", mode="after")
    @classmethod
    def validate_unique_topics(cls, v):
        """Ensure topics are non-empty strings with no normalized duplicates."""
        normalized_topics = []
        for topic in v:
            if not isinstance(topic, str):
                raise ValueError("topics must be strings")
            normalized = normalize_topic(topic)
            if not normalized:
                raise ValueError("topics cannot be blank")
            normalized_topics.append(normalized)
        if len(normalized_topics) != len(set(normalized_topics)):
            raise ValueError("duplicate topics detected")
        return v
