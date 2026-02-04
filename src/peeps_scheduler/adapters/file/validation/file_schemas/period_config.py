"""Schemas for period configuration (JSON files)."""

from pydantic import BaseModel, ConfigDict, field_validator
from ..fields import EmailAddressStr, EventSpecList
from ..helpers import normalize_topic


class CancelledAvailabilityJsonSchema(BaseModel):
    """Schema for member's cancelled availability (email-based)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    member_email: EmailAddressStr
    events: EventSpecList

    @field_validator("events", mode="after")
    @classmethod
    def validate_non_empty_events(cls, v):
        """Ensure events is a non-empty list."""
        if not v:
            raise ValueError("events cannot be empty")
        return v


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

    @field_validator("target_emails", mode="after")
    @classmethod
    def validate_non_empty_target_emails(cls, v):
        """Ensure target_emails is a non-empty list."""
        if not v:
            raise ValueError("target_emails cannot be empty")
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
