import pytest
from pydantic import ValidationError
from tests.adapters.file.validation.conftest import assert_error_for_field, assert_error_for_model
from peeps_scheduler.adapters.file.validation.file_schemas.period_config import (
    CancelledAvailabilityJsonSchema,
    PartnershipRequestJsonSchema,
    PeriodConfigJsonSchema,
)

pytestmark = pytest.mark.unit


class TestPartnershipRequestJsonSchema:
    """Tests for PartnershipRequestJsonSchema"""

    def test_valid_partnership_request(self):
        """Happy path: Valid partnership with requester and target emails."""
        data = {
            "requester_email": "alice@test.com",
            "target_emails": ["bob@test.com", "charlie@test.com"],
        }
        schema = PartnershipRequestJsonSchema.model_validate(data)

        assert schema.requester_email == "alice@test.com"
        assert len(schema.target_emails) == 2
        assert "bob@test.com" in schema.target_emails

    def test_valid_partnership_single_target(self):
        """Edge case: Single target email."""
        data = {
            "requester_email": "alice@test.com",
            "target_emails": ["bob@test.com"],
        }
        schema = PartnershipRequestJsonSchema.model_validate(data)

        assert schema.requester_email == "alice@test.com"
        assert schema.target_emails == ["bob@test.com"]

    def test_valid_partnership_multiple_targets(self):
        """Edge case: Multiple target emails."""
        data = {
            "requester_email": "alice@test.com",
            "target_emails": ["bob@test.com", "charlie@test.com"],
        }
        schema = PartnershipRequestJsonSchema.model_validate(data)

        assert schema.requester_email == "alice@test.com"
        assert len(schema.target_emails) == 2
        assert "bob@test.com" in schema.target_emails
        assert "charlie@test.com" in schema.target_emails

    def test_partnership_no_targets_raises(self):
        """Error case: No target emails provided."""
        data = {
            "requester_email": "alice@test.com",
            "target_emails": [],
        }
        with pytest.raises(ValidationError) as e:
            PartnershipRequestJsonSchema.model_validate(data)

        assert_error_for_model(e.value.errors(), "target_emails")

    def test_invalid_requester_email_raises(self):
        """Error case: Invalid requester email format."""
        data = {
            "requester_email": "not-an-email",
            "target_emails": ["bob@test.com"],
        }
        with pytest.raises(ValidationError) as e:
            PartnershipRequestJsonSchema.model_validate(data)

        assert_error_for_model(e.value.errors(), "valid email")

    def test_invalid_target_email_raises(self):
        """Error case: Invalid target email format."""
        data = {
            "requester_email": "alice@test.com",
            "target_emails": ["bob@test.com", "not-an-email"],
        }
        with pytest.raises(ValidationError) as e:
            PartnershipRequestJsonSchema.model_validate(data)

        assert_error_for_model(e.value.errors(), "valid email")

    def test_requester_in_targets_raises(self):
        """Error case: Requester cannot be in target_emails."""
        data = {
            "requester_email": "alice@test.com",
            "target_emails": ["alice@test.com", "bob@test.com"],
        }
        with pytest.raises(ValidationError) as e:
            PartnershipRequestJsonSchema.model_validate(data)

        assert_error_for_model(e.value.errors(), "requester")

    def test_missing_requester_email_raises(self):
        """Error case: Missing requester_email field."""
        data = {
            "target_emails": ["bob@test.com"],
        }
        with pytest.raises(ValidationError) as e:
            PartnershipRequestJsonSchema.model_validate(data)

        assert_error_for_model(e.value.errors(), "Field required")

    def test_missing_target_emails_raises(self):
        """Error case: Missing target_emails field."""
        data = {
            "requester_email": "alice@test.com",
        }
        with pytest.raises(ValidationError) as e:
            PartnershipRequestJsonSchema.model_validate(data)

        assert_error_for_model(e.value.errors(), "Field required")


class TestCancelledAvailabilityJsonSchema:
    """Tests for CancelledAvailabilityJsonSchema (email-based, new format)."""

    def test_valid_cancelled_availability(self, ctx):
        """Happy path: Valid cancelled availability with email and events."""
        data = {
            "member_email": "alice@test.com",
            "events": ["Saturday January 4 - 1pm", "Friday January 10th - 3pm"],
        }
        schema = CancelledAvailabilityJsonSchema.model_validate(data, context={"ctx": ctx})

        assert schema.member_email == "alice@test.com"
        assert len(schema.events) == 2
        assert all(hasattr(e, "start") for e in schema.events)

    def test_valid_cancelled_availability_single_event(self, ctx):
        """Edge case: Single cancelled event."""
        data = {
            "member_email": "bob@test.com",
            "events": ["Saturday January 4 - 1pm"],
        }
        schema = CancelledAvailabilityJsonSchema.model_validate(data, context={"ctx": ctx})

        assert schema.member_email == "bob@test.com"
        assert len(schema.events) == 1

    def test_empty_events_raises(self, ctx):
        """Error case: Empty events list."""
        data = {
            "member_email": "alice@test.com",
            "events": [],
        }
        with pytest.raises(ValidationError) as e:
            CancelledAvailabilityJsonSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_field(e.value.errors(), "events")


class TestPeriodConfigJsonSchema:
    """Tests for PeriodConfigJsonSchema"""

    def test_period_config_minimal_valid(self, ctx):
        data = {}

        schema = PeriodConfigJsonSchema.model_validate(data, context={"ctx": ctx})

        assert schema.cancelled_events == []
        assert schema.cancelled_member_availability == []
        assert schema.partnership_requests == []
        assert schema.topics == []

    def test_period_config_full_valid(self, ctx):
        data = {
            "cancelled_events": ["Saturday January 4 - 1pm"],
            "cancelled_member_availability": [
                {"member_email": "alice@test.com", "events": ["Sunday January 5 - 2pm"]}
            ],
            "partnership_requests": [
                {"requester_email": "alice@test.com", "target_emails": ["bob@test.com"]}
            ],
            "topics": ["Balance for Spins and Turns", "Angles for Shaping & Slotting"],
        }

        schema = PeriodConfigJsonSchema.model_validate(data, context={"ctx": ctx})

        assert len(schema.cancelled_events) == 1
        assert len(schema.cancelled_member_availability) == 1
        assert len(schema.partnership_requests) == 1
        assert len(schema.topics) == 2

    @pytest.mark.parametrize(
        "invalid_topics, error",
        [
            (["", "Valid Topic"], "blank"),  # Blank topic
            (
                ["   ", "Another Valid Topic"],
                "blank",
            ),  # Whitespace-only topic
            (
                ["  (extra info)  ", "Another Valid Topic"],
                "blank",
            ),  # Whitespace-only after normalization
            (["Duplicate Topic", "duplicate topic"], "duplicate"),  # Duplicates after normalization
            ([123, "Valid Topic"], "string"),  # Non-string topic
        ],
    )
    def test_period_config_invalid_topics_raises(self, ctx, invalid_topics, error):
        data = {
            "topics": invalid_topics,
        }
        with pytest.raises(ValidationError) as e:
            PeriodConfigJsonSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), error)
