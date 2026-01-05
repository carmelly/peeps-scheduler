import pytest
from pydantic import ValidationError
from peeps_scheduler.validation.file_schemas.partnerships_json import (
    PartnershipRequestJsonSchema,
    PartnershipsJsonSchema,
)
from tests.validation.conftest import assert_error_for_field, assert_error_for_model
from tests.validation.fixtures import partnerships_json_data


@pytest.mark.unit
class TestPartnershipRequestJsonSchema:
    def test_valid_defaults(self):
        schema = PartnershipRequestJsonSchema.model_validate({"1": [2, 3]})

        assert isinstance(schema.requester_id, int)
        assert schema.requester_id == 1

        assert isinstance(schema.target_ids, list)
        assert schema.target_ids == [2, 3]
        assert all(isinstance(x, int) for x in schema.target_ids)

    @pytest.mark.parametrize(
        "data, msg",
        [
            ("not a dict", "dictionary"),
            ({"abc": [1]}, "convertible to int"),
            ({"46": "not a list"}, "must be a list"),
        ],
    )
    def test_invalid_request_shape_raises(self, data, msg):
        with pytest.raises(ValidationError) as e:
            PartnershipRequestJsonSchema.model_validate(data)

        assert_error_for_model(e.value.errors(), msg)

    def test_duplicate_targets_raises(self):
        data = {"46": [43, 43]}

        with pytest.raises(ValidationError) as e:
            PartnershipRequestJsonSchema.model_validate(data)

        assert_error_for_field(e.value.errors(), "target_ids", "duplicate")

    def test_self_request_raises(self):
        data = {"19": [19]}

        with pytest.raises(ValidationError) as e:
            PartnershipRequestJsonSchema.model_validate(data)

        assert_error_for_field(e.value.errors(), "target_ids", "self")

    @pytest.mark.parametrize(
        "data",
        [
            {},  # no entries
            {"19": [20], "20": [19]},  # more than one entry
        ],
    )
    def test_must_have_exactly_one_entry(self, data):
        with pytest.raises(ValidationError) as e:
            PartnershipRequestJsonSchema.model_validate(data)

        assert_error_for_model(e.value.errors(), "exactly one")


@pytest.mark.unit
class TestPartnershipsJsonSchema:
    """Tests for partnerships.json file-level schema."""

    def test_valid_partnerships_file(self):
        """Happy path: Valid partnerships.json with multiple partnerships."""
        schema = PartnershipsJsonSchema.model_validate(partnerships_json_data())

        assert isinstance(schema.partnerships, list)
        assert len(schema.partnerships) == 3
        assert all(isinstance(p, PartnershipRequestJsonSchema) for p in schema.partnerships)
        # Check first partnership (from fixture)
        assert schema.partnerships[0].requester_id == 19
        assert schema.partnerships[0].target_ids == [20]
        # Check last partnership (from fixture)
        assert schema.partnerships[2].requester_id == 46
        assert schema.partnerships[2].target_ids == [43, 31]

    def test_empty_partnerships_allowed(self):
        """Edge case: Empty partnerships dict is valid (no partnership requests)."""
        data = {}

        schema = PartnershipsJsonSchema.model_validate(data)

        assert isinstance(schema.partnerships, list)
        assert len(schema.partnerships) == 0

    @pytest.mark.parametrize(
        "data, msg",
        [
            ("not a dict", "must be a dictionary"),
            ([{"19": [20]}], "must be a dictionary"),
            (None, "must be a dictionary"),
            (42, "must be a dictionary"),
        ],
    )
    def test_invalid_partnerships_format_raises(self, data, msg):
        """Error case: Non-dict partnerships data should fail."""
        with pytest.raises(ValidationError) as e:
            PartnershipsJsonSchema.model_validate(data)

        assert_error_for_model(e.value.errors(), msg)

    @pytest.mark.skip(reason="Schema coerces duplicates instead of rejecting. Fix in Phase 2 (Issue #059)")
    def test_duplicate_requester_ids_raise(self):
        """Error case: Duplicate requester IDs should fail validation (future-proofing)."""
        # Note: Current JSON dict format prevents duplicate keys, but this tests
        # the defensive validator for when partnerships.json changes to list format.
        # See partnerships_json.py:100-105 TODO comment.
        from peeps_scheduler.validation.file_schemas.partnerships_json import (
            PartnershipRequestJsonSchema,
            PartnershipsJsonSchema,
        )

        # Create partnership list with duplicate requester_ids
        # (This would be possible with list format, not current dict format)
        partnership1 = PartnershipRequestJsonSchema.model_validate({"19": [20]})
        partnership2 = PartnershipRequestJsonSchema.model_validate({"19": [21]})

        # Using partnerships=[...] simulates future list-based JSON format
        with pytest.raises(ValueError) as exc_info:
            PartnershipsJsonSchema(partnerships=[partnership1, partnership2])

        assert "duplicate requester IDs" in str(exc_info.value)
