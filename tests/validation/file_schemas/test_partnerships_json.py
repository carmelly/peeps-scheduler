import pytest
from pydantic import ValidationError
from peeps_scheduler.validation.file_schemas.partnerships_json import PartnershipRequestJsonSchema
from tests.validation.conftest import assert_error_for_field, assert_error_for_model


@pytest.mark.unit
class TestPartnershipRequestJsonSchema:
    def test_valid_defaults(self):
        data = {"46": [43, 31]}
        schema = PartnershipRequestJsonSchema.model_validate(data)

        assert isinstance(schema.requester_id, int)
        assert schema.requester_id == 46

        assert isinstance(schema.target_ids, list)
        assert schema.target_ids == [43, 31]
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
