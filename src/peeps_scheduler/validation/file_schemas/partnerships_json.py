from pydantic import BaseModel, ConfigDict, Field, PositiveInt, field_validator, model_validator


class PartnershipRequestJsonSchema(BaseModel):
    """Schema for validating a single partnership request.

    Accepts a dict with a single key-value pair where:
    - key (str): requester_id (will be converted to int)
    - value (list[int]): target_ids (list of partner IDs)
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    requester_id: PositiveInt = Field(alias="requester_id")
    target_ids: list[PositiveInt] = Field(alias="target_ids")

    @model_validator(mode="before")
    @classmethod
    def parse_request_dict(cls, data):
        """Parse dict input into requester_id and target_ids fields."""
        if not isinstance(data, dict):
            raise ValueError("request must be a dictionary")

        if len(data) != 1:
            raise ValueError("request must have exactly one entry")

        # Extract the single key-value pair
        requester_id_str, target_ids = next(iter(data.items()))

        try:
            requester_id = int(requester_id_str)
        except (ValueError, TypeError) as e:
            raise ValueError("requester_id must be convertible to int") from e

        if not isinstance(target_ids, list):
            raise ValueError("target_ids must be a list")

        return {"requester_id": requester_id, "target_ids": target_ids}

    @field_validator("target_ids", mode="after")
    @classmethod
    def validate_no_duplicates(cls, v):
        """Ensure no duplicate target IDs."""
        if len(v) != len(set(v)):
            raise ValueError("target_ids must not contain duplicate values")
        return v

    @field_validator("target_ids", mode="after")
    @classmethod
    def validate_no_self_request(cls, v, info):
        """Ensure requester_id is not in target_ids."""
        requester_id = info.data.get("requester_id")
        if requester_id in v:
            raise ValueError("target_ids must not contain self request")
        return v
