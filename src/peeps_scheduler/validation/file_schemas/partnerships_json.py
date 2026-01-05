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


class PartnershipsJsonSchema(BaseModel):
    """Schema for validating partnerships.json file.

    Accepts a flat dict where each key is a requester_id (string) and each value
    is a list of target IDs. Converts to a list of PartnershipRequestJsonSchema objects.

    Example input: {"19": [20], "20": [19], "46": [43, 31]}
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    partnerships: list[PartnershipRequestJsonSchema] = Field(
        default_factory=list,
        description="List of partnership requests from members",
    )

    @model_validator(mode="before")
    @classmethod
    def convert_dict_to_list(cls, data):
        """Convert flat dict format to list of partnership dicts.

        Transforms: {"19": [20], "20": [19]}
        Into: {"partnerships": [{"19": [20]}, {"20": [19]}]}

        This allows each entry to be validated as a PartnershipRequestJsonSchema.
        """
        if not isinstance(data, dict):
            raise ValueError("partnerships.json must be a dictionary")

        # Convert each key-value pair to a separate dict for PartnershipRequestJsonSchema
        partnership_list = [{k: v} for k, v in data.items()]
        return {"partnerships": partnership_list}

    @model_validator(mode="after")
    def validate_no_duplicate_requesters(self):
        """Ensure no duplicate requester IDs across all partnerships.

        NOTE: This validator currently does NOT protect against duplicate keys in the
        partnerships.json file. When JSON with duplicate keys is parsed (e.g.,
        {"19": [20], "19": [25]}), Python keeps only the last value, so duplicates are
        lost before this validator runs.

        This validator is kept for future use: if partnerships.json structure changes
        to a list format (e.g., [{"requester_id": 19, "target_ids": [20]}, ...]),
        this validator will catch true duplicates in the file.

        TODO: Consider changing partnerships.json structure from flat dict to list
        to enable proper duplicate detection.
        """
        requester_ids = [p.requester_id for p in self.partnerships]
        if len(requester_ids) != len(set(requester_ids)):
            raise ValueError("partnerships must not contain duplicate requester IDs")
        return self
