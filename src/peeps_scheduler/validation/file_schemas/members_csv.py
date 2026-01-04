from datetime import date, datetime
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveInt,
    RootModel,
    field_validator,
    model_validator,
)
from peeps_scheduler.validation.fields import EmailAddressStr, PersonNameStr, RoleEnum
from peeps_scheduler.validation.helpers import normalize_email_for_match, validate_unique


class MemberCsvRowSchema(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    id: PositiveInt = Field(alias="id")
    full_name: PersonNameStr = Field(alias="Name")
    display_name: PersonNameStr | None = Field(default=None, alias="Display Name")
    email_address: EmailAddressStr = Field(alias="Email Address")
    role: RoleEnum = Field(alias="Role")

    index: NonNegativeInt = Field(alias="Index")
    priority: NonNegativeInt = Field(alias="Priority")
    total_attended: NonNegativeInt = Field(alias="Total Attended")

    active: bool = Field(alias="Active")
    date_joined: date = Field(alias="Date Joined")

    @field_validator("display_name", mode="before")
    @classmethod
    def coerce_empty_display_name_to_none(cls, v):
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @field_validator("date_joined", mode="before")
    @classmethod
    def validate_date_joined(cls, v):
        """Date Joined must be valid date string with format "%m/%d/%Y."""
        if not isinstance(v, str):
            raise ValueError("Date Joined must be a string")
        try:
            return datetime.strptime(v, "%m/%d/%Y").date()
        except ValueError as e:
            raise ValueError(f"invalid date format: {v}") from e


class MembersCsvFileSchema(RootModel[list[MemberCsvRowSchema]]):
    @model_validator(mode="after")
    def validate_unique_fields(self):
        rows = self.root

        member_ids = [row.id for row in rows]
        validate_unique(member_ids, msg="duplicate member id")

        indices = [row.index for row in rows]
        validate_unique(indices, msg="duplicate index")

        emails = [
            normalize_email_for_match(row.email_address) for row in rows if row.email_address
        ]
        validate_unique(emails, msg="duplicate email")

        names = [row.full_name.casefold() for row in rows if row.full_name]
        validate_unique(names, msg="duplicate name")

        display_names = [
            row.display_name.casefold()
            for row in rows
            if row.display_name and row.display_name.strip()
        ]
        validate_unique(display_names, msg="duplicate display name")

        return self
