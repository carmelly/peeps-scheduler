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
from peeps_scheduler.validation.fields import (
    OptionalEmailAddressStr,
    OptionalPersonNameStr,
    PersonNameStr,
    RoleEnum,
)
from peeps_scheduler.validation.helpers import normalize_email_for_match, validate_unique


class MemberCsvRowSchema(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    id: PositiveInt = Field(alias="id")
    full_name: PersonNameStr = Field(alias="Name")
    display_name: OptionalPersonNameStr = Field(default=None, alias="Display Name")
    email_address: OptionalEmailAddressStr = Field(default=None, alias="Email Address")
    role: RoleEnum = Field(alias="Role")

    index: NonNegativeInt = Field(alias="Index")
    priority: NonNegativeInt = Field(alias="Priority")
    total_attended: NonNegativeInt = Field(alias="Total Attended")

    active: bool = Field(alias="Active")
    date_joined: date = Field(alias="Date Joined")

    @field_validator("date_joined", mode="before")
    @classmethod
    def validate_date_joined(cls, v):
        """Date Joined must be a valid date string."""
        if isinstance(v, date):
            return v
        if not isinstance(v, str):
            raise ValueError("Date Joined must be a string")
        for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(v, fmt).date()
            except ValueError:
                continue
        # TODO: Remove support for "%m/%d/%Y" after historical files are normalized.
        raise ValueError(f"invalid date format: {v}")


class MembersCsvFileSchema(RootModel[list[MemberCsvRowSchema]]):
    @model_validator(mode="after")
    def validate_unique_fields(self):
        rows = self.root

        member_ids = [row.id for row in rows]
        validate_unique(member_ids, msg="duplicate member id")

        indices = [row.index for row in rows]
        validate_unique(indices, msg="duplicate index")

        emails = [normalize_email_for_match(row.email_address) for row in rows if row.email_address]
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

    @model_validator(mode="after")
    def validate_active_member_email(self):
        rows = self.root
        for row in rows:
            if row.active and row.email_address is None:
                raise ValueError("active members must have an email address")
        return self

    @model_validator(mode="after")
    def validate_priority_order(self):
        rows = sorted(self.root, key=lambda row: row.index)
        priorities = [row.priority for row in rows]
        if priorities != sorted(priorities, reverse=True):
            raise ValueError("priority order must be non-increasing by index")
        return self
