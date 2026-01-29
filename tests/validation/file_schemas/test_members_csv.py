import pytest
from pydantic import ValidationError
from peeps_scheduler.models import Role
from peeps_scheduler.validation.file_schemas.members_csv import (
    MemberCsvRowSchema,
    MembersCsvFileSchema,
)
from tests.validation.conftest import assert_error_for_field, assert_error_for_model
from tests.validation.fixtures import member_data

pytestmark = pytest.mark.unit


class TestMemberCsvRowSchema:
    def test_valid_defaults(self, ctx):
        schema = MemberCsvRowSchema.model_validate(member_data(), context={"ctx": ctx})
        assert schema.id == 1
        assert schema.full_name == "Alice Alpha"
        assert schema.display_name == "Alice"
        assert schema.email_address == "alice@test.com"
        assert schema.role == Role.LEADER
        assert schema.index == 0
        assert schema.priority == 1
        assert schema.total_attended == 0
        assert schema.active is True
        assert schema.date_joined.year == 2020

    def test_display_name_none_allowed(self, ctx):
        schema = MemberCsvRowSchema.model_validate(
            member_data({"Display Name": None}),
            context={"ctx": ctx},
        )
        assert schema.display_name is None

    @pytest.mark.parametrize("date_value", ["1/2/2020", "2020-01-02"])
    def test_date_joined_accepts_multiple_formats(self, ctx, date_value):
        schema = MemberCsvRowSchema.model_validate(
            member_data({"Date Joined": date_value}),
            context={"ctx": ctx},
        )
        assert schema.date_joined.year == 2020

    @pytest.mark.parametrize(
        "data, msg",
        [
            ("not a date", "invalid date"),
            ("", "invalid date"),
            (2020, "must be a string"),
        ],
    )
    def test_invalid_date_joined_raises(self, ctx, data, msg):
        with pytest.raises(ValidationError) as e:
            MemberCsvRowSchema.model_validate(
                member_data({"Date Joined": data}),
                context={"ctx": ctx},
            )
        assert_error_for_field(e.value.errors(), "Date Joined", msg)


class TestMembersCsvFileSchema:
    def test_valid_defaults(self, ctx):
        schema = MembersCsvFileSchema.model_validate(
            [
                member_data({"id": "1", "Index": "0", "Email Address": "alice@test.com"}),
                member_data(
                    {
                        "id": "2",
                        "Index": "1",
                        "Name": "Bob Beta",
                        "Display Name": "Bob",
                        "Email Address": "bob@test.com",
                    }
                ),
            ],
            context={"ctx": ctx},
        )

        assert len(schema.root) == 2
        assert all(isinstance(row, MemberCsvRowSchema) for row in schema.root)

    def test_duplicate_ids_raise(self, ctx):
        with pytest.raises(ValidationError) as e:
            MembersCsvFileSchema.model_validate(
                [
                    member_data({"id": "1", "Index": "0"}),
                    member_data({"id": "1", "Index": "1"}),
                ],
                context={"ctx": ctx},
            )

        assert_error_for_model(e.value.errors(), "duplicate member id")

    def test_duplicate_index_raise(self, ctx):
        with pytest.raises(ValidationError) as e:
            MembersCsvFileSchema.model_validate(
                [
                    member_data({"id": "1", "Index": "0"}),
                    member_data({"id": "2", "Index": "0"}),
                ],
                context={"ctx": ctx},
            )

        assert_error_for_model(e.value.errors(), "duplicate index")

    def test_duplicate_email_raises(self, ctx):
        with pytest.raises(ValidationError) as e:
            MembersCsvFileSchema.model_validate(
                [
                    member_data({"id": "1", "Email Address": "AliCe@TEST.com", "Index": "0"}),
                    member_data({"id": "2", "Email Address": "alice@test.com", "Index": "1"}),
                ],
                context={"ctx": ctx},
            )

        assert_error_for_model(e.value.errors(), "duplicate email")

    def test_duplicate_name_raises(self, ctx):
        with pytest.raises(ValidationError) as e:
            MembersCsvFileSchema.model_validate(
                [
                    member_data({"id": "1", "Name": "Alice Alpha", "Index": "0"}),
                    member_data(
                        {
                            "id": "2",
                            "Name": "alice alpha",
                            "Index": "1",
                            "Email Address": "bob@test.com",
                        }
                    ),
                ],
                context={"ctx": ctx},
            )

        assert_error_for_model(e.value.errors(), "duplicate name")

    def test_duplicate_display_name_raises(self, ctx):
        with pytest.raises(ValidationError) as e:
            MembersCsvFileSchema.model_validate(
                [
                    member_data({"id": "1", "Display Name": "Alice", "Index": "0"}),
                    member_data(
                        {
                            "id": "2",
                            "Name": "Bob Beta",
                            "Display Name": "alice",
                            "Index": "1",
                            "Email Address": "bob@test.com",
                        }
                    ),
                ],
                context={"ctx": ctx},
            )

        assert_error_for_model(e.value.errors(), "duplicate display name")

    def test_active_member_without_email_raises(self, ctx):
        """Error case: Active member with empty email should fail validation."""
        with pytest.raises(ValidationError) as e:
            MembersCsvFileSchema.model_validate(
                [
                    member_data({
                        "id": "1",
                        "Index": "0",
                        "Active": "TRUE",
                        "Email Address": "",
                    }),
                ],
                context={"ctx": ctx},
            )
        assert_error_for_model(e.value.errors(), "email")

    def test_inactive_member_without_email_passes(self, ctx):
        """Edge case: Inactive member without email should pass validation."""
        schema = MembersCsvFileSchema.model_validate(
            [
                member_data({
                    "id": "1",
                    "Index": "0",
                    "Active": "FALSE",
                    "Email Address": "",
                }),
            ],
            context={"ctx": ctx},
        )
        assert len(schema.root) == 1
        assert schema.root[0].active is False

    def test_priority_order_mismatch_raises(self, ctx):
        """Error case: priority must be non-increasing by index order."""
        with pytest.raises(ValidationError) as e:
            MembersCsvFileSchema.model_validate(
                [
                    member_data(
                        {
                            "id": "1",
                            "Index": "0",
                            "Priority": "1",
                            "Email Address": "alice@test.com",
                        }
                    ),
                    member_data(
                        {
                            "id": "2",
                            "Index": "1",
                            "Priority": "2",
                            "Email Address": "bob@test.com",
                            "Name": "Bob Beta",
                            "Display Name": "Bob",
                        }
                    ),
                ],
                context={"ctx": ctx},
            )

        assert_error_for_model(e.value.errors(), "priority order")
