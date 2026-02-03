import pytest
from pydantic import BaseModel, EmailStr, ValidationError, field_validator
from peeps_scheduler.adapters.file.validation.errors import (
    PeriodValidationError,
    ValidationFailure,
)


class SimpleModel(BaseModel):
    email: EmailStr
    role: str

    @field_validator("role")
    @classmethod
    def validate_role(cls, v):
        allowed = {"leader", "follower"}
        if v not in allowed:
            raise ValueError(f"expected 'leader' or 'follower', got '{v}'")
        return v


def _make_validation_error() -> ValidationError:
    try:
        SimpleModel(email="invalid", role="invalid")
    except ValidationError as ve:
        return ve
    raise AssertionError("Expected ValidationError")


@pytest.mark.unit
def test_validation_failure_from_file_error():
    ve = _make_validation_error()
    failure = ValidationFailure.from_file_error("members.csv", ve)
    assert failure.failure_type == "file"
    assert failure.filenames == ["members.csv"]
    assert failure.pydantic_error is ve
    assert "Validation failed in members.csv" in failure.message


@pytest.mark.unit
def test_validation_failure_from_cross_file_error():
    failure = ValidationFailure.from_cross_file_error(
        ["members.csv", "responses.csv"], "response email not found"
    )
    assert failure.failure_type == "cross-file"
    assert "Cross-file validation error" in failure.message
    assert "members.csv" in failure.message
    assert "responses.csv" in failure.message


@pytest.mark.unit
def test_period_validation_error_formats_message():
    ve = _make_validation_error()
    failures = [
        ValidationFailure.from_file_error("members.csv", ve),
        ValidationFailure.from_cross_file_error(["members.csv"], "missing member"),
    ]
    error = PeriodValidationError(failures)
    message = str(error)
    assert "Validation failed with 2 error" in message
    assert "members.csv" in message


@pytest.mark.unit
def test_period_validation_error_all_errors_includes_file():
    ve = _make_validation_error()
    failures = [ValidationFailure.from_file_error("members.csv", ve)]
    error = PeriodValidationError(failures)
    all_errors = error.all_errors()
    assert len(all_errors) > 0
    assert all_errors[0]["file"] == "members.csv"
