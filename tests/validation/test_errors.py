"""Tests for FileValidationError wrapper."""

from unittest.mock import Mock
import pytest
from pydantic import BaseModel, EmailStr, ValidationError, field_validator
from peeps_scheduler.validation.errors import FileValidationError, MultiFileValidationError


class SimpleModel(BaseModel):
    """Simple model for generating validation errors."""
    email: EmailStr
    role: str

    @field_validator('role')
    @classmethod
    def validate_role(cls, v):
        allowed = {'leader', 'follower'}
        if v not in allowed:
            raise ValueError(f"expected 'leader' or 'follower', got '{v}'")
        return v


@pytest.fixture
def validation_error():
    """Create a ValidationError for testing."""
    try:
        SimpleModel(email="invalid", role="invalid")
    except ValidationError as ve:
        return ve


@pytest.mark.unit
class TestFileValidationErrorWrapping:
    """Test that FileValidationError correctly wraps Pydantic ValidationError."""

    def test_wraps_validation_error_with_file_context(self, validation_error):
        """Test wrapping ValidationError and accessing file context."""
        error = FileValidationError("members.csv", validation_error)

        assert error.file_path == "members.csv"
        assert error.validation_error is validation_error


@pytest.mark.unit
class TestFileValidationErrorErrorsMethod:
    """Test the .errors() method returns structured Pydantic format."""

    def test_errors_returns_pydantic_format(self, validation_error):
        """Test .errors() returns structured list matching Pydantic format."""
        error = FileValidationError("members.csv", validation_error)

        errors = error.errors()

        # Check structure
        assert isinstance(errors, list)
        assert len(errors) > 0
        assert all(isinstance(e, dict) for e in errors)

        # Check Pydantic fields present
        for err in errors:
            assert 'loc' in err
            assert 'msg' in err
            assert 'type' in err

        # Check it matches underlying error
        assert errors == validation_error.errors()


@pytest.mark.unit
class TestFileValidationErrorStringFormat:
    """Test the __str__() method produces human-readable format."""

    def test_str_format_and_indentation(self, validation_error):
        """Test string format matches specification: header, indentation, field names."""
        error = FileValidationError("members.csv", validation_error)

        result = str(error)
        lines = result.split('\n')

        # Header format check
        assert lines[0] == "Validation failed in members.csv:"

        # Indentation check
        for line in lines[1:]:
            if line.strip():  # Non-empty lines
                assert line.startswith('  '), f"Line not indented: {line}"

        # Content checks
        assert "members.csv" in result
        assert len(result) > len("Validation failed in members.csv:")

    @pytest.mark.parametrize("filename", [
        "events.csv",
        "data/input/members.csv",
        "members (2024-01-01).csv",
        "成員.csv",
    ])
    def test_str_with_various_filenames(self, validation_error, filename):
        """Test string format works with various filename formats."""
        error = FileValidationError(filename, validation_error)

        result = str(error)

        assert f"Validation failed in {filename}:" in result
        assert filename in result


@pytest.mark.unit
class TestFileValidationErrorTruncation:
    """Test truncation of errors when exceeding 10 errors."""

    def test_truncates_errors_at_10_with_more_message(self):
        """Test that more than 10 errors is truncated with '... and N more errors'."""
        class ManyFieldsModel(BaseModel):
            f1: int
            f2: int
            f3: int
            f4: int
            f5: int
            f6: int
            f7: int
            f8: int
            f9: int
            f10: int
            f11: int
            f12: int

        try:
            ManyFieldsModel(f1="x", f2="x", f3="x", f4="x", f5="x",
                          f6="x", f7="x", f8="x", f9="x", f10="x",
                          f11="x", f12="x")
        except ValidationError as ve:
            error = FileValidationError("data.csv", ve)
            result = str(error)

            # Count error lines
            error_lines = [line for line in result.split('\n')[1:] if line.startswith('  ')]

            # Should not exceed ~11 lines (10 errors + more message)
            assert len(error_lines) <= 11

            # Should indicate more errors exist
            if len(ve.errors()) > 10:
                assert ("and" in result and "more" in result) or "... and" in result


@pytest.mark.unit
class TestFileValidationErrorUseCases:
    """Test realistic usage scenarios."""

    def test_can_be_raised_and_caught_as_exception(self, validation_error):
        """Test FileValidationError functions as proper Exception."""
        error = FileValidationError("members.csv", validation_error)

        # Can be raised
        with pytest.raises(FileValidationError):
            raise error

        # Caught error has readable message
        try:
            raise error
        except FileValidationError as caught:
            message = str(caught)
            assert "Validation failed in members.csv:" in message
            assert len(message) > 20

    def test_multiple_field_errors_in_output(self, validation_error):
        """Test output includes multiple field errors."""
        error = FileValidationError("members.csv", validation_error)

        # Should have multiple errors
        assert len(error.errors()) >= 2

        result = str(error)
        # Output should be substantive
        assert "Validation failed in members.csv:" in result
        error_lines = [line for line in result.split('\n')[1:] if line.strip()]
        assert len(error_lines) >= 2

    def test_row_based_error_formatting(self):
        """Test formatting of row-based errors with numeric row numbers."""
        # Create mock ValidationError with row number in loc
        mock_ve = Mock(spec=ValidationError)
        mock_ve.errors.return_value = [
            {
                'loc': (3, 'email'),
                'msg': 'invalid email format',
                'type': 'value_error',
            },
            {
                'loc': (7, 'role'),
                'msg': "expected 'leader' or 'follower', got 'dancer'",
                'type': 'value_error',
            },
        ]

        error = FileValidationError("members.csv", mock_ve)
        result = str(error)

        # Should format row-based errors
        assert "Row 3, field 'email': invalid email format" in result
        assert "Row 7, field 'role':" in result
        assert "Validation failed in members.csv:" in result


@pytest.mark.unit
class TestMultiFileValidationError:
    """Test MultiFileValidationError aggregates errors from multiple files."""

    def test_aggregates_multiple_file_errors(self, validation_error):
        """Test aggregates multiple FileValidationErrors and mentions all files."""
        error1 = FileValidationError("members.csv", validation_error)
        error2 = FileValidationError("responses.csv", validation_error)
        error3 = FileValidationError("cancellations.json", validation_error)

        multi_error = MultiFileValidationError([error1, error2, error3])
        result = str(multi_error)

        # Should mention all 3 files in the error message
        assert "members.csv" in result
        assert "responses.csv" in result
        assert "cancellations.json" in result
        # Should indicate it's a multi-file error
        assert "3 files" in result

    def test_all_errors_adds_file_context(self, validation_error):
        """Test all_errors() method adds 'file' key to each error dict."""
        error1 = FileValidationError("members.csv", validation_error)
        error2 = FileValidationError("responses.csv", validation_error)

        multi_error = MultiFileValidationError([error1, error2])
        all_errors = multi_error.all_errors()

        # Should have errors from both files
        assert len(all_errors) > 0

        # Each error dict should have 'file' key added
        files_seen = set()
        for error_dict in all_errors:
            assert "file" in error_dict
            files_seen.add(error_dict["file"])

        # Should have errors from both files
        assert "members.csv" in files_seen
        assert "responses.csv" in files_seen

    def test_str_format_with_multiple_files(self, validation_error):
        """Test __str__() formats errors from multiple files clearly."""
        error1 = FileValidationError("members.csv", validation_error)
        error2 = FileValidationError("responses.csv", validation_error)

        multi_error = MultiFileValidationError([error1, error2])
        result = str(multi_error)

        # Should have the header mentioning number of files
        assert "Validation errors in 2 files" in result

        # Should contain both file names
        assert "members.csv" in result
        assert "responses.csv" in result

        # Should contain validation error details
        assert "Validation failed in" in result