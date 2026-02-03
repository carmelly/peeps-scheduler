"""Validation error handling and wrapping."""

from dataclasses import dataclass
from typing import Literal
from pydantic import ValidationError

MAX_ERRORS_DISPLAYED = 10


@dataclass
class ValidationFailure:
    """Represents a single validation failure (file-level or cross-file).

    Attributes:
        failure_type: Whether this is a file-level or cross-file validation failure
        filenames: List of involved filenames (one for file-level, multiple for cross-file)
        message: Human-readable error message
        pydantic_error: Original Pydantic ValidationError (for file-level failures only)
    """

    failure_type: Literal["file", "cross-file"]
    filenames: list[str]
    message: str
    pydantic_error: ValidationError | None = None

    @staticmethod
    def from_file_error(filename: str, validation_error: ValidationError) -> "ValidationFailure":
        """Create a file-level ValidationFailure from a Pydantic ValidationError."""
        return ValidationFailure(
            failure_type="file",
            filenames=[filename],
            message=_format_file_error(filename, validation_error),
            pydantic_error=validation_error,
        )

    @staticmethod
    def from_cross_file_error(filenames: list[str], message: str) -> "ValidationFailure":
        """Create a cross-file ValidationFailure from an error message."""
        formatted_message = f"Cross-file validation error ({', '.join(filenames)}): {message}"
        return ValidationFailure(
            failure_type="cross-file",
            filenames=filenames,
            message=formatted_message,
            pydantic_error=None,
        )

    def errors(self) -> list[dict]:
        """Return structured errors in Pydantic format (for file-level failures only)."""
        if self.pydantic_error:
            return self.pydantic_error.errors()
        return []


class PeriodValidationError(Exception):
    """Collection of validation failures from validating a period.

    Can contain both file-level and cross-file validation failures.
    """

    def __init__(self, failures: list[ValidationFailure]):
        self.failures = failures
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format all failures into a human-readable message."""
        if not self.failures:
            return "Validation failed"

        if len(self.failures) == 1:
            return self.failures[0].message

        lines = [f"Validation failed with {len(self.failures)} error(s):"]
        for failure in self.failures:
            lines.append(f"  - {failure.message}")
        return "\n".join(lines)

    def all_errors(self) -> list[dict]:
        """Return all errors with 'file' key added (for file-level failures)."""
        result = []
        for failure in self.failures:
            if failure.pydantic_error:
                for error_dict in failure.errors():
                    error_with_file = {**error_dict, "file": failure.filenames[0]}
                    result.append(error_with_file)
        return result

    def has_file_errors(self) -> bool:
        """Check if any failures are file-level errors."""
        return any(f.failure_type == "file" for f in self.failures)

    def has_cross_file_errors(self) -> bool:
        """Check if any failures are cross-file errors."""
        return any(f.failure_type == "cross-file" for f in self.failures)


def _format_file_error(filename: str, validation_error: ValidationError) -> str:
    """Format a file-level validation error."""
    lines = [f"Validation failed in {filename}:"]
    all_errors = validation_error.errors()

    # Show first MAX_ERRORS_DISPLAYED errors
    for error in all_errors[:MAX_ERRORS_DISPLAYED]:
        loc = error.get("loc", ())
        msg = error.get("msg", "")

        # Check if it's a row error (first element in loc is a row number)
        if loc and isinstance(loc[0], int):
            row = loc[0]
            field = loc[1] if len(loc) > 1 else "unknown"
            lines.append(f"  Row {row}, field '{field}': {msg}")
        else:
            lines.append(f"  File-level: {msg}")

    # Add truncation message if needed
    if len(all_errors) > MAX_ERRORS_DISPLAYED:
        remaining = len(all_errors) - MAX_ERRORS_DISPLAYED
        lines.append(f"  ... and {remaining} more error(s)")

    return "\n".join(lines)
