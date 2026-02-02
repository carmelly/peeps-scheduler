"""Validation error handling and wrapping."""

from pydantic import ValidationError

MAX_ERRORS_DISPLAYED = 10


class FileValidationError(Exception):
    """Wraps Pydantic ValidationError with file context.

    Automatically infers the filename from the validation error location.
    """

    def __init__(self, filename: str, validation_error: ValidationError):
        self.validation_error = validation_error
        self.filename = filename

    @staticmethod
    def _infer_filename(error: ValidationError) -> str:
        """Infer which file caused a validation error based on error location.

        Args:
            error: Pydantic ValidationError

        Returns:
            Filename string (members.csv, responses.csv, or period_config.json)
        """
        fields = set()
        for err in error.errors():
            loc = err.get("loc") or ()
            fields.add(loc[0] if loc else None)
        if len(fields) == 1:
            field = next(iter(fields))
            if field == "members":
                return "members.csv"
            if field == "responses":
                return "responses.csv"
            if field in {
                "cancelled_events",
                "cancelled_member_availability",
                "partnership_requests",
                "topics",
            }:
                return "period_config.json"
        return "unknown"

    def errors(self) -> list[dict]:
        """Return structured access to errors in Pydantic format."""
        return self.validation_error.errors()

    def __str__(self) -> str:
        """Return human-readable format with file context."""
        lines = [f"Validation failed in {self.filename}:"]

        # Get all errors
        all_errors = self.errors()

        # Show first MAX_ERRORS_DISPLAYED errors
        for error in all_errors[:MAX_ERRORS_DISPLAYED]:
            # Extract field name and message
            loc = error.get("loc", ())
            msg = error.get("msg", "")

            # Check if it's a row error (first element in loc is a row number)
            if loc and isinstance(loc[0], int):
                row = loc[0]
                # Get field name (might be nested)
                field = loc[1] if len(loc) > 1 else "unknown"
                lines.append(f"  Row {row}, field '{field}': {msg}")
            else:
                # File-level error
                lines.append(f"  File-level: {msg}")

        # Add truncation message if needed
        if len(all_errors) > MAX_ERRORS_DISPLAYED:
            remaining = len(all_errors) - MAX_ERRORS_DISPLAYED
            lines.append(f"  ... and {remaining} more error(s)")

        return "\n".join(lines)


class MultiFileValidationError(Exception):
    """Aggregates validation errors from multiple files."""

    def __init__(self, file_errors: list[FileValidationError]):
        self.file_errors = file_errors

    def all_errors(self) -> list[dict]:
        """Return all errors with 'file' key added to each error dict."""
        result = []
        for file_error in self.file_errors:
            for error_dict in file_error.errors():
                # Add 'file' key to each error dict
                error_with_file = {**error_dict, "file": file_error.filename}
                result.append(error_with_file)
        return result

    def __str__(self) -> str:
        """Return human-readable format with all file errors combined."""
        num_files = len(self.file_errors)
        lines = [f"Validation errors in {num_files} files:"]

        # Add each file's errors
        for file_error in self.file_errors:
            file_error_str = str(file_error)
            # Append the file error block
            lines.append(file_error_str)

        return "\n".join(lines)

        return "\n".join(lines)
