"""Validation error handling and wrapping."""

from pydantic import ValidationError

MAX_ERRORS_DISPLAYED = 10


class FileValidationError(Exception):
    """Wraps Pydantic ValidationError with file context."""

    def __init__(self, file_path: str, validation_error: ValidationError):
        self.file_path = file_path
        self.validation_error = validation_error

    def errors(self) -> list[dict]:
        """Return structured access to errors in Pydantic format."""
        return self.validation_error.errors()

    def __str__(self) -> str:
        """Return human-readable format with file context."""
        lines = [f"Validation failed in {self.file_path}:"]

        # Get all errors
        all_errors = self.errors()

        # Show first MAX_ERRORS_DISPLAYED errors
        for error in all_errors[:MAX_ERRORS_DISPLAYED]:
            # Extract field name and message
            loc = error.get("loc", ())
            msg = error.get("msg", "")

            # Pydantic's loc is the attribute/index path from the root schema to
            # the failing field. The row index is the first int in that path, not
            # necessarily loc[0] or loc[1] - nested wrapper models can add extra
            # non-int segments first (e.g. ("responses", "responses", 5, "Min
            # Interval Days") for a row nested two attributes deep).
            row_index = next((i for i, part in enumerate(loc) if isinstance(part, int)), None)
            if row_index is not None:
                row = loc[row_index]
                field_parts = loc[row_index + 1 :]
                field = ".".join(str(part) for part in field_parts) if field_parts else "unknown"
                lines.append(f"  Row {row}, field '{field}': {msg}")
            else:
                # Genuinely file-level error: no row to point to
                field = ".".join(str(part) for part in loc) if loc else "unknown"
                lines.append(f"  File-level ({field}): {msg}")

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
                error_with_file = {**error_dict, "file": file_error.file_path}
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
