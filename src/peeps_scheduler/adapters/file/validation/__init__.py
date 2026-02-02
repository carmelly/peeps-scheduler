"""
Validation layer for Peeps Scheduler input files.

This package provides Pydantic-based validation and error handling for loading
a complete scheduling period from already-loaded file data.

It validates in-memory structures only; file IO happens elsewhere.

Public API:
  - FileValidationError (single-file validation errors with auto-inferred filenames)
  - MultiFileValidationError (cross-file validation errors)

Validation logic is orchestrated in FilePeriodLoader.load_period().

"""

from .errors import (
    FileValidationError,
    MultiFileValidationError,
)

__all__ = [
    "FileValidationError",
    "MultiFileValidationError",
]
