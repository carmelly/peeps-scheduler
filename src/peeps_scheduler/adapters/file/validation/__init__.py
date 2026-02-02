"""
Validation layer for Peeps Scheduler input files.

This package provides Pydantic-based validation and orchestration for loading
a complete scheduling period from already-loaded file data.

It validates in-memory structures only; file IO happens elsewhere.

Public API:
  - validate_period_data (main entry point for validating period data)
  - FileValidationError (single-file errors)
  - MultiFileValidationError (cross-file errors)

"""

from .errors import (
    FileValidationError,
    MultiFileValidationError,
)
from .period import validate_period_data

__all__ = [
    "FileValidationError",
    "MultiFileValidationError",
    "validate_period_data",
]
