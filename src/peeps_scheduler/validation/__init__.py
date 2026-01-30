"""
Validation layer for Peeps Scheduler input files.

This package provides Pydantic-based validation and orchestration for loading
a complete scheduling period from already-loaded file data.

It validates in-memory structures only; file IO happens elsewhere.

Public API:
  - load_and_validate_period
  - PeriodData
  - FileValidationError (single-file errors)
  - MultiFileValidationError (cross-file errors)
"""

from .errors import (
    FileValidationError,
    MultiFileValidationError,
)
from .period import (
    PeriodData,
    load_and_validate_period,
)

__all__ = [
    "FileValidationError",
    "MultiFileValidationError",
    "PeriodData",
    "load_and_validate_period",
]
