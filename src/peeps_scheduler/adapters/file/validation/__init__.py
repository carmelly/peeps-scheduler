"""
Validation layer for Peeps Scheduler input files.

This package provides Pydantic-based validation and error handling for loading
a complete scheduling period from already-loaded file data.

It validates in-memory structures only; file IO happens elsewhere.

Public API:
  - validate_period (main entry point for period validation)
  - ValidationFailure (file-level and cross-file validation errors)
  - PeriodValidationError (collection of validation failures for a period)
"""

from .errors import PeriodValidationError, ValidationFailure
from .period import validate_period

__all__ = [
    "PeriodValidationError",
    "ValidationFailure",
    "validate_period",
]
