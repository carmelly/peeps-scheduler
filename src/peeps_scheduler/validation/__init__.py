"""
Validation layer for Peeps Scheduler input files.

This module provides Pydantic-based validation for CSV/JSON files and orchestration
for loading complete scheduling periods. It validates in-memory structures only;
file IO happens elsewhere.

Public API:
  - Schemas: Direct Pydantic validation (MembersCsvFileSchema, ResponsesCsvFileSchema, etc.)
  - Builders: Validated data → domain objects (build_peeps, build_events, etc.)
  - Period orchestration: Complete period loading (load_and_validate_period, PeriodData)
"""

# Builders (schemas → domain objects)
from peeps_scheduler.validation.builders import (
    build_cancelled_availability,
    build_cancelled_events,
    build_events,
    build_partnerships,
    build_peeps,
)

# Errors
from peeps_scheduler.validation.errors import FileValidationError, MultiFileValidationError

# Schemas for direct Pydantic validation
from peeps_scheduler.validation.file_schemas.attendance_json import (
    ActualAttendanceJsonSchema,
    AttendanceEventJsonSchema,
    RosterEntryJsonSchema,
)
from peeps_scheduler.validation.file_schemas.members_csv import (
    MemberCsvRowSchema,
    MembersCsvFileSchema,
)
from peeps_scheduler.validation.file_schemas.period import PeriodFileSchema
from peeps_scheduler.validation.file_schemas.responses_csv import (
    EventRowCsvSchema,
    ResponseCsvRowSchema,
    ResponsesCsvFileSchema,
)
from peeps_scheduler.validation.file_schemas.results_json import (
    ResultEventJsonSchema,
    ResultsJsonSchema,
)

# Period orchestration (high-level API)
from peeps_scheduler.validation.period import (
    PeriodData,
    load_and_validate_period,
    load_period_files,
    to_period_data,
)

__all__ = [
    "ActualAttendanceJsonSchema",
    "AttendanceEventJsonSchema",
    "CancelledAvailabilityJsonSchema",
    "CancelledEventJsonSchema",
    "EventRowCsvSchema",
    "FileValidationError",
    "MemberCsvRowSchema",
    "MembersCsvFileSchema",
    "MultiFileValidationError",
    "PartnershipRequestJsonSchema",
    "PeriodData",
    "PeriodFileSchema",
    "ResponseCsvRowSchema",
    "ResponsesCsvFileSchema",
    "ResultEventJsonSchema",
    "ResultsJsonSchema",
    "RosterEntryJsonSchema",
    "build_cancelled_availability",
    "build_cancelled_events",
    "build_events",
    "build_partnerships",
    "build_peeps",
    "load_and_validate_period",
    "load_period_files",
    "to_period_data",
]
