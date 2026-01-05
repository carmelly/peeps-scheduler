"""
Validation layer for Peeps Scheduler input files.

This module provides Pydantic-based validation for CSV/JSON files and orchestration
for loading complete scheduling periods. It validates in-memory structures only;
file IO happens elsewhere.

Public API:
  - Schemas: Direct Pydantic validation (MembersCsvFileSchema, ResponsesCsvFileSchema, etc.)
  - Validation wrappers: CSV/JSON → validated dicts (validate_members, validate_responses, etc.)
  - Converters: Validated data → domain objects (member_to_peep, convert_to_events, etc.)
  - Period orchestration: Complete period loading (load_and_validate_period, PeriodData)
"""

# Validation wrappers (CSV/JSON → validated dicts) and converters (schemas → domain objects)
from peeps_scheduler.validation.converters import (
    convert_to_events,
    convert_to_peeps,
    event_spec_to_event,
    extract_cancellations,
    extract_partnerships,
    member_to_peep,
    validate_attendance,
    validate_cancellations,
    validate_members,
    validate_partnerships,
    validate_responses,
    validate_results,
)

# Errors
from peeps_scheduler.validation.errors import FileValidationError, MultiFileValidationError

# Schemas for direct Pydantic validation
from peeps_scheduler.validation.file_schemas.attendance_json import (
    ActualAttendanceJsonSchema,
    AttendanceEventJsonSchema,
    RosterEntryJsonSchema,
)
from peeps_scheduler.validation.file_schemas.cancellations_json import (
    CancelledAvailabilityJsonSchema,
    CancelledEventJsonSchema,
)
from peeps_scheduler.validation.file_schemas.members_csv import (
    MemberCsvRowSchema,
    MembersCsvFileSchema,
)
from peeps_scheduler.validation.file_schemas.partnerships_json import PartnershipRequestJsonSchema
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
    validate_period_data,
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
    "convert_to_events",
    "convert_to_peeps",
    "event_spec_to_event",
    "extract_cancellations",
    "extract_partnerships",
    "load_and_validate_period",
    "load_period_files",
    "member_to_peep",
    "to_period_data",
    "validate_attendance",
    "validate_cancellations",
    "validate_members",
    "validate_partnerships",
    "validate_period_data",
    "validate_responses",
    "validate_results",
]
