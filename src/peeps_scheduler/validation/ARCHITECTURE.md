# Validation Layer Architecture

## Overview

The validation layer provides Pydantic-based validation for all input data files used by Peeps Scheduler. This is **separate from database schema validation** (handled by `db/validate.py`) and focuses exclusively on validating input files before they are processed by the application.

**Design Philosophy**: Collect all validation errors and present them to users in one comprehensive report, rather than failing on the first error. This allows users to fix multiple issues in a single iteration.

---

## Directory Structure

```text
src/peeps_scheduler/validation/
├── __init__.py             # Public API exports and module documentation
├── fields.py               # Reusable field types + validators
├── helpers.py              # Normalization helpers
├── parsers.py              # Event parsing utilities
├── file_schemas/           # File-specific schemas
│   ├── attendance_json.py  # actual_attendance.json schemas
│   ├── cancellations_json.py  # cancelled events + availability schemas
│   ├── members_csv.py      # members.csv schemas
│   ├── partnerships_json.py  # partnerships.json schemas
│   ├── period.py           # PeriodFileSchema (aggregate)
│   ├── responses_csv.py    # responses.csv schemas (includes event rows)
│   └── results_json.py     # results.json schemas
└── ARCHITECTURE.md         # This file: design documentation
```

### Module Purposes

**`__init__.py`**

- Module-level documentation
- Usage examples for consumers

**`fields.py`**

- Reusable field types and validators shared across schemas

**`parsers.py`**

- Event parsing utilities (event name + datetime parsing)

**`file_schemas/members_csv.py`**

- Schemas for `members.csv`

**`file_schemas/responses_csv.py`**

- Schemas for `responses.csv` and optional event header rows

**`file_schemas/attendance_json.py` / `file_schemas/results_json.py`**

- Schemas for `actual_attendance.json` and `results.json`

**`file_schemas/cancellations_json.py`**

- Schemas for cancelled events and cancelled availability entries

**`file_schemas/partnerships_json.py`**

- Schemas for partnership requests

**`file_schemas/period.py`**

- `PeriodFileSchema` aggregates all file shapes into one validation entry point
- Cross-file validation helpers live alongside the schema for now

**`helpers.py`**

- Normalization helpers (email normalization)

---

## Error Handling Strategy

Validation errors are surfaced via Pydantic's `ValidationError`. When validating
lists of rows or nested models, Pydantic collects all errors and exposes them
through `e.errors()` as structured error dicts.

### Implementation Approach

```python
from pydantic import ValidationError

try:
    period = PeriodFileSchema.model_validate(payload, context={"ctx": ctx})
except ValidationError as e:
    errors = e.errors()  # list of {"loc": ..., "msg": ..., "type": ...}
```

### Error Message Format

Each Pydantic error dict contains:

- **loc**: Tuple path to the failing field (e.g., `("responses", 3, "Email Address")`)
- **msg**: Human-readable message
- **type**: Error code
- **input**: The invalid value (when available)

---

## Schema Design by File Type

## Event Name Formats

Availability/event strings can appear in two formats:

- **Old format**: date + time range only; duration is not embedded in the string.
  When responses include event rows, those rows supply the duration minutes and
  availability must use this old format.
- **New format**: date + time range + duration embedded in the string. When no
  event rows are present, availability strings may use this format.

Parsing produces an EventSpec with a start datetime and an optional duration
value. Old-format strings yield `duration_minutes=None`; new-format strings
yield a concrete duration. Results/attendance `date` values represent the event
start datetime in `YYYY-MM-DD HH:MM` and are treated as unique identifiers
within a file (timezone derived from validation context).

Event-name fields are context-dependent and require `ValidationContext`
(year + timezone) to parse. Old-format-only fields use `EventNameOldFormatStr`;
availability uses `EventSpecList` to parse into EventSpec entries.

### 1. Responses (responses.csv)

**Purpose**: Validates member response data with availability strings.
Responses can optionally include event header rows (for old-format availability).

**Fields**:

- `Timestamp` (required): Response submission timestamp (MM/DD/YYYY HH:MM:SS)
- `Name` (required): Member name (string, non-empty)
- `Display Name` (optional): Display name (string, non-empty)
- `Email Address` (required): Valid email format
- `Primary Role` (required): "Leader" or "Follower" (case-insensitive)
- `Secondary Role` (optional): Switch preference enum value
- `Max Sessions` (required): Integer >= 0
- `Availability` (required): List or comma-separated date strings (EventSpecList)
- `Min Interval Days` (required): Integer >= 0
- Event rows (optional): `Name` (old-format only), `Event Duration` minutes

**Custom Validations**:

- Email format validation (RFC 5322 basic compliance)
- Role enum validation with helpful error messages
- Availability string parsing (EventSpecList):
  - Each date string must parse successfully
  - Weekday must match actual date
  - Time ranges must be valid (end > start)
  - Duration must match CLASS_CONFIG values (when present)
  - Event rows enforce old-format names only (no duration embedded)
- Switch preference must match exact expected strings

**Cross-Field Validations**:

- Availability strings must all use the same format (all old or all new)
- If event rows exist, availability must use the old format and match event rows

**Error Examples**:

```text
Row 3, Email Address: 'jane.smith.gmail.com' is not a valid email
  → Missing '@' symbol. Format should be 'name@domain.com'

Row 7, Availability: Weekday 'Monday' doesn't match date 'January 15th 2025' (which is a Wednesday)
  → Verify the date is correct

Row 10, Availability: Duration 75 minutes not valid
  → Must be 60, 90, or 120 minutes (configured class durations)
```

### 2. Members (members.csv)

**Purpose**: Validates member roster data

**Fields**:

- `id` (required): Integer > 0, must be unique
- `Name` (required): Full name (non-empty string)
- `Display Name` (optional): Display name (non-empty string)
- `Email Address` (required): Valid email format, must be unique
- `Role` (required): "Leader" or "Follower"
- `Index` (required): Integer >= 0
- `Priority` (required): Integer >= 0
- `Total Attended` (required): Integer >= 0
- `Active` (required): "TRUE" or "FALSE" (case-insensitive)
- `Date Joined` (required): Date string in MM/DD/YYYY format

**Custom Validations**:

- ID uniqueness check across all rows
- Email uniqueness check (using normalized emails for Gmail)
- Date Joined must be a valid MM/DD/YYYY date string

**Cross-File Validations** (performed in PeriodFileSchema):

- Response emails must exist in members.csv
- Roster entries in results/attendance must reference valid member IDs

**Error Examples**:

```text
Row 15, id: Duplicate ID '42' found
  → IDs must be unique across all members

Row 20, Email Address: Active member missing email address
  → Active members must have a valid email

Row 8, Active: 'Yes' is not valid
  → Must be exactly 'TRUE' or 'FALSE'
```

### 3. Cancellations (cancellations.json)

**Purpose**: Validates cancelled events and member availability cancellations

**Structure** (split at load time):

```json
{
  "cancelled_events": ["Friday January 10th - 5pm to 6:30pm", ...]
}
```

```json
[
  {
    "email": "member@example.com",
    "events": ["Friday January 17th - 5pm to 6:30pm", ...]
  }
]
```

**Validations**:

- `cancelled_events` must be a list of event strings
- `cancelled_availability` must be a list of objects with valid email + events list
- Event strings must parse successfully with the validation context

**Error Examples**:

```text
cancelled_events[2]: 'Friday February 30th' is invalid
  → February only has 28/29 days

cancelled_availability[0].email: 'invalid.email' is not valid
  → Must be valid email format

cancelled_availability[3]: Duplicate entry for 'john@example.com'
  → Each member can only have one cancellation entry
```

### 4. Partnerships (partnerships.json)

**Purpose**: Validates partnership requests between members

**Structure** (list of single-request dicts):

```json
[{ "1": [2, 3] }, { "4": [5] }]
```

**Validations**:

- Keys (requester IDs) must be valid integers
- Values must be lists of integers
- Partner IDs must be valid integers
- No self-partnerships (requester cannot partner with themselves)
- All IDs must reference valid members (if member list provided)

**Error Examples**:

```text
partnerships["abc"]: Requester ID must be an integer
  → Found 'abc', expected numeric ID

partnerships[5]: Partner list must be array of integers
  → Found object, expected [1, 2, 3]

partnerships[10]: Member 10 cannot partner with themselves
  → Remove member 10 from their own partner list
```

### 5. Attendance (actual_attendance.json)

**Purpose**: Validates actual event attendance and attendee roster

**Structure**:

```json
{
  "valid_events": [
    {
      "id": 0,
      "date": "2025-05-03 11:00",
      "duration_minutes": 90,
      "attendees": [
        {
          "id": 8,
          "name": "Emma Johnson",
          "role": "Follower"
        }
      ]
    }
  ]
}
```

**Validations**:

- `valid_events` must be a list of event objects
- Each event must have: id, date, duration_minutes, attendees
- Event date must match the format `YYYY-MM-DD HH:MM`
- Each attendee must have: id, name, role
- Attendees list must be non-empty
- No duplicate attendee IDs within attendees
- Event start datetimes and legacy ids are unique within the file

**Error Examples**:

```text
valid_events[0]: Missing required field 'date'
  → All events must have a valid date in 'YYYY-MM-DD HH:MM' format

valid_events[2].attendees[5]: Invalid role 'lead'
  → Role must be exactly 'Leader' or 'Follower'

valid_events[1].attendees[3].id: Member 42 not found in peeps
  → Attendee ID must reference a valid member from members.csv

valid_events[0].date: 'invalid date string' does not match expected format
  → Date must be in format 'YYYY-MM-DD HH:MM' (e.g., '2025-05-03 11:00')
```

### 6. Results (results.json)

**Purpose**: Validates scheduler output events (ignores metrics)

**Structure**:

```json
{
  "valid_events": [
    {
      "id": 0,
      "date": "2020-01-04 13:00",
      "duration_minutes": 90,
      "attendees": [{ "id": 8, "name": "Emma Johnson", "role": "Follower" }],
      "alternates": [{ "id": 9, "name": "Taylor Smith", "role": "Leader" }]
    }
  ]
}
```

**Validations**:

- `valid_events` must be a list of event objects
- Each event must have: id, date, duration_minutes, attendees, alternates
- Event date must match `YYYY-MM-DD HH:MM` (timezone applied from context)
- Attendees list must be non-empty
- No duplicate attendee IDs within attendees
- No duplicate alternate IDs within alternates
- No overlap between attendee and alternate IDs
- Event start datetimes and legacy ids are unique within the file

---

## Integration with file_io.py

Validation is performed after parsing file contents into in-memory structures.
`file_io` remains responsible for reading CSV/JSON and shaping raw data; schemas
validate the resulting payload.

### Integration Pattern

```python
def load_period(paths, ctx):
    payload = {
        "members": load_members_csv(paths.members),
        "responses": load_responses_csv(paths.responses),
        "results": load_results_json(paths.results),
        "attendance": load_attendance_json(paths.attendance),
        "cancelled_events": load_cancelled_events_json(paths.cancelled_events),
        "cancelled_availability": load_cancelled_availability_json(paths.cancelled_availability),
        "partnerships": load_partnerships_json(paths.partnerships),
    }
    return PeriodFileSchema.model_validate(payload, context={"ctx": ctx})
```

---

## Key Validation Concerns Addressed

This design addresses issues identified in the task guidance:

1. **Email format validation** (#005, #031)
   - Implemented in ResponsesCsvFileSchema and MembersCsvFileSchema
   - Uses email-validator library for RFC compliance
   - Provides clear error messages for common mistakes

2. **Date format and weekday-date matching** (#030, #034)
   - Custom validator in availability string parsing
   - Checks weekday name matches actual date
   - Validates month day counts (no Feb 30th, etc.)

3. **Duplicate event detection** (#032)
   - Enforced in list-level schemas (event rows, results, attendance)
   - Detects duplicate start datetimes and legacy ids

4. **Event datetime format consistency** (#036)
   - Validates time range format in availability strings
   - Ensures duration matches CLASS_CONFIG
   - Consistent parsing between responses and cancellations

5. **Unknown email references** (#037)
   - Cross-file validation between responses and members (PeriodFileSchema)
   - Normalized email comparison (Gmail dot-ignoring)

---

## Testing Strategy

Tests are written **first** (TDD workflow):

### CSV Schema Tests

- Valid data passes validation
- Invalid emails rejected with helpful messages
- Invalid roles rejected with enum guidance
- Availability string parsing edge cases
- Duplicate detection (IDs, emails)

### JSON Schema Tests

- Valid JSON structures pass
- Missing required fields rejected
- Invalid event formats rejected
- Duplicate detection in cancellations
- Self-partnership rejection

### Integration Tests

- End-to-end PeriodFileSchema validation
- Cross-file validation (responses + members, roster entries, cancellations)
- Error collection (multiple errors in one file)

---

## Future Enhancements

Potential improvements beyond Phase 2 scope:

1. **Configurable Validation**: Allow users to disable specific validations
2. **Auto-Fix Suggestions**: Provide suggested corrections for common errors
3. **Batch Validation**: Validate multiple files at once with combined report
4. **Web UI Integration**: Display validation errors in web interface (future phases)
5. **Schema Versioning**: Support multiple schema versions for backward compatibility

---

## Summary

This validation layer provides:

- ✅ Comprehensive Pydantic schemas for all input file types
- ✅ Complete error collection (not fail-fast)
- ✅ User-friendly error messages with fix guidance
- ✅ Separation from database validation (clear responsibility)
- ✅ Ready for TDD implementation (Tasks 2.2-2.4)
- ✅ Clean integration path with file_io.py (Task 2.5)
- ✅ Addresses all known validation issues (#005, #030, #031, #032, #034, #036, #037)
