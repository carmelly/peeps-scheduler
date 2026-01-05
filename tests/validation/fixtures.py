"""Shared test data factories for validation test suite.

These factories create default test data that can be customized via overrides.
Use the convention: call factory with defaults, only override what you're testing.

Example:
    member_schema = MemberCsvRowSchema.model_validate(
        member_data({"Email Address": "custom@test.com"}),
        context={"ctx": ctx}
    )
"""


def member_data(overrides: dict | None = None) -> dict:
    """Factory for valid MemberCsvRowSchema test data.

    Creates a default active member with common test values.
    """
    defaults = {
        "id": "1",
        "Name": "Alice Alpha",
        "Display Name": "Alice",
        "Email Address": "alice@test.com",
        "Role": "Leader",
        "Index": "0",
        "Priority": "1",
        "Total Attended": "0",
        "Active": "TRUE",
        "Date Joined": "1/1/2020",
    }
    return {**defaults, **(overrides or {})}


def response_data(overrides: dict | None = None) -> dict:
    """Factory for valid ResponseCsvRowSchema test data.

    Creates a default response with availability and preferences.
    """
    defaults = {
        "Timestamp": "1/1/2020 12:00:00",
        "Name": "Alice Alpha",
        "Display Name": "Alice",
        "Email Address": "alice@test.com",
        "Primary Role": "Leader",
        "Secondary Role": "I only want to be scheduled in my primary role",
        "Max Sessions": "2",
        "Availability": "Saturday January 4 - 1pm",
        "Min Interval Days": "0",
    }
    return {**defaults, **(overrides or {})}


def event_row_data(overrides: dict | None = None) -> dict:
    """Factory for valid EventRowCsvSchema test data.

    Creates a default event row with timing and duration.
    """
    defaults = {
        "Name": "Saturday January 4 - 1pm",
        "Event Duration": "90",
    }
    return {**defaults, **(overrides or {})}


def cancellations_data(overrides: dict | None = None) -> dict:
    """Factory for valid cancellations JSON test data.

    Creates default cancelled events and availability entries.
    """
    defaults = {
        "cancelled_events": [
            "Friday January 10th - 5:30pm to 7pm",  # new format
        ],
        "cancelled_availability": [
            {
                "email": "alice@test.com",
                "events": [
                    "Saturday January 4 - 1pm",  # old format
                    "Friday January 10th - 3pm",  # old with ordinal suffix
                ],
            }
        ],
    }
    return {**defaults, **(overrides or {})}


def attendance_event_data(overrides: dict | None = None) -> dict:
    """Factory for valid AttendanceEventJsonSchema test data.

    Creates a default event with attendee roster.
    """
    defaults = {
        "id": 4,
        "date": "2020-01-04 13:00",
        "duration_minutes": 120,
        "attendees": [
            {"id": 38, "name": "Alice", "role": "leader"},
            {"id": 25, "name": "Bob", "role": "follower"},
        ],
    }
    return {**defaults, **(overrides or {})}


def attendance_data(overrides: dict | None = None) -> dict:
    """Factory for valid ActualAttendanceJsonSchema test data.

    Creates a wrapper with list of attendance events.
    """
    defaults = {
        "valid_events": [attendance_event_data()],
    }
    return {**defaults, **(overrides or {})}


def result_event_data(overrides: dict | None = None) -> dict:
    """Factory for valid ResultEventJsonSchema test data.

    Creates an event with attendees and alternates.
    Inherits from attendance_event_data with additional alternates field.
    """
    defaults = {
        **attendance_event_data(),
        "alternates": [
            {"id": 41, "name": "Dave", "role": "leader"},
            {"id": 27, "name": "Eve", "role": "follower"},
        ],
    }
    return {**defaults, **(overrides or {})}


def results_data(overrides: dict | None = None) -> dict:
    """Factory for valid ResultsJsonSchema test data.

    Creates a wrapper with events, counts, and weight.
    """
    defaults = {
        "valid_events": [result_event_data()],
        "num_unique_attendees": 2,
        "system_weight": 10,
    }
    return {**defaults, **(overrides or {})}


def partnerships_json_data(overrides: dict | None = None) -> dict:
    """Factory for valid partnerships.json file test data."""
    defaults = {
        "19": [20],
        "20": [19],
        "46": [43, 31],
    }
    return {**defaults, **(overrides or {})}
