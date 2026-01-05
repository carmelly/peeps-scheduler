"""Factory functions and validation wrappers for schema-to-domain conversion."""

from pydantic import ValidationError
from peeps_scheduler.constants import DEFAULT_EVENT_DURATION
from peeps_scheduler.models import Event, Peep
from peeps_scheduler.validation.errors import FileValidationError
from peeps_scheduler.validation.fields import ValidationContext
from peeps_scheduler.validation.file_schemas.attendance_json import ActualAttendanceJsonSchema
from peeps_scheduler.validation.file_schemas.cancellations_json import (
    CancellationsJsonSchema,
)
from peeps_scheduler.validation.file_schemas.members_csv import (
    MemberCsvRowSchema,
    MembersCsvFileSchema,
)
from peeps_scheduler.validation.file_schemas.partnerships_json import (
    PartnershipsJsonSchema,
)
from peeps_scheduler.validation.file_schemas.responses_csv import (
    ResponsesCsvFileSchema,
)
from peeps_scheduler.validation.file_schemas.results_json import ResultsJsonSchema
from peeps_scheduler.validation.helpers import normalize_email_for_match
from peeps_scheduler.validation.parsers import EventSpec


def member_to_peep(
    member_data: MemberCsvRowSchema, response_data: ResponsesCsvFileSchema | None = None
) -> Peep:
    """
    Convert validated member and response data to Peep domain object.

    Args:
        member_data: MemberCsvRowSchema from validate_members()
        response_data: ResponsesCsvFileSchema from validate_responses(), or None

    Returns:
        Peep domain object with all fields mapped correctly
    """

    # Base fields from member data
    peep_data = {
        "id": member_data.id,
        "full_name": member_data.full_name,
        "display_name": member_data.display_name,
        "email": member_data.email_address or "",
        "role": member_data.role,
        "index": member_data.index,
        "priority": member_data.priority,
        "total_attended": member_data.total_attended,
        "active": member_data.active,
        "date_joined": member_data.date_joined,
        "responded": response_data is not None,
    }

    # Override/augment with response data if provided
    if response_data:
        response = response_data.responses[0]
        peep_data["role"] = response.primary_role
        peep_data["availability"] = [event.start for event in response.availability]
        peep_data["switch_pref"] = response.secondary_role
        peep_data["event_limit"] = response.max_sessions
        peep_data["min_interval_days"] = response.min_interval_days

    return Peep(**peep_data)


def event_spec_to_event(spec: EventSpec) -> Event:
    """
    Convert validated event spec to Event domain object.
    """
    date = spec.start
    duration_minutes = spec.duration_minutes
    # if spec was validated with no duration, assign the default duration now
    if spec.duration_minutes is None:
        duration_minutes = DEFAULT_EVENT_DURATION
    return Event(
        date=date,
        duration_minutes=duration_minutes,
    )


def convert_to_peeps(
    member_dicts: list[MemberCsvRowSchema], response_dicts: ResponsesCsvFileSchema | dict
) -> list[Peep]:
    """
    Convert validated members + responses to Peep domain objects.

    Matches members with responses by email, uses member_to_peep() factory.

    Args:
        member_dicts: List of validated MemberCsvRowSchema from validate_members()

        response_dicts: Validated ResponsesCsvFileSchema from validate_responses()
                        or empty dict {} for cases with no responses

    Returns:
        List of Peep domain objects with response data integrated
    """
    # Build lookup dict: response by email
    responses_map = {}

    # Handle both schema object and empty dict cases
    if isinstance(response_dicts, ResponsesCsvFileSchema):
        responses_list = response_dicts.responses
    else:
        responses_list = []

    for response in responses_list:
        # Response is from a ResponsesCsvFileSchema
        email = normalize_email_for_match(response.email_address)
        responses_map[email] = response

    peeps = []
    for member in member_dicts:
        email = normalize_email_for_match(member.email_address)

        # Find matching response by email
        matching_response = responses_map.get(email)

        # Pass response schema if found, otherwise pass None
        # If matching response found, wrap in a ResponsesCsvFileSchema
        response_to_pass = None
        if matching_response:
            response_to_pass = ResponsesCsvFileSchema(responses=[matching_response], event_rows=[])
        peep = member_to_peep(member, response_to_pass)
        peeps.append(peep)

    return peeps


def convert_to_events(response_dicts: ResponsesCsvFileSchema | dict) -> list[Event]:
    """
    Convert validated responses data to Event domain objects.

    Deduplicates events by start datetime (multiple people may have same availability).

    Args:
        response_dicts: Validated ResponsesCsvFileSchema from validate_responses()
                        or empty dict {} for cases with no responses

    Returns:
        List of Event domain objects, deduplicated by start datetime
    """

    # Handle empty dict case
    if isinstance(response_dicts, dict):
        return []

    # if responses data includes event rows, only use those to
    # get events for period
    if response_dicts.event_rows:
        return [
            Event(date=event.start_dt, duration_minutes=event.duration_minutes)
            for event in response_dicts.event_rows
        ]

    # if no event rows, gather all events from response
    # availability data
    unique_events_map = {}  # Key: start datetime, Value: Event

    responses_list = response_dicts.responses
    for response in responses_list:
        # Extract availability list from response
        availability_list = response.availability

        for event_spec in availability_list:
            # Event start datetime is a unique identifier
            event_start = event_spec.start
            if event_start not in unique_events_map:
                event = Event(
                    date=event_start,
                    duration_minutes=event_spec.duration_minutes or DEFAULT_EVENT_DURATION,
                )
                unique_events_map[event_start] = event

    return list(unique_events_map.values())


def extract_cancellations(
    validated_cancellations: CancellationsJsonSchema | None,
) -> tuple[set[str], dict[str, set[str]]]:
    """
    Extract cancellation data from validated cancellations.

    Args:
        validated_cancellations: Validated CancellationsJsonSchema, or None if file missing

    Returns:
        Tuple of (cancelled_event_ids, cancelled_availability)
        Returns (set(), {}) if input is None
    """
    if validated_cancellations is None:
        return set(), {}

    cancelled_events = validated_cancellations.cancelled_events
    cancelled_event_ids = set()
    for event in cancelled_events:
        # Extract event_id from start datetime (format: YYYY-MM-DD HH:MM)
        start = event.start
        if start:
            event_id = start.strftime("%Y-%m-%d %H:%M")
            cancelled_event_ids.add(event_id)

    cancelled_avail = validated_cancellations.cancelled_availability
    cancelled_availability = {}
    for avail in cancelled_avail:
        email = avail.email
        if not email:
            continue

        events = avail.events
        for event in events:
            start = event.start
            if start:
                event_id = start.strftime("%Y-%m-%d %H:%M")
                if email not in cancelled_availability:
                    cancelled_availability[email] = set()
                cancelled_availability[email].add(event_id)

    return cancelled_event_ids, cancelled_availability


def extract_partnerships(
    validated_partnerships: PartnershipsJsonSchema | None,
) -> dict[int, set[int]]:
    """
    Extract partnership mapping from validated partnerships.

    Args:
        validated_partnerships: Validated PartnershipsJsonSchema, or None if file missing

    Returns:
        Dict mapping requester_id → set of partner_ids
        Returns {} if input is None
    """
    if validated_partnerships is None:
        return {}

    partnerships = {}
    partnerships_list = validated_partnerships.partnerships
    for partnership in partnerships_list:
        requester_id = partnership.requester_id
        target_ids = partnership.target_ids
        if requester_id:
            partnerships[requester_id] = set(target_ids) if target_ids else set()

    return partnerships


def validate_members(
    raw_rows: list[dict], file_path: str, context: dict | None = None
) -> list[MemberCsvRowSchema]:
    """
    Validate members CSV data and return validated schema objects.

    Args:
        raw_rows: Raw member CSV rows as dicts
        file_path: Path to file being validated (for error context)
        context: Optional validation context dict

    Returns:
        List of validated MemberCsvRowSchema objects

    Raises:
        FileValidationError: If validation fails
    """
    try:
        return MembersCsvFileSchema.model_validate(raw_rows).root
    except ValidationError as e:
        raise FileValidationError(file_path, e) from e


def validate_responses(
    raw_data: dict, ctx: ValidationContext, file_path: str
) -> ResponsesCsvFileSchema:
    """
    Validate responses CSV data and return validated schema object.

    Args:
        raw_data: Raw responses data with 'responses' and 'event_rows' keys
        ctx: Validation context (year, timezone)
        file_path: Path to file being validated (for error context)

    Returns:
        Validated ResponsesCsvFileSchema object

    Raises:
        FileValidationError: If validation fails
    """
    try:
        schema = ResponsesCsvFileSchema.model_validate(raw_data, context={"ctx": ctx})
        return schema
    except ValidationError as e:
        raise FileValidationError(file_path, e) from e


def validate_cancellations(
    raw_data: dict, ctx: ValidationContext, file_path: str
) -> CancellationsJsonSchema:
    """
    Validate cancellations JSON data and return validated schema object.

    Args:
        raw_data: Raw cancellations data
        ctx: Validation context (year, timezone)
        file_path: Path to file being validated (for error context)

    Returns:
        Validated CancellationsJsonSchema object

    Raises:
        FileValidationError: If validation fails
    """
    try:
        schema = CancellationsJsonSchema.model_validate(raw_data, context={"ctx": ctx})
        return schema
    except ValidationError as e:
        raise FileValidationError(file_path, e) from e


def validate_partnerships(raw_data: dict, file_path: str) -> PartnershipsJsonSchema:
    """
    Validate partnerships JSON data and return validated schema object.

    Accepts partnerships.json format: a flat dict where each key is a requester_id
    (string) and each value is a list of target member IDs.
    Example: {"19": [20], "20": [19], "46": [43, 31]}

    Args:
        raw_data: Raw partnerships data (flat dict)
        file_path: Path to file being validated (for error context)

    Returns:
        Validated PartnershipsJsonSchema object

    Raises:
        FileValidationError: If validation fails
    """
    try:
        schema = PartnershipsJsonSchema.model_validate(raw_data)
        return schema
    except ValidationError as e:
        raise FileValidationError(file_path, e) from e


def validate_attendance(
    raw_data: dict, ctx: ValidationContext, file_path: str
) -> ActualAttendanceJsonSchema:
    """
    Validate attendance JSON data and return validated schema object.

    Args:
        raw_data: Raw attendance data
        ctx: Validation context (year, timezone)
        file_path: Path to file being validated (for error context)

    Returns:
        Validated ActualAttendanceJsonSchema object

    Raises:
        FileValidationError: If validation fails
    """
    try:
        # Transform "events" to "valid_events" for schema compatibility
        # transformed_data = {**raw_data, "valid_events": raw_data.get("events", [])}
        schema = ActualAttendanceJsonSchema.model_validate(raw_data, context={"ctx": ctx})
        return schema
    except ValidationError as e:
        raise FileValidationError(file_path, e) from e


def validate_results(raw_data: dict, ctx: ValidationContext, file_path: str) -> ResultsJsonSchema:
    """
    Validate results JSON data and return validated ResultsJsonSchema object.

    Args:
        raw_data: Raw results data
        ctx: Validation context (year, timezone)
        file_path: Path to file being validated (for error context)

    Returns:
        Validated ResultsJsonSchema object

    Raises:
        FileValidationError: If validation fails
    """
    try:
        schema = ResultsJsonSchema.model_validate(raw_data, context={"ctx": ctx})
        return schema
    except ValidationError as e:
        raise FileValidationError(file_path, e) from e
