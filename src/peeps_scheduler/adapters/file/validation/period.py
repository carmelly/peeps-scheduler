from zoneinfo import ZoneInfo
from pydantic import BaseModel, RootModel, ValidationError
from peeps_scheduler.adapters.file.validation.errors import PeriodValidationError, ValidationFailure
from peeps_scheduler.adapters.file.validation.fields import EventSpecList, ValidationContext
from peeps_scheduler.adapters.file.validation.file_schemas import (
    ActualAttendanceJsonSchema,
    MembersCsvFileSchema,
    PeriodConfigJsonSchema,
    ResponsesCsvFileSchema,
    ResultsJsonSchema,
)
from peeps_scheduler.adapters.file.validation.file_schemas.attendance_json import (
    RosterEntryJsonSchema,
)
from peeps_scheduler.adapters.file.validation.file_schemas.members_csv import MemberCsvRowSchema
from peeps_scheduler.adapters.file.validation.file_schemas.period_config import (
    CancelledAvailabilityJsonSchema,
    PartnershipRequestJsonSchema,
)
from peeps_scheduler.adapters.file.validation.file_schemas.responses_csv import ResponseCsvRowSchema
from peeps_scheduler.adapters.file.validation.helpers import (
    normalize_email_for_match,
    normalize_topic,
)
from peeps_scheduler.adapters.file.validation.parsers import EventSpec


def validate_period(file_dict: dict[str, dict], year: int, tz: ZoneInfo) -> dict[str, dict]:
    """Validate all period-related files.

    Args:
        file_dict: Dictionary mapping filenames to raw file data.
        year: Year of the period (for context).
        tz: Timezone of the period (for context).

    Returns:
        validated_data: Dictionary mapping filenames to validated data.

    Raises:
        PeriodValidationError: If any validation errors occur.
    """
    ctx = ValidationContext(year=year, tz=tz)

    # Validate individual files -- raises PeriodValidationError on errors
    validated_schemas = _validate_files(file_dict, ctx)

    # Perform cross-file validation -- raises PeriodValidationError on errors
    _validate_cross_file(validated_schemas, ctx)

    # Don't catch errors here; let them propagate

    return validated_schemas


def _validate_files(file_dict: dict[str, dict], ctx: ValidationContext) -> dict[str, dict]:
    """Validate individual files in the period.
    Args:
        file_dict: Dictionary mapping filenames to raw file data.
        ctx: Validation context.
    Returns:
        validated_schemas: Dictionary mapping filenames to validated data.
    Raises:
        PeriodValidationError: If any validation errors occur.
    """
    failures: list[ValidationFailure] = []

    # Validate required files
    members_filename = "members.csv"
    members_schema, members_error = _validate_file(
        MembersCsvFileSchema, file_dict.get(members_filename, {}), members_filename, ctx
    )
    if members_error is not None:
        failures.append(members_error)

    responses_filename = "responses.csv"
    responses_schema, responses_error = _validate_file(
        ResponsesCsvFileSchema, file_dict.get(responses_filename, {}), responses_filename, ctx
    )
    if responses_error is not None:
        failures.append(responses_error)

    results_filename = "results.json"
    results_schema = None
    if (results_data := file_dict.get(results_filename)) is not None:
        results_schema, results_error = _validate_file(
            ResultsJsonSchema, results_data, results_filename, ctx
        )
        if results_error is not None:
            failures.append(results_error)

    attendance_filename = "actual_attendance.json"
    attendance_schema = None
    if (attendance_data := file_dict.get(attendance_filename)) is not None:
        attendance_schema, attendance_error = _validate_file(
            ActualAttendanceJsonSchema, attendance_data, attendance_filename, ctx
        )
        if attendance_error is not None:
            failures.append(attendance_error)

    config_filename = "period_config.json"
    config_schema = PeriodConfigJsonSchema(model_validate={})  # default empty config
    if (config_data := file_dict.get(config_filename)) is not None:
        config_schema, config_error = _validate_file(
            PeriodConfigJsonSchema, config_data, config_filename, ctx
        )
        if config_error is not None:
            failures.append(config_error)

    if failures:
        raise PeriodValidationError(failures)

    # all files validated successfully, return validated data
    return {
        members_filename: members_schema,
        responses_filename: responses_schema,
        results_filename: results_schema,
        attendance_filename: attendance_schema,
        config_filename: config_schema,
    }


def _validate_file(
    schema_class, raw_data: dict, filename: str, ctx: ValidationContext
) -> tuple[dict, ValidationError]:
    """Validate raw_data against schema_class, returning a tuple of the validated schema and any validation error."""
    schema = None
    error = None
    try:
        schema = schema_class.model_validate(raw_data, context={"ctx": ctx})
    except ValidationError as exc:
        error = ValidationFailure.from_file_error(filename, exc)
    return schema, error


def _validate_cross_file(
    validated_schemas: dict[str, BaseModel | RootModel], ctx: ValidationContext
) -> None:
    """Validate constraints across files, collecting any validation failures.

    Args:
        validated_schemas: Dictionary mapping filenames to validated schema instances.
        ctx: Validation context.
    Raises:
        PeriodValidationError: If any cross-file validation errors occur.
    """
    # Directly access required schemas
    members_schema: MembersCsvFileSchema = validated_schemas["members.csv"]
    responses_schema: ResponsesCsvFileSchema = validated_schemas["responses.csv"]
    # Default to None for optional schemas
    results_schema: ResultsJsonSchema | None = validated_schemas.get("results.json")
    attendance_schema: ActualAttendanceJsonSchema | None = validated_schemas.get(
        "actual_attendance.json"
    )
    config_schema: PeriodConfigJsonSchema | None = validated_schemas.get("period_config.json")
    failures: list[ValidationFailure] = []
    member_rows = members_schema.root
    member_by_id = {row.id: row for row in members_schema.root}
    member_emails = {normalize_email_for_match(row.email_address) for row in member_rows}

    # Validate responses reference known members
    try:
        validate_response_members(member_rows, responses_schema.responses)
    except ValueError as exc:
        failures.append(
            ValidationFailure.from_cross_file_error(
                ["responses.csv", "members.csv"],
                str(exc),
            )
        )

    member_availability_by_email = {
        normalize_email_for_match(row.email_address): row.availability
        for row in responses_schema.responses
    }

    event_starts = {event.start for event in responses_schema.events}

    # Validate roster entries (results/attendance)
    roster_entries: list[RosterEntryJsonSchema] = []
    if results_schema:
        for event in results_schema.valid_events:
            roster_entries.extend(event.attendees)
            roster_entries.extend(event.alternates)

    if attendance_schema:
        for event in attendance_schema.valid_events:
            roster_entries.extend(event.attendees)

    try:
        validate_roster_entries(member_by_id, roster_entries)
    except ValueError as exc:
        related_files = ["members.csv"]
        if results_schema:
            related_files.append("results.json")
        if attendance_schema:
            related_files.append("actual_attendance.json")
        failures.append(
            ValidationFailure.from_cross_file_error(
                related_files,
                str(exc),
            )
        )

    # Validate config file references
    if config_schema is None:
        config_schema = PeriodConfigJsonSchema.model_validate(
            {}
        )  # empty config for validation purposes

    try:
        validate_partnerships(member_emails, config_schema.partnership_requests)
    except ValueError as exc:
        failures.append(
            ValidationFailure.from_cross_file_error(
                ["period_config.json", "members.csv"],
                str(exc),
            )
        )

    try:
        validate_cancellations(
            event_starts,
            member_emails,
            member_availability_by_email,
            config_schema.cancelled_events,
            config_schema.cancelled_member_availability,
        )
    except ValueError as exc:
        failures.append(
            ValidationFailure.from_cross_file_error(
                ["period_config.json", "responses.csv", "members.csv"],
                str(exc),
            )
        )

    try:
        validate_topics(config_schema.topics)
    except ValueError as exc:
        failures.append(
            ValidationFailure.from_cross_file_error(
                ["period_config.json"],
                str(exc),
            )
        )

    # Validate topic consistency between responses and config
    has_response_topics = any(response.deep_dive_topics for response in responses_schema.responses)
    if config_schema.topics and not has_response_topics:
        failures.append(
            ValidationFailure.from_cross_file_error(
                ["period_config.json", "responses.csv"],
                "Deep Dive Topics missing from responses.csv",
            )
        )
    if has_response_topics and not config_schema.topics:
        failures.append(
            ValidationFailure.from_cross_file_error(
                ["period_config.json", "responses.csv"],
                "topics missing from period_config.json",
            )
        )

    if not failures:
        filter_response_topics(responses_schema.responses, config_schema.topics)

    # Validate event references in results/attendance
    try:
        validate_event_references(
            event_starts,
            results_schema,
            attendance_schema,
        )
    except ValueError as exc:
        related_files = ["responses.csv"]
        if results_schema:
            related_files.append("results.json")
        if attendance_schema:
            related_files.append("actual_attendance.json")
        failures.append(
            ValidationFailure.from_cross_file_error(
                related_files,
                str(exc),
            )
        )

    if failures:
        raise PeriodValidationError(failures)


def validate_response_members(
    member_rows: list[MemberCsvRowSchema],
    responses: list[ResponseCsvRowSchema],
) -> None:
    """Ensure responses reference active members in the roster."""
    member_by_email = {normalize_email_for_match(row.email_address): row for row in member_rows}
    missing_emails: list[str] = []
    inactive_names: list[str] = []

    for response in responses:
        normalized = normalize_email_for_match(response.email_address)
        member = member_by_email.get(normalized)
        if not member:
            missing_emails.append(response.email_address)
            continue
        if not member.active:
            inactive_names.append(response.full_name)

    if missing_emails:
        raise ValueError(f"response email not found: {sorted(set(missing_emails))}")
    if inactive_names:
        raise ValueError(f"response from inactive member: {sorted(set(inactive_names))}")


def validate_roster_entries(
    member_by_id: dict[int, MemberCsvRowSchema],
    entries: list[RosterEntryJsonSchema],
) -> None:
    """Validate roster entries reference real members and match names."""
    missing_roster_ids: list[int] = []
    name_mismatches: list[int] = []

    for entry in entries:
        member = member_by_id.get(entry.id)
        if not member:
            missing_roster_ids.append(entry.id)
            continue
        if member.display_name:
            expected = member.display_name
            if entry.name.casefold() != expected.casefold():
                name_mismatches.append(entry.id)
        else:
            expected = member.full_name
            if entry.name.casefold() != expected.casefold():
                name_mismatches.append(entry.id)

    if missing_roster_ids:
        raise ValueError(f"roster id not found: {sorted(set(missing_roster_ids))}")
    if name_mismatches:
        raise ValueError(f"display name mismatch for roster id(s): {sorted(set(name_mismatches))}")


def validate_partnerships(
    member_emails: set[str],
    partnerships: list[PartnershipRequestJsonSchema] | None,
) -> None:
    """Ensure partnership requests reference valid member emails and are unique."""
    if not partnerships:
        return

    requester_emails = set(request.requester_email for request in partnerships)
    if len(requester_emails) != len(partnerships):
        raise ValueError("duplicate requester email in partnerships")

    for request in partnerships:
        requester_email = request.requester_email
        if normalize_email_for_match(requester_email) not in member_emails:
            raise ValueError(f"requester email not found: {requester_email}")

        missing_emails = [
            target_email
            for target_email in request.target_emails
            if normalize_email_for_match(target_email) not in member_emails
        ]
        if missing_emails:
            raise ValueError(f"target email not found: {sorted(set(missing_emails))}")


def validate_topics(topics: list[str] | None) -> None:
    """Ensure topics are non-empty strings with no normalized duplicates."""
    if not topics:
        return

    normalized_lookup: dict[str, str] = {}
    for topic in topics:
        if not isinstance(topic, str):
            raise ValueError("topics must be strings")
        normalized = normalize_topic(topic)
        if not normalized:
            raise ValueError("topics cannot be blank")
        if normalized in normalized_lookup and normalized_lookup[normalized] != topic:
            raise ValueError(
                "topics contains duplicate entries after normalization: "
                f"'{normalized_lookup[normalized]}' and '{topic}'"
            )
        normalized_lookup.setdefault(normalized, topic)


def filter_response_topics(responses: list, topics: list[str] | None) -> None:
    """Filter response deep_dive_topics to only those in the period topic list."""
    if not topics:
        for response in responses:
            response.deep_dive_topics = []
        return

    lookup = {normalize_topic(topic): topic for topic in topics}
    for response in responses:
        filtered = []
        for topic in response.deep_dive_topics:
            normalized = normalize_topic(topic)
            if normalized in lookup:
                filtered.append(lookup[normalized])
        response.deep_dive_topics = filtered


def validate_cancellations(
    event_starts: set,
    member_emails: set[str],
    member_availability_by_email: dict[str, EventSpecList],
    cancelled_events: list[EventSpec] | None,
    cancelled_availability: list[CancelledAvailabilityJsonSchema] | None,
) -> None:
    """Ensure cancellation references map to known events and members."""
    if not event_starts:
        if cancelled_events or cancelled_availability:
            raise ValueError("cancellations require responses with events")
        return

    if cancelled_events:
        missing_cancelled = [
            event.raw for event in cancelled_events if event.start not in event_starts
        ]
        if missing_cancelled:
            raise ValueError(f"cancelled event not found: {missing_cancelled}")

    if cancelled_availability:
        missing_emails = [
            entry.member_email
            for entry in cancelled_availability
            if normalize_email_for_match(entry.member_email) not in member_emails
        ]
        if missing_emails:
            raise ValueError(
                f"cancelled availability email not found: {sorted(set(missing_emails))}"
            )

        missing_events = []
        for entry in cancelled_availability:
            for event in entry.events:
                if event.start not in event_starts:
                    missing_events.append(event.raw)
        if missing_events:
            raise ValueError(f"cancelled availability event not found: {missing_events}")

        # Check that cancelled events were in the member's original availability
        for entry in cancelled_availability:
            member_email_norm = normalize_email_for_match(entry.member_email)
            member_avail = member_availability_by_email.get(member_email_norm, [])
            member_starts = {event.start for event in member_avail}
            missing_member_events = [
                event.raw for event in entry.events if event.start not in member_starts
            ]
            if missing_member_events:
                raise ValueError(
                    f"cancelled availability event not in member's original availability for {entry.member_email}: {missing_member_events}"
                )


def validate_event_references(
    event_starts: set,
    results: ResultsJsonSchema | None,
    attendance: ActualAttendanceJsonSchema | None,
) -> None:
    """
    Ensure results and attendance event dates match extracted responses.events.

    Note: Attendance events can exist without responses (apply-results workflow),
    but results always require responses since they represent scheduled output.
    """
    if results:
        if not event_starts:
            raise ValueError("results require responses with events")
        missing_result_events = [
            event.start_dt for event in results.valid_events if event.start_dt not in event_starts
        ]
        if missing_result_events:
            raise ValueError(f"result event not found: {missing_result_events}")

    if attendance and event_starts:
        missing_attendance_events = [
            event.start_dt
            for event in attendance.valid_events
            if event.start_dt not in event_starts
        ]
        if missing_attendance_events:
            raise ValueError(f"attendance event not found: {missing_attendance_events}")
