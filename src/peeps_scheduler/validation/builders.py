"""Factory functions and validation wrappers for schema-to-domain conversion."""

from peeps_scheduler.models import CancelledMemberAvailability, Event, PartnershipRequest, Peep
from peeps_scheduler.validation.file_schemas.attendance_json import ActualAttendanceJsonSchema
from peeps_scheduler.validation.file_schemas.members_csv import MemberCsvRowSchema
from peeps_scheduler.validation.file_schemas.period import (
    CancelledAvailabilityJsonSchema,
    PartnershipRequestJsonSchema,
)
from peeps_scheduler.validation.file_schemas.responses_csv import (
    ResponsesCsvFileSchema,
)
from peeps_scheduler.validation.file_schemas.results_json import ResultsJsonSchema
from peeps_scheduler.validation.helpers import normalize_email_for_match
from peeps_scheduler.validation.parsers import EventSpec


def _member_to_peep(
    member_data: MemberCsvRowSchema,
    response_data: ResponsesCsvFileSchema | None = None,
    events_by_datetime: dict | None = None,
) -> Peep:
    """
    Convert validated member and response data to Peep domain object.

    Args:
        member_data: Validated MemberCsvRowSchema
        response_data: Validated ResponsesCsvFileSchema, or None
        events_by_datetime: Event lookup by start datetime

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
        if events_by_datetime is None:
            raise ValueError(
                "events_by_datetime is required when response_data is provided "
                f"for member id={member_data.id}, email={member_data.email_address!r}"
            )
        peep_data["availability"] = [
            events_by_datetime[event.start] for event in response.availability
        ]
        peep_data["switch_pref"] = response.secondary_role
        peep_data["event_limit"] = response.max_sessions
        peep_data["min_interval_days"] = response.min_interval_days
        peep_data["topic_votes"] = response.deep_dive_topics

    return Peep(**peep_data)


def _event_spec_to_event(event_id: int, spec: EventSpec) -> Event:
    """
    Convert validated event spec to Event domain object.
    """
    return Event(id=event_id, date=spec.start, duration_minutes=spec.duration_minutes)


def build_peeps(
    member_dicts: list[MemberCsvRowSchema],
    response_dicts: ResponsesCsvFileSchema | dict,
    events: list[Event],
) -> list[Peep]:
    """
    Convert validated members + responses to Peep domain objects.

    Matches members with responses by email, uses member_to_peep() factory.

    Args:
        member_dicts: List of validated MemberCsvRowSchema objects

        response_dicts: Validated ResponsesCsvFileSchema or empty dict {} for cases with no responses
        events: Event objects used to resolve availability

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
    events_by_datetime = {event.date: event for event in events}

    for member in member_dicts:
        email = normalize_email_for_match(member.email_address)

        # Find matching response by email
        matching_response = responses_map.get(email)

        # Pass response schema if found, otherwise pass None
        # If matching response found, wrap in a ResponsesCsvFileSchema
        response_to_pass = None
        if matching_response:
            response_to_pass = ResponsesCsvFileSchema(responses=[matching_response], event_rows=[])

        peep = _member_to_peep(member, response_to_pass, events_by_datetime)
        peeps.append(peep)

    return peeps


def build_events(event_specs: list[EventSpec], preserve_order: bool) -> list[Event]:
    """
    Build Event domain objects from validated EventSpec instances.

    Args:
        event_specs: Validated EventSpec list.
        preserve_order: Controls whether the resulting events keep the original
            input order or are sorted chronologically.

            - When event rows (e.g. explicit `event_rows` in the input file) are
              present and their order should be preserved, call this function
              with ``preserve_order=True`` so that the output list matches the
              input ordering.
            - When there are no such event rows and a chronological schedule is
              desired, call this function with ``preserve_order=False`` so that
              events are sorted by their start datetime.

    Returns:
        List of Event objects.
    """
    ordered_specs = (
        event_specs if preserve_order else sorted(event_specs, key=lambda spec: spec.start)
    )
    events = [_event_spec_to_event(index, spec) for index, spec in enumerate(ordered_specs)]
    return events


def build_cancelled_events(
    cancelled_event_specs: list[EventSpec], events: list[Event]
) -> list[Event]:
    """
    Build domain dataclasses from validated schemas by resolving object references.

    Args:
        cancelled_event_specs: Validated EventSpec list
        events: Event objects to resolve EventSpecs against
    Returns:
        list of Event object references
    """
    # maps events by start datetime for lookup
    events_by_datetime = {event.date: event for event in events}

    # since period is already validated, all references should resolve
    cancelled_events = [events_by_datetime.get(spec.start) for spec in cancelled_event_specs]
    return cancelled_events


def build_cancelled_availability(
    schemas: list[CancelledAvailabilityJsonSchema], peeps: list[Peep], events: list[Event]
) -> list[CancelledMemberAvailability]:
    """
    Build domain dataclasses from validated schemas by resolving object references.

    Args:
        schemas: Validated CancelledAvailabilityJsonSchema list (emails/EventSpecs parsed)
        peeps: Peep objects to resolve emails against
        events: Event objects to resolve EventSpecs against

    Returns:
        CancelledMemberAvailability dataclasses with Peep/Event object references
    """
    # maps peeps by normalized email for lookup
    peeps_by_email = {normalize_email_for_match(peep.email): peep for peep in peeps}
    # maps events by start datetime for lookup
    events_by_datetime = {event.date: event for event in events}

    # since period is already validated, all references should resolve
    cancelled_availability_list = [
        CancelledMemberAvailability(
            peep=peeps_by_email.get(normalize_email_for_match(schema.member_email)),
            events=[events_by_datetime.get(event_spec.start) for event_spec in schema.events],
        )
        for schema in schemas
    ]
    return cancelled_availability_list


def build_partnerships(
    schemas: list[PartnershipRequestJsonSchema], peeps: list[Peep]
) -> list[PartnershipRequest]:
    """
    Build domain dataclasses from validated schemas by resolving object references.

    Args:
        schemas: Validated PartnershipRequestJsonSchema list
        peeps: Peep objects to resolve emails against

    Returns:
        PartnershipRequest dataclasses with Peep object references
    """
    # maps peeps by normalized email for lookup
    peeps_by_email = {normalize_email_for_match(peep.email): peep for peep in peeps}

    # since period is already validated, all references should resolve
    partnership_requests = [
        PartnershipRequest(
            requester=peeps_by_email.get(normalize_email_for_match(schema.requester_email)),
            target_peeps=[
                peeps_by_email.get(normalize_email_for_match(target_email))
                for target_email in schema.target_emails
            ],
        )
        for schema in schemas
    ]
    return partnership_requests


def build_attendance_events(
    attendance: ActualAttendanceJsonSchema | None, peeps: list[Peep]
) -> list[Event]:
    """Build Event objects from validated attendance data."""
    if attendance is None:
        return []
    peeps_by_id = {peep.id: peep for peep in peeps}
    events = []
    for attendance_event in attendance.valid_events:
        event = Event(
            id=attendance_event.legacy_id,
            date=attendance_event.start_dt,
            duration_minutes=attendance_event.duration_minutes,
            topic=attendance_event.topic,
        )
        for attendee in attendance_event.attendees:
            peep = peeps_by_id[attendee.id]
            event.add_attendee(peep, attendee.role)
        events.append(event)
    return events


def build_results_events(results: ResultsJsonSchema | None, peeps: list[Peep]) -> list[Event]:
    """Build Event objects from validated results data."""
    if results is None:
        return []
    peeps_by_id = {peep.id: peep for peep in peeps}
    events = []
    for result_event in results.valid_events:
        event = Event(
            id=result_event.legacy_id,
            date=result_event.start_dt,
            duration_minutes=result_event.duration_minutes,
            topic=result_event.topic,
        )
        for attendee in result_event.attendees:
            peep = peeps_by_id[attendee.id]
            event.add_attendee(peep, attendee.role)
        for alternate in result_event.alternates:
            peep = peeps_by_id[alternate.id]
            event.add_alternate(peep, alternate.role)
        events.append(event)
    return events
