"""Factory functions and validation wrappers for schema-to-domain conversion."""

from peeps_scheduler.constants import DEFAULT_EVENT_DURATION
from peeps_scheduler.models import (
    CancelledMemberAvailability,
    Event,
    PartnershipRequest,
    Peep,
    SwitchPreference,
)
from peeps_scheduler.validation.file_schemas.members_csv import MemberCsvRowSchema
from peeps_scheduler.validation.file_schemas.period import (
    CancelledAvailabilityJsonSchema,
    PartnershipRequestJsonSchema,
)
from peeps_scheduler.validation.file_schemas.responses_csv import (
    ResponsesCsvFileSchema,
)
from peeps_scheduler.validation.helpers import normalize_email_for_match
from peeps_scheduler.validation.parsers import EventSpec


def _member_to_peep(
    member_data: MemberCsvRowSchema, response_data: ResponsesCsvFileSchema | None = None
) -> Peep:
    """
    Convert validated member and response data to Peep domain object.

    Args:
        member_data: Validated MemberCsvRowSchema
        response_data: Validated ResponsesCsvFileSchema, or None

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
        peep_data["switch_pref"] = response.secondary_role or SwitchPreference.PRIMARY_ONLY
        peep_data["event_limit"] = response.max_sessions
        peep_data["min_interval_days"] = response.min_interval_days

    return Peep(**peep_data)


def _event_spec_to_event(spec: EventSpec) -> Event:
    """
    Convert validated event spec to Event domain object.
    """
    duration_minutes = spec.duration_minutes
    # if spec was validated with no duration, assign the default duration now
    if spec.duration_minutes is None:
        duration_minutes = DEFAULT_EVENT_DURATION
    return Event(
        date=spec.start,
        duration_minutes=duration_minutes,
    )


def build_peeps(
    member_dicts: list[MemberCsvRowSchema], response_dicts: ResponsesCsvFileSchema | dict
) -> list[Peep]:
    """
    Convert validated members + responses to Peep domain objects.

    Matches members with responses by email, uses member_to_peep() factory.

    Args:
        member_dicts: List of validated MemberCsvRowSchema objects

        response_dicts: Validated ResponsesCsvFileSchema or empty dict {} for cases with no responses

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

        peep = _member_to_peep(member, response_to_pass)
        peeps.append(peep)

    return peeps


def build_events(
    event_specs: list[EventSpec],
) -> list[Event]:
    """
    Build domain dataclasses from validated schemas.

    Args:
        event_specs: Validated EventSpec list
    Returns:
        Event objects
    """
    events = [_event_spec_to_event(spec) for spec in event_specs]
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
