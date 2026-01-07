from pydantic import BaseModel, ConfigDict, model_validator
from peeps_scheduler.validation.file_schemas.attendance_json import (
    ActualAttendanceJsonSchema,
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
from peeps_scheduler.validation.file_schemas.responses_csv import ResponsesCsvFileSchema
from peeps_scheduler.validation.file_schemas.results_json import ResultsJsonSchema
from peeps_scheduler.validation.helpers import normalize_email_for_match


class PeriodFileSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")

    members: MembersCsvFileSchema
    responses: ResponsesCsvFileSchema
    results: ResultsJsonSchema | None = None
    attendance: ActualAttendanceJsonSchema | None = None
    cancelled_events: CancelledEventJsonSchema | None = None
    cancelled_availability: list[CancelledAvailabilityJsonSchema] | None = None
    partnerships: list[PartnershipRequestJsonSchema] | None = None

    @model_validator(mode="after")
    def validate_cross_file(self):
        member_rows = self.members.root
        member_ids = {row.id for row in member_rows}
        member_by_id = {row.id: row for row in member_rows}
        member_emails = {normalize_email_for_match(row.email_address) for row in member_rows}

        response_emails = [
            normalize_email_for_match(row.email_address) for row in self.responses.responses
        ]
        validate_response_emails(member_emails, response_emails)

        event_starts = {event.start for event in self.responses.events}

        roster_entries: list[RosterEntryJsonSchema] = []

        if self.results:
            for event in self.results.valid_events:
                roster_entries.extend(event.attendees)
                roster_entries.extend(event.alternates)

        if self.attendance:
            for event in self.attendance.valid_events:
                roster_entries.extend(event.attendees)

        validate_roster_entries(member_by_id, roster_entries)
        validate_partnerships(member_ids, self.partnerships)
        validate_cancellations(
            event_starts,
            member_emails,
            self.cancelled_events,
            self.cancelled_availability,
        )
        validate_event_references(
            event_starts,
            self.results,
            self.attendance,
        )

        return self


def validate_response_emails(member_emails: set[str], response_emails: list[str]) -> None:
    """Ensure response emails exist in the member roster."""
    missing_responses = [email for email in response_emails if email not in member_emails]
    if missing_responses:
        raise ValueError(f"response email not found: {sorted(set(missing_responses))}")


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
    member_ids: set[int],
    partnerships: list[PartnershipRequestJsonSchema] | None,
) -> None:
    """Ensure partnership requests reference valid member ids."""
    if not partnerships:
        return

    for request in partnerships:
        if request.requester_id not in member_ids:
            raise ValueError(f"requester id not found: {request.requester_id}")
        missing_partners = [
            partner_id for partner_id in request.target_ids if partner_id not in member_ids
        ]
        if missing_partners:
            raise ValueError(f"partner id not found: {sorted(set(missing_partners))}")


def validate_cancellations(
    event_starts: set,
    member_emails: set[str],
    cancelled_events: CancelledEventJsonSchema | None,
    cancelled_availability: list[CancelledAvailabilityJsonSchema] | None,
) -> None:
    """Ensure cancellation references map to known events and members."""
    if cancelled_events:
        missing_cancelled = [
            event.raw
            for event in cancelled_events.cancelled_events
            if event.start not in event_starts
        ]
        if missing_cancelled:
            raise ValueError(f"cancelled event not found: {missing_cancelled}")

    if cancelled_availability:
        missing_emails = [
            entry.email
            for entry in cancelled_availability
            if normalize_email_for_match(entry.email) not in member_emails
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


def validate_event_references(
    event_starts: set,
    results: ResultsJsonSchema | None,
    attendance: ActualAttendanceJsonSchema | None,
) -> None:
    """Ensure results and attendance event dates match extracted responses.events."""
    if results:
        missing_result_events = [
            event.start_dt
            for event in results.valid_events
            if event.start_dt not in event_starts
        ]
        if missing_result_events:
            raise ValueError(f"result event not found: {missing_result_events}")

    if attendance:
        missing_attendance_events = [
            event.start_dt
            for event in attendance.valid_events
            if event.start_dt not in event_starts
        ]
        if missing_attendance_events:
            raise ValueError(f"attendance event not found: {missing_attendance_events}")
