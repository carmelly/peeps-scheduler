from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from peeps_scheduler.validation.fields import EmailAddressStr, EventSpecList
from peeps_scheduler.validation.file_schemas.attendance_json import (
    ActualAttendanceJsonSchema,
    RosterEntryJsonSchema,
)
from peeps_scheduler.validation.file_schemas.members_csv import (
    MemberCsvRowSchema,
    MembersCsvFileSchema,
)
from peeps_scheduler.validation.file_schemas.responses_csv import ResponsesCsvFileSchema
from peeps_scheduler.validation.file_schemas.results_json import ResultsJsonSchema
from peeps_scheduler.validation.helpers import normalize_email_for_match
from peeps_scheduler.validation.parsers import EventSpec


class CancelledAvailabilityJsonSchema(BaseModel):
    """Schema for member's cancelled availability (email-based)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    member_email: EmailAddressStr
    events: EventSpecList

    @model_validator(mode="before")
    @classmethod
    def check_required_fields(cls, data):
        """Check that required fields are present."""
        if not isinstance(data, dict):
            return data

        if "member_email" not in data:
            raise ValueError("member_email is required")
        if "events" not in data:
            raise ValueError("events is required")

        return data


class PartnershipRequestJsonSchema(BaseModel):
    """Schema for individual partnership request (email-based)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    requester_email: EmailAddressStr
    target_emails: list[EmailAddressStr]

    @field_validator("target_emails", mode="after")
    @classmethod
    def validate_no_self_partnership(cls, v, info):
        """Ensure requester_email is NOT in target_emails."""
        if info.data.get("requester_email") in v:
            raise ValueError("requester cannot be in target_emails")
        return v


class PeriodFileSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")

    members: MembersCsvFileSchema
    responses: ResponsesCsvFileSchema
    results: ResultsJsonSchema | None = None
    attendance: ActualAttendanceJsonSchema = None
    cancelled_events: EventSpecList = []
    cancelled_member_availability: list[CancelledAvailabilityJsonSchema] = []
    partnership_requests: list[PartnershipRequestJsonSchema] = []

    @model_validator(mode="after")
    def validate_cross_file(self):
        member_rows = self.members.root
        member_by_id = {row.id: row for row in member_rows}
        member_emails = {normalize_email_for_match(row.email_address) for row in member_rows}

        response_emails = [
            normalize_email_for_match(row.email_address) for row in self.responses.responses
        ]
        validate_response_emails(member_emails, response_emails)

        member_availability_by_email = {
            normalize_email_for_match(row.email_address): row.availability
            for row in self.responses.responses
        }

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
        validate_partnerships(member_emails, self.partnership_requests)
        validate_cancellations(
            event_starts,
            member_emails,
            member_availability_by_email,
            self.cancelled_events,
            self.cancelled_member_availability,
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


def validate_cancellations(
    event_starts: set,
    member_emails: set[str],
    member_availability_by_email: dict[str, EventSpecList],
    cancelled_events: list[EventSpec] | None,
    cancelled_availability: list[CancelledAvailabilityJsonSchema] | None,
) -> None:
    """Ensure cancellation references map to known events and members."""
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
    """Ensure results and attendance event dates match extracted responses.events."""
    if results:
        missing_result_events = [
            event.start_dt for event in results.valid_events if event.start_dt not in event_starts
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
