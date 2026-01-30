import datetime
from collections import defaultdict
from pathlib import Path
from pydantic import ValidationError
from peeps_scheduler.constants import DATE_FORMAT, DEFAULT_TIMEZONE
from peeps_scheduler.data_manager import get_data_manager
from peeps_scheduler.models import SwitchPreference
from peeps_scheduler.validation.errors import FileValidationError
from peeps_scheduler.validation.fields import ValidationContext
from peeps_scheduler.validation.file_schemas.period import PeriodFileSchema
from peeps_scheduler.validation.helpers import normalize_email_for_match
from peeps_scheduler.validation.parsers import parse_event_name
from peeps_scheduler.validation.period import _infer_validation_file, load_period_files


def _load_period_schema(period_path: Path, year: int) -> PeriodFileSchema:
    raw = load_period_files(str(period_path))
    ctx = ValidationContext(year=year, tz=DEFAULT_TIMEZONE)
    try:
        return PeriodFileSchema.model_validate(raw, context={"ctx": ctx})
    except ValidationError as exc:
        file_path = _infer_validation_file(exc, Path(period_path))
        raise FileValidationError(str(file_path), exc) from exc


def parse_availability(period_schema: PeriodFileSchema):
    members = period_schema.members.root
    member_by_email = {
        normalize_email_for_match(row.email_address): row for row in members if row.email_address
    }
    event_name_by_start = {
        event.start: event.raw for event in period_schema.responses.events
    }
    event_id_by_start = {
        event.start: event.start.strftime(DATE_FORMAT) for event in period_schema.responses.events
    }
    cancelled_event_starts = {event.start for event in period_schema.cancelled_events}

    availability = defaultdict(
        lambda: {"leader": [], "follower": [], "leader_fill": [], "follower_fill": []}
    )
    unavailable = []
    responders = set()

    cancelled_availability_by_email = {}
    cancelled_availability_details = {}

    for entry in period_schema.cancelled_member_availability:
        normalized = normalize_email_for_match(entry.member_email)
        cancelled_availability_by_email[normalized] = {event.start for event in entry.events}
        member = member_by_email.get(normalized)
        if member:
            display_name = member.display_name or member.full_name
            cancelled_availability_details[display_name] = sorted(
                {
                    event_id_by_start.get(event.start, event.start.strftime(DATE_FORMAT))
                    for event in entry.events
                }
            )

    for response in period_schema.responses.responses:
        normalized = normalize_email_for_match(response.email_address)
        member = member_by_email.get(normalized)
        if not member:
            continue
        display_name = member.display_name or member.full_name
        responders.add(normalized)

        role = response.primary_role
        switch_pref = response.secondary_role or SwitchPreference.PRIMARY_ONLY
        available_dates = []

        for event in response.availability:
            if event.start in cancelled_event_starts:
                continue
            if event.start in cancelled_availability_by_email.get(normalized, set()):
                continue
            available_dates.append(event_name_by_start.get(event.start, event.raw))

        if not available_dates:
            unavailable.append(display_name)
            continue

        for date in available_dates:
            availability[date][role.value].append(display_name)
            if switch_pref != SwitchPreference.PRIMARY_ONLY:
                availability[date][f"{role.opposite().value}_fill"].append(display_name)

    non_responders = [
        member.display_name or member.full_name
        for email, member in member_by_email.items()
        if email not in responders and member.active
    ]

    cancelled_event_ids = {
        event_id_by_start.get(event.start, event.start.strftime(DATE_FORMAT))
        for event in period_schema.cancelled_events
    }

    return (
        availability,
        unavailable,
        non_responders,
        cancelled_event_ids,
        cancelled_availability_details,
    )


def print_availability(
    availability,
    unavailable,
    non_responders,
    year=None,
    cancelled_events=None,
    cancelled_availability_details=None,
):
    cancelled_events = cancelled_events or set()
    cancelled_availability_details = cancelled_availability_details or {}
    if year is None:
        year = datetime.datetime.now().year

    print("=" * 80)
    print("AVAILABILITY REPORT")
    print("=" * 80)

    if cancelled_events or cancelled_availability_details:
        print()

        # Show cancelled events first
        if cancelled_events:
            print("CANCELLED EVENTS:")
            for event_id in sorted(cancelled_events):
                print(f"  - {event_id}")

        # Show cancelled availability
        if cancelled_availability_details:
            print("\nCANCELLED AVAILABILITY (excluded from above):")
            for name in sorted(cancelled_availability_details.keys()):
                events = cancelled_availability_details[name]
                events_str = ", ".join(sorted(events))
                print(f"  - {name}: {events_str}")

    def _sort_key(label: str):
        parsed = parse_event_name(label, year, DEFAULT_TIMEZONE)
        return parsed.start

    for date in sorted(availability.keys(), key=_sort_key):
        print(f"\n{date}")
        print(
            f"    Leaders  ({len(availability[date]['leader'])}): {', '.join(availability[date]['leader'])} ( + {', '.join(availability[date]['leader_fill'])})"
        )
        print(
            f"    Followers({len(availability[date]['follower'])}): {', '.join(availability[date]['follower'])} ( + {', '.join(availability[date]['follower_fill'])})"
        )

    print("\nNo availability:")
    for name in sorted(unavailable):
        print(f"  - {name}")

    print("\nDid not respond:")
    for name in sorted(non_responders):
        print(f"  - {name}")


def run_availability_report(data_folder):
    """Generate and print availability report for a given data period."""
    dm = get_data_manager()
    period_path = dm.get_period_path(data_folder)

    # Extract year from data_folder (e.g., "2026-01" -> 2026)
    # Handle both absolute paths and folder names
    folder_name = Path(data_folder).name
    try:
        year = (
            int(folder_name[:4])
            if folder_name and len(folder_name) >= 4 and folder_name[:4].isdigit()
            else None
        )
    except (ValueError, TypeError):
        year = None
    if year is None:
        year = datetime.datetime.now().year

    period_schema = _load_period_schema(period_path, year)
    availability, unavailable, non_responders, cancelled_events, cancelled_availability_details = (
        parse_availability(period_schema)
    )

    print_availability(
        availability,
        unavailable,
        non_responders,
        year=year,
        cancelled_events=cancelled_events,
        cancelled_availability_details=cancelled_availability_details,
    )
