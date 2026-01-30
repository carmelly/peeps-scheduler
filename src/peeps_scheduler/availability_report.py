import datetime
from collections import defaultdict
from pathlib import Path
from peeps_scheduler.adapters.file.loader import FilePeriodLoader
from peeps_scheduler.data_manager import get_data_manager
from peeps_scheduler.models import PeriodData, SwitchPreference


def parse_availability(period_data: PeriodData):
    peeps = period_data.peeps

    responders = [peep for peep in peeps if peep.responded]
    unavailable = [peep for peep in responders if not peep.availability]
    non_responders = [peep for peep in peeps if not peep.responded and peep.active]

    availability = defaultdict(
        lambda: {"leader": [], "follower": [], "leader_fill": [], "follower_fill": []}
    )
    for peep in peeps:
        for event in peep.availability:
            availability[event][peep.role.value].append(peep.name)
            if peep.switch_pref != SwitchPreference.PRIMARY_ONLY:
                availability[event][f"{peep.role.opposite().value}_fill"].append(peep.name)

    return (
        availability,
        unavailable,
        non_responders,
    )


def print_availability(
    availability,
    unavailable,
    non_responders,
    year=None,
    cancelled_events=None,
    cancelled_availability=None,
):
    if year is None:
        year = datetime.datetime.now().year

    print("=" * 80)
    print("AVAILABILITY REPORT")
    print("=" * 80)

    if cancelled_events or cancelled_availability:
        print()

        # Show cancelled events first
        if cancelled_events:
            print("CANCELLED EVENTS:")
            for event in sorted(cancelled_events, key=lambda e: e.formatted_date()):
                print(f"  - {event.formatted_date()}")

        # Show cancelled availability
        if cancelled_availability:
            print("\nCANCELLED AVAILABILITY (excluded from above):")
            for ca in cancelled_availability:
                events = sorted([event.formatted_date() for event in ca.events])
                events_str = ", ".join(sorted(events))
                print(f"  - {ca.peep.name}: {events_str}")

    for event in sorted(availability.keys(), key=lambda e: e.formatted_date()):
        print(f"\n{event.formatted_date()}")
        print(
            f"    Leaders  ({len(availability[event]['leader'])}): {', '.join(availability[event]['leader'])} ( + {', '.join(availability[event]['leader_fill'])})"
        )
        print(
            f"    Followers({len(availability[event]['follower'])}): {', '.join(availability[event]['follower'])} ( + {', '.join(availability[event]['follower_fill'])})"
        )

    print("\nNo availability:")
    for peep in sorted(unavailable, key=lambda p: p.name):
        print(f"  - {peep.name}")

    print("\nDid not respond:")
    for peep in sorted(non_responders, key=lambda p: p.name):
        print(f"  - {peep.name}")


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

    loader = FilePeriodLoader(Path(period_path).parent, year, True, False)
    period_data: PeriodData = loader.load_period(folder_name)
    availability, unavailable, non_responders = parse_availability(period_data)

    print_availability(
        availability,
        unavailable,
        non_responders,
        year=year,
        cancelled_events=period_data.cancelled_events,
        cancelled_availability=period_data.cancelled_member_availability,
    )
