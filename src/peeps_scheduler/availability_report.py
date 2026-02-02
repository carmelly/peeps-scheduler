from collections import defaultdict
from dataclasses import dataclass
from peeps_scheduler.models import (
    CancelledMemberAvailability,
    Event,
    Peep,
    PeriodData,
    SwitchPreference,
)


@dataclass
class AvailabilityReport:
    availability: dict[Event, dict[str, list[Peep]]]
    unavailable: list[Peep]
    non_responders: list[Peep]


def _init_availability_dict():
    return defaultdict(
        lambda: {"leader": [], "follower": [], "leader_fill": [], "follower_fill": []}
    )


def generate_availability_report(
    period_data: PeriodData, exclude_cancelled=True
) -> AvailabilityReport:
    active_peeps = [peep for peep in period_data.peeps if peep.active]
    responders = [peep for peep in active_peeps if peep.responded]
    non_responders = [peep for peep in active_peeps if not peep.responded]

    if exclude_cancelled:
        # remove cancelled events from peep availability
        for event in period_data.cancelled_events:
            for peep in responders:
                if event in peep.availability:
                    peep.availability.remove(event)

        # remove cancelled member availability events from peep availability
        for cancelled_availability in period_data.cancelled_member_availability:
            for event in cancelled_availability.events:
                cancelled_availability.peep.availability.remove(event)

    unavailable = [peep for peep in responders if not peep.availability]

    # build availability report
    availability = _init_availability_dict()
    for peep in active_peeps:
        for event in peep.availability:
            availability[event][peep.role.value].append(peep.name)
            if peep.switch_pref != SwitchPreference.PRIMARY_ONLY:
                availability[event][f"{peep.role.opposite().value}_fill"].append(peep.name)

    return AvailabilityReport(
        availability=availability,
        unavailable=unavailable,
        non_responders=non_responders,
    )


def print_event_availability(event, availability):
    print(f"\n{event.formatted_date()}")
    print(
        f"    Leaders  ({len(availability[event]['leader'])}): {', '.join(availability[event]['leader'])} ( + {', '.join(availability[event]['leader_fill'])})"
    )
    print(
        f"    Followers({len(availability[event]['follower'])}): {', '.join(availability[event]['follower'])} ( + {', '.join(availability[event]['follower_fill'])})"
    )


def print_cancellations(
    cancelled_events: list[Event] | None = None,
    cancelled_availability: list[CancelledMemberAvailability] | None = None,
    original_availability_report: AvailabilityReport | None = None,
):
    if cancelled_events:
        print("CANCELLED EVENTS:")
        orig_availability = (
            original_availability_report.availability
            if original_availability_report
            else _init_availability_dict()
        )
        for event in sorted(cancelled_events, key=lambda e: e.formatted_date()):
            print_event_availability(
                event,
                orig_availability,
            )

    if cancelled_availability:
        print("\nCANCELLED MEMBER AVAILABILITY:")
        for ca in cancelled_availability:
            events = sorted([event.formatted_date() for event in ca.events])
            events_str = ", ".join(sorted(events))
            print(f"  - {ca.peep.name}: {events_str}")


def print_availability(
    availability_report: AvailabilityReport,  # after cancellations removed
    original_availability_report: AvailabilityReport | None = None,
    cancelled_events: list[Event] | None = None,
    cancelled_availability: list[CancelledMemberAvailability] | None = None,
):
    print("=" * 80)
    print("AVAILABILITY REPORT")
    print("=" * 80)

    if cancelled_events or cancelled_availability:
        print()
        print_cancellations(cancelled_events or [], cancelled_availability or [])

    availability = availability_report.availability
    for event in sorted(availability.keys(), key=lambda e: e.formatted_date()):
        print_event_availability(event, availability)

    print("\nNo availability:")
    for peep in sorted(availability_report.unavailable, key=lambda p: p.name):
        print(f"  - {peep.name}")

    if original_availability_report:
        new_unavailable = [
            peep
            for peep in availability_report.unavailable
            if peep not in original_availability_report.unavailable
        ]
        if new_unavailable:
            print("\nNo availability after cancellations:")

            for peep in sorted(new_unavailable, key=lambda p: p.name):
                print(f"  - {peep.name}")

    if availability_report.non_responders:
        print("\nDid not respond:")
        for peep in sorted(availability_report.non_responders, key=lambda p: p.name):
            print(f"  - {peep.name}")


def run_availability_report(period_data: PeriodData):
    """Generate and print availability report for period data"""

    # loader = FilePeriodLoader(Path(data_folder).parent, year, True, False)
    # period_data: PeriodData = loader.load_period(folder_name)
    report = generate_availability_report(period_data)
    original_report = generate_availability_report(period_data, exclude_cancelled=False)

    print_availability(
        original_availability=original_report,
        availability=report,
        cancelled_events=period_data.cancelled_events,
        cancelled_availability=period_data.cancelled_member_availability,
    )
