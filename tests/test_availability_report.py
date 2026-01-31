from __future__ import annotations

from peeps_scheduler.adapters.file.validation.period import PeriodData
from peeps_scheduler.availability_report import (
    parse_availability,
    print_availability,
)
from peeps_scheduler.models import CancelledMemberAvailability, Role, SwitchPreference


def test_parse_availability_counts_switch_preferences(peep_factory, event_factory):
    event = event_factory(id=5, duration_minutes=90)
    leader = peep_factory(
        id=5,
        role=Role.LEADER,
        availability=[event],
        switch_pref=SwitchPreference.SWITCH_IF_PRIMARY_FULL,
    )
    follower = peep_factory(
        id=6,
        role=Role.FOLLOWER,
        availability=[event],
        switch_pref=SwitchPreference.SWITCH_IF_NEEDED,
    )
    unavailable = peep_factory(id=7, role=Role.LEADER, availability=[])
    non_responder = peep_factory(id=8, role=Role.FOLLOWER, availability=[], responded=False)

    period_data = PeriodData(peeps=[leader, follower, unavailable, non_responder], events=[event])

    availability, unavailable_peeps, non_responders = parse_availability(period_data)

    assert availability[event]["leader"] == ["TestPeep5"]
    assert availability[event]["follower"] == ["TestPeep6"]
    assert "TestPeep5" in availability[event]["follower_fill"]
    assert "TestPeep6" in availability[event]["leader_fill"]
    assert unavailable_peeps == [unavailable]
    assert non_responders == [non_responder]


def test_print_availability_outputs_sections(capsys, event_factory, peep_factory):
    event = event_factory(id=6)
    availability = {
        event: {
            "leader": ["Leader One"],
            "follower": ["Follower One"],
            "leader_fill": ["Follower One"],
            "follower_fill": ["Leader One"],
        }
    }
    unavailable = [peep_factory(id=7, role=Role.LEADER, availability=[])]
    non_responders = [peep_factory(id=8, role=Role.FOLLOWER, availability=[], responded=False)]
    cancelled = [event]
    cancelled_availability = [CancelledMemberAvailability(peep=unavailable[0], events=[event])]

    print_availability(
        availability,
        unavailable,
        non_responders,
        year=2025,
        cancelled_events=cancelled,
        cancelled_availability=cancelled_availability,
    )

    output = capsys.readouterr().out

    assert "AVAILABILITY REPORT" in output
    assert event.formatted_date() in output
    assert "CANCELLED EVENTS" in output
    assert "Did not respond" in output
