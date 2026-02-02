from __future__ import annotations

from peeps_scheduler.availability_report import (
    AvailabilityReport,
    generate_availability_report,
    print_availability,
)
from peeps_scheduler.models import CancelledMemberAvailability, PeriodData, Role, SwitchPreference


def test_parse_availability_basic(peep_factory, event_factory):
    """Test basic parsing of availability report."""
    event1 = event_factory(id=1)
    event2 = event_factory(id=2)

    peep1 = peep_factory(id=1, role=Role.LEADER, availability=[event1, event2])
    peep2 = peep_factory(id=2, role=Role.FOLLOWER, availability=[event2])
    peep3 = peep_factory(id=3, role=Role.LEADER, availability=[])
    peep4 = peep_factory(id=4, role=Role.FOLLOWER, availability=[], responded=False)

    period_data = PeriodData(
        peeps=[peep1, peep2, peep3, peep4],
        events=[event1, event2],
    )

    report = generate_availability_report(period_data)
    availability, unavailable_peeps, non_responders = (
        report.availability,
        report.unavailable,
        report.non_responders,
    )

    assert availability[event1]["leader"] == ["TestPeep1"]
    assert availability[event1]["follower"] == []
    assert availability[event2]["leader"] == ["TestPeep1"]
    assert availability[event2]["follower"] == ["TestPeep2"]

    assert unavailable_peeps == [peep3]
    assert non_responders == [peep4]


def test_parse_availability_removes_cancelled_events(peep_factory, event_factory):
    """Test that cancelled events are excluded from availability report."""
    event1 = event_factory(id=1)
    cancelled_event = event_factory(id=2)

    peep1 = peep_factory(id=1, role=Role.LEADER, availability=[event1, cancelled_event])
    peep2 = peep_factory(id=2, role=Role.FOLLOWER, availability=[cancelled_event])

    period_data = PeriodData(
        peeps=[peep1, peep2],
        events=[event1, cancelled_event],
        cancelled_events=[cancelled_event],
    )

    report = generate_availability_report(period_data)
    availability, unavailable_peeps, _non_responders = (
        report.availability,
        report.unavailable,
        report.non_responders,
    )

    # Check that cancelled event is exluded from report and that peep2 now shows as unavailable
    assert cancelled_event not in availability
    assert unavailable_peeps == [peep2]


def test_parse_availability_removes_cancelled_member_availability(peep_factory, event_factory):
    """Test that cancelled member availability is excluded from availability report."""
    event1 = event_factory(id=1)

    peep1 = peep_factory(id=1, role=Role.LEADER, availability=[event1])

    cancelled_availability = CancelledMemberAvailability(peep=peep1, events=[event1])

    period_data = PeriodData(
        peeps=[peep1],
        events=[event1],
        cancelled_member_availability=[cancelled_availability],
    )

    report = generate_availability_report(period_data)
    availability, unavailable_peeps, _non_responders = (
        report.availability,
        report.unavailable,
        report.non_responders,
    )

    # Check that event has no availability since peep1's availability was cancelled
    assert availability[event1]["leader"] == []
    # Check that peep1 now shows as unavailable
    assert unavailable_peeps == [peep1]


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

    report = generate_availability_report(period_data)
    availability, unavailable_peeps, non_responders = (
        report.availability,
        report.unavailable,
        report.non_responders,
    )

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

    report = AvailabilityReport(
        availability=availability,
        unavailable=unavailable,
        non_responders=non_responders,
    )
    print_availability(
        availability_report=report,
        original_availability_report=None,
        cancelled_events=cancelled,
        cancelled_availability=cancelled_availability,
    )

    output = capsys.readouterr().out

    assert "AVAILABILITY REPORT" in output
    assert event.formatted_date() in output
    assert "CANCELLED EVENTS" in output
    assert "Did not respond" in output
