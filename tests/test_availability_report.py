from __future__ import annotations

from unittest.mock import MagicMock, patch
import pytest
from pydantic import BaseModel, ValidationError
from peeps_scheduler.adapters.file.validation.errors import FileValidationError
from peeps_scheduler.adapters.file.validation.period import PeriodData
from peeps_scheduler.availability_report import (
    parse_availability,
    print_availability,
    run_availability_report,
)
from peeps_scheduler.models import CancelledMemberAvailability, Role, SwitchPreference


def _sample_period_data(peep_factory, event_factory) -> PeriodData:
    event = event_factory(id=1, duration_minutes=90)
    leader = peep_factory(
        id=1,
        role=Role.LEADER,
        availability=[event],
        switch_pref=SwitchPreference.SWITCH_IF_PRIMARY_FULL,
    )
    follower = peep_factory(
        id=2,
        role=Role.FOLLOWER,
        availability=[event],
        switch_pref=SwitchPreference.SWITCH_IF_NEEDED,
    )
    non_responder = peep_factory(id=3, role=Role.FOLLOWER, availability=[], responded=False)
    cancelled_peep = peep_factory(id=4, role=Role.LEADER, availability=[])
    cancelled = CancelledMemberAvailability(peep=cancelled_peep, events=[event])
    return PeriodData(
        peeps=[leader, follower, non_responder, cancelled_peep],
        events=[event],
        results_events=[],
        attendance_events=[],
        cancelled_events=[event],
        cancelled_member_availability=[cancelled],
        partnership_requests=[],
        topics=["Topic One"],
    )


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
    non_responder = peep_factory(
        id=8, role=Role.FOLLOWER, availability=[], responded=False
    )

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


def test_run_availability_report_prints_sections(
    tmp_path, capsys, peep_factory, event_factory
):
    period_slug = "2025-01"
    period_path = tmp_path / "peeps-data" / "original" / period_slug
    period_path.mkdir(parents=True)

    period_data = _sample_period_data(peep_factory, event_factory)
    loader_instance = MagicMock()
    loader_instance.load_period.return_value = period_data

    with patch("peeps_scheduler.availability_report.get_data_manager") as mock_dm, patch(
        "peeps_scheduler.availability_report.FilePeriodLoader", return_value=loader_instance
    ) as mock_loader_cls:
        mock_dm.return_value.get_period_path.return_value = period_path

        run_availability_report(str(period_path))

    output = capsys.readouterr().out

    assert "AVAILABILITY REPORT" in output
    assert period_data.events[0].formatted_date() in output
    assert "CANCELLED EVENTS" in output
    assert "No availability" in output
    assert "Did not respond" in output

    mock_loader_cls.assert_called_once_with(period_path.parent, 2025, True, False)
    loader_instance.load_period.assert_called_once_with(period_slug)


def test_run_availability_report_propagates_validation_error(tmp_path):
    period_slug = "2025-02"
    period_path = tmp_path / "peeps-data" / "original" / period_slug
    period_path.mkdir(parents=True)

    class DummyModel(BaseModel):
        x: int

    with pytest.raises(ValidationError) as excinfo:
        DummyModel.model_validate({"x": "invalid"})

    loader_instance = MagicMock()
    loader_instance.load_period.side_effect = FileValidationError("members.csv", excinfo.value)

    with patch("peeps_scheduler.availability_report.get_data_manager") as mock_dm, patch(
        "peeps_scheduler.availability_report.FilePeriodLoader", return_value=loader_instance
    ):
        mock_dm.return_value.get_period_path.return_value = period_path

        with pytest.raises(FileValidationError):
            run_availability_report(str(period_path))
