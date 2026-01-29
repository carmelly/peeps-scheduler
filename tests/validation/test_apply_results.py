"""Tests for apply-results validation flow."""

import csv
import json
import pytest
from peeps_scheduler.validation.errors import FileValidationError
from peeps_scheduler.validation.period import load_and_validate_period
from tests.validation.fixtures import (
    attendance_data,
    attendance_event_data,
    member_data,
    response_data,
)

pytestmark = pytest.mark.integration


def _write_csv(path, fieldnames, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_load_and_validate_period_includes_attendance(tmp_path):
    """Happy path: members, responses, and attendance validate together."""
    members_path = tmp_path / "members.csv"
    responses_path = tmp_path / "responses.csv"
    attendance_path = tmp_path / "actual_attendance.json"

    members = [
        member_data(
            {
                "id": "1",
                "Name": "Alice Alpha",
                "Display Name": "Alice",
                "Email Address": "alice@test.com",
            }
        ),
        member_data(
            {
                "id": "2",
                "Name": "Bob Beta",
                "Display Name": "Bob",
                "Email Address": "bob@test.com",
                "Role": "Follower",
                "Index": "1",
            }
        ),
    ]
    _write_csv(
        members_path,
        [
            "id",
            "Name",
            "Display Name",
            "Email Address",
            "Role",
            "Index",
            "Priority",
            "Total Attended",
            "Active",
            "Date Joined",
        ],
        members,
    )

    responses = [
        response_data(
            {
                "Name": "Alice Alpha",
                "Display Name": "Alice",
                "Email Address": "alice@test.com",
            }
        ),
        response_data(
            {
                "Name": "Bob Beta",
                "Display Name": "Bob",
                "Email Address": "bob@test.com",
                "Primary Role": "Follower",
            }
        ),
    ]
    _write_csv(
        responses_path,
        [
            "Timestamp",
            "Name",
            "Display Name",
            "Email Address",
            "Primary Role",
            "Secondary Role",
            "Max Sessions",
            "Availability",
            "Min Interval Days",
        ],
        responses,
    )

    attendance_payload = attendance_data(
        {
            "valid_events": [
                attendance_event_data(
                    {
                        "id": 1,
                        "date": "2020-01-04 13:00",
                        "duration_minutes": 90,
                        "attendees": [
                            {"id": 1, "name": "Alice", "role": "leader"},
                            {"id": 2, "name": "Bob", "role": "follower"},
                        ],
                    }
                )
            ]
        }
    )
    attendance_path.write_text(json.dumps(attendance_payload))

    period_data = load_and_validate_period(str(tmp_path), 2020, allow_missing_responses=True)

    assert len(period_data.attendance_events) == 1


def test_load_and_validate_period_allows_missing_responses(tmp_path):
    """Responses are optional; validation should still succeed without responses.csv."""
    members_path = tmp_path / "members.csv"
    attendance_path = tmp_path / "actual_attendance.json"

    members = [
        member_data(
            {
                "id": "1",
                "Name": "Alice Alpha",
                "Display Name": "Alice",
                "Email Address": "alice@test.com",
            }
        ),
        member_data(
            {
                "id": "2",
                "Name": "Bob Beta",
                "Display Name": "Bob",
                "Email Address": "bob@test.com",
                "Role": "Follower",
                "Index": "1",
            }
        ),
    ]
    _write_csv(
        members_path,
        [
            "id",
            "Name",
            "Display Name",
            "Email Address",
            "Role",
            "Index",
            "Priority",
            "Total Attended",
            "Active",
            "Date Joined",
        ],
        members,
    )

    attendance_payload = attendance_data(
        {
            "valid_events": [
                attendance_event_data(
                    {
                        "id": 1,
                        "date": "2020-01-04 13:00",
                        "duration_minutes": 90,
                        "attendees": [
                            {"id": 1, "name": "Alice", "role": "leader"},
                            {"id": 2, "name": "Bob", "role": "follower"},
                        ],
                    }
                )
            ]
        }
    )
    attendance_path.write_text(json.dumps(attendance_payload))

    period_data = load_and_validate_period(str(tmp_path), 2020, allow_missing_responses=True)

    assert len(period_data.attendance_events) == 1


def test_load_and_validate_period_requires_responses_for_results(tmp_path):
    """Results require responses; missing responses should raise a validation error."""
    members_path = tmp_path / "members.csv"
    results_path = tmp_path / "results.json"

    members = [
        member_data(
            {
                "id": "1",
                "Name": "Alice Alpha",
                "Display Name": "Alice",
                "Email Address": "alice@test.com",
            }
        ),
    ]
    _write_csv(
        members_path,
        [
            "id",
            "Name",
            "Display Name",
            "Email Address",
            "Role",
            "Index",
            "Priority",
            "Total Attended",
            "Active",
            "Date Joined",
        ],
        members,
    )

    results_payload = {
        "valid_events": [
            {
                "id": 1,
                "date": "2020-01-04 13:00",
                "duration_minutes": 90,
                "attendees": [{"id": 1, "name": "Alice", "role": "leader"}],
                "alternates": [],
            }
        ]
    }
    results_path.write_text(json.dumps(results_payload))

    with pytest.raises(FileValidationError):
        load_and_validate_period(str(tmp_path), 2020, allow_missing_responses=True)
