import csv
import json
from datetime import datetime
from pathlib import Path
import pytest
from tests.adapters.file.validation.fixtures import (
    attendance_data,
    attendance_event_data,
    member_data,
    response_data,
)
from peeps_scheduler.adapters.file.loader import (
    FilePeriodLoader,
    _load_csv_file,
    _normalize_text,
    _split_response_rows,
)
from peeps_scheduler.adapters.file.validation.errors import FileValidationError
from peeps_scheduler.adapters.file.validation.period import PeriodData
from peeps_scheduler.constants import DEFAULT_TIMEZONE
from peeps_scheduler.models import (
    CancelledMemberAvailability,
    Event,
    PartnershipRequest,
    Peep,
)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


@pytest.mark.integration
class TestFilePeriodLoaderIntegration:
    def test_load_period_returns_period_data(self, period_root, period_slug, period_dir):
        loader = FilePeriodLoader(base_path=period_root, year=2020)

        period_data = loader.load_period(period_slug)

        assert isinstance(period_data, PeriodData)
        assert len(period_data.peeps) >= 1
        assert len(period_data.events) >= 1

    def test_load_period_validates_cross_file_constraints(
        self, period_root, period_slug, period_dir
    ):
        loader = FilePeriodLoader(base_path=period_root, year=2020)

        period_data = loader.load_period(period_slug)

        assert isinstance(period_data.peeps, list)
        assert isinstance(period_data.events, list)
        assert isinstance(period_data.cancelled_events, list)
        assert isinstance(period_data.partnership_requests, list)
        assert isinstance(period_data.topics, list)

    def test_load_period_happy_path_comprehensive(self, period_root, period_slug, period_dir):
        loader = FilePeriodLoader(base_path=period_root, year=2020)

        period_data = loader.load_period(period_slug)

        assert isinstance(period_data, PeriodData)
        assert isinstance(period_data.cancelled_member_availability, list)
        assert isinstance(period_data.partnership_requests, list)
        assert isinstance(period_data.topics, list)

        alice = period_data.peeps[0]
        assert isinstance(alice, Peep)
        assert alice.id == 1
        assert alice.full_name == "Alice Alpha"

        bob = period_data.peeps[1]
        assert bob.id == 2

        carol = period_data.peeps[2]
        assert carol.id == 3

        assert isinstance(period_data.events[0], Event)
        jan4 = period_data.events[0]
        assert jan4.date == datetime(2020, 1, 4, 13, 0, tzinfo=DEFAULT_TIMEZONE)

        jan11 = period_data.cancelled_events[0]
        assert jan11.date == datetime(2020, 1, 11, 13, 0, tzinfo=DEFAULT_TIMEZONE)

        cancelled_availability = period_data.cancelled_member_availability[0]
        assert isinstance(cancelled_availability, CancelledMemberAvailability)
        assert cancelled_availability.peep == bob
        assert cancelled_availability.events == [jan4]

        assert isinstance(period_data.partnership_requests[0], PartnershipRequest)
        assert period_data.partnership_requests[0].requester == alice
        assert period_data.partnership_requests[0].target_peeps == [bob, carol]
        assert period_data.topics == [
            "Balance for Spins and Turns",
            "Angles for Shaping & Slotting",
        ]

    def test_load_period_includes_attendance_when_present(self, period_root, period_slug):
        period_dir = period_root / period_slug
        period_dir.mkdir(parents=True)

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
            period_dir / "members.csv",
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
            period_dir / "responses.csv",
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
        (period_dir / "actual_attendance.json").write_text(json.dumps(attendance_payload))

        loader = FilePeriodLoader(base_path=period_root, year=2020, require_responses=False)
        period_data = loader.load_period(period_slug)

        assert len(period_data.attendance_events) == 1

    def test_load_period_allows_missing_responses_when_require_responses_false(
        self, period_root, period_slug
    ):
        period_dir = period_root / period_slug
        period_dir.mkdir(parents=True)

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
            period_dir / "members.csv",
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

        loader = FilePeriodLoader(base_path=period_root, year=2020, require_responses=False)
        period_data = loader.load_period(period_slug)

        assert len(period_data.peeps) == 1

    def test_load_period_requires_responses_when_results_present(self, period_root, period_slug):
        period_dir = period_root / period_slug
        period_dir.mkdir(parents=True)

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
            period_dir / "members.csv",
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
        (period_dir / "results.json").write_text(json.dumps(results_payload))

        loader = FilePeriodLoader(base_path=period_root, year=2020, require_responses=False)

        with pytest.raises(FileValidationError):
            loader.load_period(period_slug)

    def test_load_period_uses_base_path_and_slug(self, tmp_path):
        base_path = tmp_path / "original"
        base_path.mkdir()
        period_slug = "2020-03"

        period_dir = base_path / period_slug
        period_dir.mkdir()
        members = [
            member_data(
                {
                    "id": "7",
                    "Name": "Unique Name",
                    "Display Name": "Unique",
                    "Email Address": "unique@test.com",
                }
            ),
        ]
        _write_csv(
            period_dir / "members.csv",
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
        _write_csv(
            period_dir / "responses.csv",
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
            [
                response_data(
                    {
                        "Name": "Unique Name",
                        "Display Name": "Unique",
                        "Email Address": "unique@test.com",
                    }
                )
            ],
        )

        loader = FilePeriodLoader(base_path=base_path, year=2020)
        period_data = loader.load_period(period_slug)

        assert period_data.peeps[0].full_name == "Unique Name"

    def test_load_period_requires_attendance_when_flag_true(self, period_root, period_slug):
        period_dir = period_root / period_slug
        period_dir.mkdir(parents=True)

        _write_csv(
            period_dir / "members.csv",
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
            [member_data({"id": "1"})],
        )
        _write_csv(
            period_dir / "responses.csv",
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
            [response_data({"Email Address": "alice@test.com"})],
        )

        loader = FilePeriodLoader(
            base_path=period_root,
            year=2020,
            require_attendance=True,
        )

        with pytest.raises(FileNotFoundError) as exc_info:
            loader.load_period(period_slug)

        assert str(period_dir / "actual_attendance.json") in str(exc_info.value)

    def test_load_period_includes_results_when_present(self, period_root, period_slug):
        period_dir = period_root / period_slug
        period_dir.mkdir(parents=True)

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
            period_dir / "members.csv",
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
        _write_csv(
            period_dir / "responses.csv",
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
            [response_data({"Email Address": "alice@test.com"})],
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
            ],
            "num_unique_attendees": 1,
            "system_weight": 1,
        }
        (period_dir / "results.json").write_text(json.dumps(results_payload))

        loader = FilePeriodLoader(base_path=period_root, year=2020)
        period_data = loader.load_period(period_slug)

        assert len(period_data.results_events) == 1

    def test_load_period_missing_members_raises_file_not_found(self, period_root, period_slug):
        period_dir = period_root / period_slug
        period_dir.mkdir(parents=True)
        _write_csv(
            period_dir / "responses.csv",
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
            [response_data({"Email Address": "alice@test.com"})],
        )

        loader = FilePeriodLoader(base_path=period_root, year=2020)

        with pytest.raises(FileNotFoundError) as exc_info:
            loader.load_period(period_slug)

        assert str(period_dir / "members.csv") in str(exc_info.value)

    def test_load_period_missing_responses_raises_file_not_found_by_default(
        self, period_root, period_slug
    ):
        period_dir = period_root / period_slug
        period_dir.mkdir(parents=True)
        _write_csv(
            period_dir / "members.csv",
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
            [member_data({"id": "1"})],
        )

        loader = FilePeriodLoader(base_path=period_root, year=2020)

        with pytest.raises(FileNotFoundError) as exc_info:
            loader.load_period(period_slug)

        assert str(period_dir / "responses.csv") in str(exc_info.value)

    def test_load_period_propagates_file_validation_error_with_filename(
        self, period_root, period_slug
    ):
        period_dir = period_root / period_slug
        period_dir.mkdir(parents=True)

        members = [
            member_data(
                {
                    "id": "1",
                    "Name": "Invalid Member",
                    "Display Name": "Invalid",
                    "Email Address": "",
                }
            ),
        ]
        _write_csv(
            period_dir / "members.csv",
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
        _write_csv(
            period_dir / "responses.csv",
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
            [response_data({"Email Address": "invalid@test.com"})],
        )

        loader = FilePeriodLoader(base_path=period_root, year=2020)

        with pytest.raises(FileValidationError) as exc_info:
            loader.load_period(period_slug)

        assert exc_info.value.filename == "members.csv"

    def test_load_period_handles_missing_period_config_file(self, period_root, period_slug):
        period_dir = period_root / period_slug
        period_dir.mkdir(parents=True)

        _write_csv(
            period_dir / "members.csv",
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
            [
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
                member_data(
                    {
                        "id": "3",
                        "Name": "Carol Clark",
                        "Display Name": "Carol",
                        "Email Address": "carol@test.com",
                        "Role": "Leader",
                        "Index": "2",
                    }
                ),
            ],
        )
        _write_csv(
            period_dir / "responses.csv",
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
            [
                response_data(
                    {
                        "Name": "Alice Alpha",
                        "Display Name": "Alice",
                        "Email Address": "alice@test.com",
                        "Primary Role": "Follower",
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
                response_data(
                    {
                        "Name": "Carol Clark",
                        "Display Name": "Carol",
                        "Email Address": "carol@test.com",
                        "Primary Role": "Leader",
                    }
                ),
            ],
        )

        loader = FilePeriodLoader(base_path=period_root, year=2020)
        period_data = loader.load_period(period_slug)

        assert isinstance(period_data, PeriodData)
        assert period_data.cancelled_events == []
        assert period_data.cancelled_member_availability == []
        assert period_data.partnership_requests == []
        assert period_data.topics == []

    def test_load_period_deduplicates_events(self, period_root, period_slug):
        period_dir = period_root / period_slug
        period_dir.mkdir(parents=True)

        _write_csv(
            period_dir / "members.csv",
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
            [
                member_data(
                    {
                        "id": "1",
                        "Name": "Alice Alpha",
                        "Display Name": "Alice",
                        "Email Address": "alice@test.com",
                        "Role": "Leader",
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
            ],
        )
        _write_csv(
            period_dir / "responses.csv",
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
            [
                response_data(
                    {
                        "Name": "Alice Alpha",
                        "Display Name": "Alice",
                        "Email Address": "alice@test.com",
                        "Primary Role": "Leader",
                        "Availability": "Saturday January 4 - 1pm",
                    }
                ),
                response_data(
                    {
                        "Name": "Bob Beta",
                        "Display Name": "Bob",
                        "Email Address": "bob@test.com",
                        "Primary Role": "Follower",
                        "Availability": "Saturday January 4 - 1pm",
                    }
                ),
            ],
        )

        loader = FilePeriodLoader(base_path=period_root, year=2020)
        period_data = loader.load_period(period_slug)

        assert len(period_data.events) == 1
        event = period_data.events[0]
        assert event.date == datetime(2020, 1, 4, 13, 0, tzinfo=DEFAULT_TIMEZONE)


@pytest.mark.unit
class TestFilePeriodLoaderHelpers:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("It\u2019s a test and \u201chello\u201d", 'It\'s a test and "hello"'),  # curly quotes
            ("  Hello World!  ", "Hello World!"),  # leading/trailing spaces
            ("New  York  City", "New York City"),  # multiple spaces
            (None, ""),  # None value
            ("", ""),  # empty string
        ],
    )
    def test_normalize_text(self, value, expected):
        value = value
        assert _normalize_text(value) == expected

    def test_load_csv_file_strips_fieldnames(self, tmp_path):
        path = tmp_path / "trim.csv"
        path.write_text(
            " Name   , Role \n Alice, Follow \n Bob, Lead \n"
        )  # extra spaces in headers

        rows = _load_csv_file(path)
        assert rows[0].keys() == {"Name", "Role"}

    def test_split_response_rows_separates_event_rows(self):
        rows = [
            {"Name": "Event: Saturday January 4 - 1pm", "Event Duration": "90"},
            {"Name": "Event: Sunday January 5 - 2pm", "Event Duration": "120"},
            {"Name": "Alice Alpha"},
        ]

        event_rows, response_rows = _split_response_rows(rows)

        assert len(event_rows) == 2
        assert event_rows[0]["Name"] == "Saturday January 4 - 1pm"
        assert event_rows[1]["Name"] == "Sunday January 5 - 2pm"
        assert response_rows == [{"Name": "Alice Alpha"}]

    def test_split_response_rows_preserves_non_event_rows(self):
        rows = [
            {"Name": "Alice Alpha"},
            {"Name": "Bob Beta"},
        ]

        event_rows, response_rows = _split_response_rows(rows)

        assert event_rows == []
        assert response_rows == rows
