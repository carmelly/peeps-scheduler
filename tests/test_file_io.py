import datetime
import json
import tempfile
from pathlib import Path
import pytest
from peeps_scheduler.file_io import (
    convert_to_json,
    extract_events,
    load_csv,
    load_data_from_json,
    load_json,
    load_peeps,
    parse_time_range,
    process_responses,
    save_event_sequence,
    save_json,
    save_peeps_csv,
)
from peeps_scheduler.models import Event, EventSequence, Peep, Role, SwitchPreference

# ============================================================================
# SHARED FIXTURES
# ============================================================================


@pytest.fixture
def valid_peeps_rows():
    return [
        {
            "id": "1",
            "Name": "Alice Alpha",
            "Display Name": "Alice",
            "Email Address": "alice@test.com",
            "Role": "Leader",
            "Index": "0",
            "Priority": "1",
            "Total Attended": "3",
            "Active": "TRUE",
            "Date Joined": "2022-01-01",
        },
        {
            "id": "2",
            "Name": "Bob Beta",
            "Display Name": "Bob",
            "Email Address": "bob@test.com",
            "Role": "Follower",
            "Index": "1",
            "Priority": "2",
            "Total Attended": "5",
            "Active": "TRUE",
            "Date Joined": "2022-01-01",
        },
        {
            "id": "3",
            "Name": "Inactive Gamma",
            "Display Name": "Gamma",
            "Email Address": "gamma@test.com",
            "Role": "Leader",
            "Index": "2",
            "Priority": "3",
            "Total Attended": "2",
            "Active": "FALSE",
            "Date Joined": "2022-01-01",
        },
    ]


@pytest.fixture
def peeps_csv_path():
    """Valid peeps.csv with one inactive and one active peep with blank email."""
    content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alice Alpha,Alice,alice@test.com,Leader,0,2,3,TRUE,2022-01-01
2,Bob Beta,Bob,bob@test.com,Follower,1,1,5,TRUE,2022-01-01
3,Charlie Gamma,Charlie,,Follower,2,0,2,FALSE,2022-01-01
"""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, newline="") as f:
        f.write(content)
        return f.name


@pytest.fixture
def responses_csv_path():
    """responses.csv with 3 event rows and 2 responder rows, matching responses_csv_rows() content."""
    content = """Name,Email Address,Primary Role,Secondary Role,Max Sessions,Availability,Min Interval Days,Event Duration,Timestamp
Event: Saturday July 5 - 1pm,,,,,,,120,
Event: Sunday July 6 - 2pm (extra info),,,,,,,90,
Event: Monday July 7 - 11am,,,,,,,60,
Alice Alpha,alice@test.com,Leader,I'm happy to dance my secondary role if it lets me attend when my primary is full,2,"Saturday July 5 - 1pm, Monday July 7 - 11am",0,,"2025-07-01 12:00"
Bob Beta,bob@test.com,Follower,I only want to be scheduled in my primary role,1,"Sunday July 6 - 2pm (extra info)",6,,"2025-07-01 12:01"
"""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, newline="") as f:
        f.write(content)
        return f.name


@pytest.fixture
def responses_csv_rows():
    """Three event rows followed by two response rows with different availabilities."""
    return [
        {"Name": "Event: Saturday July 5 - 1pm", "Event Duration": "120"},
        {"Name": "Event: Sunday July 6 - 2pm (extra info)", "Event Duration": "90"},
        {"Name": "Event: Monday July 7 - 11am", "Event Duration": "60"},
        {
            "Name": "Alice Alpha",
            "Email Address": "alice@test.com",
            "Primary Role": "Leader",
            "Secondary Role": "I'm happy to dance my secondary role if it lets me attend when my primary is full",
            "Max Sessions": "2",
            "Availability": "Saturday July 5 - 1pm, Monday July 7 - 11am",
            "Min Interval Days": "0",
            "Timestamp": "2025-07-01 12:00",
        },
        {
            "Name": "Bob Beta",
            "Email Address": "bob@test.com",
            "Primary Role": "Follower",
            "Secondary Role": "I only want to be scheduled in my primary role",
            "Max Sessions": "1",
            "Availability": "Sunday July 6 - 2pm (extra info)",
            "Min Interval Days": "6",
            "Timestamp": "2025-07-01 12:01",
        },
    ]


@pytest.fixture
def sample_peeps():
    """Two sample peeps with all fields filled out."""
    return [
        Peep(
            id=1,
            full_name="Alice Alpha",
            display_name="Alice",
            email="alice@test.com",
            role=Role.LEADER,
            index=0,
            priority=1,
            total_attended=3,
            active=True,
            date_joined="2022-01-01",
        ),
        Peep(
            id=2,
            full_name="Bob Beta",
            display_name="Bob",
            email="bob@test.com",
            role=Role.FOLLOWER,
            index=1,
            priority=2,
            total_attended=5,
            active=True,
            date_joined="2022-01-01",
        ),
    ]


# ============================================================================
# TEST CLASSES
# ============================================================================


class TestCSVLoading:
    """Tests for CSV file loading and validation."""

    def test_load_csv_success_with_required_columns(self):
        content = "col1,col2,col3\na,b,c\n"
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
            tmp.write(content)
            tmp_path = Path(tmp.name)

        result = load_csv(tmp_path, required_columns=["col1", "col2"])
        assert isinstance(result, list)
        assert result[0]["col1"] == "a"
        assert result[0]["col3"] == "c"
        tmp_path.unlink()

    def test_load_csv_raises_on_missing_required_columns(self):
        content = "col1,col2\na,b\n"
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
            tmp.write(content)
            tmp_path = Path(tmp.name)

        with pytest.raises(ValueError):
            load_csv(tmp_path, required_columns=["col1", "col3"])
        tmp_path.unlink()

    def test_load_csv_strips_whitespace_from_fields(self, tmp_path):
        path = tmp_path / "trim.csv"
        path.write_text(" Name , Role \n Alice , Follow \n Bob , Lead \n")
        rows = load_csv(path)
        assert rows[0]["Name"] == "Alice"
        assert rows[0]["Role"] == "Follow"
        assert rows[1]["Name"] == "Bob"
        assert rows[1]["Role"] == "Lead"

    def test_load_csv_sanitizes_curly_quotes(self, tmp_path):
        """Test that curly quotes are converted to straight quotes."""
        path = tmp_path / "quotes.csv"
        # Using curly quotes: \u2018 \u2019 (single) \u201c \u201d (double)
        content = "Name,Description\nAlice,It\u2019s a test\nBob,He said \u201chello\u201d\n"
        path.write_text(content, encoding="utf-8")
        rows = load_csv(path)
        assert rows[0]["Description"] == "It's a test"  # Curly ' → straight '
        assert rows[1]["Description"] == 'He said "hello"'  # Curly " " → straight "

    def test_load_csv_normalizes_multiple_spaces(self, tmp_path):
        """Test that multiple spaces are normalized to single space."""
        path = tmp_path / "spaces.csv"
        # Double spaces in data
        path.write_text("Name,Location\nAlice,New  York\nBob,Los   Angeles\n")
        rows = load_csv(path)
        assert rows[0]["Location"] == "New York"  # Double space → single
        assert rows[1]["Location"] == "Los Angeles"  # Triple space → single

    def test_load_csv_sanitizes_mixed_formatting(self, tmp_path):
        """Test that curly quotes and multiple spaces are both sanitized."""
        path = tmp_path / "mixed.csv"
        # Combination of curly quotes and multiple spaces
        content = (
            "Name,Event\nAlice,Friday January  9th - 5:30pm to 7pm\nBob,It\u2019s  available\n"
        )
        path.write_text(content, encoding="utf-8")
        rows = load_csv(path)
        assert rows[0]["Event"] == "Friday January 9th - 5:30pm to 7pm"  # Double space → single
        assert rows[1]["Event"] == "It's available"  # Curly ' → straight ', double space → single


class TestJSONOperations:
    """Tests for JSON loading, saving, and serialization."""

    def test_load_json_file_not_found(self, tmp_path):
        """load_json should return None if file doesn't exist."""
        nonexistent_file = tmp_path / "missing.json"
        assert load_json(nonexistent_file) is None

    def test_save_json_serializes_dates(self, tmp_path):
        """Test save_json handles datetime.date, datetime.datetime, and fallback types."""
        data = {
            "today": datetime.date(2025, 7, 21),
            "now": datetime.datetime(2025, 7, 21, 15, 0),
            "fallback": {"custom": "data"},
        }
        out_path = tmp_path / "dates.json"
        save_json(data, out_path)

        loaded = json.loads(out_path.read_text())
        assert loaded["today"] == "2025-07-21"
        assert "2025" in loaded["now"]
        assert isinstance(loaded["fallback"], dict)

    def test_save_json_serializes_enum(self, tmp_path):
        """Ensure save_json uses .value for Enums."""
        data = {"role": Role.LEADER}
        out_path = tmp_path / "enum.json"
        save_json(data, out_path)

        result = json.loads(out_path.read_text())
        assert result["role"] == "leader"

    def test_save_json_fallback_str_for_unknown_type(self, tmp_path):
        """Ensure save_json falls back to str(obj) for unknown types like set."""
        path = tmp_path / "fallback.json"
        data = {"example": {1, 2, 3}}  # sets are not JSON serializable by default
        save_json(data, path)

        with path.open() as f:
            contents = json.load(f)

        # The set should have been stringified like "{1, 2, 3}"
        assert contents["example"].startswith("{") and "1" in contents["example"]


class TestTimeDateParsing:
    """Tests for time range and event date parsing functions."""

    def test_parse_time_range_basic(self):
        """Test parsing basic time ranges."""
        start, end, duration = parse_time_range("5pm to 6:30pm")
        assert start == "17:00"
        assert end == "18:30"
        assert duration == 90

    def test_parse_time_range_with_minutes(self):
        """Test parsing time range with minutes in start time."""
        start, end, duration = parse_time_range("5:30pm to 7pm")
        assert start == "17:30"
        assert end == "19:00"
        assert duration == 90

    def test_parse_time_range_afternoon(self):
        """Test parsing afternoon time range."""
        start, end, duration = parse_time_range("4pm to 5:30pm")
        assert start == "16:00"
        assert end == "17:30"
        assert duration == 90

    def test_parse_time_range_two_hours(self):
        """Test parsing 2-hour time range."""
        start, end, duration = parse_time_range("4pm to 6pm")
        assert start == "16:00"
        assert end == "18:00"
        assert duration == 120

    def test_parse_time_range_invalid_format(self):
        """Test that invalid format raises ValueError."""
        with pytest.raises(ValueError, match="invalid time range format"):
            parse_time_range("5pm - 6pm")  # Wrong separator

    def test_parse_time_range_invalid_time(self):
        """Test that invalid time format raises ValueError."""
        with pytest.raises(ValueError, match="time out of range"):
            parse_time_range("25pm to 6pm")  # Invalid hour

    def test_parse_time_range_end_before_start(self):
        """Test that end time before start time raises ValueError."""
        with pytest.raises(ValueError, match="end time must be after start time"):
            parse_time_range("6pm to 5pm")


class TestPeepLoading:
    """Tests for peep loading and validation."""

    def test_load_peeps(self, peeps_csv_path):
        """Check that peeps load correctly and inactive peep with blank email is allowed."""
        peeps = load_peeps(peeps_csv_path)
        assert len(peeps) == 3
        assert peeps[0].full_name == "Alice Alpha"
        assert peeps[0].email == "alice@test.com"
        assert peeps[2].active is False
        assert peeps[2].email == ""

    def test_load_peeps_allows_inactive_peep_with_email(self, tmp_path):
        """Inactive peeps with non-blank emails should be allowed without error."""
        csv_path = tmp_path / "peeps.csv"
        csv_path.write_text(
            "id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined\n"
            "1,Alice,Alice,alice@test.com,Leader,0,1,3,TRUE,2022-01-01\n"
            "2,Inactive,Inactive,inactive@test.com,Follower,1,2,2,FALSE,2022-01-01\n"
        )
        peeps = load_peeps(csv_path)
        assert len(peeps) == 2
        assert peeps[1].active is False
        assert peeps[1].email == "inactive@test.com"


class TestResponseProcessing:
    """Tests for processing response data and updating peeps."""

    def test_process_responses(self, peeps_csv_path, responses_csv_rows):
        """Ensure peeps are updated with correct role, preferences, and availability."""
        peeps = load_peeps(peeps_csv_path)
        event_map = extract_events(responses_csv_rows)
        updated_peeps, responses = process_responses(responses_csv_rows, peeps, event_map)

        assert len(responses) == 2
        peep_map = {p.email: p for p in updated_peeps if p.email}

        alice = peep_map["alice@test.com"]
        assert alice.role == Role.LEADER
        assert alice.switch_pref == SwitchPreference.SWITCH_IF_PRIMARY_FULL
        assert alice.availability == [0, 2]

        bob = peep_map["bob@test.com"]
        assert bob.role == Role.FOLLOWER
        assert bob.availability == [1]

    def test_unknown_event_in_availability_logs_warning(self, peeps_csv_path):
        """Availability listing a date that wasn't defined in Event rows should log warning and skip."""
        rows = [
            {"Name": "Event: Saturday July 5 - 1pm", "Event Duration": "120"},
            {
                "Name": "Alice Alpha",
                "Email Address": "alice@test.com",
                "Primary Role": "Leader",
                "Secondary Role": "I'm happy to dance my secondary role if it lets me attend when my primary is full",
                "Max Sessions": "2",
                "Availability": "Saturday July 5 - 1pm, Monday July 7 - 11am",  # Monday not in events
                "Min Interval Days": "0",
                "Timestamp": "2025-07-01 12:00",
            },
        ]
        peeps = load_peeps(peeps_csv_path)
        event_map = extract_events(rows)

        # Should not raise, just log warning and skip unknown event
        updated_peeps, _responses = process_responses(rows, peeps, event_map)

        # Alice should only have availability for the known event (Saturday July 5)
        alice = next(p for p in updated_peeps if p.email == "alice@test.com")
        assert len(alice.availability) == 1  # Only Saturday event, Monday skipped

    def test_process_responses_missing_name_skipped(self, valid_peeps_rows):
        """Row missing name should be skipped without error."""
        peeps = [Peep.from_csv(p) for p in valid_peeps_rows]
        rows = [
            {
                "Name": "",
                "Email Address": "x",
                "Primary Role": "Leader",
                "Secondary Role": "NONE",
                "Max Sessions": "1",
                "Availability": "July 5 - 1pm",
                "Timestamp": "2025-07-01 12:00",
            }
        ]
        assert process_responses(rows, peeps, {})[1] == []

    def test_process_responses_missing_email_raises(self, valid_peeps_rows):
        """Blank email in active response row should raise ValueError."""
        peeps = [Peep.from_csv(p) for p in valid_peeps_rows]
        rows = [
            {
                "Name": "Alice",
                "Email Address": "",
                "Primary Role": "Leader",
                "Secondary Role": "NONE",
                "Max Sessions": "1",
                "Availability": "July 5 - 1pm",
                "Timestamp": "2025-07-01 12:00",
            }
        ]
        with pytest.raises(ValueError, match="missing email"):
            process_responses(rows, peeps, {})

    def test_process_responses_unknown_email_raises(self, valid_peeps_rows):
        """Email not matching any peep should raise."""
        peeps = [Peep.from_csv(p) for p in valid_peeps_rows]
        rows = [
            {
                "Name": "Unknown",
                "Email Address": "notfound@test.com",
                "Primary Role": "Leader",
                "Secondary Role": "NONE",
                "Max Sessions": "1",
                "Availability": "July 5 - 1pm",
                "Timestamp": "2025-07-01 12:00",
            }
        ]
        with pytest.raises(ValueError, match="no matching peep found for email"):
            process_responses(rows, peeps, {})


class TestDataSaving:
    """Tests for saving peeps, sequences, and events."""

    def test_save_peeps_csv(self, sample_peeps):
        """Ensure save_peeps_csv writes correct rows and creates file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            output_path = tmpdir / "members_updated.csv"
            save_peeps_csv(sample_peeps, output_path)

            assert output_path.exists()

            with output_path.open() as f:
                lines = f.readlines()
                assert lines[0].startswith("id,Name,Display Name")  # Header
                assert "Alice Alpha" in lines[1]
                assert "Bob Beta" in lines[2]

    def test_save_event_sequence(self, tmp_path):
        """Test saving an EventSequence to JSON."""
        # Step 1: Create peeps
        peep1 = Peep(
            id=1,
            full_name="Alice",
            display_name="Alice",
            email="alice@example.com",
            role=Role.LEADER,
            index=0,
            priority=1,
        )
        peep2 = Peep(
            id=2,
            full_name="Bob",
            display_name="Bob",
            email="bob@example.com",
            role=Role.FOLLOWER,
            index=1,
            priority=2,
        )

        # Step 2: Create events
        event1 = Event(id=0, date=datetime.datetime(2025, 7, 5, 13), duration_minutes=120)
        event2 = Event(id=1, date=datetime.datetime(2025, 7, 6, 14), duration_minutes=90)

        # Step 3: Add attendees
        event1.add_attendee(peep1, Role.LEADER)
        event1.add_attendee(peep2, Role.FOLLOWER)
        event2.add_attendee(peep1, Role.LEADER)

        # Step 4: Build EventSequence
        peeps = [peep1, peep2]
        events = [event1, event2]
        sequence = EventSequence(events, peeps)
        sequence.valid_events = events

        # Step 5: Save to temp file
        output_path = tmp_path / "sequence.json"
        save_event_sequence(sequence, output_path)

        # Step 6: Verify file contents
        with output_path.open() as f:
            data = json.load(f)

        assert "valid_events" in data
        assert "peeps" in data
        valid_events = data["valid_events"]
        assert len(valid_events) == 2
        assert any(e["id"] == 0 for e in valid_events)
        assert any("attendees" in e for e in valid_events)
        assert len(data["peeps"]) == 2


class TestIntegration:
    """End-to-end integration tests."""

    def test_convert_to_json_and_load_data_from_json_roundtrip(
        self, peeps_csv_path, responses_csv_path
    ):
        """Test full roundtrip from CSVs -> output.json -> object loading."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            output_json_path = f.name

        convert_to_json(responses_csv_path, peeps_csv_path, output_json_path)
        loaded_peeps, loaded_events = load_data_from_json(output_json_path)

        assert len(loaded_peeps) == 3  # includes inactive peep
        assert len(loaded_events) == 3
        assert loaded_events[0].duration_minutes == 120

        # Alice
        alice = next(p for p in loaded_peeps if p.email == "alice@test.com")
        assert alice.role == Role.LEADER
        assert alice.switch_pref == SwitchPreference.SWITCH_IF_PRIMARY_FULL
        assert alice.availability == [0, 2]
        assert alice.responded is True

        # Bob
        bob = next(p for p in loaded_peeps if p.email == "bob@test.com")
        assert bob.role == Role.FOLLOWER
        assert bob.switch_pref == SwitchPreference.PRIMARY_ONLY
        assert bob.availability == [1]
        assert bob.responded is True
