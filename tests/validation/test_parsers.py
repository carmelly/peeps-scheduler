from datetime import datetime
import pytest
from peeps_scheduler.validation.parsers import EventSpec, parse_event_name


@pytest.mark.unit
class TestParseEventName:
    """Test parser function parse_event_name()."""

    @pytest.mark.parametrize(
        "event_name", ["Saturday January 4 - 1pm", "Saturday January 4 - 1:00pm"]
    )
    def test_old_format_valid(self, event_name, ctx):
        """Test parsing of old format event names without duration."""
        parsed: EventSpec = parse_event_name(event_name, ctx.year, ctx.tz)
        assert parsed.start == datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)
        assert parsed.duration_minutes is None

    @pytest.mark.parametrize(
        "event_name,expected_duration",
        [
            ("Saturday January 4 - 1pm to 3pm", 120),
            ("Saturday January 4 - 1pm to 2:30pm", 90),
        ],
    )
    def test_new_format_valid(self, event_name, expected_duration, ctx):
        """Test parsing of new format event names with duration."""
        parsed: EventSpec = parse_event_name(event_name, ctx.year, ctx.tz)
        assert parsed.start == datetime(2020, 1, 4, 13, 0, tzinfo=ctx.tz)
        assert parsed.duration_minutes == expected_duration

    def test_empty_event_name_raises(self, ctx):
        """Test that empty event name raises ValueError."""
        with pytest.raises(ValueError, match=r"invalid event name: \"\""):
            parse_event_name("", ctx.year, ctx.tz)

    @pytest.mark.parametrize(
        "event_name, expected_start",
        [
            ("Sunday March 1st - 1pm", datetime(2020, 3, 1, 13, 0)),
            ("Thursday April 2nd - 10am", datetime(2020, 4, 2, 10, 0)),
            ("Sunday May 3rd - 5pm", datetime(2020, 5, 3, 17, 0)),
            ("Tuesday February 4th - 9am", datetime(2020, 2, 4, 9, 0)),
        ],
    )
    def test_event_name_valid_with_ordinal_suffix(self, event_name, expected_start, ctx):
        """Test parsing of event names with ordinal suffixes."""
        # Set expected timezone to match context
        expected_start = expected_start.replace(tzinfo=ctx.tz)

        parsed: EventSpec = parse_event_name(event_name, ctx.year, ctx.tz)
        assert parsed.start == expected_start

    @pytest.mark.parametrize(
        "invalid_name",
        [
            "January 4 - 1pm",  # Missing weekday
            "Saturday Feb 14 - 1pm",  # Invalid month format
            "Saturday January 4 1pm",  # Missing hyphen
            "invalid name",  # Totally invalid
        ],
    )
    def test_parse_event_name_invalid_raises(self, invalid_name, ctx):
        """Test that invalid event names raise ValueError."""
        with pytest.raises(ValueError, match=r"invalid event name"):
            parse_event_name(invalid_name, ctx.year, ctx.tz)

    def test_weekday_mismatch_raises(self, ctx):
        """Test that event name weekday not matching date raises ValueError."""
        event_name = "Friday January 4 - 1pm"  # Jan 4, 2020 is a Saturday
        with pytest.raises(ValueError, match=r"weekday does not match"):
            parse_event_name(event_name, ctx.year, ctx.tz)

    def test_end_time_before_start_raises(self, ctx):
        """Test that end time before start time raises ValueError."""
        event_name = "Saturday January 4 - 3pm to 1pm"
        with pytest.raises(ValueError, match=r"end time must be after start time"):
            parse_event_name(event_name, ctx.year, ctx.tz)

    def test_invalid_end_time_format_raises(self, ctx):
        """Test that invalid end time format raises ValueError."""
        event_name = "Saturday January 4 - 1pm to invalid"
        with pytest.raises(ValueError, match=r"invalid event duration"):
            parse_event_name(event_name, ctx.year, ctx.tz)
