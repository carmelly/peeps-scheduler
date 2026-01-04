import re
from dataclasses import dataclass
from datetime import datetime
from peeps_scheduler.constants import DATE_FORMAT


@dataclass(frozen=True)
class EventSpec:
    """Data class representing a parsed event specification."""

    start: datetime
    duration_minutes: int | None
    raw: str


def parse_event_name(event_name: str, year: int, tz: datetime.tzinfo) -> EventSpec:
    if not event_name:
        raise ValueError('invalid event name: ""')

    raw = event_name
    # Remove ordinal suffixes from date
    event_name = event_name.strip().lower()
    event_name = re.sub(r"(\d)(st|nd|rd|th)", r"\1", event_name)

    # split optional duration
    if " to " in event_name:
        start_part, end_part = event_name.split(" to ", 1)
    else:
        start_part, end_part = event_name, None

    # Parse start datetime
    start_part = f"{year} {start_part}"  # Add year for parsing
    supported_formats = ["%Y %A %B %d - %I%p", "%Y %A %B %d - %I:%M%p"]
    start_dt = None
    for fmt in supported_formats:
        try:
            start_dt = datetime.strptime(start_part.strip(), fmt)
            break
        except ValueError:
            continue
    if start_dt is None:
        raise ValueError(f"invalid event name: {event_name}")
    start_dt = start_dt.replace(tzinfo=tz)  # Set timezone

    # Validate weekday
    given_weekday = start_part.strip().split()[1].lower()  # Second part because we added year
    actual_weekday = start_dt.strftime("%A").lower()
    if given_weekday != actual_weekday:
        raise ValueError(
            f"weekday does not match date: {event_name} (weekday should be {actual_weekday})"
        )

    # Derive duration if end_part is given
    duration_minutes = None
    if end_part:
        end_formats = ["%I%p", "%I:%M%p"]
        end_time = None
        for fmt in end_formats:
            try:
                end_time = datetime.strptime(end_part.strip(), fmt)
                break
            except ValueError:
                continue
        if end_time is None:
            raise ValueError(f"invalid event duration: {event_name}")
        end_dt = start_dt.replace(hour=end_time.hour, minute=end_time.minute)
        if end_dt <= start_dt:
            raise ValueError("end time must be after start time")
        duration_minutes = int((end_dt - start_dt).total_seconds() // 60)

    return EventSpec(start=start_dt, duration_minutes=duration_minutes, raw=raw)


def parse_event_datetime(v, tz: datetime.tzinfo):
    if isinstance(v, datetime):
        dt = v
    elif isinstance(v, str):
        try:
            dt = datetime.strptime(v, DATE_FORMAT)
        except ValueError as e:
            raise ValueError(f"invalid event datetime: {v}") from e
    else:
        raise ValueError("date must be a string")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt
