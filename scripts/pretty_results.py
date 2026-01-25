#!/usr/bin/env python3
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Allow running from the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from peeps_scheduler.constants import DATE_FORMAT
from peeps_scheduler.data_manager import get_data_manager


def _load_results(results_path: Path) -> dict:
    with results_path.open(encoding="utf-8") as f:
        return json.load(f)


def _event_sort_key(event: dict) -> tuple[int, object]:
    date_str = event.get("date", "")
    try:
        return (0, datetime.strptime(date_str, DATE_FORMAT))
    except (TypeError, ValueError):
        return (1, date_str or "")


def _format_event_header(event: dict) -> str:
    date_str = event.get("date", "")
    duration = event.get("duration_minutes")
    try:
        dt = datetime.strptime(date_str, DATE_FORMAT)
        weekday = dt.strftime("%A")
        month = dt.strftime("%B")
        day = dt.day
        hour = dt.hour % 12 or 12
        minute = dt.minute
        ampm = "am" if dt.hour < 12 else "pm"
        time_str = f"{hour}{ampm}" if minute == 0 else f"{hour}:{minute:02d}{ampm}"
        date_part = f"{weekday} {month} {day} - {time_str}"
    except (TypeError, ValueError):
        date_part = date_str or "Unknown date"

    if duration:
        return f"{date_part}, {duration} mins"
    return date_part


def _print_event(event: dict) -> None:
    topic = event.get("topic")
    leaders = event.get("leaders_string", "")
    followers = event.get("followers_string", "")
    scores = event.get("topic_scores", [])

    header = _format_event_header(event)
    if topic:
        header += f" -> {topic}"
    print(header)

    if leaders:
        print(f"  {leaders}")
    if followers:
        print(f"  {followers}")

    if scores:
        print("  Topic scores:")
        for item in scores:
            topic_name = item.get("topic", "")
            score = item.get("score", 0)
            print(f"    - {topic_name}: {score}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pretty-print results.json for quick review.",
    )
    parser.add_argument(
        "--period-folder",
        required=True,
        help="Period slug (e.g., 2026-02)",
    )
    parser.add_argument(
        "--results-file",
        default="results.json",
        help="Results filename (default: results.json)",
    )

    args = parser.parse_args()
    dm = get_data_manager()
    period_path = dm.get_period_path(args.period_folder)
    results_path = period_path / args.results_file

    if not results_path.exists():
        raise FileNotFoundError(f"results.json not found: {results_path}")

    results = _load_results(results_path)
    events = results.get("valid_events", [])

    for event in sorted(events, key=_event_sort_key):
        _print_event(event)
        print()


if __name__ == "__main__":
    main()
