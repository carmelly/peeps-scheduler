from __future__ import annotations

import csv
import json
import logging
import re
from pathlib import Path
from peeps_scheduler.data_manager import get_data_manager
from peeps_scheduler.file_io import load_csv, normalize_email

TOPICS_COLUMN = "Deep Dive Topics"
TOP_K = 6


def assign_topics(period_slug: str) -> None:
    dm = get_data_manager()
    period_path = dm.get_period_path(period_slug)
    assign_topics_for_period(period_path)


def _load_valid_topics(period_config_path: Path) -> list[str]:
    with period_config_path.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict) or "topics" not in data:
        raise ValueError("period_config.json must contain a 'topics' list.")
    topics = data["topics"]
    if not isinstance(topics, list):
        raise ValueError("period_config.json 'topics' must be a list.")
    return [str(topic) for topic in topics]


def _strip_parenthetical(value: str) -> str:
    if not value:
        return ""
    return re.sub(r"\([^)]*\)", "", value)


def _normalize_topic(value: str) -> str:
    return " ".join(_strip_parenthetical(value).split()).strip()


def _parse_topics_value(value: str) -> list[str]:
    # Drop parenthetical descriptions before splitting on commas.
    cleaned = _strip_parenthetical(value)
    parts = [part.strip() for part in cleaned.split(",")]
    normalized = []
    for part in parts:
        normalized_part = _normalize_topic(part)
        if normalized_part:
            normalized.append(normalized_part)
    return normalized


def _build_topic_lookup(valid_topics: list[str]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for topic in valid_topics:
        normalized = _normalize_topic(topic)
        if not normalized:
            raise ValueError("topics list contains a blank topic.")
        if normalized in lookup and lookup[normalized] != topic:
            raise ValueError(
                "topics list contains duplicate topics after normalization: "
                f"'{lookup[normalized]}' and '{topic}'"
            )
        lookup.setdefault(normalized, topic)
    return lookup


def _responses_has_topics_column(responses_path: Path) -> bool:
    with responses_path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        try:
            raw_headers = next(reader)
        except StopIteration:
            logging.warning("responses.csv is empty; skipping topic assignment.")
            return False

    headers = [header.strip() for header in raw_headers]
    if TOPICS_COLUMN not in headers:
        logging.warning(
            "Deep Dive Topics column not found in responses.csv; skipping topic assignment."
        )
        return False
    return True


def _load_topics_by_email(
    responses_path: Path, topic_lookup: dict[str, str]
) -> dict[str, set[str]]:
    topics_by_email: dict[str, set[str]] = {}

    with responses_path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        try:
            raw_headers = next(reader)
        except StopIteration:
            return topics_by_email

        headers = [header.strip() for header in raw_headers]
        if TOPICS_COLUMN not in headers:
            raise ValueError("responses.csv missing Deep Dive Topics column.")

        email_index = headers.index("Email Address") if "Email Address" in headers else None
        topics_index = headers.index(TOPICS_COLUMN)
        expected_len = len(headers)

        for row in reader:
            if not row:
                continue

            if len(row) < expected_len:
                row = row + [""] * (expected_len - len(row))
            elif len(row) > expected_len:
                # Merge unquoted commas back into the topics field.
                extra = len(row) - expected_len
                merged = ",".join(row[topics_index : topics_index + extra + 1])
                row = [*row[:topics_index], merged, *row[topics_index + extra + 1 :]]

            if email_index is None:
                continue
            email = normalize_email(row[email_index].strip())
            if not email:
                continue

            raw_topics = row[topics_index].strip() if topics_index < len(row) else ""
            topics = _parse_topics_value(raw_topics)
            valid = {topic_lookup[topic] for topic in topics if topic in topic_lookup}
            if not valid:
                continue
            topics_by_email.setdefault(email, set()).update(valid)

    return topics_by_email


def _build_member_email_map(members_path: Path) -> dict[object, str]:
    members = load_csv(str(members_path))
    mapping: dict[object, str] = {}
    for row in members:
        raw_id = row.get("id", "").strip()
        if not raw_id:
            continue
        email = normalize_email(row.get("Email Address", ""))
        try:
            mapping[int(raw_id)] = email
        except ValueError:
            mapping[raw_id] = email
    return mapping


def _email_for_attendee(attendee_id: object, id_to_email: dict[object, str]) -> str:
    if attendee_id in id_to_email:
        return id_to_email[attendee_id]
    return id_to_email.get(str(attendee_id), "")


def assign_topics_for_period(period_path: Path) -> None:
    period_path = Path(period_path)
    results_path = period_path / "results.json"
    responses_path = period_path / "responses.csv"
    members_path = period_path / "members.csv"
    period_config_path = period_path / "period_config.json"

    if not responses_path.exists():
        raise FileNotFoundError(f"responses.csv not found: {responses_path}")
    if not members_path.exists():
        raise FileNotFoundError(f"members.csv not found: {members_path}")
    if not results_path.exists():
        raise FileNotFoundError(f"results.json not found: {results_path}")

    if not _responses_has_topics_column(responses_path):
        return

    if not period_config_path.exists():
        raise FileNotFoundError(f"period_config.json not found: {period_config_path}")
    valid_topics = _load_valid_topics(period_config_path)
    topic_lookup = _build_topic_lookup(valid_topics)
    topics_by_email = _load_topics_by_email(responses_path, topic_lookup)

    id_to_email = _build_member_email_map(members_path)

    results = json.loads(results_path.read_text(encoding="utf-8"))
    events = results.get("valid_events", [])
    event_keys = list(range(len(events)))

    event_attendees: dict[int, set[str]] = {}
    candidates_by_event: dict[int, list[tuple[str, int]]] = {}
    max_scores: dict[int, int] = {}
    score_summaries: dict[int, list[tuple[str, int]]] = {}

    for event_index, event in enumerate(events):
        attendees = set()
        for attendee in event.get("attendees", []):
            email = _email_for_attendee(attendee.get("id"), id_to_email)
            if email:
                attendees.add(email)
        event_attendees[event_index] = attendees

        score_map: dict[str, int] = {topic: 0 for topic in valid_topics}
        for email in attendees:
            for topic in topics_by_email.get(email, set()):
                score_map[topic] = score_map.get(topic, 0) + 1

        positive_candidates = [(topic, score) for topic, score in score_map.items() if score > 0]
        if positive_candidates:
            positive_candidates.sort(key=lambda item: (-item[1], item[0]))
            candidates = positive_candidates[:TOP_K]
        else:
            candidates = []

        # Always include zero-score fallbacks to guarantee an assignment.
        candidate_topics = {topic for topic, _ in candidates}
        fallback_topics = [topic for topic in sorted(valid_topics) if topic not in candidate_topics]
        candidates.extend((topic, 0) for topic in fallback_topics)

        if not candidates:
            raise ValueError("no candidates available for topic assignment.")

        candidates_by_event[event_index] = candidates
        max_scores[event_index] = max(score for _, score in candidates)
        score_summaries[event_index] = sorted(
            score_map.items(), key=lambda item: (-item[1], item[0])
        )

    ordered_events = sorted(
        event_keys,
        key=lambda idx: (len(candidates_by_event[idx]), events[idx].get("id", idx), idx),
    )
    tuple_order = event_keys

    best_total = -1
    best_min_score = -1
    best_tuple: tuple[str, ...] | None = None
    best_assignment: dict[int, str] = {}

    assigned_topics: dict[int, str] = {}
    topic_used_by: dict[str, set[str]] = {}

    remaining_max = [max_scores[idx] for idx in ordered_events]

    # DFS over full assignments to maximize total score under the overlap constraint.
    def dfs(position: int, current_total: int, current_min: int) -> None:
        nonlocal best_total, best_min_score, best_tuple, best_assignment

        if position == len(ordered_events):
            assignment_tuple = tuple(assigned_topics.get(idx, "") for idx in tuple_order)
            if current_total > best_total or (
                current_total == best_total
                and (
                    current_min > best_min_score
                    or (
                        current_min == best_min_score
                        and (best_tuple is None or assignment_tuple < best_tuple)
                    )
                )
            ):
                best_total = current_total
                best_min_score = current_min
                best_tuple = assignment_tuple
                best_assignment = dict(assigned_topics)
            return

        # Prune if even the best possible remaining scores can't beat the current best.
        upper_bound = current_total + sum(remaining_max[position:])
        if upper_bound < best_total:
            return

        event_key = ordered_events[position]
        attendees = event_attendees[event_key]

        for topic, score in candidates_by_event[event_key]:
            used_by = topic_used_by.get(topic)
            if used_by is not None and not used_by.isdisjoint(attendees):
                continue

            assigned_topics[event_key] = topic

            previous_used = None
            if used_by is None:
                topic_used_by[topic] = set(attendees)
            else:
                previous_used = used_by
                topic_used_by[topic] = used_by | attendees

            next_min = score if position == 0 else min(current_min, score)
            dfs(position + 1, current_total + score, next_min)

            if previous_used is None:
                topic_used_by.pop(topic, None)
            else:
                topic_used_by[topic] = previous_used

            assigned_topics.pop(event_key, None)

    dfs(0, 0, 0)

    if len(best_assignment) != len(events):
        raise ValueError("could not assign topics to all events.")

    for idx, event in enumerate(events):
        event["topic"] = best_assignment.get(idx)
        event["topic_scores"] = [
            {"topic": topic, "score": score} for topic, score in score_summaries.get(idx, [])
        ]

    results_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
