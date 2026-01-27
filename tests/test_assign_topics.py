import json
from pathlib import Path
import pytest
from peeps_scheduler.assign_topics import assign_topics_for_period

pytestmark = pytest.mark.unit


def write_members_csv(path: Path, rows: list[tuple[int, str, str, str]]) -> None:
    header = (
        "id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined"
    )
    lines = [header]
    for idx, name, display, email in rows:
        lines.append(f"{idx},{name},{display},{email},Leader,{idx},0,0,TRUE,2025-01-01")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_responses_csv(path: Path, rows: list[tuple[str, str]]) -> None:
    header = "Email Address,Deep Dive Topics"
    lines = [header]
    for email, topics in rows:
        lines.append(f"{email},{topics}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_results_json(path: Path, events: list[dict], extra: dict | None = None) -> dict:
    data = {
        "valid_events": events,
        "peeps": [],
        "num_unique_attendees": 0,
        "priority_fulfilled": 0,
        "system_weight": 0,
    }
    if extra:
        data.update(extra)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return data


def make_event(event_id: int, attendees: list[int], alternates: list[int] | None = None) -> dict:
    alternates = alternates or []
    return {
        "id": event_id,
        "date": "2026-02-01 18:00",
        "duration_minutes": 90,
        "attendees": [
            {"id": attendee_id, "name": f"Peep {attendee_id}", "role": "leader"}
            for attendee_id in attendees
        ],
        "alternates": [
            {"id": alternate_id, "name": f"Peep {alternate_id}", "role": "leader"}
            for alternate_id in alternates
        ],
        "leaders_string": "",
        "followers_string": "",
    }


def write_period_config_json_with_topics(path: Path, topics: list[str]) -> None:
    path.write_text(json.dumps({"topics": topics}, indent=2), encoding="utf-8")


class TestAssignTopicsConstraints:
    def test_assign_topics_avoids_overlap_reuse(self, tmp_path):
        period_path = tmp_path / "2026-02"
        period_path.mkdir()

        write_members_csv(
            period_path / "members.csv",
            [
                (1, "Alice Alpha", "Alice", "alice@example.com"),
                (2, "Bob Beta", "Bob", "bob@example.com"),
                (3, "Carol Gamma", "Carol", "carol@example.com"),
            ],
        )
        write_responses_csv(
            period_path / "responses.csv",
            [
                ("alice@example.com", "Topic A"),
                ("bob@example.com", "Topic A"),
                ("carol@example.com", "Topic A, Topic C"),
            ],
        )
        write_period_config_json_with_topics(
            period_path / "period_config.json", ["Topic A", "Topic C"]
        )
        write_results_json(
            period_path / "results.json",
            [
                make_event(1, attendees=[1, 2]),
                make_event(2, attendees=[2, 3]),
            ],
        )

        assign_topics_for_period(period_path)

        updated = json.loads((period_path / "results.json").read_text(encoding="utf-8"))
        assert updated["valid_events"][0]["topic"] == "Topic A"
        assert updated["valid_events"][1]["topic"] == "Topic C"

    def test_assign_topics_allows_reuse_when_disjoint(self, tmp_path):
        period_path = tmp_path / "2026-02"
        period_path.mkdir()

        write_members_csv(
            period_path / "members.csv",
            [
                (1, "Alice Alpha", "Alice", "alice@example.com"),
                (2, "Bob Beta", "Bob", "bob@example.com"),
            ],
        )
        write_responses_csv(
            period_path / "responses.csv",
            [
                ("alice@example.com", "Topic A"),
                ("bob@example.com", "Topic A"),
            ],
        )
        write_period_config_json_with_topics(period_path / "period_config.json", ["Topic A"])
        write_results_json(
            period_path / "results.json",
            [
                make_event(1, attendees=[1]),
                make_event(2, attendees=[2]),
            ],
        )

        assign_topics_for_period(period_path)

        updated = json.loads((period_path / "results.json").read_text(encoding="utf-8"))
        assert updated["valid_events"][0]["topic"] == "Topic A"
        assert updated["valid_events"][1]["topic"] == "Topic A"


class TestAssignTopicsScoring:
    def test_assign_topics_picks_valid_topic_when_scores_empty(self, tmp_path):
        period_path = tmp_path / "2026-02"
        period_path.mkdir()

        write_members_csv(
            period_path / "members.csv",
            [(1, "Alice Alpha", "Alice", "alice@example.com")],
        )
        write_responses_csv(period_path / "responses.csv", [("alice@example.com", "Topic B")])
        write_period_config_json_with_topics(period_path / "period_config.json", ["Topic A"])
        write_results_json(period_path / "results.json", [make_event(1, attendees=[1])])

        assign_topics_for_period(period_path)

        updated = json.loads((period_path / "results.json").read_text(encoding="utf-8"))
        assert updated["valid_events"][0]["topic"] == "Topic A"

    def test_assign_topics_ignores_alternates(self, tmp_path):
        period_path = tmp_path / "2026-02"
        period_path.mkdir()

        write_members_csv(
            period_path / "members.csv",
            [
                (1, "Alice Alpha", "Alice", "alice@example.com"),
                (2, "Bob Beta", "Bob", "bob@example.com"),
            ],
        )
        write_responses_csv(
            period_path / "responses.csv",
            [
                ("alice@example.com", "Write In"),
                ("bob@example.com", "Topic B"),
            ],
        )
        write_period_config_json_with_topics(
            period_path / "period_config.json", ["Topic A", "Topic B"]
        )
        write_results_json(
            period_path / "results.json",
            [make_event(1, attendees=[1], alternates=[2])],
        )

        assign_topics_for_period(period_path)

        updated = json.loads((period_path / "results.json").read_text(encoding="utf-8"))
        assert updated["valid_events"][0]["topic"] == "Topic A"

    def test_assign_topics_uses_lexicographic_fallback(self, tmp_path):
        period_path = tmp_path / "2026-02"
        period_path.mkdir()

        write_members_csv(
            period_path / "members.csv",
            [(1, "Alice Alpha", "Alice", "alice@example.com")],
        )
        write_responses_csv(period_path / "responses.csv", [("alice@example.com", "Write In")])
        write_period_config_json_with_topics(
            period_path / "period_config.json", ["Topic B", "Topic A"]
        )
        write_results_json(period_path / "results.json", [make_event(1, attendees=[1])])

        assign_topics_for_period(period_path)

        updated = json.loads((period_path / "results.json").read_text(encoding="utf-8"))
        assert updated["valid_events"][0]["topic"] == "Topic A"

    def test_assign_topics_ignores_invalid_topics_with_valid_choices(self, tmp_path):
        period_path = tmp_path / "2026-02"
        period_path.mkdir()

        write_members_csv(
            period_path / "members.csv",
            [
                (1, "Alice Alpha", "Alice", "alice@example.com"),
                (2, "Bob Beta", "Bob", "bob@example.com"),
            ],
        )
        write_responses_csv(
            period_path / "responses.csv",
            [
                ("alice@example.com", "Write In, Topic A"),
                ("bob@example.com", "Write In"),
            ],
        )
        write_period_config_json_with_topics(
            period_path / "period_config.json", ["Topic A", "Topic B"]
        )
        write_results_json(period_path / "results.json", [make_event(1, attendees=[1, 2])])

        assign_topics_for_period(period_path)

        updated = json.loads((period_path / "results.json").read_text(encoding="utf-8"))
        assert updated["valid_events"][0]["topic"] == "Topic A"

    def test_assign_topics_breaks_score_ties_lexicographically(self, tmp_path):
        period_path = tmp_path / "2026-02"
        period_path.mkdir()

        write_members_csv(
            period_path / "members.csv",
            [
                (1, "Alice Alpha", "Alice", "alice@example.com"),
                (2, "Bob Beta", "Bob", "bob@example.com"),
            ],
        )
        write_responses_csv(
            period_path / "responses.csv",
            [
                ("alice@example.com", "Topic B"),
                ("bob@example.com", "Topic A"),
            ],
        )
        write_period_config_json_with_topics(
            period_path / "period_config.json", ["Topic B", "Topic A"]
        )
        write_results_json(period_path / "results.json", [make_event(1, attendees=[1, 2])])

        assign_topics_for_period(period_path)

        updated = json.loads((period_path / "results.json").read_text(encoding="utf-8"))
        assert updated["valid_events"][0]["topic"] == "Topic A"


class TestAssignTopicsInputHandling:
    def test_assign_topics_warns_without_column(self, tmp_path, caplog):
        period_path = tmp_path / "2026-02"
        period_path.mkdir()

        write_members_csv(
            period_path / "members.csv",
            [(1, "Alice Alpha", "Alice", "alice@example.com")],
        )
        responses_path = period_path / "responses.csv"
        responses_path.write_text("Email Address,Name\nalice@example.com,Alice\n", encoding="utf-8")
        write_period_config_json_with_topics(period_path / "period_config.json", ["Topic A"])
        write_results_json(period_path / "results.json", [make_event(1, attendees=[1])])

        assign_topics_for_period(period_path)

        assert "Deep Dive Topics" in caplog.text
        assert "skipping topic assignment" in caplog.text.lower()

    def test_assign_topics_no_change_without_column(self, tmp_path):
        period_path = tmp_path / "2026-02"
        period_path.mkdir()

        write_members_csv(
            period_path / "members.csv",
            [(1, "Alice Alpha", "Alice", "alice@example.com")],
        )
        responses_path = period_path / "responses.csv"
        responses_path.write_text("Email Address,Name\nalice@example.com,Alice\n", encoding="utf-8")
        write_period_config_json_with_topics(period_path / "period_config.json", ["Topic A"])
        original = write_results_json(period_path / "results.json", [make_event(1, attendees=[1])])

        assign_topics_for_period(period_path)

        updated = json.loads((period_path / "results.json").read_text(encoding="utf-8"))
        assert updated == original

    def test_assign_topics_strips_parenthetical_descriptions(self, tmp_path):
        period_path = tmp_path / "2026-02"
        period_path.mkdir()

        write_members_csv(
            period_path / "members.csv",
            [(1, "Alice Alpha", "Alice", "alice@example.com")],
        )
        write_responses_csv(
            period_path / "responses.csv",
            [
                (
                    "alice@example.com",
                    "Rhythm & Blues (swung timing, swung body action, rhythmic footwork)",
                )
            ],
        )
        write_period_config_json_with_topics(period_path / "period_config.json", ["Rhythm & Blues"])
        write_results_json(period_path / "results.json", [make_event(1, attendees=[1])])

        assign_topics_for_period(period_path)

        updated = json.loads((period_path / "results.json").read_text(encoding="utf-8"))
        assert updated["valid_events"][0]["topic"] == "Rhythm & Blues"


class TestAssignTopicsOutput:
    def test_assign_topics_preserves_results_fields(self, tmp_path):
        period_path = tmp_path / "2026-02"
        period_path.mkdir()

        write_members_csv(
            period_path / "members.csv",
            [(1, "Alice Alpha", "Alice", "alice@example.com")],
        )
        write_responses_csv(period_path / "responses.csv", [("alice@example.com", "Topic A")])
        write_period_config_json_with_topics(period_path / "period_config.json", ["Topic A"])
        original = write_results_json(
            period_path / "results.json",
            [make_event(1, attendees=[1])],
            extra={
                "num_unique_attendees": 7,
                "priority_fulfilled": 3,
                "system_weight": 11,
                "peeps": [{"id": 1, "name": "Alice"}],
            },
        )

        assign_topics_for_period(period_path)

        updated = json.loads((period_path / "results.json").read_text(encoding="utf-8"))
        assert updated["num_unique_attendees"] == original["num_unique_attendees"]
        assert updated["priority_fulfilled"] == original["priority_fulfilled"]
        assert updated["system_weight"] == original["system_weight"]
        assert updated["peeps"] == original["peeps"]

        original_event = dict(original["valid_events"][0])
        updated_event = dict(updated["valid_events"][0])
        assert updated_event.pop("topic") == "Topic A"
        assert isinstance(updated_event.pop("topic_scores"), list)
        assert updated_event == original_event

    def test_assign_topics_writes_topic_scores_sorted(self, tmp_path):
        period_path = tmp_path / "2026-02"
        period_path.mkdir()

        write_members_csv(
            period_path / "members.csv",
            [
                (1, "Alice Alpha", "Alice", "alice@example.com"),
                (2, "Bob Beta", "Bob", "bob@example.com"),
            ],
        )
        write_responses_csv(
            period_path / "responses.csv",
            [
                ("alice@example.com", "Topic B"),
                ("bob@example.com", "Topic A"),
            ],
        )
        write_period_config_json_with_topics(
            period_path / "period_config.json", ["Topic A", "Topic B", "Topic C"]
        )
        write_results_json(period_path / "results.json", [make_event(1, attendees=[1, 2])])

        assign_topics_for_period(period_path)

        updated = json.loads((period_path / "results.json").read_text(encoding="utf-8"))
        scores = updated["valid_events"][0]["topic_scores"]
        assert scores == [
            {"topic": "Topic A", "score": 1},
            {"topic": "Topic B", "score": 1},
            {"topic": "Topic C", "score": 0},
        ]
