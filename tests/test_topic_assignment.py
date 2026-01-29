"""
Tests for topic assignment behavior and results serialization.
"""

import json
import peeps_scheduler.constants as constants
from peeps_scheduler.models import EventSequence, Role
from peeps_scheduler.scheduler import Scheduler
from peeps_scheduler.validation.period import PeriodData


def create_topic_scheduler(period_data, tmp_path):
    scheduler = Scheduler(
        period_data=period_data,
        data_folder="test",
        max_events=constants.DEFAULT_MAX_EVENTS,
    )
    scheduler.period_path = tmp_path
    scheduler.result_json = str(tmp_path / "results.json")
    return scheduler


def build_sequence(events, peeps):
    sequence = EventSequence(events, peeps)
    sequence.valid_events = events
    return sequence


class TestSchedulerTopicAssignment:
    def test_assign_topics_avoids_overlap_reuse(self, tmp_path, peep_factory, event_factory):
        peeps = [
            peep_factory(id=1, email="alice@example.com", topic_votes=["Topic A"]),
            peep_factory(id=2, email="bob@example.com", topic_votes=["Topic A"]),
            peep_factory(id=3, email="carol@example.com", topic_votes=["Topic A", "Topic C"]),
        ]
        event_one = event_factory(id=1)
        event_one.add_attendee(peeps[0], Role.LEADER)
        event_one.add_attendee(peeps[1], Role.LEADER)

        event_two = event_factory(id=2)
        event_two.add_attendee(peeps[1], Role.LEADER)
        event_two.add_attendee(peeps[2], Role.LEADER)

        sequence = build_sequence([event_one, event_two], peeps)
        period_data = PeriodData(peeps=peeps, events=[], topics=["Topic A", "Topic C"])
        scheduler = create_topic_scheduler(period_data, tmp_path)

        scheduler._assign_topics(sequence)

        assert event_one.topic == "Topic A"
        assert event_two.topic == "Topic C"

    def test_assign_topics_allows_reuse_when_disjoint(self, tmp_path, peep_factory, event_factory):
        peeps = [
            peep_factory(id=1, email="alice@example.com", topic_votes=["Topic A"]),
            peep_factory(id=2, email="bob@example.com", topic_votes=["Topic A"]),
        ]
        event_one = event_factory(id=1)
        event_one.add_attendee(peeps[0], Role.LEADER)
        event_two = event_factory(id=2)
        event_two.add_attendee(peeps[1], Role.LEADER)

        sequence = build_sequence([event_one, event_two], peeps)
        period_data = PeriodData(peeps=peeps, events=[], topics=["Topic A"])
        scheduler = create_topic_scheduler(period_data, tmp_path)

        scheduler._assign_topics(sequence)

        assert event_one.topic == "Topic A"
        assert event_two.topic == "Topic A"

    def test_assign_topics_picks_valid_topic_when_scores_empty(
        self, tmp_path, peep_factory, event_factory
    ):
        peeps = [peep_factory(id=1, email="alice@example.com", topic_votes=["Topic B"])]
        event_one = event_factory(id=1)
        event_one.add_attendee(peeps[0], Role.LEADER)

        sequence = build_sequence([event_one], peeps)
        period_data = PeriodData(peeps=peeps, events=[], topics=["Topic A"])
        scheduler = create_topic_scheduler(period_data, tmp_path)

        scheduler._assign_topics(sequence)

        assert event_one.topic == "Topic A"

    def test_assign_topics_ignores_alternates(self, tmp_path, peep_factory, event_factory):
        peeps = [
            peep_factory(id=1, email="alice@example.com", topic_votes=["Write In"]),
            peep_factory(id=2, email="bob@example.com", topic_votes=["Topic B"]),
        ]
        event_one = event_factory(id=1)
        event_one.add_attendee(peeps[0], Role.LEADER)
        event_one.add_alternate(peeps[1], Role.LEADER)

        sequence = build_sequence([event_one], peeps)
        period_data = PeriodData(peeps=peeps, events=[], topics=["Topic A", "Topic B"])
        scheduler = create_topic_scheduler(period_data, tmp_path)

        scheduler._assign_topics(sequence)

        assert event_one.topic == "Topic A"

    def test_assign_topics_uses_lexicographic_fallback(self, tmp_path, peep_factory, event_factory):
        peeps = [peep_factory(id=1, email="alice@example.com", topic_votes=["Write In"])]
        event_one = event_factory(id=1)
        event_one.add_attendee(peeps[0], Role.LEADER)

        sequence = build_sequence([event_one], peeps)
        period_data = PeriodData(peeps=peeps, events=[], topics=["Topic B", "Topic A"])
        scheduler = create_topic_scheduler(period_data, tmp_path)

        scheduler._assign_topics(sequence)

        assert event_one.topic == "Topic A"

    def test_assign_topics_ignores_invalid_topics_with_valid_choices(
        self, tmp_path, peep_factory, event_factory
    ):
        peeps = [
            peep_factory(id=1, email="alice@example.com", topic_votes=["Write In", "Topic A"]),
            peep_factory(id=2, email="bob@example.com", topic_votes=["Write In"]),
        ]
        event_one = event_factory(id=1)
        event_one.add_attendee(peeps[0], Role.LEADER)
        event_one.add_attendee(peeps[1], Role.LEADER)

        sequence = build_sequence([event_one], peeps)
        period_data = PeriodData(peeps=peeps, events=[], topics=["Topic A", "Topic B"])
        scheduler = create_topic_scheduler(period_data, tmp_path)

        scheduler._assign_topics(sequence)

        assert event_one.topic == "Topic A"

    def test_assign_topics_optimizes_global_score(self, tmp_path, peep_factory, event_factory):
        peeps = [
            peep_factory(id=1, email="alice@example.com", topic_votes=["Topic A", "Topic B"]),
            peep_factory(id=2, email="bob@example.com", topic_votes=["Topic A"]),
            peep_factory(id=3, email="carol@example.com", topic_votes=["Topic A"]),
        ]
        event_one = event_factory(id=1)
        event_one.add_attendee(peeps[0], Role.LEADER)
        event_one.add_attendee(peeps[1], Role.LEADER)

        event_two = event_factory(id=2)
        event_two.add_attendee(peeps[1], Role.LEADER)
        event_two.add_attendee(peeps[2], Role.LEADER)

        sequence = build_sequence([event_one, event_two], peeps)
        period_data = PeriodData(peeps=peeps, events=[], topics=["Topic A", "Topic B"])
        scheduler = create_topic_scheduler(period_data, tmp_path)

        scheduler._assign_topics(sequence)

        assert event_one.topic == "Topic B"
        assert event_two.topic == "Topic A"

    def test_assign_topics_breaks_ties_on_min_score(
        self, tmp_path, peep_factory, event_factory
    ):
        peeps = [
            peep_factory(id=1, email="alice@example.com", topic_votes=["Topic A"]),
            peep_factory(id=2, email="bob@example.com", topic_votes=["Topic A"]),
            peep_factory(id=3, email="carol@example.com", topic_votes=["Topic A"]),
            peep_factory(id=4, email="dana@example.com", topic_votes=["Topic B"]),
            peep_factory(id=5, email="erin@example.com", topic_votes=["Topic A"]),
        ]
        event_one = event_factory(id=1)
        event_one.add_attendee(peeps[0], Role.LEADER)
        event_one.add_attendee(peeps[1], Role.LEADER)
        event_one.add_attendee(peeps[2], Role.LEADER)
        event_one.add_attendee(peeps[3], Role.LEADER)

        event_two = event_factory(id=2)
        event_two.add_attendee(peeps[0], Role.LEADER)
        event_two.add_attendee(peeps[4], Role.LEADER)

        sequence = build_sequence([event_one, event_two], peeps)
        period_data = PeriodData(peeps=peeps, events=[], topics=["Topic A", "Topic B"])
        scheduler = create_topic_scheduler(period_data, tmp_path)

        scheduler._assign_topics(sequence)

        assert event_one.topic == "Topic B"
        assert event_two.topic == "Topic A"

    def test_assign_topics_respects_overlap_with_zero_scores(
        self, tmp_path, peep_factory, event_factory
    ):
        peeps = [
            peep_factory(id=1, email="alice@example.com", topic_votes=["Write In"]),
            peep_factory(id=2, email="bob@example.com", topic_votes=["Write In"]),
        ]
        event_one = event_factory(id=1)
        event_one.add_attendee(peeps[0], Role.LEADER)

        event_two = event_factory(id=2)
        event_two.add_attendee(peeps[0], Role.LEADER)
        event_two.add_attendee(peeps[1], Role.LEADER)

        sequence = build_sequence([event_one, event_two], peeps)
        period_data = PeriodData(peeps=peeps, events=[], topics=["Topic A", "Topic B"])
        scheduler = create_topic_scheduler(period_data, tmp_path)

        scheduler._assign_topics(sequence)

        assert event_one.topic == "Topic A"
        assert event_two.topic == "Topic B"

    def test_assign_topics_breaks_score_ties_lexicographically(
        self, tmp_path, peep_factory, event_factory
    ):
        peeps = [
            peep_factory(id=1, email="alice@example.com", topic_votes=["Topic B"]),
            peep_factory(id=2, email="bob@example.com", topic_votes=["Topic A"]),
        ]
        event_one = event_factory(id=1)
        event_one.add_attendee(peeps[0], Role.LEADER)
        event_one.add_attendee(peeps[1], Role.LEADER)

        sequence = build_sequence([event_one], peeps)
        period_data = PeriodData(peeps=peeps, events=[], topics=["Topic B", "Topic A"])
        scheduler = create_topic_scheduler(period_data, tmp_path)

        scheduler._assign_topics(sequence)

        assert event_one.topic == "Topic A"

    def test_save_sequence_includes_topic_assignments(
        self, tmp_path, peep_factory, event_factory
    ):
        peeps = [peep_factory(id=1, email="alice@example.com", topic_votes=["Topic A"])]
        event_one = event_factory(id=1)
        event_one.add_attendee(peeps[0], Role.LEADER)
        sequence = build_sequence([event_one], peeps)

        period_data = PeriodData(peeps=peeps, events=[], topics=["Topic A"])
        scheduler = create_topic_scheduler(period_data, tmp_path)

        scheduler._assign_topics(sequence)
        scheduler._save_sequence(sequence)

        updated = json.loads((tmp_path / "results.json").read_text(encoding="utf-8"))
        assert updated["topic_assignments"] == {"1": "Topic A"}
        assert updated["valid_events"][0]["topic"] == "Topic A"

    def test_save_sequence_skips_topics_without_configuration(
        self, tmp_path, peep_factory, event_factory
    ):
        peeps = [peep_factory(id=1, email="alice@example.com", topic_votes=["Topic A"])]
        event_one = event_factory(id=1)
        event_one.add_attendee(peeps[0], Role.LEADER)
        sequence = build_sequence([event_one], peeps)

        period_data = PeriodData(peeps=peeps, events=[], topics=[])
        scheduler = create_topic_scheduler(period_data, tmp_path)

        scheduler._save_sequence(sequence)

        updated = json.loads((tmp_path / "results.json").read_text(encoding="utf-8"))
        assert "topic_assignments" not in updated
