"""
Topic assignment helpers.

Purpose:
Pick a topic for each scheduled event that maximizes attendee happiness while
avoiding topic reuse across overlapping attendee groups. We score a topic by
how many attendees voted for it, then choose the assignment that maximizes
total score, then maximizes the minimum per-event score, with a stable
lexicographic tie-break.

Algorithm overview:
- For each scheduled event, score every topic by counting how many attendees voted for it.
- Build a candidate list per event: all positive-score topics (sorted by score desc, then name),
  followed by zero-score fallbacks (sorted lexicographically) so every event can be assigned.
- Search all valid assignments with a depth-first search:
  - Hard constraint: the same topic cannot be used by two events that share any attendee.
  - Objective: maximize total score, then maximize the minimum score, then use
    lexicographic order of assigned topics as a stable tie-break.

This stays fast because the number of topics is small (<10) and events are limited.
"""

import logging
from dataclasses import dataclass
from peeps_scheduler.models import EventSequence


@dataclass(frozen=True)
class _TopicCandidate:
    topic: str
    score: int


@dataclass(frozen=True)
class _EventTopicProfile:
    event_id: int
    attendee_ids: frozenset[int]
    candidates: tuple[_TopicCandidate, ...]
    score_summary: tuple[_TopicCandidate, ...]

    def max_score(self) -> int:
        if not self.candidates:
            return 0
        return max(candidate.score for candidate in self.candidates)


class _TopicAssignmentState:
    def __init__(self):
        self.assigned: dict[int, str] = {}
        self.topic_usage: dict[str, set[int]] = {}

    def can_assign(self, topic: str, attendee_ids: frozenset[int]) -> bool:
        # A topic can only be reused if it doesn't overlap with any prior attendees.
        used_by = self.topic_usage.get(topic)
        return not used_by or used_by.isdisjoint(attendee_ids)

    def assign(self, event_id: int, topic: str, attendee_ids: frozenset[int]) -> set[int] | None:
        previous = self.topic_usage.get(topic)
        self.assigned[event_id] = topic
        if previous is None:
            self.topic_usage[topic] = set(attendee_ids)
        else:
            self.topic_usage[topic] = previous | set(attendee_ids)
        return previous

    def unassign(self, event_id: int, topic: str, previous: set[int] | None) -> None:
        if previous is None:
            self.topic_usage.pop(topic, None)
        else:
            self.topic_usage[topic] = previous
        self.assigned.pop(event_id, None)


def assign_topics_to_events(sequence: EventSequence, topics: list[str]) -> None:
    """Assign topics to events in-place based on attendee votes and overlap rules."""
    if not topics or not sequence.valid_events:
        return

    # 1) Build per-event scores and candidate lists from attendee topic votes.
    profiles = _build_event_profiles(sequence, topics)
    if not profiles:
        return

    # 2) Search for the best assignment across all events.
    assignment = _choose_best_assignment(profiles)
    # 3) Log the scores that led to the chosen assignment.
    _log_assignment_scores(profiles, assignment)

    # 4) Apply the chosen topics to the scheduled events.
    for event in sequence.valid_events:
        if event.id in assignment:
            event.topic = assignment[event.id]


def _build_event_profiles(
    sequence: EventSequence,
    topics: list[str],
) -> list[_EventTopicProfile]:
    """Build per-event scoring profiles from attendee topic votes."""
    # Collect votes by peep id so we can score each event quickly.
    topics_by_peep_id: dict[int, set[str]] = {
        peep.id: set(peep.topic_votes) for peep in sequence.peeps if peep.topic_votes
    }

    profiles: list[_EventTopicProfile] = []
    for event in sequence.valid_events:
        # Only count actual attendees (alternates are excluded from event.attendees).
        attendee_ids = frozenset(peep.id for peep in event.attendees)
        score_map: dict[str, int] = {topic: 0 for topic in topics}
        for peep in event.attendees:
            for topic in topics_by_peep_id.get(peep.id, set()):
                if topic in score_map:
                    score_map[topic] += 1

        # Candidate ordering: positive scores first, then lexicographic fallbacks.
        positive_candidates = [
            _TopicCandidate(topic, score) for topic, score in score_map.items() if score > 0
        ]
        positive_candidates.sort(key=lambda candidate: (-candidate.score, candidate.topic))

        candidate_topics = {candidate.topic for candidate in positive_candidates}
        fallback_topics = [
            _TopicCandidate(topic, 0) for topic in sorted(topics) if topic not in candidate_topics
        ]
        candidates = tuple([*positive_candidates, *fallback_topics])
        if not candidates:
            raise ValueError("no candidates available for topic assignment.")

        score_summary = tuple(
            sorted(
                [_TopicCandidate(topic, score) for topic, score in score_map.items()],
                key=lambda candidate: (-candidate.score, candidate.topic),
            )
        )
        profiles.append(
            _EventTopicProfile(
                event_id=event.id,
                attendee_ids=attendee_ids,
                candidates=candidates,
                score_summary=score_summary,
            )
        )

    return profiles


def _choose_best_assignment(profiles: list[_EventTopicProfile]) -> dict[int, str]:
    """Search the assignment space and return the best topic per event."""
    event_order = [profile.event_id for profile in profiles]
    order_index = {event_id: idx for idx, event_id in enumerate(event_order)}
    # Tightest candidate sets first keeps the search small and deterministic.
    ordered_profiles = sorted(
        profiles,
        key=lambda profile: (
            len(profile.candidates),
            profile.event_id,
            order_index[profile.event_id],
        ),
    )

    best_total = -1
    best_min_score = -1
    best_tuple: tuple[str, ...] | None = None
    best_assignment: dict[int, str] = {}
    state = _TopicAssignmentState()

    remaining_max = [profile.max_score() for profile in ordered_profiles]

    # Recursive DFS that assigns topics event-by-event and updates the best scoring assignment.
    def dfs(position: int, current_total: int, current_min: int) -> None:
        nonlocal best_total, best_min_score, best_tuple, best_assignment

        if position == len(ordered_profiles):
            assignment_tuple = tuple(state.assigned.get(event_id, "") for event_id in event_order)
            # Tie-break priority: total score, then minimum score, then lexicographic order.
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
                best_assignment = dict(state.assigned)
            return

        # Prune if even the best possible remaining scores can't beat the current best.
        upper_bound = current_total + sum(remaining_max[position:])
        if upper_bound < best_total:
            return

        profile = ordered_profiles[position]

        # Depth-first assignment of candidates for this event, propagating running scores.
        for candidate in profile.candidates:
            if not state.can_assign(candidate.topic, profile.attendee_ids):
                continue

            # Assign topic if it doesn't overlap with prior events' attendees.
            previous_used = state.assign(profile.event_id, candidate.topic, profile.attendee_ids)
            next_min = candidate.score if position == 0 else min(current_min, candidate.score)
            dfs(position + 1, current_total + candidate.score, next_min)
            state.unassign(profile.event_id, candidate.topic, previous_used)

    dfs(0, 0, 0)

    if len(best_assignment) != len(profiles):
        raise ValueError("could not assign topics to all events.")

    return best_assignment


def _log_assignment_scores(profiles: list[_EventTopicProfile], assignment: dict[int, str]) -> None:
    """Emit debug logs showing per-event scores for the chosen assignment."""
    for profile in profiles:
        assigned = assignment.get(profile.event_id, "")
        assigned_score = next(
            (candidate.score for candidate in profile.score_summary if candidate.topic == assigned),
            0,
        )
        score_details = ", ".join(
            f"{candidate.topic}: {candidate.score}" for candidate in profile.score_summary
        )
        logging.debug(
            "Topic assignment scores for event %s -> %s (%s): %s",
            profile.event_id,
            assigned,
            assigned_score,
            score_details,
        )
