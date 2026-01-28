import pytest
from pydantic import ValidationError
from peeps_scheduler.validation.file_schemas.attendance_json import (
    ActualAttendanceJsonSchema,
    RosterEntryJsonSchema,
)
from peeps_scheduler.validation.file_schemas.members_csv import MemberCsvRowSchema
from peeps_scheduler.validation.file_schemas.period import (
    CancelledAvailabilityJsonSchema,
    PartnershipRequestJsonSchema,
    PeriodFileSchema,
    validate_cancellations,
    validate_event_references,
    validate_partnerships,
    validate_response_emails,
    validate_roster_entries,
    validate_topics,
)
from peeps_scheduler.validation.file_schemas.results_json import ResultsJsonSchema
from peeps_scheduler.validation.parsers import parse_event_name
from tests.validation.conftest import assert_error_for_model

pytestmark = pytest.mark.unit


def response_data(overrides: dict | None = None) -> dict:
    defaults = {
        "Timestamp": "1/1/2020 12:00:00",
        "Name": "Alice Alpha",
        "Display Name": "Alice",
        "Email Address": "alice@test.com",
        "Primary Role": "Leader",
        "Secondary Role": "I only want to be scheduled in my primary role",
        "Max Sessions": "2",
        "Availability": "Saturday January 4 - 1pm",
        "Min Interval Days": "0",
    }
    return {**defaults, **(overrides or {})}


def event_row_data(overrides: dict | None = None) -> dict:
    defaults = {
        "Name": "Saturday January 4 - 1pm",
        "Event Duration": "90",
    }
    return {**defaults, **(overrides or {})}


def member_data(overrides: dict | None = None) -> dict:
    defaults = {
        "id": "1",
        "Name": "Alice Alpha",
        "Display Name": "Alice",
        "Email Address": "alice@test.com",
        "Role": "Leader",
        "Index": "0",
        "Priority": "1",
        "Total Attended": "0",
        "Active": "TRUE",
        "Date Joined": "1/1/2020",
    }
    return {**defaults, **(overrides or {})}


def period_data(overrides: dict | None = None) -> dict:
    defaults = {
        "members": [
            member_data(
                {
                    "id": "1",
                    "Index": "0",
                    "Name": "Alice Alpha",
                    "Display Name": "Alice",
                    "Email Address": "alice@test.com",
                }
            ),
            member_data(
                {
                    "id": "2",
                    "Index": "1",
                    "Name": "Bob Beta",
                    "Display Name": "Bob",
                    "Email Address": "bob@test.com",
                }
            ),
        ],
        "responses": {
            "responses": [
                response_data({"Name": "Alice Alpha", "Email Address": "alice@test.com"}),
                response_data({"Name": "Bob Beta", "Email Address": "bob@test.com"}),
            ],
        },
    }
    return {**defaults, **(overrides or {})}


def attendance_event_data(overrides: dict | None = None) -> dict:
    defaults = {
        "id": 4,
        "date": "2020-01-04 13:00",
        "duration_minutes": 120,
        "attendees": [
            {"id": 38, "name": "Alice", "role": "leader"},
            {"id": 25, "name": "Bob", "role": "follower"},
        ],
    }
    return {**defaults, **(overrides or {})}


def result_event_data(overrides: dict | None = None) -> dict:
    defaults = {
        **attendance_event_data(),
        "alternates": [
            {"id": 41, "name": "Dave", "role": "leader"},
            {"id": 27, "name": "Eve", "role": "follower"},
        ],
    }
    return {**defaults, **(overrides or {})}


class TestPeriodFileSchema:
    """Integration tests for PeriodFileSchema with cross-file validation."""

    def test_valid_minimal(self, ctx):
        """Happy path: Minimal valid period data."""
        schema = PeriodFileSchema.model_validate(period_data(), context={"ctx": ctx})

        assert len(schema.members.root) == 2
        assert len(schema.responses.responses) == 2
        assert schema.responses.event_rows is None
        assert schema.results is None
        assert schema.attendance is None
        assert schema.cancelled_events == []
        assert schema.cancelled_member_availability == []
        assert schema.partnership_requests == []
        assert schema.topics == []

    def test_response_email_not_found_raises(self, ctx):
        data = period_data(
            {
                "responses": {
                    "responses": [
                        response_data({"Email Address": "missing@test.com", "Name": "Zoe Zeta"})
                    ]
                }
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "response email")

    def test_valid_topics(self, ctx):
        data = period_data(
            {
                "topics": ["Balance for Spins and Turns", "Angles for Shaping & Slotting"],
                "responses": {
                    "responses": [
                        response_data(
                            {
                                "Name": "Alice Alpha",
                                "Email Address": "alice@test.com",
                                "Deep Dive Topics": "Balance for Spins and Turns",
                            }
                        ),
                        response_data({"Name": "Bob Beta", "Email Address": "bob@test.com"}),
                    ],
                },
            }
        )
        schema = PeriodFileSchema.model_validate(data, context={"ctx": ctx})
        assert schema.topics == ["Balance for Spins and Turns", "Angles for Shaping & Slotting"]

    def test_topics_not_list_raises(self, ctx):
        data = period_data({"topics": "Balance for Spins and Turns"})
        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})
        assert_error_for_model(e.value.errors(), "topics")

    def test_topics_without_column_raises(self, ctx):
        data = period_data(
            {
                "topics": ["Topic A"],
                "responses": {
                    "responses": [response_data()],
                },
            }
        )
        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})
        assert_error_for_model(e.value.errors(), "Deep Dive Topics missing")

    def test_column_without_topics_raises(self, ctx):
        data = period_data(
            {
                "responses": {
                    "responses": [response_data({"Deep Dive Topics": "Topic A"})],
                }
            }
        )
        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})
        assert_error_for_model(e.value.errors(), "topics missing")

    def test_topics_with_column_valid(self, ctx):
        data = period_data(
            {
                "topics": ["Topic A"],
                "responses": {
                    "responses": [response_data({"Deep Dive Topics": "Topic A"})],
                },
            }
        )
        schema = PeriodFileSchema.model_validate(data, context={"ctx": ctx})
        assert schema.topics == ["Topic A"]

    def test_no_topics_no_column_valid(self, ctx):
        data = period_data(
            {
                "responses": {
                    "responses": [response_data()],
                }
            }
        )
        schema = PeriodFileSchema.model_validate(data, context={"ctx": ctx})
        assert schema.topics == []

    def test_results_roster_id_not_found_raises(self, ctx):
        data = period_data(
            {
                "results": {
                    "valid_events": [
                        result_event_data(
                            {
                                "attendees": [{"id": 99, "name": "Alice", "role": "leader"}],
                                "alternates": [],
                            }
                        )
                    ]
                }
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "roster id")

    def test_results_roster_display_name_mismatch_raises(self, ctx):
        data = period_data(
            {
                "results": {
                    "valid_events": [
                        result_event_data(
                            {
                                "attendees": [{"id": 1, "name": "Alice Alpha", "role": "leader"}],
                                "alternates": [],
                            }
                        )
                    ]
                }
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "display name")

    def test_valid_results_roster_name_fallback_to_full_name(self, ctx):
        data = period_data(
            {
                "members": [
                    member_data(
                        {
                            "id": "1",
                            "Index": "0",
                            "Name": "Alice Alpha",
                            "Display Name": "",
                            "Email Address": "alice@test.com",
                        }
                    )
                ],
                "responses": {"responses": [response_data({"Email Address": "alice@test.com"})]},
                "results": {
                    "valid_events": [
                        result_event_data(
                            {
                                "attendees": [{"id": 1, "name": "Alice Alpha", "role": "leader"}],
                                "alternates": [],
                            }
                        )
                    ]
                },
            }
        )

        schema = PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert schema.results is not None

    def test_results_roster_full_name_mismatch_raises(self, ctx):
        data = period_data(
            {
                "members": [
                    member_data(
                        {
                            "id": "1",
                            "Index": "0",
                            "Name": "Alice Alpha",
                            "Display Name": "",
                            "Email Address": "alice@test.com",
                        }
                    )
                ],
                "responses": {"responses": [response_data({"Email Address": "alice@test.com"})]},
                "results": {
                    "valid_events": [
                        result_event_data(
                            {
                                "attendees": [{"id": 1, "name": "Alice", "role": "leader"}],
                                "alternates": [],
                            }
                        )
                    ]
                },
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "display name")

    def test_attendance_roster_id_not_found_raises(self, ctx):
        data = period_data(
            {
                "attendance": {
                    "valid_events": [
                        attendance_event_data(
                            {"attendees": [{"id": 99, "name": "Alice", "role": "leader"}]}
                        )
                    ]
                }
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "roster id")

    def test_partnership_target_email_not_found_raises(self, ctx):
        data = period_data(
            {
                "partnership_requests": [
                    {
                        "requester_email": "alice@test.com",
                        "target_emails": ["bob@test.com", "missing@test.com"],
                    }
                ],
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "target email")

    def test_partnership_requester_email_not_found_raises(self, ctx):
        data = period_data(
            {
                "partnership_requests": [
                    {
                        "requester_email": "missing@test.com",
                        "target_emails": ["alice@test.com"],
                    }
                ],
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "requester email")

    def test_cancelled_event_not_found_raises(self, ctx):
        data = period_data(
            {
                "responses": {
                    "responses": [response_data()],
                    "event_rows": [
                        event_row_data({"Name": "Saturday January 4 - 1pm", "Event Duration": "90"})
                    ],
                },
                "cancelled_events": ["Sunday January 5 - 2pm"],
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "cancelled event")

    def test_cancelled_availability_email_not_found_raises(self, ctx):
        data = period_data(
            {
                "responses": {
                    "responses": [response_data()],
                    "event_rows": [
                        event_row_data({"Name": "Saturday January 4 - 1pm", "Event Duration": "90"})
                    ],
                },
                "cancelled_member_availability": [
                    {
                        "member_email": "missing@test.com",
                        "events": ["Saturday January 4 - 1pm"],
                    }
                ],
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "cancelled availability email")

    def test_cancelled_availability_event_not_found_raises(self, ctx):
        data = period_data(
            {
                "responses": {
                    "responses": [response_data()],
                    "event_rows": [
                        event_row_data({"Name": "Saturday January 4 - 1pm", "Event Duration": "90"})
                    ],
                },
                "cancelled_member_availability": [
                    {
                        "member_email": "alice@test.com",
                        "events": ["Sunday January 5 - 2pm"],
                    }
                ],
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "cancelled availability event")

    def test_cancelled_availability_event_not_in_member_availability_raises(self, ctx):
        """Integration test: Event exists globally but not in member's availability."""
        data = period_data(
            {
                "responses": {
                    "responses": [response_data()],
                    "event_rows": [
                        event_row_data(),
                        event_row_data(
                            {"Name": "Saturday January 11 - 3pm", "Event Duration": "90"}
                        ),
                    ],
                },
                "cancelled_member_availability": [
                    {
                        "member_email": "alice@test.com",
                        "events": [
                            "Saturday January 11 - 3pm"
                        ],  # event in event rows but not in Alice's availability
                    }
                ],
            }
        )
        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "event not in member's original availability")

    def test_results_event_not_found_raises(self, ctx):
        """Test that results event not in extracted responses events raises error."""
        data = period_data(
            {
                "responses": {
                    "responses": [response_data()],
                    "event_rows": [
                        event_row_data({"Name": "Saturday January 4 - 1pm", "Event Duration": "90"})
                    ],
                },
                "results": {
                    "valid_events": [
                        {
                            "id": 2,
                            "date": "2020-01-05 14:00",  # Sunday 2pm - Not in event_rows (only Saturday 1pm)
                            "duration_minutes": 120,
                            "attendees": [{"id": 1, "name": "Alice", "role": "leader"}],
                            "alternates": [],
                        }
                    ],
                    "num_unique_attendees": 1,
                    "system_weight": 10,
                },
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        assert len(e.value.errors()) > 0
        errors_str = str(e.value.errors())
        assert "result event" in errors_str.lower() or "event" in errors_str.lower()

    def test_attendance_event_not_found_raises(self, ctx):
        """Test that attendance event not in extracted responses events raises error."""
        data = period_data(
            {
                "responses": {
                    "responses": [response_data()],
                    "event_rows": [
                        event_row_data({"Name": "Saturday January 4 - 1pm", "Event Duration": "90"})
                    ],
                },
                "attendance": {
                    "valid_events": [
                        {
                            "id": 1,
                            "date": "2020-01-05 14:00",  # Sunday 2pm - Not in event_rows (only Saturday 1pm)
                            "duration_minutes": 120,
                            "attendees": [{"id": 1, "name": "Alice", "role": "leader"}],
                        }
                    ]
                },
            }
        )

        with pytest.raises(ValidationError) as e:
            PeriodFileSchema.model_validate(data, context={"ctx": ctx})

        # Should fail validation with at least one error
        assert len(e.value.errors()) > 0
        errors_str = str(e.value.errors())
        assert "attendance event" in errors_str.lower() or "event" in errors_str.lower()


@pytest.mark.unit
class TestValidateResponseEmails:
    """Unit tests for validate_response_emails function."""

    def test_valid(self):
        """Happy path: All response emails exist in member emails."""
        member_emails = {"alice@test.com", "bob@test.com"}
        response_emails = ["alice@test.com", "bob@test.com"]
        # Should not raise
        validate_response_emails(member_emails, response_emails)

    def test_missing_raises(self):
        """Error case: Response email not in member roster."""
        with pytest.raises(ValueError) as e:
            validate_response_emails({"alice@test.com"}, ["alice@test.com", "missing@test.com"])
        assert "response email not found" in str(e.value)


@pytest.mark.unit
class TestValidateRosterEntries:
    """Unit tests for validate_roster_entries function."""

    def test_valid(self):
        """Happy path: Roster entries match member data."""
        member = MemberCsvRowSchema.model_validate(member_data())
        roster = RosterEntryJsonSchema.model_validate({"id": 1, "name": "Alice", "role": "leader"})
        # Should not raise
        validate_roster_entries({member.id: member}, [roster])

    def test_missing_id_raises(self):
        """Error case: Roster ID not found in members."""
        member = MemberCsvRowSchema.model_validate(member_data())
        roster = RosterEntryJsonSchema.model_validate({"id": 99, "name": "Alice", "role": "leader"})
        with pytest.raises(ValueError) as e:
            validate_roster_entries({member.id: member}, [roster])
        assert "roster id not found" in str(e.value)

    def test_display_name_mismatch_raises(self):
        """Error case: Display name doesn't match."""
        member = MemberCsvRowSchema.model_validate(member_data())
        roster = RosterEntryJsonSchema.model_validate(
            {"id": member.id, "name": "Bob", "role": "leader"}
        )
        with pytest.raises(ValueError) as e:
            validate_roster_entries({member.id: member}, [roster])
        assert "display name mismatch" in str(e.value)


@pytest.mark.unit
class TestValidatePartnerships:
    """Unit tests for validate_partnerships function."""

    def test_valid(self):
        """Happy path: All partnership emails exist."""
        member_emails = {"alice@test.com", "bob@test.com", "carol@test.com"}
        partnerships = [
            PartnershipRequestJsonSchema.model_validate(
                {
                    "requester_email": "alice@test.com",
                    "target_emails": ["bob@test.com", "carol@test.com"],
                }
            )
        ]
        # Should not raise
        validate_partnerships(member_emails, partnerships)

    def test_none(self):
        """Edge case: No partnerships to validate."""
        member_emails = {"alice@test.com"}
        # Should not raise
        validate_partnerships(member_emails, None)
        validate_partnerships(member_emails, [])

    def test_requester_not_found_raises(self):
        """Error case: Requester email not in members."""
        member_emails = {"bob@test.com"}
        partnerships = [
            PartnershipRequestJsonSchema.model_validate(
                {
                    "requester_email": "missing@test.com",
                    "target_emails": ["bob@test.com"],
                }
            )
        ]
        with pytest.raises(ValueError) as e:
            validate_partnerships(member_emails, partnerships)
        assert "requester email not found" in str(e.value)

    def test_target_not_found_raises(self):
        """Error case: Target email not in members."""
        member_emails = {"alice@test.com", "bob@test.com"}
        partnerships = [
            PartnershipRequestJsonSchema.model_validate(
                {
                    "requester_email": "alice@test.com",
                    "target_emails": ["bob@test.com", "missing@test.com"],
                }
            )
        ]
        with pytest.raises(ValueError) as e:
            validate_partnerships(member_emails, partnerships)
        assert "target email not found" in str(e.value)

    def test_duplicate_requester_emails_raises(self):
        """Error case: Duplicate requester emails in multiple entries."""
        member_emails = {"alice@test.com", "bob@test.com", "carol@test.com"}
        partnerships = [
            PartnershipRequestJsonSchema.model_validate(
                {"requester_email": "alice@test.com", "target_emails": ["bob@test.com"]}
            ),
            PartnershipRequestJsonSchema.model_validate(
                {"requester_email": "alice@test.com", "target_emails": ["carol@test.com"]}
            ),
        ]
        with pytest.raises(ValueError) as e:
            validate_partnerships(member_emails, partnerships)
        assert "duplicate requester email" in str(e.value)


@pytest.mark.unit
class TestValidateTopics:
    """Unit tests for validate_topics function."""

    def test_valid(self):
        validate_topics(["Balance for Spins and Turns", "Angles for Shaping & Slotting"])

    def test_none_or_empty(self):
        validate_topics(None)
        validate_topics([])

    def test_blank_raises(self):
        with pytest.raises(ValueError) as e:
            validate_topics(["", "Balance for Spins and Turns"])
        assert "topics cannot be blank" in str(e.value)

    def test_non_string_raises(self):
        with pytest.raises(ValueError) as e:
            validate_topics(["Angles for Shaping & Slotting", 3])
        assert "topics must be strings" in str(e.value)

    def test_duplicate_after_normalization_raises(self):
        with pytest.raises(ValueError) as e:
            validate_topics(
                [
                    "Rhythm & Blues (swung timing, swung body action, rhythmic footwork)",
                    "Rhythm & Blues",
                ]
            )
        assert "duplicate entries after normalization" in str(e.value)


@pytest.mark.unit
class TestFilterResponseTopics:
    """Unit tests for filter_response_topics function."""

    def test_filters_to_matching_topics(self, ctx):
        data = period_data(
            {
                "topics": ["Balance for Spins and Turns"],
                "responses": {
                    "responses": [
                        response_data(
                            {"Deep Dive Topics": ("Balance for Spins and Turns, Unknown Topic")}
                        )
                    ],
                },
            }
        )
        schema = PeriodFileSchema.model_validate(data, context={"ctx": ctx})
        assert schema.responses.responses[0].deep_dive_topics == ["Balance for Spins and Turns"]

    def test_filters_to_empty_when_no_topics(self, ctx):
        data = period_data(
            {
                "topics": [],
                "responses": {"responses": [response_data()]},
            }
        )
        schema = PeriodFileSchema.model_validate(data, context={"ctx": ctx})
        assert schema.responses.responses[0].deep_dive_topics == []


@pytest.mark.unit
class TestValidateCancellations:
    """Unit tests for validate_cancellations function."""

    def test_valid(self, ctx):
        """Happy path: All cancellations reference valid events and members."""
        event_starts = {
            parse_event_name("Saturday January 4 - 1pm", ctx.year, ctx.tz).start,
            parse_event_name("Sunday January 5 - 2pm", ctx.year, ctx.tz).start,
        }
        member_emails = {"alice@test.com", "bob@test.com"}
        member_availability = {
            "alice@test.com": [
                parse_event_name("Saturday January 4 - 1pm", ctx.year, ctx.tz),
                parse_event_name("Sunday January 5 - 2pm", ctx.year, ctx.tz),
            ]
        }
        cancelled_events = [parse_event_name("Saturday January 4 - 1pm", ctx.year, ctx.tz)]
        cancelled_availability = [
            CancelledAvailabilityJsonSchema.model_validate(
                {"member_email": "alice@test.com", "events": ["Sunday January 5 - 2pm"]},
                context={"ctx": ctx},
            )
        ]
        # Should not raise
        validate_cancellations(
            event_starts,
            member_emails,
            member_availability,
            cancelled_events,
            cancelled_availability,
        )

    def test_none(self, ctx):
        """Edge case: No cancellations to validate."""
        event_starts = set()
        member_emails = {"alice@test.com"}
        member_availability = {}
        # Should not raise
        validate_cancellations(event_starts, member_emails, member_availability, None, None)
        validate_cancellations(event_starts, member_emails, member_availability, [], [])

    def test_event_not_found_raises(self, ctx):
        """Error case: Cancelled event not in event_starts."""
        event_starts = {parse_event_name("Saturday January 4 - 1pm", ctx.year, ctx.tz).start}
        member_emails = {"alice@test.com"}
        member_availability = {}
        cancelled_events = [parse_event_name("Sunday January 5 - 2pm", ctx.year, ctx.tz)]
        with pytest.raises(ValueError) as e:
            validate_cancellations(
                event_starts, member_emails, member_availability, cancelled_events, None
            )
        assert "cancelled event not found" in str(e.value)

    def test_email_not_found_raises(self, ctx):
        """Error case: Cancelled availability email not in members."""
        event_starts = {parse_event_name("Saturday January 4 - 1pm", ctx.year, ctx.tz).start}
        member_emails = {"alice@test.com"}
        member_availability = {}
        cancelled_availability = [
            CancelledAvailabilityJsonSchema.model_validate(
                {"member_email": "missing@test.com", "events": ["Saturday January 4 - 1pm"]},
                context={"ctx": ctx},
            )
        ]
        with pytest.raises(ValueError) as e:
            validate_cancellations(
                event_starts, member_emails, member_availability, None, cancelled_availability
            )
        assert "cancelled availability email not found" in str(e.value)

    def test_availability_event_not_found_raises(self, ctx):
        """Error case: Cancelled availability event not in event_starts."""
        event_starts = {parse_event_name("Saturday January 4 - 1pm", ctx.year, ctx.tz).start}
        member_emails = {"alice@test.com"}
        member_availability = {}
        cancelled_availability = [
            CancelledAvailabilityJsonSchema.model_validate(
                {"member_email": "alice@test.com", "events": ["Sunday January 5 - 2pm"]},
                context={"ctx": ctx},
            )
        ]
        with pytest.raises(ValueError) as e:
            validate_cancellations(
                event_starts, member_emails, member_availability, None, cancelled_availability
            )
        assert "cancelled availability event not found" in str(e.value)

    def test_event_not_in_member_availability_raises(self, ctx):
        """Error case: Event exists but wasn't in member's original availability."""
        sat_event = parse_event_name("Saturday January 4 - 1pm", ctx.year, ctx.tz)
        sun_event = parse_event_name("Sunday January 5 - 2pm", ctx.year, ctx.tz)
        event_starts = {sat_event.start, sun_event.start}
        member_emails = {"alice@test.com"}
        # Alice only has Saturday in her availability, not Sunday
        member_availability = {"alice@test.com": [sat_event]}
        cancelled_availability = [
            CancelledAvailabilityJsonSchema.model_validate(
                {"member_email": "alice@test.com", "events": ["Sunday January 5 - 2pm"]},
                context={"ctx": ctx},
            )
        ]
        with pytest.raises(ValueError) as e:
            validate_cancellations(
                event_starts, member_emails, member_availability, None, cancelled_availability
            )
        assert "event not in member's original availability" in str(e.value)


@pytest.mark.unit
class TestValidateEventReferences:
    """Unit tests for validate_event_references function."""

    def test_valid(self, ctx):
        """Happy path: All results and attendance events exist in event_starts."""
        results = ResultsJsonSchema.model_validate(
            {
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
                "system_weight": 10,
            },
            context={"ctx": ctx},
        )
        attendance = ActualAttendanceJsonSchema.model_validate(
            {
                "valid_events": [
                    {
                        "id": 1,
                        "date": "2020-01-05 14:00",
                        "duration_minutes": 90,
                        "attendees": [{"id": 1, "name": "Alice", "role": "leader"}],
                    }
                ]
            },
            context={"ctx": ctx},
        )
        # Build event_starts from the parsed events
        event_starts = {results.valid_events[0].start_dt, attendance.valid_events[0].start_dt}
        # Should not raise
        validate_event_references(event_starts, results, attendance)

    def test_none(self):
        """Edge case: No results or attendance to validate."""
        event_starts = set()
        # Should not raise
        validate_event_references(event_starts, None, None)

    def test_result_not_found_raises(self, ctx):
        """Error case: Results event not in event_starts."""
        from datetime import datetime
        from zoneinfo import ZoneInfo

        # event_starts has one event, results has a different event
        event_starts = {datetime(2020, 1, 4, 13, 0, tzinfo=ZoneInfo("America/Los_Angeles"))}
        results = ResultsJsonSchema.model_validate(
            {
                "valid_events": [
                    {
                        "id": 1,
                        "date": "2020-01-05 14:00",  # Different date
                        "duration_minutes": 90,
                        "attendees": [{"id": 1, "name": "Alice", "role": "leader"}],
                        "alternates": [],
                    }
                ],
                "num_unique_attendees": 1,
                "system_weight": 10,
            },
            context={"ctx": ctx},
        )
        with pytest.raises(ValueError) as e:
            validate_event_references(event_starts, results, None)
        assert "result event not found" in str(e.value)

    def test_attendance_not_found_raises(self, ctx):
        """Error case: Attendance event not in event_starts."""
        from datetime import datetime
        from zoneinfo import ZoneInfo

        # event_starts has one event, attendance has a different event
        event_starts = {datetime(2020, 1, 4, 13, 0, tzinfo=ZoneInfo("America/Los_Angeles"))}
        attendance = ActualAttendanceJsonSchema.model_validate(
            {
                "valid_events": [
                    {
                        "id": 1,
                        "date": "2020-01-05 14:00",
                        "duration_minutes": 90,
                        "attendees": [{"id": 1, "name": "Alice", "role": "leader"}],
                    }
                ]
            },
            context={"ctx": ctx},
        )
        with pytest.raises(ValueError) as e:
            validate_event_references(event_starts, None, attendance)
        assert "attendance event not found" in str(e.value)


@pytest.mark.unit
class TestCancelledAvailabilityJsonSchema:
    """Tests for CancelledAvailabilityJsonSchema (email-based, new format)."""

    def test_valid_cancelled_availability(self, ctx):
        """Happy path: Valid cancelled availability with email and events."""
        data = {
            "member_email": "alice@test.com",
            "events": ["Saturday January 4 - 1pm", "Friday January 10th - 3pm"],
        }
        schema = CancelledAvailabilityJsonSchema.model_validate(data, context={"ctx": ctx})

        assert schema.member_email == "alice@test.com"
        assert len(schema.events) == 2
        assert all(hasattr(e, "start") for e in schema.events)

    def test_valid_cancelled_availability_single_event(self, ctx):
        """Edge case: Single cancelled event."""
        data = {
            "member_email": "bob@test.com",
            "events": ["Saturday January 4 - 1pm"],
        }
        schema = CancelledAvailabilityJsonSchema.model_validate(data, context={"ctx": ctx})

        assert schema.member_email == "bob@test.com"
        assert len(schema.events) == 1

    def test_invalid_email_format_raises(self, ctx):
        """Error case: Invalid email format."""
        data = {
            "member_email": "not-an-email",
            "events": ["Saturday January 4 - 1pm"],
        }
        with pytest.raises(ValidationError) as e:
            CancelledAvailabilityJsonSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "valid email")

    def test_missing_member_email_raises(self, ctx):
        """Error case: Missing member_email field."""
        data = {
            "events": ["Saturday January 4 - 1pm"],
        }
        with pytest.raises(ValidationError) as e:
            CancelledAvailabilityJsonSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "member_email")

    def test_missing_events_raises(self, ctx):
        """Error case: Missing events field."""
        data = {
            "member_email": "alice@test.com",
        }
        with pytest.raises(ValidationError) as e:
            CancelledAvailabilityJsonSchema.model_validate(data, context={"ctx": ctx})

        assert_error_for_model(e.value.errors(), "events")


@pytest.mark.unit
class TestPartnershipRequestJsonSchema:
    """Tests for PartnershipRequestJsonSchema"""

    def test_valid_partnership_request(self):
        """Happy path: Valid partnership with requester and target emails."""
        data = {
            "requester_email": "alice@test.com",
            "target_emails": ["bob@test.com", "charlie@test.com"],
        }
        schema = PartnershipRequestJsonSchema.model_validate(data)

        assert schema.requester_email == "alice@test.com"
        assert len(schema.target_emails) == 2
        assert "bob@test.com" in schema.target_emails

    def test_valid_partnership_single_target(self):
        """Edge case: Single target email."""
        data = {
            "requester_email": "alice@test.com",
            "target_emails": ["bob@test.com"],
        }
        schema = PartnershipRequestJsonSchema.model_validate(data)

        assert schema.requester_email == "alice@test.com"
        assert schema.target_emails == ["bob@test.com"]

    def test_invalid_requester_email_raises(self):
        """Error case: Invalid requester email format."""
        data = {
            "requester_email": "not-an-email",
            "target_emails": ["bob@test.com"],
        }
        with pytest.raises(ValidationError) as e:
            PartnershipRequestJsonSchema.model_validate(data)

        assert_error_for_model(e.value.errors(), "valid email")

    def test_invalid_target_email_raises(self):
        """Error case: Invalid target email format."""
        data = {
            "requester_email": "alice@test.com",
            "target_emails": ["bob@test.com", "not-an-email"],
        }
        with pytest.raises(ValidationError) as e:
            PartnershipRequestJsonSchema.model_validate(data)

        assert_error_for_model(e.value.errors(), "valid email")

    def test_requester_in_targets_raises(self):
        """Error case: Requester cannot be in target_emails."""
        data = {
            "requester_email": "alice@test.com",
            "target_emails": ["alice@test.com", "bob@test.com"],
        }
        with pytest.raises(ValidationError) as e:
            PartnershipRequestJsonSchema.model_validate(data)

        assert_error_for_model(e.value.errors(), "requester")

    def test_missing_requester_email_raises(self):
        """Error case: Missing requester_email field."""
        data = {
            "target_emails": ["bob@test.com"],
        }
        with pytest.raises(ValidationError) as e:
            PartnershipRequestJsonSchema.model_validate(data)

        assert_error_for_model(e.value.errors(), "Field required")

    def test_missing_target_emails_raises(self):
        """Error case: Missing target_emails field."""
        data = {
            "requester_email": "alice@test.com",
        }
        with pytest.raises(ValidationError) as e:
            PartnershipRequestJsonSchema.model_validate(data)

        assert_error_for_model(e.value.errors(), "Field required")
