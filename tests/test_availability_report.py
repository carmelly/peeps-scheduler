import json
import pytest
from peeps_scheduler.availability_report import parse_availability
from peeps_scheduler.constants import DEFAULT_TIMEZONE
from peeps_scheduler.validation.fields import ValidationContext
from peeps_scheduler.validation.file_schemas.period import PeriodFileSchema
from peeps_scheduler.validation.period import load_period_files
from pydantic import ValidationError


def _load_period_schema(path, year):
    raw = load_period_files(str(path))
    ctx = ValidationContext(year=year, tz=DEFAULT_TIMEZONE)
    return PeriodFileSchema.model_validate(raw, context={"ctx": ctx})


def test_parse_availability_applies_cancellations(tmp_path):
    members_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alex Leader,Alex,alex@test.com,Leader,0,4,0,TRUE,2025-01-01
2,Dana Follower,Dana,dana@test.com,Follower,1,4,0,TRUE,2025-01-01
"""
    responses_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days
02/01/2025 10:00:00,alex@test.com,Alex,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0
02/01/2025 10:00:00,dana@test.com,Dana,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm",,0
"""
    period_config_content = {
        "cancelled_events": ["Sunday March 2 - 5pm"],
        "cancelled_member_availability": [
            {"member_email": "alex@test.com", "events": ["Saturday March 1 - 5pm"]}
        ],
    }

    members_path = tmp_path / "members.csv"
    responses_path = tmp_path / "responses.csv"
    period_config_path = tmp_path / "period_config.json"

    members_path.write_text(members_content)
    responses_path.write_text(responses_content)
    period_config_path.write_text(json.dumps(period_config_content))

    period_schema = _load_period_schema(tmp_path, year=2025)
    availability, unavailable, non_responders, _, _ = parse_availability(period_schema)

    assert "Saturday March 1 - 5pm" in availability
    assert availability["Saturday March 1 - 5pm"]["leader"] == []
    assert availability["Saturday March 1 - 5pm"]["follower"] == ["Dana"]
    assert "Sunday March 2 - 5pm" not in availability
    assert unavailable == ["Alex"]
    assert non_responders == []


def test_parse_availability_raises_for_unknown_cancellation_email(tmp_path):
    members_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alex Leader,Alex,alex@test.com,Leader,0,4,0,TRUE,2025-01-01
"""
    responses_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days
02/01/2025 10:00:00,alex@test.com,Alex,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm",,0
"""
    period_config_content = {
        "cancelled_events": [],
        "cancelled_member_availability": [
            {"member_email": "unknown@test.com", "events": ["Saturday March 1 - 5pm"]}
        ],
    }

    members_path = tmp_path / "members.csv"
    responses_path = tmp_path / "responses.csv"
    period_config_path = tmp_path / "period_config.json"

    members_path.write_text(members_content)
    responses_path.write_text(responses_content)
    period_config_path.write_text(json.dumps(period_config_content))

    with pytest.raises(ValidationError, match="cancelled availability email not found"):
        _load_period_schema(tmp_path, year=2025)
