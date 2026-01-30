import json
import pytest
from .validation.conftest import ctx  # noqa: F401  -- expose ctx fixture for adapters.file tests


@pytest.fixture
def period_slug():
    return "2020-01"


@pytest.fixture
def period_root(tmp_path):
    root = tmp_path / "original"
    root.mkdir()
    return root


@pytest.fixture
def period_dir(period_root, period_slug):
    period_dir = period_root / period_slug
    period_dir.mkdir(parents=True)

    members_csv = period_dir / "members.csv"
    members_csv.write_text(
        "id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined\n"
        "1,Alice Alpha,Alice,alice@test.com,follower,0,3,0,TRUE,1/1/2020\n"
        "2,Bob Beta,Bob,bob@test.com,follower,1,3,1,TRUE,1/2/2020\n"
        "3,Carol Clark,Carol,carol@test.com,leader,2,2,4,TRUE,1/3/2020\n"
    )

    responses_csv = period_dir / "responses.csv"
    responses_csv.write_text(
        "Timestamp,Name,Display Name,Email Address,Primary Role,Secondary Role,"
        "Max Sessions,Availability,Min Interval Days,Deep Dive Topics\n"
        "1/1/2020 12:00:00,Alice Alpha,Alice,alice@test.com,Follower,"
        "I only want to be scheduled in my primary role,2,"
        "Saturday January 4 - 1pm,0,Balance for Spins and Turns\n"
        "1/1/2020 12:15:00,Bob Beta,Bob,bob@test.com,Follower,,1,"
        "Saturday January 4 - 1pm,0,\n"
        "1/1/2020 12:30:00,Carol Clark,Carol,carol@test.com,Leader,,3,"
        "Saturday January 11 - 1pm,0,\n"
    )

    period_config_json = period_dir / "period_config.json"
    period_config_json.write_text(
        json.dumps(
            {
                "cancelled_events": ["Saturday January 11 - 1pm"],
                "cancelled_member_availability": [
                    {
                        "member_email": "bob@test.com",
                        "events": ["Saturday January 4 - 1pm"],
                    }
                ],
                "partnership_requests": [
                    {
                        "requester_email": "alice@test.com",
                        "target_emails": ["bob@test.com", "carol@test.com"],
                    }
                ],
                "topics": ["Balance for Spins and Turns", "Angles for Shaping & Slotting"],
            }
        )
    )

    return period_dir
