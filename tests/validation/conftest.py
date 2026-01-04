from zoneinfo import ZoneInfo
import pytest
from peeps_scheduler.validation.fields import ValidationContext


@pytest.fixture
def ctx(): 
    return ValidationContext(year=2020, tz=ZoneInfo("America/Los_Angeles"))


def assert_error_for_field(errors, field, msg_substring=None):
    matching = [e for e in errors if e["loc"] and e["loc"][0] == field]

    assert matching, {
        "expected_field": field,
        "all_errors": errors,
    }

    if msg_substring:
        assert any(msg_substring in e["msg"] for e in matching), {
            "expected_message": msg_substring,
            "matching_errors": matching,
            "all_errors": errors,
        }


def assert_error_for_model(errors, msg_substring):
    assert any(msg_substring in e["msg"] for e in errors), {
        "expected_message": msg_substring,
        "all_errors": errors,
    }
