import pytest
from peeps_scheduler.validation.helpers import normalize_email_for_match, validate_unique


@pytest.mark.unit
class TestNormalizeEmailForMatch:
    @pytest.mark.parametrize(
        "email, expected",
        [
            ("", ""),
            ("Alice@Example.com", "alice@example.com"),
            ("  Alice@Example.com  ", "alice@example.com"),
            ("alice.smith@gmail.com", "alicesmith@gmail.com"),
            ("alice..smith@gmail.com", "alicesmith@gmail.com"),
            (".alice.smith@gmail.com", "alicesmith@gmail.com"),
            ("alice.smith.@gmail.com", "alicesmith@gmail.com"),
            ("ALICE.SMITH@GMAIL.COM", "alicesmith@gmail.com"),
            ("alice.smith@test.com", "alice.smith@test.com"),
        ],
    )
    def test_normalization(self, email, expected):
        assert normalize_email_for_match(email) == expected


@pytest.mark.unit
class TestValidateUnique:
    def test_no_duplicates(self):
        validate_unique([1, 2, 3], msg="duplicate value")

    def test_duplicates_raise(self):
        with pytest.raises(ValueError, match="duplicate value"):
            validate_unique([1, 2, 2], msg="duplicate value")

    def test_duplicates_with_key(self):
        items = [{"id": 1}, {"id": 2}, {"id": 1}]
        with pytest.raises(ValueError, match="duplicate id"):
            validate_unique(items, key=lambda item: item["id"], msg="duplicate id")
