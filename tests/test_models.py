import pytest
from peeps_scheduler.models import Role, SwitchPreference


@pytest.mark.unit
class TestSwitchPreference:
    """Characterization tests for legacy SwitchPreference.from_string()."""

    @pytest.mark.parametrize("preference_str,expected_value", [
        ("I only want to be scheduled in my primary role", SwitchPreference.PRIMARY_ONLY),
        ("I'm happy to dance my secondary role if it lets me attend when my primary is full", SwitchPreference.SWITCH_IF_PRIMARY_FULL),
        ("I'm willing to dance my secondary role only if it's needed to enable filling a session", SwitchPreference.SWITCH_IF_NEEDED),
    ])
    def test_switch_preference_from_string(self, preference_str, expected_value):
        pref = SwitchPreference.from_string(preference_str)
        assert pref == expected_value

    def test_switch_preference_from_string_invalid_raises_with_buggy_message(self):
        """Characterization: currently raises ValueError with 'unknown role' (bug)."""
        with pytest.raises(ValueError, match=r"^unknown role:"):
            SwitchPreference.from_string("Invalid preference string")


@pytest.mark.unit
class TestRoleHandling:
    """Test role string/enum conversion."""

    @pytest.mark.parametrize("role_str,expected_role", [
        ("leader", Role.LEADER),
        ("Leader", Role.LEADER),
        ("LEADER", Role.LEADER),
        ("LeAdEr", Role.LEADER),
        ("follower", Role.FOLLOWER),
        ("Follower", Role.FOLLOWER),
    ])
    def test_role_from_string_normalizes_case(self, role_str, expected_role):
        role = Role.from_string(role_str)
        assert role == expected_role
   
    def test_role_from_string_invalid_raises(self):
        """Test that invalid role strings raise ValueError."""
        with pytest.raises(ValueError, match=r"unknown role:"):
            Role.from_string("invalid role")

    @pytest.mark.parametrize(
    "role,expected",
    [
        (Role.LEADER, Role.FOLLOWER),
        (Role.FOLLOWER, Role.LEADER),
    ],
)
    def test_role_opposite(self, role, expected):
        """Test that role opposite() method works correctly."""
        assert role.opposite() == expected  
