import pytest
from peeps_scheduler.models import Role


@pytest.mark.unit
class TestRoleHandling:
    """Test role string/enum conversion."""

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
