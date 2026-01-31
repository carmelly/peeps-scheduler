import datetime
import json
import tempfile
from pathlib import Path
from peeps_scheduler.file_io import save_json, save_peeps_csv
from peeps_scheduler.models import Role

# ============================================================================
# TEST CLASSES
# ============================================================================


class TestJSONOperations:
    """Tests for JSON loading, saving, and serialization."""

    def test_save_json_serializes_dates(self, tmp_path):
        """Test save_json handles datetime.date, datetime.datetime, and fallback types."""
        data = {
            "today": datetime.date(2025, 7, 21),
            "now": datetime.datetime(2025, 7, 21, 15, 0),
            "fallback": {"custom": "data"},
        }
        out_path = tmp_path / "dates.json"
        save_json(data, out_path)

        loaded = json.loads(out_path.read_text())
        assert loaded["today"] == "2025-07-21"
        assert "2025" in loaded["now"]
        assert isinstance(loaded["fallback"], dict)

    def test_save_json_serializes_enum(self, tmp_path):
        """Ensure save_json uses .value for Enums."""
        data = {"role": Role.LEADER}
        out_path = tmp_path / "enum.json"
        save_json(data, out_path)

        result = json.loads(out_path.read_text())
        assert result["role"] == "leader"

    def test_save_json_fallback_str_for_unknown_type(self, tmp_path):
        """Ensure save_json falls back to str(obj) for unknown types like set."""
        path = tmp_path / "fallback.json"
        data = {"example": {1, 2, 3}}  # sets are not JSON serializable by default
        save_json(data, path)

        with path.open() as f:
            contents = json.load(f)

        # The set should have been stringified like "{1, 2, 3}"
        assert contents["example"].startswith("{") and "1" in contents["example"]


class TestDataSaving:
    """Tests for saving peeps, sequences, and events."""

    def test_save_peeps_csv(self, sample_peeps):
        """Ensure save_peeps_csv writes correct rows and creates file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            output_path = tmpdir / "members_updated.csv"
            save_peeps_csv(sample_peeps, output_path)

            assert output_path.exists()

            with output_path.open() as f:
                lines = f.readlines()
                assert lines[0].startswith("id,Name,Display Name")  # Header
                assert "Alice Alpha" in lines[1]
                assert "Bob Beta" in lines[2]
