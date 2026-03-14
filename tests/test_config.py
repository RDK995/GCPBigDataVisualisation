from pathlib import Path

import pytest

from src.config import OpenMeteoLocation, Settings


def test_open_meteo_location_parse():
    location = OpenMeteoLocation.parse("london:51.5072:-0.1276")

    assert location.location_id == "london"
    assert location.latitude == 51.5072
    assert location.longitude == -0.1276


def test_settings_validate_requires_core_gcp_config(tmp_path: Path):
    settings = Settings(
        gcp_project_id="",
        gcs_bucket="",
        local_data_dir=tmp_path / "data",
        dead_letter_dir=tmp_path / "dead_letter",
    )

    with pytest.raises(ValueError, match="GCP_PROJECT_ID"):
        settings.validate()


def test_settings_validate_rejects_empty_indicator_list(tmp_path: Path):
    settings = Settings(
        gcp_project_id="project",
        gcs_bucket="bucket",
        world_bank_indicator_ids_raw="",
        local_data_dir=tmp_path / "data",
        dead_letter_dir=tmp_path / "dead_letter",
    )

    with pytest.raises(ValueError, match="WORLD_BANK_INDICATOR_IDS"):
        settings.validate()
