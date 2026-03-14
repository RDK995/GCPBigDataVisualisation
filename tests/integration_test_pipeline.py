from pathlib import Path

from src.config import Settings
from src.main import PipelineContext, RawBatch, run


class RecordingUploader:
    def __init__(self):
        self.uploads = []

    def upload_run_file(self, local_file, extract_run_id):
        self.uploads.append((local_file.name, extract_run_id))
        return f"gs://bucket/{local_file.name}"


class RecordingTransformRunner:
    def __init__(self):
        self.calls = []

    def run_files(self, filenames):
        self.calls.append(("run_files", filenames))

    def run_all(self, exclude_files=None):
        self.calls.append(("run_all", exclude_files))


def test_pipeline_run_orchestrates_extract_load_transform(monkeypatch, tmp_path: Path):
    uploader = RecordingUploader()
    transform_runner = RecordingTransformRunner()

    monkeypatch.setattr(
        "src.main._build_pipeline_context",
        lambda _settings: PipelineContext(
            gcs_client=None,
            uploader=uploader,
            bq_client=object(),
            transform_runner=transform_runner,
        ),
    )
    monkeypatch.setattr(
        "src.main._build_world_bank_batch",
        lambda settings, ingestion_ts, extract_run_id: RawBatch(
            filename_prefix="world_bank_indicator",
            table_ref="project.analytics_raw.world_bank_indicator",
            records=[
                {
                    "ingested_at": ingestion_ts,
                    "extract_run_id": extract_run_id,
                    "source_file": "",
                    "indicator_id": "SP.POP.TOTL",
                    "payload": {"date": "2025"},
                }
            ],
            dead_letters=[],
        ),
    )
    monkeypatch.setattr(
        "src.main._build_open_meteo_batch",
        lambda settings, ingestion_ts, extract_run_id: RawBatch(
            filename_prefix="open_meteo_response",
            table_ref="project.analytics_raw.open_meteo_response",
            records=[
                {
                    "ingested_at": ingestion_ts,
                    "extract_run_id": extract_run_id,
                    "source_file": "",
                    "location_id": "london",
                    "latitude": 51.5,
                    "longitude": -0.12,
                    "payload": {"hourly": {"time": ["2025-01-01T00:00"]}},
                }
            ],
            dead_letters=[],
        ),
    )

    loads = []
    monkeypatch.setattr("src.main.write_jsonl", lambda records, output_path: loads.append((records, output_path)))
    monkeypatch.setattr(
        "src.main.BigQueryLoader",
        lambda bq_client, table_ref: type(
            "Loader",
            (),
            {"load_jsonl": lambda self, gcs_uri: loads.append((table_ref, gcs_uri))},
        )(),
    )

    run(
        Settings(
            gcp_project_id="project",
            gcs_bucket="bucket",
            local_data_dir=tmp_path / "data",
            dead_letter_dir=tmp_path / "dead_letter",
        )
    )

    assert transform_runner.calls[0] == ("run_files", ["001_create_raw.sql"])
    assert transform_runner.calls[-1] == ("run_all", {"001_create_raw.sql"})
    assert len(uploader.uploads) == 2
    assert len(loads) == 4
