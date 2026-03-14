from pathlib import Path

from src.config import Settings
from src.main import PipelineContext, RawBatch, run


class StubUploader:
    def __init__(self):
        self.uploads = []

    def upload_run_file(self, local_file, extract_run_id):
        self.uploads.append((local_file, extract_run_id))
        return f"gs://bucket/{local_file.name}"


class StubBigQueryClient:
    pass


class StubTransformRunner:
    def __init__(self):
        self.run_files_calls = []
        self.run_all_calls = []

    def run_files(self, filenames):
        self.run_files_calls.append(filenames)

    def run_all(self, exclude_files=None):
        self.run_all_calls.append(exclude_files)


def test_run_executes_batches_and_transforms(monkeypatch, tmp_path: Path):
    uploader = StubUploader()
    bq_client = StubBigQueryClient()
    transform_runner = StubTransformRunner()

    monkeypatch.setattr(
        "src.main._build_pipeline_context",
        lambda _settings: PipelineContext(
            gcs_client=None,
            uploader=uploader,
            bq_client=bq_client,
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
            records=[],
            dead_letters=[],
        ),
    )

    loaded_batches = []

    def fake_load_raw_batch(batch, extract_run_id, uploader_arg, bq_client_arg, settings):
        loaded_batches.append((batch, extract_run_id, uploader_arg, bq_client_arg, settings))

    monkeypatch.setattr("src.main._load_raw_batch", fake_load_raw_batch)

    run(
        Settings(
            gcp_project_id="project",
            gcs_bucket="bucket",
            local_data_dir=tmp_path / "data",
            dead_letter_dir=tmp_path / "dead_letter",
        )
    )

    assert transform_runner.run_files_calls == [["001_create_raw.sql"]]
    assert len(loaded_batches) == 1
    assert loaded_batches[0][0].filename_prefix == "world_bank_indicator"
    assert transform_runner.run_all_calls == [{"001_create_raw.sql"}]


def test_run_skips_transforms_when_no_data(monkeypatch, tmp_path: Path):
    transform_runner = StubTransformRunner()

    monkeypatch.setattr(
        "src.main._build_pipeline_context",
        lambda _settings: PipelineContext(
            gcs_client=None,
            uploader=StubUploader(),
            bq_client=StubBigQueryClient(),
            transform_runner=transform_runner,
        ),
    )
    empty_batch = RawBatch(
        filename_prefix="empty",
        table_ref="project.analytics_raw.empty",
        records=[],
        dead_letters=[],
    )
    monkeypatch.setattr("src.main._build_world_bank_batch", lambda *args: empty_batch)
    monkeypatch.setattr("src.main._build_open_meteo_batch", lambda *args: empty_batch)

    load_calls = []
    monkeypatch.setattr("src.main._load_raw_batch", lambda *args: load_calls.append(args))

    run(
        Settings(
            gcp_project_id="project",
            gcs_bucket="bucket",
            local_data_dir=tmp_path / "data",
            dead_letter_dir=tmp_path / "dead_letter",
        )
    )

    assert transform_runner.run_files_calls == [["001_create_raw.sql"]]
    assert load_calls == []
    assert transform_runner.run_all_calls == []


def test_run_writes_dead_letters_even_without_valid_rows(monkeypatch, tmp_path: Path):
    transform_runner = StubTransformRunner()

    monkeypatch.setattr(
        "src.main._build_pipeline_context",
        lambda _settings: PipelineContext(
            gcs_client=None,
            uploader=StubUploader(),
            bq_client=StubBigQueryClient(),
            transform_runner=transform_runner,
        ),
    )
    dead_letter_batch = RawBatch(
        filename_prefix="world_bank_indicator",
        table_ref="project.analytics_raw.world_bank_indicator",
        records=[],
        dead_letters=[{"reason": "invalid_record", "payload": {}}],
    )
    monkeypatch.setattr("src.main._build_world_bank_batch", lambda *args: dead_letter_batch)
    monkeypatch.setattr(
        "src.main._build_open_meteo_batch",
        lambda *args: RawBatch(
            filename_prefix="open_meteo_response",
            table_ref="project.analytics_raw.open_meteo_response",
            records=[],
            dead_letters=[],
        ),
    )

    load_calls = []
    monkeypatch.setattr("src.main._load_raw_batch", lambda *args: load_calls.append(args))

    run(
        Settings(
            gcp_project_id="project",
            gcs_bucket="bucket",
            local_data_dir=tmp_path / "data",
            dead_letter_dir=tmp_path / "dead_letter",
        )
    )

    assert len(load_calls) == 1
    assert transform_runner.run_all_calls == []
