# GCP Big Data Visualisation Starter

Production-ready starter skeleton for an **API -> GCS -> BigQuery -> SQL transforms -> Looker** pipeline.

## Proposed Repository Tree

```text
.
├── .env.example
├── Dockerfile
├── Makefile
├── README.md
├── docs
│   ├── architecture.md
│   ├── looker.md
│   └── operations.md
├── infra
│   └── cloudbuild.yaml
├── looker
│   ├── models
│   │   └── analytics.model.lkml
│   └── views
│       └── entity_status_daily.view.lkml
├── pyproject.toml
├── samples
│   ├── fake_api_payload.json
│   └── raw_table_schema.json
├── scripts
│   └── run_local.sh
├── src
│   ├── clients
│   │   ├── api_client.py
│   │   ├── bigquery_client.py
│   │   └── gcs_client.py
│   ├── config.py
│   ├── extract
│   │   ├── checkpoint.py
│   │   ├── extractor.py
│   │   └── paginator.py
│   ├── load
│   │   ├── bq_loader.py
│   │   ├── file_writer.py
│   │   └── gcs_uploader.py
│   ├── main.py
│   ├── transform
│   │   ├── runner.py
│   │   └── sql
│   │       ├── 001_create_raw.sql
│   │       ├── 010_stg_entities.sql
│   │       └── 020_mart_example.sql
│   └── utils
│       ├── dates.py
│       ├── logging.py
│       └── retries.py
└── tests
    ├── integration_test_pipeline_placeholder.py
    ├── test_api_client.py
    ├── test_retries.py
    └── test_transform_helpers.py
```

## Quick Start

1. Install dependencies:
   ```bash
   poetry install
   ```
2. Create local env file:
   ```bash
   cp .env.example .env
   ```
3. Set required environment variables (`API_TOKEN`, `GCP_PROJECT_ID`, `GCS_BUCKET`).
4. Run the pipeline:
   ```bash
   make run
   ```

## Environment Variables

See `.env.example`. Highlights:
- `API_BASE_URL`, `API_TOKEN`, `API_INCREMENTAL_FIELD`
- `CHECKPOINT_FILE` for incremental state
- `GCS_BUCKET`, `GCS_RAW_PREFIX`
- `BQ_RAW_DATASET`, `BQ_STAGING_DATASET`, `BQ_MART_DATASET`

## Pipeline Flow

1. Read checkpoint (`state/checkpoint.json`).
2. Incrementally fetch paginated API records.
3. Land raw JSONL locally.
4. Upload JSONL to `gs://<bucket>/raw/...`.
5. Load JSONL into BigQuery raw table.
6. Run SQL transforms into staging and mart.
7. Write updated checkpoint for safe reruns.

## Operational Design Notes

- **Idempotency**: merge-based staging SQL + incremental checkpointing.
- **Schema drift**: BigQuery load job allows field additions and unknown fields.
- **Retries/backoff**: API client wrapped with `tenacity` exponential jitter.
- **Dead-letter strategy**: `DEAD_LETTER_DIR` for malformed records (hook point in extractor/loader).
- **Rate limits**: place API-specific sleep/429 handling in `src/clients/api_client.py` TODO section.

## Run with Docker

```bash
docker build -t gcp-bigdata-visualisation:local .
docker run --rm --env-file .env gcp-bigdata-visualisation:local
```

## Deploy Notes (Cloud Run Job)

Starter `infra/cloudbuild.yaml` builds container and deploys Cloud Run Job. For production:
- Store secrets in Secret Manager.
- Use Workload Identity / service accounts with minimum IAM.
- Trigger via Cloud Scheduler hourly/daily.

## dbt Optional Path

This starter runs SQL without dbt first. If adopting dbt later:
- Move `src/transform/sql` scripts into dbt models.
- Keep raw ingestion/orchestration in Python.
- Add dbt run/test step after raw load.

## Explicit TODOs You Must Implement

- `TODO(api): fill in endpoint`
- `TODO(auth): implement token refresh`
- `TODO(model): map source fields`
- `TODO(deploy): production-grade env/secrets`
