# Operations Runbook

## Reliability

- Retry policy: 5 attempts with exponential jitter.
- Checkpoint persisted after successful extract+load+transform.
- Merge SQL ensures reruns are safe.

## Error handling strategy

- Future: send invalid records to `DEAD_LETTER_DIR` and/or `gs://.../error/`.
- Include `extract_run_id` in file paths and metadata for traceability.

## Suggested alerts

- Cloud Run Job failures
- No data landed in expected freshness window
- BigQuery bytes processed anomaly
