# Operations Runbook

## Reliability

- Retry policy: 5 attempts with exponential jitter.
- Checkpoint persisted after successful extract+load+transform.
- Merge SQL ensures reruns are safe.

## Deployment prerequisites

- Artifact Registry repository exists for the image target in `infra/cloudbuild.yaml`.
- Cloud Run Job runtime service account has:
- `roles/storage.objectAdmin` on the raw bucket or a narrower custom role.
- `roles/bigquery.jobUser` and dataset/table write access.
- `roles/secretmanager.secretAccessor` for runtime secrets.
- Cloud Build service account can deploy Cloud Run jobs and push images.

## Runtime configuration

- Non-secret settings are passed as Cloud Run Job env vars during deploy.
- Secrets are passed with Cloud Run Job `--set-secrets`.
- Default local paths are redirected to `/tmp/...` for Cloud Run runtime compatibility.

Example secret mapping:

```text
API_TOKEN=api-token:latest
```

## Scheduler

- Trigger the Cloud Run Job with Cloud Scheduler after deployment.
- Use a dedicated scheduler service account with permission to invoke the job.
- Start with one run per hour or day, then adjust based on data freshness needs.

## Error handling strategy

- Future: send invalid records to `DEAD_LETTER_DIR` and/or `gs://.../error/`.
- Include `extract_run_id` in file paths and metadata for traceability.

## Suggested alerts

- Cloud Run Job failures
- No data landed in expected freshness window
- BigQuery bytes processed anomaly
