# Metabase Readiness

- Connect self-hosted Metabase directly to BigQuery tables in `analytics_mart`.
- Use `country_indicator_year` for annual country/indicator trend reporting.
- Use `weather_hourly` for hourly weather dashboards by `location_id`.
- Keep reusable business logic in BigQuery staging and mart SQL rather than Metabase custom expressions.

## Recommended data sources

- `analytics_mart.country_indicator_year`
- `analytics_mart.weather_hourly`

## Self-hosted deployment

- A local/self-hosted Metabase stack definition is provided in `infra/metabase/docker-compose.yml`.
- On machines without Docker Compose, use `make metabase-up` and `make metabase-down`; the Make targets start the same Metabase + Postgres topology with plain `docker run`.
- Set `METABASE_ENCRYPTION_SECRET_KEY` before startup so saved database credentials are encrypted at rest.
- After Metabase is running, add BigQuery as a database connection in the Metabase admin UI.

Example:

```bash
export METABASE_ENCRYPTION_SECRET_KEY='replace-with-a-long-random-secret'
make metabase-up
```

## BigQuery connection guidance

- Prefer a dedicated service account for Metabase read access.
- Grant read access only to the reporting datasets and tables it needs.
- Keep the BigQuery credentials outside Git and inject them through your Metabase deployment environment.
- Keep `MB_ENCRYPTION_SECRET_KEY` stable for the lifetime of the Metabase deployment.

## Suggested dashboard patterns

- World Bank dashboard:
- KPI cards for latest values by indicator.
- Time series by `year`.
- Country and indicator filters.

- Weather dashboard:
- Time series by `timestamp`.
- Location filter on `location_id`.
- Temperature, precipitation, and wind charts from the mart table.

## Modeling guidance

- Keep field names business-friendly in SQL because Metabase does not have a LookML-style semantic modeling layer.
- Prefer BigQuery views or mart tables over heavy question-level custom logic for reusable metrics.
- Partition-aware marts improve Metabase query performance and reduce BigQuery query cost.
