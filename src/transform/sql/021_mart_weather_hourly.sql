CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_MART_DATASET}.weather_hourly` (
  location_id STRING,
  timestamp TIMESTAMP,
  temperature_2m FLOAT64,
  precipitation FLOAT64,
  wind_speed_10m FLOAT64
)
PARTITION BY DATE(timestamp)
CLUSTER BY location_id;

MERGE `${GCP_PROJECT_ID}.${BQ_MART_DATASET}.weather_hourly` AS target
USING (
  SELECT
    location_id,
    timestamp,
    temperature_2m,
    precipitation,
    wind_speed_10m,
    ROW_NUMBER() OVER (
      PARTITION BY location_id, timestamp
      ORDER BY ingested_at DESC, extract_run_id DESC
    ) AS row_num
  FROM `${GCP_PROJECT_ID}.${BQ_STAGING_DATASET}.stg_open_meteo_hourly`
) AS source
ON target.location_id = source.location_id
  AND target.timestamp = source.timestamp
WHEN MATCHED AND source.row_num = 1 THEN
  UPDATE SET
    temperature_2m = source.temperature_2m,
    precipitation = source.precipitation,
    wind_speed_10m = source.wind_speed_10m
WHEN NOT MATCHED AND source.row_num = 1 THEN
  INSERT (
    location_id,
    timestamp,
    temperature_2m,
    precipitation,
    wind_speed_10m
  )
  VALUES (
    source.location_id,
    source.timestamp,
    source.temperature_2m,
    source.precipitation,
    source.wind_speed_10m
  );
