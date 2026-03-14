CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_STAGING_DATASET}.stg_open_meteo_hourly` (
  location_id STRING,
  latitude FLOAT64,
  longitude FLOAT64,
  timestamp TIMESTAMP,
  temperature_2m FLOAT64,
  precipitation FLOAT64,
  wind_speed_10m FLOAT64,
  ingested_at TIMESTAMP,
  extract_run_id STRING
)
PARTITION BY DATE(timestamp)
CLUSTER BY location_id;

MERGE `${GCP_PROJECT_ID}.${BQ_STAGING_DATASET}.stg_open_meteo_hourly` AS target
USING (
  SELECT
    location_id,
    latitude,
    longitude,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M', JSON_VALUE(time_json, '$'), '${OPEN_METEO_TIMEZONE}') AS timestamp,
    SAFE_CAST(JSON_VALUE(temp_json, '$') AS FLOAT64) AS temperature_2m,
    SAFE_CAST(JSON_VALUE(precip_json, '$') AS FLOAT64) AS precipitation,
    SAFE_CAST(JSON_VALUE(wind_json, '$') AS FLOAT64) AS wind_speed_10m,
    ingested_at,
    extract_run_id,
    ROW_NUMBER() OVER (
      PARTITION BY location_id, PARSE_TIMESTAMP('%Y-%m-%dT%H:%M', JSON_VALUE(time_json, '$'), '${OPEN_METEO_TIMEZONE}')
      ORDER BY ingested_at DESC, extract_run_id DESC
    ) AS row_num
  FROM `${GCP_PROJECT_ID}.${BQ_RAW_DATASET}.${OPEN_METEO_RAW_TABLE}`,
  UNNEST(JSON_EXTRACT_ARRAY(payload, '$.hourly.time')) AS time_json WITH OFFSET idx
  LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(payload, '$.hourly.temperature_2m')) AS temp_json
    WITH OFFSET temp_idx
    ON idx = temp_idx
  LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(payload, '$.hourly.precipitation')) AS precip_json
    WITH OFFSET precip_idx
    ON idx = precip_idx
  LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(payload, '$.hourly.wind_speed_10m')) AS wind_json
    WITH OFFSET wind_idx
    ON idx = wind_idx
  WHERE JSON_VALUE(time_json, '$') IS NOT NULL
) AS source
ON target.location_id = source.location_id
  AND target.timestamp = source.timestamp
WHEN MATCHED AND source.row_num = 1 THEN
  UPDATE SET
    latitude = source.latitude,
    longitude = source.longitude,
    temperature_2m = source.temperature_2m,
    precipitation = source.precipitation,
    wind_speed_10m = source.wind_speed_10m,
    ingested_at = source.ingested_at,
    extract_run_id = source.extract_run_id
WHEN NOT MATCHED AND source.row_num = 1 THEN
  INSERT (
    location_id,
    latitude,
    longitude,
    timestamp,
    temperature_2m,
    precipitation,
    wind_speed_10m,
    ingested_at,
    extract_run_id
  )
  VALUES (
    source.location_id,
    source.latitude,
    source.longitude,
    source.timestamp,
    source.temperature_2m,
    source.precipitation,
    source.wind_speed_10m,
    source.ingested_at,
    source.extract_run_id
  );
