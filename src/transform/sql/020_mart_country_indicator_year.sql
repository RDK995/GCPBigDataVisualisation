CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_MART_DATASET}.country_indicator_year` (
  country_iso3 STRING,
  country_name STRING,
  indicator_id STRING,
  indicator_name STRING,
  year INT64,
  value_num FLOAT64
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(1900, 2200, 1))
CLUSTER BY country_iso3, indicator_id;

MERGE `${GCP_PROJECT_ID}.${BQ_MART_DATASET}.country_indicator_year` AS target
USING (
  SELECT
    country_iso3,
    country_name,
    indicator_id,
    indicator_name,
    year,
    value_num,
    ROW_NUMBER() OVER (
      PARTITION BY country_iso3, indicator_id, year
      ORDER BY ingested_at DESC, extract_run_id DESC
    ) AS row_num
  FROM `${GCP_PROJECT_ID}.${BQ_STAGING_DATASET}.stg_world_bank_indicator`
) AS source
ON target.country_iso3 = source.country_iso3
  AND target.indicator_id = source.indicator_id
  AND target.year = source.year
WHEN MATCHED AND source.row_num = 1 THEN
  UPDATE SET
    country_name = source.country_name,
    indicator_name = source.indicator_name,
    value_num = source.value_num
WHEN NOT MATCHED AND source.row_num = 1 THEN
  INSERT (
    country_iso3,
    country_name,
    indicator_id,
    indicator_name,
    year,
    value_num
  )
  VALUES (
    source.country_iso3,
    source.country_name,
    source.indicator_id,
    source.indicator_name,
    source.year,
    source.value_num
  );
