-- BI-consumable mart table for Looker.
CREATE SCHEMA IF NOT EXISTS `${GCP_PROJECT_ID}.mart`;

CREATE OR REPLACE TABLE `${GCP_PROJECT_ID}.mart.entity_status_daily`
PARTITION BY snapshot_date
CLUSTER BY status AS
SELECT
  DATE(updated_at) AS snapshot_date,
  status,
  COUNT(DISTINCT entity_id) AS entity_count,
  MAX(updated_at) AS latest_entity_update_at
FROM `${GCP_PROJECT_ID}.staging.stg_entities`
GROUP BY 1, 2;
