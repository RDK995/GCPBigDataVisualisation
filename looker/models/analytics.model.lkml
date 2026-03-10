connection: "bigquery_connection"

include: "/views/*.view"

explore: entity_status_daily {
  label: "Entity Status Daily"
  description: "Looker-ready mart explore for status trend dashboards."
}
