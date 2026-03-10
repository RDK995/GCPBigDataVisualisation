view: entity_status_daily {
  sql_table_name: `{{ _user_attributes['gcp_project'] }}.mart.entity_status_daily` ;;

  dimension_group: snapshot_date {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.snapshot_date ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  measure: entity_count {
    type: sum
    sql: ${TABLE}.entity_count ;;
  }
}
