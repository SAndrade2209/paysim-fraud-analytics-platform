###############################################################################
# Outputs — Snowflake module
###############################################################################

output "fraud_db_name" {
  description = "Snowflake database name"
  value       = snowflake_database.fraud_db.name
}

output "warehouse_name" {
  description = "Snowflake virtual warehouse name"
  value       = snowflake_warehouse.fraud_wh.name
}

output "loader_role_name" {
  description = "Role used by the ingestion service"
  value       = snowflake_account_role.fraud_loader.name
}

output "analyst_role_name" {
  description = "Read-only role for BI / notebook consumers"
  value       = snowflake_account_role.fraud_analyst.name
}
