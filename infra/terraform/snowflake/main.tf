###############################################################################
# PaySim Fraud Analytics — Snowflake Infrastructure
# Provisions: database · schemas · warehouse · role · grants
###############################################################################

terraform {
  required_version = ">= 1.5"

  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.90"
    }
  }
}

# ─── Provider ─────────────────────────────────────────────────────────────────
# Legacy account locator format — no org needed
provider "snowflake" {
  account  = var.snowflake_account
  user     = var.snowflake_user
  authenticator = "JWT"
  private_key   = file(var.snowflake_private_key_path)
  role     = "ACCOUNTADMIN"
}

# ─── Database ─────────────────────────────────────────────────────────────────
resource "snowflake_database" "fraud_db" {
  name                        = "FRAUD_DB"
  comment                     = "PaySim fraud analytics — all layers (RAW → STAGING → TRUSTED)"
  data_retention_time_in_days = 0
}

# ─── Schemas ──────────────────────────────────────────────────────────────────
resource "snowflake_schema" "raw" {
  database = snowflake_database.fraud_db.name
  name     = "RAW"
  comment  = "Landing zone: raw CSV data ingested from GCS via Snowpipe / COPY INTO"
}

resource "snowflake_schema" "kitchen" {
  database = snowflake_database.fraud_db.name
  name     = "STAGING"
  comment  = "Staging layer: standardised, deduplicated transactions (dbt staging models)"
}

resource "snowflake_schema" "trusted" {
  database = snowflake_database.fraud_db.name
  name     = "TRUSTED"
  comment  = "Trusted layer: dimensions, facts and aggregates (dbt trusted models)"
}

# ─── Virtual Warehouse ────────────────────────────────────────────────────────
resource "snowflake_warehouse" "fraud_wh" {
  name                      = "FRAUD_WH"
  comment                   = "PaySim analytics workload warehouse"
  warehouse_size            = "X-SMALL"
  auto_suspend              = 300
  auto_resume               = true
  initially_suspended       = true
  enable_query_acceleration = false
}

# ─── Roles ────────────────────────────────────────────────────────────────────
resource "snowflake_account_role" "fraud_loader" {
  name    = "FRAUD_LOADER"
  comment = "Role used by the GCS→Snowflake ingestion service account"
}

resource "snowflake_account_role" "fraud_analyst" {
  name    = "FRAUD_ANALYST"
  comment = "Read-only role for BI / notebook consumers"
}

# ─── Grants — FRAUD_LOADER ────────────────────────────────────────────────────
resource "snowflake_grant_privileges_to_account_role" "loader_use_wh" {
  account_role_name = snowflake_account_role.fraud_loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.fraud_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_use_db" {
  account_role_name = snowflake_account_role.fraud_loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.fraud_db.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_raw_schema" {
  account_role_name = snowflake_account_role.fraud_loader.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE STAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.fraud_db.name}\".\"${snowflake_schema.raw.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_raw_tables" {
  account_role_name = snowflake_account_role.fraud_loader.name
  privileges        = ["INSERT", "UPDATE", "SELECT", "TRUNCATE"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.fraud_db.name}\".\"${snowflake_schema.raw.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_kitchen_schema" {
  account_role_name = snowflake_account_role.fraud_loader.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = "\"${snowflake_database.fraud_db.name}\".\"${snowflake_schema.kitchen.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_trusted_schema" {
  account_role_name = snowflake_account_role.fraud_loader.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = "\"${snowflake_database.fraud_db.name}\".\"${snowflake_schema.trusted.name}\""
  }
}

# ─── Grants — FRAUD_ANALYST ───────────────────────────────────────────────────
resource "snowflake_grant_privileges_to_account_role" "analyst_use_wh" {
  account_role_name = snowflake_account_role.fraud_analyst.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.fraud_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_use_db" {
  account_role_name = snowflake_account_role.fraud_analyst.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.fraud_db.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_trusted_read" {
  account_role_name = snowflake_account_role.fraud_analyst.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.fraud_db.name}\".\"${snowflake_schema.trusted.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "analyst_trusted_tables" {
  account_role_name = snowflake_account_role.fraud_analyst.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.fraud_db.name}\".\"${snowflake_schema.trusted.name}\""
    }
  }
}
