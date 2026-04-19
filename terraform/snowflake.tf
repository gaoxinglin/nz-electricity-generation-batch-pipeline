# ──────────────────────────────────────────────
# Database & Schemas
# ──────────────────────────────────────────────

resource "snowflake_database" "this" {
  name = var.snowflake_database
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.this.name
  name     = "RAW"
}

resource "snowflake_schema" "analytics" {
  database = snowflake_database.this.name
  name     = "ANALYTICS"
}

# ──────────────────────────────────────────────
# Warehouses (XS, auto-suspend 60s)
# ──────────────────────────────────────────────

resource "snowflake_warehouse" "transform" {
  name           = "TRANSFORM_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
}

resource "snowflake_warehouse" "dashboard" {
  name           = "DASHBOARD_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
}

# ──────────────────────────────────────────────
# File Format: csv_format
# ──────────────────────────────────────────────

resource "snowflake_file_format" "csv_format" {
  database                     = snowflake_database.this.name
  schema                       = snowflake_schema.raw.name
  name                         = "CSV_FORMAT"
  format_type                  = "CSV"
  skip_header                  = 1
  field_optionally_enclosed_by = "\""
  empty_field_as_null          = true
  null_if                      = ["", "NULL", "null"]
}

# ──────────────────────────────────────────────
# External Stage: raw_stage → S3 raw/ prefix
# ──────────────────────────────────────────────

resource "snowflake_stage" "raw_stage" {
  database = snowflake_database.this.name
  schema   = snowflake_schema.raw.name
  name     = "RAW_STAGE"
  url      = "s3://${var.s3_bucket_name}/raw/"

  credentials = "AWS_KEY_ID='${aws_iam_access_key.snowflake_s3.id}' AWS_SECRET_KEY='${aws_iam_access_key.snowflake_s3.secret}'"
}

# ──────────────────────────────────────────────
# Table: raw_generation (59 columns)
# 57 CSV columns (all VARCHAR) + trading_month + _source_file_modified_at
# ──────────────────────────────────────────────

resource "snowflake_table" "raw_generation" {
  database = snowflake_database.this.name
  schema   = snowflake_schema.raw.name
  name     = "RAW_GENERATION"

  # 7 header columns
  column {
    name = "SITE_CODE"
    type = "VARCHAR"
  }
  column {
    name = "POC_CODE"
    type = "VARCHAR"
  }
  column {
    name = "NWK_CODE"
    type = "VARCHAR"
  }
  column {
    name = "GEN_CODE"
    type = "VARCHAR"
  }
  column {
    name = "FUEL_CODE"
    type = "VARCHAR"
  }
  column {
    name = "TECH_CODE"
    type = "VARCHAR"
  }
  column {
    name = "TRADING_DATE"
    type = "VARCHAR"
  }

  # 50 trading period columns (TP1-TP50)
  column {
    name = "TP1"
    type = "VARCHAR"
  }
  column {
    name = "TP2"
    type = "VARCHAR"
  }
  column {
    name = "TP3"
    type = "VARCHAR"
  }
  column {
    name = "TP4"
    type = "VARCHAR"
  }
  column {
    name = "TP5"
    type = "VARCHAR"
  }
  column {
    name = "TP6"
    type = "VARCHAR"
  }
  column {
    name = "TP7"
    type = "VARCHAR"
  }
  column {
    name = "TP8"
    type = "VARCHAR"
  }
  column {
    name = "TP9"
    type = "VARCHAR"
  }
  column {
    name = "TP10"
    type = "VARCHAR"
  }
  column {
    name = "TP11"
    type = "VARCHAR"
  }
  column {
    name = "TP12"
    type = "VARCHAR"
  }
  column {
    name = "TP13"
    type = "VARCHAR"
  }
  column {
    name = "TP14"
    type = "VARCHAR"
  }
  column {
    name = "TP15"
    type = "VARCHAR"
  }
  column {
    name = "TP16"
    type = "VARCHAR"
  }
  column {
    name = "TP17"
    type = "VARCHAR"
  }
  column {
    name = "TP18"
    type = "VARCHAR"
  }
  column {
    name = "TP19"
    type = "VARCHAR"
  }
  column {
    name = "TP20"
    type = "VARCHAR"
  }
  column {
    name = "TP21"
    type = "VARCHAR"
  }
  column {
    name = "TP22"
    type = "VARCHAR"
  }
  column {
    name = "TP23"
    type = "VARCHAR"
  }
  column {
    name = "TP24"
    type = "VARCHAR"
  }
  column {
    name = "TP25"
    type = "VARCHAR"
  }
  column {
    name = "TP26"
    type = "VARCHAR"
  }
  column {
    name = "TP27"
    type = "VARCHAR"
  }
  column {
    name = "TP28"
    type = "VARCHAR"
  }
  column {
    name = "TP29"
    type = "VARCHAR"
  }
  column {
    name = "TP30"
    type = "VARCHAR"
  }
  column {
    name = "TP31"
    type = "VARCHAR"
  }
  column {
    name = "TP32"
    type = "VARCHAR"
  }
  column {
    name = "TP33"
    type = "VARCHAR"
  }
  column {
    name = "TP34"
    type = "VARCHAR"
  }
  column {
    name = "TP35"
    type = "VARCHAR"
  }
  column {
    name = "TP36"
    type = "VARCHAR"
  }
  column {
    name = "TP37"
    type = "VARCHAR"
  }
  column {
    name = "TP38"
    type = "VARCHAR"
  }
  column {
    name = "TP39"
    type = "VARCHAR"
  }
  column {
    name = "TP40"
    type = "VARCHAR"
  }
  column {
    name = "TP41"
    type = "VARCHAR"
  }
  column {
    name = "TP42"
    type = "VARCHAR"
  }
  column {
    name = "TP43"
    type = "VARCHAR"
  }
  column {
    name = "TP44"
    type = "VARCHAR"
  }
  column {
    name = "TP45"
    type = "VARCHAR"
  }
  column {
    name = "TP46"
    type = "VARCHAR"
  }
  column {
    name = "TP47"
    type = "VARCHAR"
  }
  column {
    name = "TP48"
    type = "VARCHAR"
  }
  column {
    name = "TP49"
    type = "VARCHAR"
  }
  column {
    name = "TP50"
    type = "VARCHAR"
  }

  # 2 pipeline-added columns
  column {
    name = "TRADING_MONTH"
    type = "VARCHAR"
  }
  column {
    name = "_SOURCE_FILE_MODIFIED_AT"
    type = "TIMESTAMP_NTZ"
  }
}

# ──────────────────────────────────────────────
# Roles & Users
# ──────────────────────────────────────────────

resource "snowflake_account_role" "transformer" {
  name = "TRANSFORMER"
}

resource "snowflake_account_role" "reader" {
  name = "READER"
}

# Transformer: read/write all schemas
resource "snowflake_grant_privileges_to_account_role" "transformer_database" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.this.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_schema" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = "\"${snowflake_database.this.name}\".\"${snowflake_schema.raw.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_analytics_schema" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = "\"${snowflake_database.this.name}\".\"${snowflake_schema.analytics.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_tables" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.this.name}\".\"${snowflake_schema.raw.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_analytics_tables" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.this.name}\".\"${snowflake_schema.analytics.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_future_tables" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.this.name}\".\"${snowflake_schema.raw.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_analytics_future_tables" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.this.name}\".\"${snowflake_schema.analytics.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_analytics_future_views" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = "\"${snowflake_database.this.name}\".\"${snowflake_schema.analytics.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_stage" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "READ"]
  on_schema_object {
    object_type = "STAGE"
    object_name = "\"${snowflake_database.this.name}\".\"${snowflake_schema.raw.name}\".\"${snowflake_stage.raw_stage.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_file_format" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_schema_object {
    object_type = "FILE FORMAT"
    object_name = "\"${snowflake_database.this.name}\".\"${snowflake_schema.raw.name}\".\"${snowflake_file_format.csv_format.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_warehouse" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transform.name
  }
}

# Reader: read-only on analytics schema
resource "snowflake_grant_privileges_to_account_role" "reader_database" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.this.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_analytics_schema" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.this.name}\".\"${snowflake_schema.analytics.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_analytics_tables" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.this.name}\".\"${snowflake_schema.analytics.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_analytics_future_tables" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.this.name}\".\"${snowflake_schema.analytics.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_analytics_views" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "VIEWS"
      in_schema          = "\"${snowflake_database.this.name}\".\"${snowflake_schema.analytics.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_analytics_future_views" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = "\"${snowflake_database.this.name}\".\"${snowflake_schema.analytics.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_warehouse" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.dashboard.name
  }
}

# Service accounts
resource "snowflake_user" "transformer" {
  name              = "TRANSFORMER_SVC"
  login_name        = "TRANSFORMER_SVC"
  password          = var.snowflake_transformer_password
  default_role      = snowflake_account_role.transformer.name
  default_warehouse = snowflake_warehouse.transform.name
  default_namespace = "${snowflake_database.this.name}.${snowflake_schema.raw.name}"
}

resource "snowflake_user" "reader" {
  name              = "READER_SVC"
  login_name        = "READER_SVC"
  password          = var.snowflake_reader_password
  default_role      = snowflake_account_role.reader.name
  default_warehouse = snowflake_warehouse.dashboard.name
  default_namespace = "${snowflake_database.this.name}.${snowflake_schema.analytics.name}"
}

resource "snowflake_grant_account_role" "transformer_to_user" {
  role_name = snowflake_account_role.transformer.name
  user_name = snowflake_user.transformer.name
}

resource "snowflake_grant_account_role" "reader_to_user" {
  role_name = snowflake_account_role.reader.name
  user_name = snowflake_user.reader.name
}
