#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing .env. Copy .env.example to .env and fill the required values." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

export AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-ap-southeast-2}}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$AWS_REGION}"
export AWS_SDK_LOAD_CONFIG="${AWS_SDK_LOAD_CONFIG:-1}"

export TF_VAR_aws_region="${TF_VAR_aws_region:-$AWS_REGION}"

if [[ -n "${S3_BUCKET_NAME:-}" && -z "${TF_VAR_s3_bucket_name:-}" ]]; then
  export TF_VAR_s3_bucket_name="$S3_BUCKET_NAME"
fi

if [[ -n "${BUDGET_ALERT_EMAIL:-}" && -z "${TF_VAR_budget_alert_email:-}" ]]; then
  export TF_VAR_budget_alert_email="$BUDGET_ALERT_EMAIL"
fi

if [[ -n "${SNOWFLAKE_ORGANIZATION_NAME:-}" && -z "${TF_VAR_snowflake_organization_name:-}" ]]; then
  export TF_VAR_snowflake_organization_name="$SNOWFLAKE_ORGANIZATION_NAME"
fi

if [[ -n "${SNOWFLAKE_ACCOUNT_NAME:-}" && -z "${TF_VAR_snowflake_account_name:-}" ]]; then
  export TF_VAR_snowflake_account_name="$SNOWFLAKE_ACCOUNT_NAME"
fi

if [[ -n "${SNOWFLAKE_USER:-}" && -z "${TF_VAR_snowflake_user:-}" ]]; then
  export TF_VAR_snowflake_user="$SNOWFLAKE_USER"
fi

if [[ -n "${SNOWFLAKE_PRIVATE_KEY_PATH:-}" && -z "${TF_VAR_snowflake_private_key_path:-}" ]]; then
  export TF_VAR_snowflake_private_key_path="$SNOWFLAKE_PRIVATE_KEY_PATH"
fi

if [[ -n "${SNOWFLAKE_DATABASE:-}" && -z "${TF_VAR_snowflake_database:-}" ]]; then
  export TF_VAR_snowflake_database="$SNOWFLAKE_DATABASE"
fi

if [[ -n "${SNOWFLAKE_TRANSFORMER_PASSWORD:-}" && -z "${TF_VAR_snowflake_transformer_password:-}" ]]; then
  export TF_VAR_snowflake_transformer_password="$SNOWFLAKE_TRANSFORMER_PASSWORD"
fi

if [[ -n "${SNOWFLAKE_READER_PASSWORD:-}" && -z "${TF_VAR_snowflake_reader_password:-}" ]]; then
  export TF_VAR_snowflake_reader_password="$SNOWFLAKE_READER_PASSWORD"
fi

# These app/dbt variables are useful elsewhere, but the Terraform Snowflake
# provider also auto-detects them and can conflict with the explicit JWT config.
unset SNOWFLAKE_ACCOUNT
unset SNOWFLAKE_PASSWORD
unset SNOWFLAKE_PRIVATE_KEY
unset SNOWFLAKE_PRIVATE_KEY_PATH
unset SNOWFLAKE_ORGANIZATION_NAME
unset SNOWFLAKE_ACCOUNT_NAME
unset SNOWFLAKE_USER
unset SNOWFLAKE_ROLE
unset SNOWFLAKE_WAREHOUSE
unset SNOWFLAKE_DATABASE
unset SNOWFLAKE_TRANSFORMER_PASSWORD
unset SNOWFLAKE_READER_PASSWORD

exec terraform -chdir="$ROOT_DIR/terraform" "$@"
