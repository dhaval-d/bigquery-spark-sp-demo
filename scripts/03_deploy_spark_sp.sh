#!/usr/bin/env bash
# ==============================================================================
# 03_deploy_spark_sp.sh
# Deploys the BigQuery Serverless Spark Stored Procedure
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${PROJECT_ROOT}/config/env_vars.sh"

echo ""
echo "=================================================================="
echo "Phase 3: Deploying BigQuery Spark Stored Procedure"
echo "=================================================================="
echo "Procedure:   ${PROJECT_ID}.${DATASET_NAME}.${PROCEDURE_NAME}"
echo "Connection:  ${PROJECT_ID}.${REGION}.${CONNECTION_ID}"
echo "Runtime:     Apache Spark ${SPARK_RUNTIME_VERSION}"
echo "=================================================================="

# Read SQL template and substitute variables
SQL_FILE="${PROJECT_ROOT}/sql/03_create_spark_sp.sql"
PROCESSED_SQL=$(sed \
    -e "s/@PROJECT_ID@/${PROJECT_ID}/g" \
    -e "s/@REGION@/${REGION}/g" \
    -e "s/@DATASET_NAME@/${DATASET_NAME}/g" \
    -e "s/@TABLE_NAME@/${TABLE_NAME}/g" \
    -e "s/@PROCEDURE_NAME@/${PROCEDURE_NAME}/g" \
    -e "s/@CONNECTION_ID@/${CONNECTION_ID}/g" \
    -e "s/@SPARK_RUNTIME_VERSION@/${SPARK_RUNTIME_VERSION}/g" \
    -e "s/@SPARK_COMPUTE_TIER@/${SPARK_COMPUTE_TIER}/g" \
    "${SQL_FILE}")

echo "Creating Spark Stored Procedure in BigQuery..."
echo "${PROCESSED_SQL}" | bq query \
    --use_legacy_sql=false \
    --location="${REGION}"

echo ""
echo "Spark Stored Procedure deployed successfully!"
echo "Verification: Checking procedure metadata..."
bq show --routine "${PROJECT_ID}:${DATASET_NAME}.${PROCEDURE_NAME}"
