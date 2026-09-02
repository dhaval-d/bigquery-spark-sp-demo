#!/usr/bin/env bash
# ==============================================================================
# 02_generate_data.sh
# Generates high-cardinality synthetic benchmark data in BigQuery
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${PROJECT_ROOT}/config/env_vars.sh"

echo ""
echo "=================================================================="
echo "Phase 2: Generating Synthetic Benchmark Data in BigQuery"
echo "=================================================================="
echo "Target Table: ${PROJECT_ID}.${DATASET_NAME}.${TABLE_NAME}"
echo "Extract Date: ${TEST_EXTRACT_DATE}"
echo "Scale (Rows): ${DATA_SCALE_ROWS}"
echo "=================================================================="

# Read SQL template and substitute environment variables
SQL_FILE="${PROJECT_ROOT}/sql/01_schema_and_generator.sql"
PROCESSED_SQL=$(sed \
    -e "s/@PROJECT_ID@/${PROJECT_ID}/g" \
    -e "s/@DATASET_NAME@/${DATASET_NAME}/g" \
    -e "s/@TABLE_NAME@/${TABLE_NAME}/g" \
    -e "s/@TEST_EXTRACT_DATE@/${TEST_EXTRACT_DATE}/g" \
    -e "s/@DATA_SCALE_ROWS@/${DATA_SCALE_ROWS}/g" \
    "${SQL_FILE}")

echo "Executing data generation query in BigQuery..."
echo "${PROCESSED_SQL}" | bq query \
    --use_legacy_sql=false \
    --location="${REGION}"

echo ""
echo "Verifying generated dataset metrics..."
STATS_QUERY="
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT terr_cd) AS distinct_territories,
    COUNT(DISTINCT store_id) AS distinct_stores,
    COUNT(DISTINCT CONCAT(terr_cd, '-', store_id)) AS distinct_partition_combinations,
    ROUND(SUM(total_amt), 2) AS total_revenue
FROM \`${PROJECT_ID}.${DATASET_NAME}.${TABLE_NAME}\`
WHERE extract_date = DATE('${TEST_EXTRACT_DATE}')
"

bq query \
    --use_legacy_sql=false \
    --location="${REGION}" \
    "${STATS_QUERY}"

echo ""
echo "Synthetic dataset generation complete!"
