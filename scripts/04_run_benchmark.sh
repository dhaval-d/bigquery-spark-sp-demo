#!/usr/bin/env bash
# ==============================================================================
# 04_run_benchmark.sh
# Executes BigQuery Spark Stored Procedure, captures execution duration,
# and compares performance against SLA (< 3 minutes).
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${PROJECT_ROOT}/config/env_vars.sh"

echo ""
echo "=================================================================="
echo "Phase 4: Running BigQuery Spark Stored Procedure Benchmark"
echo "=================================================================="
echo "Invoking:    ${PROJECT_ID}.${DATASET_NAME}.${PROCEDURE_NAME}"
echo "Date Filter: ${TEST_EXTRACT_DATE}"
echo "GCS Target:  ${GCS_EXPORT_PATH}"
echo "Target SLA:  < 3 minutes (180 seconds)"
echo "=================================================================="

# 1. Clean previous GCS export destination
echo ""
echo "Cleaning existing target destination: ${GCS_EXPORT_PATH}..."
gcloud storage rm --recursive "${GCS_EXPORT_PATH}" 2>/dev/null || true

# 2. Execute Stored Procedure
CALL_QUERY="CALL \`${PROJECT_ID}.${DATASET_NAME}.${PROCEDURE_NAME}\`(DATE('${TEST_EXTRACT_DATE}'), '${GCS_EXPORT_PATH}');"

echo ""
echo "Executing Spark Stored Procedure via BigQuery..."
echo "Query: ${CALL_QUERY}"
echo "Start Time: $(date)"

START_TIME=$(date +%s)

echo "${CALL_QUERY}" | bq query \
    --use_legacy_sql=false \
    --location="${REGION}"

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "=================================================================="
echo "Execution Completed!"
echo "End Time:       $(date)"
echo "Total Duration: ${DURATION} seconds ($((DURATION / 60))m $((DURATION % 60))s)"

if [ "${DURATION}" -le 180 ]; then
    echo "SLA Status:     PASSED (< 3 minutes SLA achieved!)"
else
    echo "SLA Status:     EXCEEDED 180s (${DURATION}s total)"
fi
echo "=================================================================="

echo ""
echo "Next Step: Run verification suite via ./scripts/05_verify_export.py"
