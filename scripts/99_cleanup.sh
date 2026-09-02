#!/usr/bin/env bash
# ==============================================================================
# 99_cleanup.sh
# Teardown script for BigQuery Spark Stored Procedure Demo resources.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${PROJECT_ROOT}/config/env_vars.sh"

echo ""
echo "=================================================================="
echo "Phase 6: Cleaning Up Demo Resources"
echo "=================================================================="
echo "This will delete:"
echo " - BigQuery Dataset:     ${PROJECT_ID}:${DATASET_NAME}"
echo " - Spark Connection:    ${PROJECT_ID}.${REGION}.${CONNECTION_ID}"
echo " - GCS Bucket:          gs://${BUCKET_NAME}"
echo "=================================================================="

read -p "Are you sure you want to delete these resources? (y/N): " confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo "Cleanup cancelled."
    exit 0
fi

# 1. Delete BigQuery Dataset (includes tables and stored procedures)
echo "Deleting BigQuery dataset ${DATASET_NAME}..."
bq rm -r -f -d "${PROJECT_ID}:${DATASET_NAME}" 2>/dev/null || true

# 2. Delete BigQuery External Connection
echo "Deleting BigQuery Spark connection ${CONNECTION_ID}..."
bq rm --connection --location="${REGION}" -f "${PROJECT_ID}.${REGION}.${CONNECTION_ID}" 2>/dev/null || true

# 3. Delete GCS Bucket
echo "Deleting GCS Bucket gs://${BUCKET_NAME}..."
gcloud storage rm --recursive "gs://${BUCKET_NAME}" 2>/dev/null || true

echo ""
echo "Cleanup completed successfully!"
