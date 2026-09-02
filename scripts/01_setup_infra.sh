#!/usr/bin/env bash
# ==============================================================================
# 01_setup_infra.sh
# Sets up GCP Infrastructure for BigQuery Spark Stored Procedure Demo:
# 1. Enables required GCP APIs
# 2. Creates GCS Bucket
# 3. Creates BigQuery Dataset
# 4. Creates BigQuery External Spark Connection
# 5. Grants required IAM roles to Connection Service Account
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${PROJECT_ROOT}/config/env_vars.sh"

echo ""
echo "=================================================================="
echo "Phase 1: Setting Up GCP Infrastructure & BigQuery Spark Connection"
echo "=================================================================="

# 1. Enable Required GCP APIs
echo ""
echo "[Step 1/5] Enabling GCP APIs..."
gcloud services enable \
    bigquery.googleapis.com \
    bigqueryconnection.googleapis.com \
    dataproc.googleapis.com \
    storage.googleapis.com \
    --project="${PROJECT_ID}"

echo "APIs enabled successfully."

# 2. Create Cloud Storage Destination Bucket
echo ""
echo "[Step 2/5] Creating Cloud Storage Bucket: gs://${BUCKET_NAME} in ${REGION}..."
if gcloud storage buckets describe "gs://${BUCKET_NAME}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    echo "Bucket gs://${BUCKET_NAME} already exists."
else
    gcloud storage buckets create "gs://${BUCKET_NAME}" \
        --project="${PROJECT_ID}" \
        --location="${REGION}" \
        --uniform-bucket-level-access
    echo "Bucket gs://${BUCKET_NAME} created."
fi

# 3. Create BigQuery Dataset
echo ""
echo "[Step 3/5] Creating BigQuery Dataset: ${DATASET_NAME} in ${REGION}..."
if bq show --dataset "${PROJECT_ID}:${DATASET_NAME}" >/dev/null 2>&1; then
    echo "Dataset ${PROJECT_ID}:${DATASET_NAME} already exists."
else
    bq --location="${REGION}" mk --dataset \
        --description="BigQuery Spark SP Demo Dataset" \
        "${PROJECT_ID}:${DATASET_NAME}"
    echo "Dataset ${PROJECT_ID}:${DATASET_NAME} created."
fi

# 4. Create BigQuery External Spark Connection
echo ""
echo "[Step 4/5] Creating BigQuery External Spark Connection: ${CONNECTION_ID}..."
if bq show --connection "${PROJECT_ID}.${REGION}.${CONNECTION_ID}" >/dev/null 2>&1; then
    echo "Spark Connection ${CONNECTION_ID} already exists."
else
    bq mk --connection \
        --location="${REGION}" \
        --project_id="${PROJECT_ID}" \
        --connection_type=SPARK \
        "${CONNECTION_ID}"
    echo "Spark Connection ${CONNECTION_ID} created."
fi

# 5. Extract Connection Service Account and Grant IAM Permissions
echo ""
echo "[Step 5/5] Configuring IAM Permissions for Spark Connection..."
CONN_JSON=$(bq show --format=json --connection "${PROJECT_ID}.${REGION}.${CONNECTION_ID}")

# Extract Service Account ID from connection JSON
CONN_SA=$(echo "${CONN_JSON}" | python3 -c 'import sys, json; data=json.load(sys.stdin); print(data.get("spark", {}).get("serviceAccountId", "") or data.get("serviceAccountId", ""))')

if [ -z "${CONN_SA}" ]; then
    echo "Error: Could not retrieve Service Account for connection ${CONNECTION_ID}"
    echo "Connection Details: ${CONN_JSON}"
    exit 1
fi

echo "Connection Service Account: ${CONN_SA}"

# Grant Storage Object Admin on bucket
echo "Granting roles/storage.objectAdmin on gs://${BUCKET_NAME} to ${CONN_SA}..."
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
    --member="serviceAccount:${CONN_SA}" \
    --role="roles/storage.objectAdmin"

# Grant BigQuery Admin / Data Editor & Session User on project
echo "Granting roles/bigquery.admin to ${CONN_SA}..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${CONN_SA}" \
    --role="roles/bigquery.admin" \
    --condition=None >/dev/null

# Grant Dataproc Editor & Worker roles
echo "Granting roles/dataproc.editor and roles/dataproc.worker to ${CONN_SA}..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${CONN_SA}" \
    --role="roles/dataproc.editor" \
    --condition=None >/dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${CONN_SA}" \
    --role="roles/dataproc.worker" \
    --condition=None >/dev/null

echo ""
echo "=================================================================="
echo "Infrastructure Setup Completed Successfully!"
echo "Spark Connection: ${PROJECT_ID}.${REGION}.${CONNECTION_ID}"
echo "Service Account:  ${CONN_SA}"
echo "GCS Destination:  gs://${BUCKET_NAME}"
echo "=================================================================="
