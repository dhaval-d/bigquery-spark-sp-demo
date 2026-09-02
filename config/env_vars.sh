#!/usr/bin/env bash
# ==============================================================================
# Configuration Variables for BigQuery Spark Stored Procedure Demo
# ==============================================================================

# Project & Region settings
export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || echo 'dd-de-workloads')}"
export REGION="${REGION:-us-east4}"

# BigQuery & GCS Resource Names
export DATASET_NAME="${DATASET_NAME:-spark_export_demo}"
export BUCKET_NAME="${BUCKET_NAME:-${PROJECT_ID}-spark-export-${REGION}}"
export CONNECTION_ID="${CONNECTION_ID:-spark-sp-conn}"
export TABLE_NAME="${TABLE_NAME:-sales_transactions}"
export PROCEDURE_NAME="${PROCEDURE_NAME:-export_sales_to_parquet}"

# GCS Target Export Path
export GCS_EXPORT_PATH="gs://${BUCKET_NAME}/export_sales"

# Demo Benchmark Test Parameters
export TEST_EXTRACT_DATE="${TEST_EXTRACT_DATE:-2026-03-01}"
export DATA_SCALE_ROWS="${DATA_SCALE_ROWS:-5000000}" # 5M rows default (~1 GB)

# PySpark / Dataproc Serverless Properties
export SPARK_RUNTIME_VERSION="2.1"
export SPARK_COMPUTE_TIER="Standard"

echo "=== Environment Configuration Loaded ==="
echo "PROJECT_ID:      ${PROJECT_ID}"
echo "REGION:          ${REGION}"
echo "DATASET_NAME:    ${DATASET_NAME}"
echo "TABLE_NAME:      ${TABLE_NAME}"
echo "BUCKET_NAME:     ${BUCKET_NAME}"
echo "CONNECTION_ID:   ${CONNECTION_ID}"
echo "PROCEDURE_NAME:  ${PROCEDURE_NAME}"
echo "GCS_EXPORT_PATH: ${GCS_EXPORT_PATH}"
echo "DATA_SCALE_ROWS: ${DATA_SCALE_ROWS}"
echo "========================================="
