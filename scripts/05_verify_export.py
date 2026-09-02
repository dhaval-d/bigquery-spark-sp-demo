#!/usr/bin/env python3
"""
05_verify_export.py
Automated Verification Suite for BigQuery Spark Stored Procedure Export.

Validates:
1. GCS Hive Partitioning hierarchy: terr_cd=*/store_id=*/extract_date=*/part-*.parquet
2. Partition directory count & Parquet file distribution
3. BigQuery vs. GCS Parquet row count matching
4. Aggregate checksum validation (Total Amount, Row Counts, Territories, Stores)
"""

import os
import sys
import subprocess
import json
import re

def run_shell(cmd):
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        print(f"Error running shell command: {cmd}\nStderr: {result.stderr}")
        return None
    return result.stdout.strip()

def run_bq_query(query, region="us-east4", as_json=True):
    args = ["bq", "query", "--use_legacy_sql=false", f"--location={region}"]
    if as_json:
        args.append("--format=json")
    result = subprocess.run(args, input=query, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        print(f"Error running BigQuery query:\nQuery: {query}\nStderr: {result.stderr}")
        return None
    return result.stdout.strip()

def main():
    project_id = os.environ.get("PROJECT_ID", "dd-de-workloads")
    region = os.environ.get("REGION", "us-east4")
    dataset_name = os.environ.get("DATASET_NAME", "spark_export_demo")
    table_name = os.environ.get("TABLE_NAME", "sales_transactions")
    bucket_name = os.environ.get("BUCKET_NAME", f"{project_id}-spark-export-{region}")
    export_path = os.environ.get("GCS_EXPORT_PATH", f"gs://{bucket_name}/export_sales")
    extract_date = os.environ.get("TEST_EXTRACT_DATE", "2026-03-01")

    print("\n" + "="*70)
    print("Phase 5: Automated Verification Suite")
    print("="*70)
    print(f"Project ID:      {project_id}")
    print(f"Dataset / Table: {dataset_name}.{table_name}")
    print(f"GCS Export URI:  {export_path}")
    print(f"Extract Date:    {extract_date}")
    print("="*70)

    # ---------------------------------------------------------
    # 1. Verify GCS Files & Hive Partition Hierarchy
    # ---------------------------------------------------------
    print("\n[Step 1/3] Inspecting GCS Hive Partition Structure...")
    list_cmd = f"gcloud storage ls --recursive '{export_path}/**'"
    raw_files = run_shell(list_cmd)
    
    if not raw_files:
        print(f"FAILED: No files found under {export_path}")
        sys.exit(1)

    all_paths = [line.strip() for line in raw_files.splitlines() if line.strip().endswith(".parquet")]
    total_parquet_files = len(all_paths)
    print(f"Found {total_parquet_files} Parquet files in GCS export.")

    # Validate Hive directory pattern
    hive_regex = re.compile(r"terr_cd=([^/]+)/store_id=([^/]+)/extract_date=([^/]+)/.*\.parquet$")
    valid_partitions = 0
    unique_territories = set()
    unique_stores = set()
    sample_partitions = []

    for path in all_paths:
        match = hive_regex.search(path)
        if match:
            valid_partitions += 1
            terr, store, dt = match.groups()
            unique_territories.add(terr)
            unique_stores.add(store)
            if len(sample_partitions) < 3:
                sample_partitions.append(f"terr_cd={terr}/store_id={store}/extract_date={dt}")

    print(f"Valid Hive Partition Files: {valid_partitions} / {total_parquet_files}")
    print(f"Discovered Territories:     {len(unique_territories)}")
    print(f"Discovered Stores:          {len(unique_stores)}")
    print(f"Sample Partition Paths:")
    for p in sample_partitions:
        print(f"  - {p}")

    if valid_partitions != total_parquet_files or valid_partitions == 0:
        print("FAILED: Parquet files do not match expected Hive partition pattern!")
        sys.exit(1)
    else:
        print("PASSED: Hive directory structure is valid.")

    # ---------------------------------------------------------
    # 2. Query BigQuery Source Metrics
    # ---------------------------------------------------------
    print("\n[Step 2/3] Fetching Source Table Metrics from BigQuery...")
    bq_source_query = f"""
    SELECT 
        COUNT(*) AS total_rows,
        COUNT(DISTINCT terr_cd) AS terr_count,
        COUNT(DISTINCT store_id) AS store_count,
        ROUND(SUM(total_amt), 2) AS sum_total_amt
    FROM `{project_id}.{dataset_name}.{table_name}`
    WHERE extract_date = DATE('{extract_date}')
    """
    
    bq_source_res = run_bq_query(bq_source_query, region=region, as_json=True)
    if not bq_source_res:
        print("FAILED to query BigQuery source table.")
        sys.exit(1)

    source_metrics = json.loads(bq_source_res)[0]
    source_rows = int(source_metrics["total_rows"])
    source_terr = int(source_metrics["terr_count"])
    source_stores = int(source_metrics["store_count"])
    source_amt = float(source_metrics["sum_total_amt"])

    print(f"Source Rows:        {source_rows:,}")
    print(f"Source Territories: {source_terr}")
    print(f"Source Stores:      {source_stores}")
    print(f"Source Total Sum:   ${source_amt:,.2f}")

    # ---------------------------------------------------------
    # 3. Verify Against Exported Parquet Data via External Table
    # ---------------------------------------------------------
    print("\n[Step 3/3] Validating Exported Parquet Data Integrity via BigQuery...")
    
    temp_ext_table = f"{dataset_name}.v_temp_export_verification"
    
    # Create external table over GCS Hive-partitioned parquet
    ext_table_query = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{temp_ext_table}`
    WITH PARTITION COLUMNS (
        terr_cd STRING,
        store_id STRING,
        extract_date DATE
    )
    OPTIONS (
        format = 'PARQUET',
        uris = ['{export_path}/*'],
        hive_partition_uri_prefix = '{export_path}'
    );
    """
    run_bq_query(ext_table_query, region=region, as_json=False)

    verify_query = f"""
    SELECT 
        COUNT(*) AS exported_rows,
        COUNT(DISTINCT terr_cd) AS exported_terr,
        COUNT(DISTINCT store_id) AS exported_stores,
        ROUND(SUM(total_amt), 2) AS exported_total_amt
    FROM `{project_id}.{temp_ext_table}`
    WHERE extract_date = DATE('{extract_date}')
    """

    verify_res = run_bq_query(verify_query, region=region, as_json=True)
    
    # Cleanup temporary external table
    run_shell(f'bq rm -f -t "{project_id}:{temp_ext_table}" >/dev/null 2>&1')

    if not verify_res:
        print("FAILED to query exported Parquet data.")
        sys.exit(1)

    exported_metrics = json.loads(verify_res)[0]
    exported_rows = int(exported_metrics["exported_rows"])
    exported_terr = int(exported_metrics["exported_terr"])
    exported_stores = int(exported_metrics["exported_stores"])
    exported_amt = float(exported_metrics["exported_total_amt"])

    print(f"Exported Rows:        {exported_rows:,}")
    print(f"Exported Territories: {exported_terr}")
    print(f"Exported Stores:      {exported_stores}")
    print(f"Exported Total Sum:   ${exported_amt:,.2f}")

    # Integrity Assertions
    print("\n" + "="*70)
    print("Integrity Verification Results:")
    print("="*70)
    
    row_diff = abs(source_rows - exported_rows)
    amt_diff = abs(source_amt - exported_amt)

    print(f"Row Count Match:     {'PASSED' if row_diff == 0 else 'FAILED (Diff: ' + str(row_diff) + ')'}")
    print(f"Territory Match:     {'PASSED' if source_terr == exported_terr else 'FAILED'}")
    print(f"Store Count Match:   {'PASSED' if source_stores == exported_stores else 'FAILED'}")
    print(f"Financial Sum Match: {'PASSED' if amt_diff < 0.01 else 'FAILED (Diff: ' + str(amt_diff) + ')'}")

    if row_diff == 0 and source_terr == exported_terr and source_stores == exported_stores and amt_diff < 0.01:
        print("\nALL VERIFICATION CHECKS PASSED SUCCESSFULLY!")
        print("="*70)
        sys.exit(0)
    else:
        print("\nVERIFICATION FAILED: Data mismatch detected.")
        print("="*70)
        sys.exit(1)

if __name__ == '__main__':
    main()
