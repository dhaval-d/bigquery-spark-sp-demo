#!/usr/bin/env python3
"""
export_job.py
Standalone PySpark script for partitioned BigQuery to GCS export.
Can be executed via Dataproc Serverless batches or invoked within BigQuery Spark Stored Procedures.
"""

import sys
import argparse
import time
from pyspark.sql import SparkSession

def parse_args():
    parser = argparse.ArgumentParser(description="Export BigQuery table to GCS in Hive-partitioned Parquet format.")
    parser.add_argument("--source_table", required=True, help="Full BigQuery table ID: project.dataset.table")
    parser.add_argument("--extract_date", required=True, help="Extract date (YYYY-MM-DD) for filtering")
    parser.add_argument("--gcs_output_path", required=True, help="Target GCS URI (gs://bucket/prefix)")
    return parser.parse_args()

def main():
    args = parse_args()
    
    print(f"=== Starting BigQuery Spark Partitioned Export ===")
    print(f"Source Table:    {args.source_table}")
    print(f"Extract Date:    {args.extract_date}")
    print(f"GCS Output Path: {args.gcs_output_path}")
    
    start_time = time.time()
    
    spark = SparkSession.builder \
        .appName("BigQuerySparkPartitionedExportJob") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Read from BigQuery Storage Read API with filter pushdown
    df = spark.read.format("bigquery") \
        .option("table", args.source_table) \
        .option("filter", f"extract_date = '{args.extract_date}'") \
        .load()

    # Repartition by partition columns to avoid small file fragmentation
    df_repartitioned = df.repartition("terr_cd", "store_id", "extract_date")

    # Write Hive-partitioned Parquet files to GCS
    df_repartitioned.write \
        .partitionBy("terr_cd", "store_id", "extract_date") \
        .mode("overwrite") \
        .parquet(args.gcs_output_path)

    elapsed = time.time() - start_time
    print(f"=== Export finished successfully in {elapsed:.2f} seconds ===")

if __name__ == '__main__':
    main()
