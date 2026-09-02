-- ==============================================================================
-- 03_create_spark_sp.sql
-- Creates the BigQuery Serverless Spark Stored Procedure (PySpark)
--
-- Features:
-- - Serverless execution powered by Dataproc Serverless engine
-- - Direct streaming via BigQuery Storage Read API with filter pushdown
-- - Dynamic Hive-style multi-level partitioning (terr_cd, store_id, extract_date)
-- - Repartitioning optimization to eliminate small-file fragmentation
-- - High-throughput distributed write directly to Cloud Storage (GCS)
-- ==============================================================================

CREATE OR REPLACE PROCEDURE `@PROJECT_ID@.@DATASET_NAME@.@PROCEDURE_NAME@`(
    p_extract_date DATE,
    p_gcs_output_path STRING
)
WITH CONNECTION `@PROJECT_ID@.@REGION@.@CONNECTION_ID@`
OPTIONS (
    engine='SPARK',
    runtime_version='@SPARK_RUNTIME_VERSION@'
)
LANGUAGE PYTHON AS R'''
import os
import sys
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    print("=== Starting BigQuery Spark Stored Procedure Export ===")
    
    # 1. Retrieve input parameters using BigQuery Spark environment variables
    # BigQuery passes parameters as JSON-encoded values in BIGQUERY_PROC_PARAM.<NAME>
    raw_date = os.environ.get("BIGQUERY_PROC_PARAM.p_extract_date")
    extract_date_str = str(json.loads(raw_date)) if raw_date else "@TEST_EXTRACT_DATE@"
    
    raw_path = os.environ.get("BIGQUERY_PROC_PARAM.p_gcs_output_path")
    gcs_output_path = str(json.loads(raw_path)) if raw_path else "gs://@BUCKET_NAME@/export_sales"
    
    print(f"Target Extract Date: {extract_date_str}")
    print(f"Target GCS Output Path: {gcs_output_path}")
    
    start_time = time.time()
    
    # 2. Initialize Spark Session with performance optimizations
    spark = SparkSession.builder \
        .appName("BigQuerySparkPartitionedExport") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # 3. Source table reference
    project_id = "@PROJECT_ID@"
    dataset_name = "@DATASET_NAME@"
    table_name = "@TABLE_NAME@"
    source_table = f"{project_id}.{dataset_name}.{table_name}"

    print(f"Reading from BigQuery source: {source_table} for date: {extract_date_str}")
    
    # 4. Read from BigQuery via Storage Read API with partition filter pushdown
    df = spark.read.format("bigquery") \
        .option("table", source_table) \
        .option("filter", f"extract_date = '{extract_date_str}'") \
        .load()

    # 5. Repartition by the partition keys to ensure 1 optimal file per partition
    # and avoid small-file fragmentation across distributed workers
    print("Repartitioning DataFrame by (terr_cd, store_id, extract_date)...")
    df_repartitioned = df.repartition("terr_cd", "store_id", "extract_date")

    # 6. Write out to GCS with Hive-style directory partitioning
    print(f"Writing Parquet files to {gcs_output_path}...")
    df_repartitioned.write \
        .partitionBy("terr_cd", "store_id", "extract_date") \
        .mode("overwrite") \
        .parquet(gcs_output_path)

    elapsed_time = time.time() - start_time
    print(f"=== Export Completed Successfully in {elapsed_time:.2f} seconds ===")

if __name__ == '__main__':
    main()
''';
