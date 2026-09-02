# Requirements & Technical Specification Document
## End-to-End Demo: BigQuery Stored Procedures for Apache Spark

---

### Executive Summary

| Attribute | Specification |
|---|---|
| **Document Purpose** | Technical specification and implementation guide for testing BigQuery Spark Stored Procedures end-to-end. |
| **Business Objective** | Replace a 50–100 minute sequential BigQuery SQL loop with a serverless distributed export completing in **< 3 minutes**, matching AWS Redshift `UNLOAD ... PARTITION BY` performance. |
| **Target Architecture** | BigQuery Serverless Spark Stored Procedure streaming data via BigQuery Storage Read API to Cloud Storage (GCS) in Hive-partitioned Parquet format. |
| **Target Output** | `gs://<bucket>/export_path/terr_cd=<id>/store_id=<id>/extract_date=<date>/part-*.parquet` |

---

## 1. System Requirements & Prerequisites

### 1.1 Required Google Cloud APIs

| API Name | Service Identifier | Purpose |
|---|---|---|
| **BigQuery API** | `bigquery.googleapis.com` | Manages datasets, tables, and stored procedure lifecycle. |
| **BigQuery Connection API** | `bigqueryconnection.googleapis.com` | Manages external connections for Spark runtimes. |
| **Dataproc Serverless API** | `dataproc.googleapis.com` | Provides the underlying compute engine for Spark stored procedures. |
| **Cloud Storage API** | `storage.googleapis.com` | Destination object store for exported Parquet files. |

```bash
# Enable required GCP APIs
gcloud services enable \
    bigquery.googleapis.com \
    bigqueryconnection.googleapis.com \
    dataproc.googleapis.com \
    storage.googleapis.com
```

### 1.2 IAM Roles and Permissions

The BigQuery Spark external connection generates a dedicated service account (`service-<PROJECT_NUMBER>@gcp-sa-bigqueryspark.iam.gserviceaccount.com`). This service account must be granted:

| Role | Target | Rationale |
|---|---|---|
| `roles/storage.objectAdmin` | GCS Destination Bucket | To write partitioned Parquet files and clean old runs. |
| `roles/bigquery.admin` (or `dataViewer` + `readSessionUser`) | Project / Dataset | To read source tables via BigQuery Storage Read API. |
| `roles/dataproc.serverlessRuntimeUser` | Project | To submit serverless Spark batches on Dataproc. |
| `roles/dataproc.worker` | Project | Required for Dataproc Serverless execution nodes. |

---

## 2. Technical Architecture & Spark Optimization

### 2.1 BigQuery Storage Read API Direct Stream
The PySpark stored procedure connects to the source table using the native `bigquery` connector (`spark.read.format("bigquery")`). This leverages direct gRPC streams with column projection and predicate pushdown (`extract_date = 'YYYY-MM-DD'`), eliminating intermediary serialization.

### 2.2 Eliminating Small Files via Repartitioning
When writing dynamically partitioned files in Spark (`.partitionBy("terr_cd", "store_id", "extract_date")`), executors that hold fragments of multiple partitions create hundreds of tiny files. By explicitly calling:
```python
df_repartitioned = df.repartition("terr_cd", "store_id", "extract_date")
```
Spark routes all rows for a unique partition key combination to a single task, writing exactly 1 clean, optimal Parquet file per partition directory.

---

## 3. Verification & Benchmark Standards

1. **Hierarchy Check**: Validate path regex `terr_cd=*/store_id=*/extract_date=*/part-*.parquet`.
2. **Row Count Parity**: 100% match between BigQuery source partition and exported Parquet files.
3. **Execution SLA**: Total stored procedure execution time under 180 seconds for 5M+ records.
