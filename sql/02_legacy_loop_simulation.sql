-- ==============================================================================
-- 02_legacy_loop_simulation.sql
-- Baseline: Sequential BigQuery SQL loop simulating the legacy export approach.
--
-- Why this is slow:
-- 1. Query compilation & orchestration overhead: ~2-4s per iteration.
-- 2. With 20 territories x 250 stores = 5,000 combinations:
--    5,000 iterations * 3s = 15,000s (~4.1 hours).
-- 3. Rate limits: BigQuery query queue concurrency limits serialize execution.
-- ==============================================================================

DECLARE v_start_time TIMESTAMP;
DECLARE v_end_time TIMESTAMP;
DECLARE v_counter INT64 DEFAULT 0;

-- Safety cap: Default to 10 iterations for fast simulation comparison,
-- or increase to simulate full duration.
DECLARE max_iterations INT64 DEFAULT 10; 

SET v_start_time = CURRENT_TIMESTAMP();

FOR combo IN (
    SELECT DISTINCT terr_cd, store_id 
    FROM `@PROJECT_ID@.@DATASET_NAME@.@TABLE_NAME@`
    WHERE extract_date = DATE('@TEST_EXTRACT_DATE@')
    ORDER BY terr_cd, store_id
    LIMIT 10
)
DO
    SET v_counter = v_counter + 1;
    
    EXECUTE IMMEDIATE FORMAT("""
        EXPORT DATA OPTIONS(
            uri='gs://@BUCKET_NAME@/legacy_export/terr_cd=%s/store_id=%s/extract_date=@TEST_EXTRACT_DATE@/part-*.parquet',
            format='PARQUET',
            overwrite=true
        ) AS
        SELECT * EXCEPT(terr_cd, store_id, extract_date)
        FROM `@PROJECT_ID@.@DATASET_NAME@.@TABLE_NAME@`
        WHERE extract_date = DATE('@TEST_EXTRACT_DATE@') 
          AND terr_cd = '%s' 
          AND store_id = '%s'
    """, combo.terr_cd, combo.store_id, combo.terr_cd, combo.store_id);

END FOR;

SET v_end_time = CURRENT_TIMESTAMP();

SELECT 
    v_counter AS iterations_completed,
    TIMESTAMP_DIFF(v_end_time, v_start_time, SECOND) AS elapsed_seconds,
    ROUND(TIMESTAMP_DIFF(v_end_time, v_start_time, MILLISECOND) / CAST(v_counter AS FLOAT64) / 1000.0, 2) AS avg_seconds_per_partition,
    ROUND((TIMESTAMP_DIFF(v_end_time, v_start_time, MILLISECOND) / CAST(v_counter AS FLOAT64) / 1000.0) * 5000 / 60.0, 2) AS projected_minutes_for_5000_partitions;
