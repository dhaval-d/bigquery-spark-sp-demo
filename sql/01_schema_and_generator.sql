-- ==============================================================================
-- 01_schema_and_generator.sql
-- Generates synthetic high-cardinality sales data in BigQuery
-- Partitioned by extract_date, Clustered by terr_cd, store_id
-- ==============================================================================

-- Declare target scale if running as procedural script
DECLARE num_rows INT64 DEFAULT 5000000; -- Change as needed (e.g. 1M, 5M, 20M, 50M)

CREATE TABLE IF NOT EXISTS `@PROJECT_ID@.@DATASET_NAME@.@TABLE_NAME@` (
    terr_cd STRING NOT NULL,
    store_id STRING NOT NULL,
    extract_date DATE NOT NULL,
    order_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    product_sku STRING NOT NULL,
    category STRING NOT NULL,
    quantity INT64 NOT NULL,
    unit_price NUMERIC NOT NULL,
    discount_pct NUMERIC NOT NULL,
    tax_amt NUMERIC NOT NULL,
    total_amt NUMERIC NOT NULL,
    payment_method STRING NOT NULL,
    order_status STRING NOT NULL,
    created_at TIMESTAMP NOT NULL
)
PARTITION BY extract_date
CLUSTER BY terr_cd, store_id
OPTIONS(
    description = "Synthetic high-volume retail transactions for Spark Stored Procedure benchmark"
);

-- Truncate / Overwrite partition for reproducible benchmark runs
DELETE FROM `@PROJECT_ID@.@DATASET_NAME@.@TABLE_NAME@` 
WHERE extract_date = DATE('@TEST_EXTRACT_DATE@');

-- Generate high-volume synthetic records
INSERT INTO `@PROJECT_ID@.@DATASET_NAME@.@TABLE_NAME@`
SELECT
    -- Territory: 20 distinct territories
    CONCAT('TERR_', LPAD(CAST(1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_terr'))), 20) AS STRING), 2, '0')) AS terr_cd,
    
    -- Store: 250 distinct stores per territory
    CONCAT('STORE_', LPAD(CAST(1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_store'))), 250) AS STRING), 4, '0')) AS store_id,
    
    -- Extract Date: Target benchmark date
    DATE('@TEST_EXTRACT_DATE@') AS extract_date,
    
    -- Order ID: Unique UUID-like identifier
    GENERATE_UUID() AS order_id,
    
    -- Customer ID: 500,000 distinct customer base
    CONCAT('CUST_', LPAD(CAST(1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_cust'))), 500000) AS STRING), 8, '0')) AS customer_id,
    
    -- Product SKU: 5,000 distinct SKUs
    CONCAT('SKU_', LPAD(CAST(1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_sku'))), 5000) AS STRING), 6, '0')) AS product_sku,
    
    -- Category
    ['Electronics', 'Apparel', 'Home & Kitchen', 'Health & Beauty', 'Sports & Outdoors', 'Groceries', 'Toys & Games', 'Automotive'][
        OFFSET(MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_cat'))), 8))
    ] AS category,
    
    -- Quantity (1 to 10)
    1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_qty'))), 10) AS quantity,
    
    -- Unit Price ($5.00 to $500.00)
    ROUND(CAST(5.0 + (MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_price'))), 49500) / 100.0) AS NUMERIC), 2) AS unit_price,
    
    -- Discount percentage (0.00 to 0.30)
    ROUND(CAST(MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_disc'))), 30) / 100.0 AS NUMERIC), 2) AS discount_pct,
    
    -- Tax amount (approx 7%)
    ROUND(CAST((1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_qty'))), 10)) * 
         (5.0 + (MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_price'))), 49500) / 100.0)) * 0.07 AS NUMERIC), 2) AS tax_amt,
    
    -- Total amount = (qty * unit_price * (1 - disc)) + tax
    ROUND(CAST(
        ((1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_qty'))), 10)) * 
         (5.0 + (MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_price'))), 49500) / 100.0)) * 
         (1.0 - (MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_disc'))), 30) / 100.0))) +
        (((1 + MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_qty'))), 10)) * 
          (5.0 + (MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_price'))), 49500) / 100.0)) * 0.07))
    AS NUMERIC), 2) AS total_amt,
    
    -- Payment Method
    ['CREDIT_CARD', 'DEBIT_CARD', 'PAYPAL', 'APPLE_PAY', 'GOOGLE_PAY', 'GIFT_CARD'][
        OFFSET(MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_pay'))), 6))
    ] AS payment_method,
    
    -- Order Status
    ['COMPLETED', 'SHIPPED', 'DELIVERED', 'PROCESSING'][
        OFFSET(MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_status'))), 4))
    ] AS order_status,
    
    -- Created at timestamp throughout the extract date
    TIMESTAMP_ADD(
        TIMESTAMP(DATE('@TEST_EXTRACT_DATE@')), 
        INTERVAL MOD(ABS(FARM_FINGERPRINT(CONCAT(idx, '_time'))), 86400) SECOND
    ) AS created_at
FROM
    (
        SELECT (a * 1000 + b) AS idx
        FROM UNNEST(GENERATE_ARRAY(0, 4999)) AS a
        CROSS JOIN UNNEST(GENERATE_ARRAY(1, 1000)) AS b
        LIMIT @DATA_SCALE_ROWS@
    );
