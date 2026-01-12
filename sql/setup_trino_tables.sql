-- ============================================
-- TRINO TABLE DEFINITIONS
-- Run this via Trino CLI to create tables over HDFS data
-- ============================================

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS hive.warehouse
WITH (location = 'hdfs://namenode:9000/data/warehouse');

USE hive.warehouse;

-- ============================================
-- ORDERS DATA TABLE
-- Points to all historical order files in HDFS
-- ============================================
CREATE TABLE IF NOT EXISTS orders_data (
    order_id VARCHAR,
    pos_id VARCHAR,
    sku VARCHAR,
    quantity INTEGER,
    order_date VARCHAR,
    order_time VARCHAR,
    customer_id VARCHAR
)
WITH (
    format = 'JSON',
    external_location = 'hdfs://namenode:9000/data/raw/orders'
);

-- ============================================
-- STOCK DATA TABLE
-- Points to all historical stock files in HDFS
-- CSV requires all VARCHAR, cast in queries
-- ============================================
CREATE TABLE IF NOT EXISTS stock_data (
    warehouse_id VARCHAR,
    sku VARCHAR,
    available_stock VARCHAR,
    reserved_stock VARCHAR,
    safety_stock VARCHAR,
    snapshot_date VARCHAR,
    snapshot_time VARCHAR
)
WITH (
    format = 'CSV',
    external_location = 'hdfs://namenode:9000/data/raw/stock',
    skip_header_line_count = 1
);

-- ============================================
-- VERIFICATION QUERIES
-- ============================================

-- Check if tables exist
SHOW TABLES;

-- Count total orders across all dates
SELECT COUNT(*) as total_orders FROM orders_data;

-- Orders per date
SELECT order_date, COUNT(*) as order_count 
FROM orders_data 
GROUP BY order_date 
ORDER BY order_date;

-- Count total stock records
SELECT COUNT(*) as total_stock_records FROM stock_data;

-- Stock snapshots per date
SELECT snapshot_date, COUNT(*) as snapshot_count 
FROM stock_data 
GROUP BY snapshot_date 
ORDER BY snapshot_date;

-- Top 10 SKUs by demand
SELECT sku, SUM(quantity) as total_demand 
FROM orders_data 
GROUP BY sku 
ORDER BY total_demand DESC 
LIMIT 10;