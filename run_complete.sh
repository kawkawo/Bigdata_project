#!/bin/bash
set -e

DATE=${1:-$(date +%Y-%m-%d)}

echo "========================================"
echo "COMPLETE AUTOMATED PIPELINE"
echo "Date: $DATE"
echo "========================================"

echo ""
echo "Step 1/4: Generating realistic data..."
docker exec procurement-orchestrator python /app/generate_data_realistic.py $DATE

echo ""
echo "Step 2/4: Uploading orders to HDFS..."
docker exec hadoop-namenode hdfs dfs -put -f /data/raw/orders/$DATE /data/raw/orders/

echo ""
echo "Step 3/4: Uploading stock to HDFS..."
docker exec hadoop-namenode hdfs dfs -put -f /data/raw/stock/$DATE /data/raw/stock/

echo ""
echo "Step 4/4: Running pipeline..."
docker exec procurement-orchestrator python /app/pipeline.py --date $DATE

echo ""
echo "========================================"
echo "RESULTS"
echo "========================================"
echo ""
echo "Generated supplier orders:"
docker exec hadoop-namenode hdfs dfs -ls /data/output/supplier_orders/$DATE/ 2>/dev/null

echo ""
echo "Sample order content:"
docker exec hadoop-namenode hdfs dfs -cat /data/output/supplier_orders/$DATE/SUP*.json 2>/dev/null | head -40

echo ""
echo "Local files:"
ls -lh data/output/supplier_orders/$DATE/ 2>/dev/null || echo "No local files yet"

