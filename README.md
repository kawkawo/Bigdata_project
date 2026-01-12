# Procurement Pipeline Project

## 📋 Overview

Simplified data pipeline for a procurement system using distributed architecture with Hadoop, PostgreSQL, and Presto.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│  PROCUREMENT DATA PIPELINE                              │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  [POS Systems] → [HDFS Raw Data] → [Presto Query]     │
│                         ↓                               │
│                  [Aggregation]                          │
│                         ↓                               │
│  [PostgreSQL] → [Net Demand Calculation]               │
│   (Master Data)         ↓                               │
│                  [Supplier Orders]                      │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Components

- **Hadoop (Pseudo-Distributed)**: HDFS for distributed file storage
- **PostgreSQL**: Master data (products, suppliers, rules)
- **Presto**: Distributed SQL query engine
- **Python**: Data generation and pipeline orchestration

## 📁 Project Structure

```
procurement-pipeline/
├── docker-compose.yml          # Container orchestration
├── setup.sh                    # Automated setup script
├── README.md                   # This file
├── data/                       # Data directories (mounted)
│   ├── raw/
│   │   ├── orders/
│   │   └── stock/
│   ├── processed/
│   ├── output/
│   └── logs/
├── python/                     # Python scripts
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── generate_data.py       # Data generator
│   └── pipeline.py            # Main pipeline
├── sql/                        # Database schemas
│   ├── 01_schema.sql
│   └── 02_seed_data.sql
└── presto-config/             # Presto catalog configs
```

## 🚀 Quick Start

### Prerequisites

- Docker Engine (CLI) installed
- docker-compose installed
- At least 3GB RAM available

### Setup (5 minutes)

```bash
# 1. Clone or create project directory
mkdir procurement-pipeline
cd procurement-pipeline

# 2. Copy all files to project directory

# 3. Make setup script executable
chmod +x setup.sh

# 4. Run setup (starts all containers)
./setup.sh
```

### Verify Installation

```bash
# Check all containers are running
docker-compose ps

# Check HDFS
docker exec hadoop-pseudo hdfs dfs -ls /

# Check PostgreSQL
docker exec -it procurement-postgres psql -U admin -d procurement -c "\dt"
```

## 📊 Usage

### 1. Generate Test Data

```bash
# Generate data for today
docker exec procurement-orchestrator python generate_data.py

# Generate data for specific date
docker exec procurement-orchestrator python generate_data.py --date 2025-12-13

# Generate more orders per POS
docker exec procurement-orchestrator python generate_data.py --date 2025-12-13 --orders-per-pos 100
```

### 2. Run the Pipeline

```bash
# Run full pipeline for today
docker exec procurement-orchestrator python pipeline.py

# Run for specific date
docker exec procurement-orchestrator python pipeline.py --date 2025-12-13
```

### 3. Check Results

```bash
# View HDFS directory structure
docker exec hadoop-pseudo hdfs dfs -ls -R /data

# View generated supplier orders (local filesystem)
ls -lh data/output/supplier_orders/2025-12-13/

# Check aggregated data in HDFS
docker exec hadoop-pseudo hdfs dfs -cat /data/processed/aggregated_orders/2025-12-13.json
```

## 🔍 Useful Commands

### Docker Management

```bash
# View logs
docker-compose logs -f [service_name]

# Restart a service
docker-compose restart [service_name]

# Stop all containers
docker-compose down

# Remove all data (CAUTION!)
docker-compose down -v
```

### HDFS Commands

```bash
# List files
docker exec hadoop-pseudo hdfs dfs -ls /data/raw/orders

# Copy file from HDFS to local
docker exec hadoop-pseudo hdfs dfs -get /data/output/file.json /data/

# Copy file from local to HDFS
docker exec hadoop-pseudo hdfs dfs -put /data/file.json /data/raw/

# Check HDFS disk usage
docker exec hadoop-pseudo hdfs dfs -du -h /data
```

### PostgreSQL Commands

```bash
# Interactive SQL shell
docker exec -it procurement-postgres psql -U admin -d procurement

# Run SQL query
docker exec procurement-postgres psql -U admin -d procurement -c "SELECT COUNT(*) FROM products;"

# View all products
docker exec procurement-postgres psql -U admin -d procurement -c "SELECT * FROM products LIMIT 10;"
```

### Presto Queries

```bash
# Connect to Presto CLI
docker exec -it procurement-presto presto-cli

# Then run queries:
SHOW CATALOGS;
SHOW SCHEMAS FROM hive;
SELECT * FROM hive.default.orders LIMIT 10;
```

## 🌐 Web Interfaces

- **HDFS NameNode UI**: http://localhost:9870
- **YARN ResourceManager**: http://localhost:8088
- **Presto Web UI**: http://localhost:8080

## 🐛 Troubleshooting

### Containers won't start

```bash
# Check available memory
free -h

# View detailed logs
docker-compose logs [service_name]

# Restart Docker daemon
sudo systemctl restart docker
```

### Out of memory errors

```bash
# Stop containers
docker-compose down

# Start containers one by one
docker-compose up -d postgres
sleep 10
docker-compose up -d hadoop
sleep 30
docker-compose up -d presto
docker-compose up -d python-orchestrator
```

### HDFS permissions issues

```bash
# Fix HDFS permissions
docker exec hadoop-pseudo hdfs dfs -chmod -R 777 /data
```

### PostgreSQL connection issues

```bash
# Check PostgreSQL is ready
docker exec procurement-postgres pg_isready -U admin -d procurement

# Restart PostgreSQL
docker-compose restart postgres
```

## 📚 Project Requirements Mapping

| Requirement | Implementation |
|------------|----------------|
| **Order Data Collection** | JSON files in `/data/raw/orders/` |
| **Warehouse Inventory** | CSV files in `/data/raw/stock/` |
| **Master Data** | PostgreSQL tables |
| **Net Demand Calculation** | Python + Presto SQL queries |
| **Supplier Order Generation** | JSON files in `/data/output/` |
| **Batch Orchestration** | Python script with scheduling capability |
| **Distributed Storage** | Hadoop HDFS |
| **Query Engine** | Presto |

## 📖 Documentation

### Data Model

See `sql/01_schema.sql` for complete database schema.

**Main Tables:**
- `suppliers`: Vendor information
- `products`: SKU catalog with supplier mappings
- `warehouses`: Storage locations
- `replenishment_rules`: Per-SKU ordering rules

### Pipeline Flow

1. **Data Ingestion**: POS order files and stock snapshots → HDFS
2. **Aggregation**: Sum order quantities per SKU using Presto
3. **Net Demand Calculation**: Apply formula with stock levels
4. **Supplier Rules**: Apply MOQ, case size, safety stock
5. **Order Generation**: Create supplier-specific order files
6. **Exception Logging**: Track anomalies and missing data

## 🎓 Learning Objectives

- ✅ Distributed file storage (HDFS)
- ✅ Batch data processing
- ✅ SQL analytics on distributed data
- ✅ OLTP vs OLAP separation
- ✅ Data pipeline orchestration
- ✅ Master data management
- ✅ Business logic implementation
