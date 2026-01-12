#!/usr/bin/env python3
"""
FULL Procurement Pipeline with Trino Integration
Uses Trino to query historical HDFS data via SQL
"""

import os
import json
import argparse
from datetime import datetime
from collections import defaultdict
import psycopg2
from psycopg2.extras import RealDictCursor
import trino
import requests

# Configuration
HDFS_NAMENODE_URL = os.getenv('HDFS_URL', 'http://namenode:9870')
LOCAL_OUTPUT_DIR = '/data/output/supplier_orders'
LOCAL_LOGS_DIR = '/data/logs'
HDFS_OUTPUT_DIR = '/data/output/supplier_orders'


class WebHDFSHelper:
    """Helper for HDFS file operations"""
    
    def __init__(self, namenode_url):
        self.namenode_url = namenode_url.rstrip('/')
        self.webhdfs_url = f"{self.namenode_url}/webhdfs/v1"
    
    def create_file(self, path, data):
        """Write file to HDFS"""
        url = f"{self.webhdfs_url}{path}?op=CREATE&overwrite=true"
        try:
            response = requests.put(url, allow_redirects=False, timeout=60)
            if response.status_code == 307:
                redirect_url = response.headers['Location']
                upload_response = requests.put(redirect_url, data=data, timeout=60)
                upload_response.raise_for_status()
                return True
            return False
        except Exception as e:
            print(f"  ✗ Error writing {path}: {e}")
            return False
    
    def mkdir(self, path):
        """Create directory in HDFS"""
        url = f"{self.webhdfs_url}{path}?op=MKDIRS"
        try:
            response = requests.put(url, timeout=30)
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"  ✗ Error creating directory {path}: {e}")
            return False


class ProcurementPipeline:
    def __init__(self, date_str):
        self.date_str = date_str
        self.exceptions = []
        self.db_conn = None
        self.trino_conn = None
        self.trino_cursor = None
        self.hdfs = WebHDFSHelper(HDFS_NAMENODE_URL)
        
    def connect_database(self):
        """Connect to PostgreSQL"""
        try:
            self.db_conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'postgres'),
                port=os.getenv('POSTGRES_PORT', '5432'),
                database=os.getenv('POSTGRES_DB', 'procurement'),
                user=os.getenv('POSTGRES_USER', 'admin'),
                password=os.getenv('POSTGRES_PASSWORD', 'admin123')
            )
            print("✓ Connected to PostgreSQL")
            return True
        except Exception as e:
            print(f"✗ Database connection failed: {e}")
            return False
    
    def connect_trino(self):
        """Connect to Trino for querying HDFS data"""
        try:
            self.trino_conn = trino.dbapi.connect(
                host=os.getenv('TRINO_HOST', 'trino'),
                port=int(os.getenv('TRINO_PORT', '8080')),
                user='trino',  # Use 'trino' user to match table ownership
                catalog='hive',
                schema='warehouse'
            )
            self.trino_cursor = self.trino_conn.cursor()
            
            # Create schema if not exists
            self.trino_cursor.execute("CREATE SCHEMA IF NOT EXISTS warehouse")
            
            # Drop and recreate tables for this date
            self.trino_cursor.execute("DROP TABLE IF EXISTS orders_data")
            self.trino_cursor.execute(f"""
                CREATE TABLE orders_data (
                    order_id VARCHAR,
                    pos_id VARCHAR,
                    sku VARCHAR,
                    quantity INTEGER,
                    order_date VARCHAR,
                    order_time VARCHAR,
                    customer_id VARCHAR
                ) WITH (
                    format = 'JSON',
                    external_location = 'hdfs://namenode:9000/data/raw/orders/{self.date_str}'
                )
            """)
            
            self.trino_cursor.execute("DROP TABLE IF EXISTS stock_data")
            self.trino_cursor.execute(f"""
                CREATE TABLE stock_data (
                    warehouse_id VARCHAR,
                    sku VARCHAR,
                    available_stock VARCHAR,
                    reserved_stock VARCHAR,
                    safety_stock VARCHAR,
                    snapshot_date VARCHAR,
                    snapshot_time VARCHAR
                ) WITH (
                    format = 'CSV',
                    external_location = 'hdfs://namenode:9000/data/raw/stock/{self.date_str}',
                    skip_header_line_count = 1
                )
            """)
            
            print("✓ Connected to Trino and created tables")
            return True
        except Exception as e:
            print(f"✗ Trino connection failed: {e}")
            print("  Make sure Trino tables are created (run setup_trino_tables.sql)")
            return False
    
    def load_master_data(self):
        """Load products, suppliers, and replenishment rules from PostgreSQL"""
        print("\n📚 Loading master data from PostgreSQL...")
        
        cursor = self.db_conn.cursor(cursor_factory=RealDictCursor)
        
        # Load products with supplier mapping
        cursor.execute("""
            SELECT p.sku, p.product_name, p.supplier_id, p.pack_size, p.case_size,
                   s.supplier_name, s.lead_time_days
            FROM products p
            JOIN suppliers s ON p.supplier_id = s.supplier_id
            WHERE p.active = TRUE AND s.active = TRUE
        """)
        products = {row['sku']: dict(row) for row in cursor.fetchall()}
        print(f"  ✓ Loaded {len(products)} products")
        
        # Load replenishment rules
        cursor.execute("""
            SELECT sku, warehouse_id, safety_stock, minimum_order_quantity
            FROM replenishment_rules
            WHERE active = TRUE
        """)
        rules = {}
        for row in cursor.fetchall():
            key = (row['sku'], row['warehouse_id'])
            rules[key] = dict(row)
        print(f"  ✓ Loaded {len(rules)} replenishment rules")
        
        cursor.close()
        return products, rules
    
    def get_historical_orders_via_trino(self):
        """
        Query ALL orders from HDFS using Trino SQL
        Works with newline-delimited JSON format
        """
        print(f"\n📦 Querying orders via Trino (up to {self.date_str})...")
        
        # SQL query to aggregate ALL orders up to target date
        query = f"""
        SELECT 
            sku,
            SUM(quantity) as total_quantity,
            COUNT(*) as order_count
        FROM orders_data
        WHERE order_date = '{self.date_str}' 
        GROUP BY sku
        ORDER BY total_quantity DESC
        """
        
        try:
            self.trino_cursor.execute(query)
            results = self.trino_cursor.fetchall()
            
            # Convert to dictionary
            historical_orders = {}
            total_demand = 0
            
            for row in results:
                sku = row[0]
                quantity = row[1]
                historical_orders[sku] = quantity
                total_demand += quantity
            
            print(f"  ✓ Total demand: {total_demand} units")
            print(f"  ✓ SKUs with demand: {len(historical_orders)}")
            
            if len(historical_orders) == 0:
                self.exceptions.append({
                    'type': 'no_data',
                    'message': 'No orders found in Trino tables'
                })
            
            return historical_orders
            
        except Exception as e:
            print(f"  ✗ Trino query failed: {e}")
            self.exceptions.append({
                'type': 'trino_error',
                'message': str(e)
            })
            return {}
    
    def get_latest_stock_via_trino(self):
        """
        Query LATEST stock levels from HDFS using Trino SQL
        Gets the most recent snapshot for each SKU
        """
        print(f"\n📊 Querying latest stock via Trino...")
        
        # Find the latest snapshot date
        date_query = f"""
        SELECT MAX(snapshot_date) as latest_date
        FROM stock_data
        WHERE snapshot_date <= '{self.date_str}'
        """
        
        try:
            self.trino_cursor.execute(date_query)
            result = self.trino_cursor.fetchone()
            latest_date = result[0] if result else None
            
            if not latest_date:
                print(f"  ✗ No stock data found before {self.date_str}")
                self.exceptions.append({
                    'type': 'no_stock',
                    'message': 'No stock snapshots available'
                })
                return {}
            
            print(f"  → Using stock snapshot from: {latest_date}")
            
            # Query stock for that date (cast VARCHAR to INTEGER)
            stock_query = f"""
            SELECT 
                sku,
                SUM(CAST(available_stock AS INTEGER)) as total_available,
                SUM(CAST(reserved_stock AS INTEGER)) as total_reserved,
                MAX(CAST(safety_stock AS INTEGER)) as max_safety_stock
            FROM stock_data
            WHERE snapshot_date = '{latest_date}'
            GROUP BY sku
            """
            
            self.trino_cursor.execute(stock_query)
            results = self.trino_cursor.fetchall()
            
            # Convert to dictionary
            stock_data = {}
            for row in results:
                sku = row[0]
                stock_data[sku] = {
                    'available': row[1],
                    'reserved': row[2],
                    'safety_stock': row[3]
                }
            
            print(f"  ✓ Loaded stock for {len(stock_data)} SKUs")
            return stock_data
            
        except Exception as e:
            print(f"  ✗ Trino query failed: {e}")
            self.exceptions.append({
                'type': 'trino_error',
                'message': str(e)
            })
            return {}
    
    def calculate_net_demand(self, historical_orders, current_stock, products, rules):
        """
        Calculate net demand: cumulative orders + safety stock - available stock
        Formula: net_demand = max(0, total_orders + safety_stock - (available - reserved))
        """
        print(f"\n🧮 Calculating net demand...")
        
        net_demand = {}
        calculation_details = []
        
        # Get all SKUs
        all_skus = set(historical_orders.keys()) | set(current_stock.keys())
        
        for sku in all_skus:
            # Check if SKU exists in master data
            if sku not in products:
                self.exceptions.append({
                    'type': 'missing_product',
                    'sku': sku,
                    'message': f'SKU {sku} not in product catalog'
                })
                continue
            
            # Get values
            total_orders = historical_orders.get(sku, 0)
            stock = current_stock.get(sku, {
                'available': 0, # Exception logged
                'reserved': 0,
                'safety_stock': 50
            })
            
            # Calculate net demand
            available_stock = stock['available'] - stock['reserved']
            required = total_orders + stock['safety_stock']
            net = max(0, required - available_stock)
            
            # Store calculation details for logging
            calculation_details.append({
                'sku': sku,
                'total_orders': total_orders,
                'available_stock': available_stock,
                'safety_stock': stock['safety_stock'],
                'required': required,
                'net_demand': net
            })
            
            # Check for abnormal demand
            if net > stock['safety_stock'] * 5:
                self.exceptions.append({
                    'type': 'demand_spike',
                    'sku': sku,
                    'net_demand': net,
                    'safety_stock': stock['safety_stock']
                })
            
            if net > 0:
                product = products[sku]
                
                # Apply case size rounding
                case_size = product['case_size']
                rounded_quantity = ((net + case_size - 1) // case_size) * case_size
                
                # Get MOQ
                rule_key = (sku, 'WH001')
                moq = rules.get(rule_key, {}).get('minimum_order_quantity', 1)
                
                # Apply MOQ
                final_quantity = max(rounded_quantity, moq)
                
                net_demand[sku] = {
                    'sku': sku,
                    'product_name': product['product_name'],
                    'supplier_id': product['supplier_id'],
                    'historical_orders': total_orders,
                    'current_stock': available_stock,
                    'safety_stock': stock['safety_stock'],
                    'raw_demand': net,
                    'rounded_demand': rounded_quantity,
                    'final_quantity': final_quantity,
                    'case_size': case_size,
                    'moq': moq
                }
        
        print(f"  ✓ Net demand calculated for {len(net_demand)} SKUs")
        
        # Show top 5 calculations
        if calculation_details:
            print(f"\n  Top 5 calculations:")
            for detail in sorted(calculation_details, key=lambda x: x['net_demand'], reverse=True)[:5]:
                print(f"    {detail['sku']}: orders={detail['total_orders']}, "
                      f"stock={detail['available_stock']}, "
                      f"safety={detail['safety_stock']} → net_demand={detail['net_demand']}")
        
        return net_demand
    
    def generate_supplier_orders(self, net_demand):
        """Generate supplier order files"""
        print(f"\n📄 Generating supplier orders...")
        
        if not net_demand:
            print("  ℹ No replenishment needed")
            return 0
        
        # Group by supplier
        supplier_orders = defaultdict(list)
        for sku, demand_info in net_demand.items():
            supplier_id = demand_info['supplier_id']
            supplier_orders[supplier_id].append(demand_info)
        
        # Create directories
        local_output_dir = f"{LOCAL_OUTPUT_DIR}/{self.date_str}"
        os.makedirs(local_output_dir, exist_ok=True)
        
        hdfs_output_dir = f"{HDFS_OUTPUT_DIR}/{self.date_str}"
        self.hdfs.mkdir(hdfs_output_dir)
        
        # Generate files
        for supplier_id, items in supplier_orders.items():
            order_document = {
                'supplier_id': supplier_id,
                'order_date': self.date_str,
                'generated_at': datetime.now().isoformat(),
                'total_items': len(items),
                'total_quantity': sum(item['final_quantity'] for item in items),
                'items': items
            }
            
            # Save locally
            local_file = f"{local_output_dir}/{supplier_id}_order.json"
            with open(local_file, 'w') as f:
                json.dump(order_document, f, indent=2)
            
            # Upload to HDFS
            hdfs_file = f"{hdfs_output_dir}/{supplier_id}_order.json"
            json_data = json.dumps(order_document, indent=2)
            
            if self.hdfs.create_file(hdfs_file, json_data):
                print(f"  ✓ {supplier_id}: {len(items)} SKUs, {order_document['total_quantity']} units")
            else:
                print(f"  ⚠ {supplier_id}: Local only (HDFS upload failed)")
        
        return len(supplier_orders)
    
    def save_exceptions_log(self):
        """Save exception report"""
        if not self.exceptions:
            print("\n✅ No exceptions")
            return
        
        log_dir = f"{LOCAL_LOGS_DIR}/exceptions"
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = f"{log_dir}/{self.date_str}_exceptions.json"
        with open(log_file, 'w') as f:
            json.dump({
                'date': self.date_str,
                'exception_count': len(self.exceptions),
                'exceptions': self.exceptions
            }, f, indent=2)
        
        print(f"\n⚠️  {len(self.exceptions)} exceptions → {log_file}")
    
    def run(self):
        """Execute pipeline with Trino"""
        print("="*70)
        print(f"PROCUREMENT PIPELINE (TRINO MODE) - {self.date_str}")
        print("="*70)
        
        # Connect to databases
        if not self.connect_database():
            return False
        
        if not self.connect_trino():
            print("\n⚠️  Cannot proceed without Trino connection")
            return False
        
        # Load master data
        products, rules = self.load_master_data()
        
        # Query historical data via Trino
        historical_orders = self.get_historical_orders_via_trino()
        current_stock = self.get_latest_stock_via_trino()
        
        # Calculate and generate orders
        net_demand = self.calculate_net_demand(historical_orders, current_stock, products, rules)
        supplier_count = self.generate_supplier_orders(net_demand)
        
        # Log exceptions
        self.save_exceptions_log()
        
        # Cleanup
        if self.trino_cursor:
            self.trino_cursor.close()
        if self.trino_conn:
            self.trino_conn.close()
        self.db_conn.close()
        
        print("\n" + "="*70)
        print("✅ PIPELINE COMPLETED!")
        print("="*70)

# Calculate totals
        total_orders = sum(historical_orders.values())
        total_units_needed = sum(d['final_quantity'] for d in net_demand.values())

        # AFFICAHGE
        print(f"\n📦 Procurement Results:")
        print(f"  - SKUs needing replenishment: {len(net_demand)}")
        print(f"  - Total units to order: {total_units_needed:,}")
        print(f"  - Supplier order files: {supplier_count}")
        print(f"\n⚠️  Quality:")
        print(f"  - Exceptions logged: {len(self.exceptions)}")

        return True


def main():
    parser = argparse.ArgumentParser(description='Run procurement pipeline with Trino')
    parser.add_argument('--date', type=str,
                       default=datetime.now().strftime('%Y-%m-%d'),
                       help='Date to process (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    pipeline = ProcurementPipeline(args.date)
    success = pipeline.run()
    
    if not success:
        exit(1)


if __name__ == '__main__':
    main()