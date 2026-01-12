#!/usr/bin/env python3
"""REALISTIC Data Generator - Creates actual shortages"""
import random
from datetime import datetime
import json
import csv
import os

SKUS = [f'SKU{str(i).zfill(3)}' for i in range(1, 26)]
POS_SYSTEMS = ['POS001', 'POS002', 'POS003', 'POS004', 'POS005']
WAREHOUSES = ['WH001', 'WH002', 'WH003']

def generate_realistic_orders(date_str):
    """Generate orders with HIGH demand"""
    for pos_id in POS_SYSTEMS:
        orders = []
        num_orders = random.randint(100, 200)
        
        for _ in range(num_orders):
            sku = random.choice(SKUS)
            quantity = random.randint(1, 15)
            
            orders.append({
                'order_id': f'ORD{random.randint(10000, 99999)}',
                'pos_id': pos_id,
                'sku': sku,
                'quantity': quantity,
                'order_date': date_str,
                'order_time': f'{random.randint(8,22)}:{random.randint(0,59)}:00',
                'customer_id': f'CUST{random.randint(1000,9999)}'
            })
        
        output_dir = f'/data/raw/orders/{date_str}'
        os.makedirs(output_dir, exist_ok=True)
        with open(f'{output_dir}/{pos_id}_orders.json', 'w') as f:
           for order in orders:
               f.write(json.dumps(order) + '\n')
        print(f"Generated {len(orders)} orders for {pos_id}")

def generate_realistic_stock(date_str):
    """Generate stock with LOW availability"""
    for warehouse_id in WAREHOUSES:
        stock_data = []
        
        for sku in SKUS:
            available = random.randint(20, 100)
            reserved = random.randint(0, int(available * 0.2))
            safety_stock = random.randint(80, 150)
            
            stock_data.append({
                'warehouse_id': warehouse_id,
                'sku': sku,
                'available_stock': available,
                'reserved_stock': reserved,
                'safety_stock': safety_stock,
                'snapshot_date': date_str,
                'snapshot_time': '23:59:59'
            })
        
        output_dir = f'/data/raw/stock/{date_str}'
        os.makedirs(output_dir, exist_ok=True)
        with open(f'{output_dir}/{warehouse_id}_stock.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=stock_data[0].keys())
            writer.writeheader()
            writer.writerows(stock_data)
        print(f"Generated stock for {warehouse_id}")

if __name__ == '__main__':
    import sys
    date_str = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    print(f"Generating REALISTIC data for {date_str}...")
    generate_realistic_orders(date_str)
    generate_realistic_stock(date_str)
    print("Done!")
