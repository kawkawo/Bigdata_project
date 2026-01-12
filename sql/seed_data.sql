-- ============================================
-- SEED DATA FOR PROCUREMENT SYSTEM
-- Sample data for testing the pipeline
-- ============================================

-- ============================================
-- INSERT SUPPLIERS
-- ============================================
INSERT INTO suppliers (supplier_id, supplier_name, contact_email, lead_time_days, minimum_order_value) VALUES
('SUP001', 'Fresh Foods Distribution', 'orders@freshfoods.com', 1, 500.00),
('SUP002', 'Dairy Products Ltd', 'sales@dairyproducts.com', 2, 300.00),
('SUP003', 'Global Beverages Inc', 'procurement@globalbev.com', 3, 1000.00),
('SUP004', 'Bakery Goods Wholesale', 'orders@bakerywholesale.com', 1, 200.00),
('SUP005', 'Frozen Foods Supply', 'sales@frozenfoods.com', 2, 800.00),
('SUP006', 'Snacks & Confectionery Co', 'orders@snacksco.com', 2, 400.00),
('SUP007', 'Household Essentials', 'sales@household.com', 3, 600.00),
('SUP008', 'Organic Produce Partners', 'orders@organicproduce.com', 1, 350.00);

-- ============================================
-- INSERT WAREHOUSES
-- ============================================
INSERT INTO warehouses (warehouse_id, warehouse_name, location, capacity) VALUES
('WH001', 'Central Distribution Center', 'Casablanca', 50000),
('WH002', 'North Regional Warehouse', 'Tangier', 30000),
('WH003', 'South Regional Warehouse', 'Marrakech', 25000);

-- ============================================
-- INSERT PRODUCTS (50 sample products)
-- ============================================
INSERT INTO products (sku, product_name, category, subcategory, supplier_id, unit_price, pack_size, case_size, is_perishable) VALUES
-- Dairy Products
('SKU001', 'Whole Milk 1L', 'Dairy', 'Milk', 'SUP002', 1.20, 1, 12, TRUE),
('SKU002', 'Cheddar Cheese 200g', 'Dairy', 'Cheese', 'SUP002', 3.50, 1, 10, TRUE),
('SKU003', 'Plain Yogurt 500g', 'Dairy', 'Yogurt', 'SUP002', 2.00, 4, 6, TRUE),
('SKU004', 'Butter 250g', 'Dairy', 'Butter', 'SUP002', 2.80, 1, 12, TRUE),

-- Beverages
('SKU005', 'Orange Juice 1L', 'Beverages', 'Juice', 'SUP003', 2.50, 1, 12, TRUE),
('SKU006', 'Cola 2L', 'Beverages', 'Soft Drinks', 'SUP003', 1.80, 1, 6, FALSE),
('SKU007', 'Mineral Water 1.5L', 'Beverages', 'Water', 'SUP003', 0.80, 6, 4, FALSE),
('SKU008', 'Green Tea Box 25 bags', 'Beverages', 'Tea', 'SUP003', 3.20, 1, 12, FALSE),

-- Bakery
('SKU009', 'White Bread 500g', 'Bakery', 'Bread', 'SUP004', 1.50, 1, 20, TRUE),
('SKU010', 'Croissants Pack of 4', 'Bakery', 'Pastries', 'SUP004', 2.80, 1, 12, TRUE),
('SKU011', 'Baguette', 'Bakery', 'Bread', 'SUP004', 0.90, 1, 24, TRUE),

-- Frozen Foods
('SKU012', 'Frozen Pizza 400g', 'Frozen', 'Ready Meals', 'SUP005', 4.50, 1, 10, TRUE),
('SKU013', 'Ice Cream 1L', 'Frozen', 'Desserts', 'SUP005', 5.20, 1, 8, TRUE),
('SKU014', 'Frozen Vegetables 500g', 'Frozen', 'Vegetables', 'SUP005', 2.30, 1, 12, TRUE),
('SKU015', 'Fish Fillets 300g', 'Frozen', 'Seafood', 'SUP005', 6.80, 1, 10, TRUE),

-- Fresh Produce
('SKU016', 'Tomatoes 1kg', 'Fresh Produce', 'Vegetables', 'SUP008', 2.00, 1, 15, TRUE),
('SKU017', 'Bananas 1kg', 'Fresh Produce', 'Fruits', 'SUP008', 1.50, 1, 20, TRUE),
('SKU018', 'Lettuce Head', 'Fresh Produce', 'Vegetables', 'SUP008', 1.20, 1, 12, TRUE),
('SKU019', 'Apples 1kg', 'Fresh Produce', 'Fruits', 'SUP008', 2.50, 1, 15, TRUE),

-- Snacks
('SKU020', 'Potato Chips 150g', 'Snacks', 'Chips', 'SUP006', 2.20, 1, 12, FALSE),
('SKU021', 'Chocolate Bar 100g', 'Snacks', 'Chocolate', 'SUP006', 1.80, 1, 24, FALSE),
('SKU022', 'Cookies Pack 200g', 'Snacks', 'Biscuits', 'SUP006', 2.50, 1, 12, FALSE),

-- Household
('SKU023', 'Dish Soap 500ml', 'Household', 'Cleaning', 'SUP007', 2.80, 1, 12, FALSE),
('SKU024', 'Toilet Paper 4-pack', 'Household', 'Paper Products', 'SUP007', 3.50, 1, 10, FALSE),
('SKU025', 'Laundry Detergent 1L', 'Household', 'Cleaning', 'SUP007', 5.20, 1, 8, FALSE);

-- ============================================
-- INSERT REPLENISHMENT RULES
-- ============================================
INSERT INTO replenishment_rules (sku, warehouse_id, safety_stock, minimum_order_quantity, reorder_point) VALUES
-- Central warehouse rules
('SKU001', 'WH001', 100, 50, 150),
('SKU002', 'WH001', 50, 30, 80),
('SKU003', 'WH001', 80, 40, 120),
('SKU004', 'WH001', 60, 30, 90),
('SKU005', 'WH001', 90, 50, 140),
('SKU006', 'WH001', 120, 60, 180),
('SKU007', 'WH001', 200, 100, 300),
('SKU008', 'WH001', 40, 20, 60),
('SKU009', 'WH001', 150, 80, 230),
('SKU010', 'WH001', 70, 40, 110),
('SKU011', 'WH001', 180, 100, 280),
('SKU012', 'WH001', 60, 30, 90),
('SKU013', 'WH001', 50, 25, 75),
('SKU014', 'WH001', 80, 40, 120),
('SKU015', 'WH001', 40, 20, 60),
('SKU016', 'WH001', 100, 50, 150),
('SKU017', 'WH001', 120, 60, 180),
('SKU018', 'WH001', 60, 30, 90),
('SKU019', 'WH001', 90, 50, 140),
('SKU020', 'WH001', 100, 50, 150),
('SKU021', 'WH001', 150, 80, 230),
('SKU022', 'WH001', 80, 40, 120),
('SKU023', 'WH001', 60, 30, 90),
('SKU024', 'WH001', 70, 35, 105),
('SKU025', 'WH001', 50, 25, 75);

-- Print confirmation
SELECT 'Database initialized successfully!' AS status;
SELECT COUNT(*) AS supplier_count FROM suppliers;
SELECT COUNT(*) AS warehouse_count FROM warehouses;
SELECT COUNT(*) AS product_count FROM products;
SELECT COUNT(*) AS rule_count FROM replenishment_rules;