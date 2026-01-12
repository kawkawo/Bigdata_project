-- ============================================
-- PROCUREMENT SYSTEM DATABASE SCHEMA
-- ============================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS replenishment_rules CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS suppliers CASCADE;
DROP TABLE IF EXISTS warehouses CASCADE;

-- ============================================
-- SUPPLIERS TABLE
-- Contains information about product suppliers
-- ============================================
CREATE TABLE suppliers (
    supplier_id VARCHAR(50) PRIMARY KEY,
    supplier_name VARCHAR(200) NOT NULL,
    contact_email VARCHAR(200),
    contact_phone VARCHAR(50),
    lead_time_days INTEGER NOT NULL DEFAULT 2,
    minimum_order_value DECIMAL(10,2) DEFAULT 0,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- WAREHOUSES TABLE
-- Contains warehouse/depot information
-- ============================================
CREATE TABLE warehouses (
    warehouse_id VARCHAR(50) PRIMARY KEY,
    warehouse_name VARCHAR(200) NOT NULL,
    location VARCHAR(200),
    capacity INTEGER,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- PRODUCTS TABLE
-- Master data for all products (SKUs)
-- ============================================
CREATE TABLE products (
    sku VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(300) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    supplier_id VARCHAR(50) REFERENCES suppliers(supplier_id),
    unit_price DECIMAL(10,2) NOT NULL,
    pack_size INTEGER NOT NULL DEFAULT 1,
    case_size INTEGER NOT NULL DEFAULT 1,
    weight_kg DECIMAL(8,3),
    is_perishable BOOLEAN DEFAULT FALSE,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- REPLENISHMENT RULES TABLE
-- Rules for ordering products from suppliers
-- ============================================
CREATE TABLE replenishment_rules (
    rule_id SERIAL PRIMARY KEY,
    sku VARCHAR(50) REFERENCES products(sku),
    warehouse_id VARCHAR(50) REFERENCES warehouses(warehouse_id),
    safety_stock INTEGER NOT NULL DEFAULT 0,
    minimum_order_quantity INTEGER NOT NULL DEFAULT 1,
    maximum_order_quantity INTEGER,
    reorder_point INTEGER,
    ordering_calendar VARCHAR(50) DEFAULT 'daily',
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sku, warehouse_id)
);

-- ============================================
-- INDEXES FOR PERFORMANCE
-- ============================================
CREATE INDEX idx_products_supplier ON products(supplier_id);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_active ON products(active);
CREATE INDEX idx_replenishment_sku ON replenishment_rules(sku);
CREATE INDEX idx_replenishment_warehouse ON replenishment_rules(warehouse_id);

-- ============================================
-- COMMENTS
-- ============================================
COMMENT ON TABLE suppliers IS 'Vendor/supplier master data';
COMMENT ON TABLE warehouses IS 'Warehouse/depot locations';
COMMENT ON TABLE products IS 'Product master data (SKU catalog)';
COMMENT ON TABLE replenishment_rules IS 'Procurement rules per SKU per warehouse';

COMMENT ON COLUMN products.pack_size IS 'Number of units in one pack (e.g., 6-pack)';
COMMENT ON COLUMN products.case_size IS 'Number of packs in one case for supplier ordering';
COMMENT ON COLUMN replenishment_rules.safety_stock IS 'Minimum stock level to maintain';
COMMENT ON COLUMN replenishment_rules.minimum_order_quantity IS 'Minimum units to order from supplier (MOQ)';