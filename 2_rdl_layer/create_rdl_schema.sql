-- =====================================================
-- RECONCILED DATA LAYER (RDL) SCHEMA
-- Intermediate layer for data cleaning and integration
-- =====================================================

-- Create separate schema for RDL
CREATE SCHEMA IF NOT EXISTS rdl;

-- =====================================================
-- RDL TABLES - Mirror operational sources with additions
-- =====================================================

-- RDL Orders (with data quality flags)
DROP TABLE IF EXISTS rdl.orders CASCADE;
CREATE TABLE rdl.orders (
    -- Original columns
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    
    -- RDL additions for data quality
    rdl_load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rdl_source_system VARCHAR(50) DEFAULT 'olist',
    rdl_is_valid BOOLEAN DEFAULT TRUE,
    rdl_validation_errors TEXT,
    rdl_processing_status VARCHAR(20) DEFAULT 'new', -- new, processed, error
    
    -- Derived fields for reconciliation
    rdl_delivery_delay_days INTEGER,
    rdl_approval_delay_hours INTEGER
);

-- RDL Customers (with geocoding status)
DROP TABLE IF EXISTS rdl.customers CASCADE;
CREATE TABLE rdl.customers (
    -- Original columns
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INTEGER,
    customer_city VARCHAR(100),
    customer_state VARCHAR(2),
    
    -- RDL additions
    rdl_load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rdl_source_system VARCHAR(50) DEFAULT 'olist',
    rdl_is_valid BOOLEAN DEFAULT TRUE,
    rdl_validation_errors TEXT,
    
    -- Geocoding fields
    rdl_latitude DECIMAL(10,8),
    rdl_longitude DECIMAL(11,8),
    rdl_geocoding_status VARCHAR(20), -- matched, approximate, not_found
    rdl_geocoding_confidence DECIMAL(3,2),
    
    -- Data quality scores
    rdl_address_completeness_score DECIMAL(3,2),
    rdl_data_quality_score DECIMAL(3,2)
);

-- RDL Products (with enrichment fields)
DROP TABLE IF EXISTS rdl.products CASCADE;
CREATE TABLE rdl.products (
    -- Original columns
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    
    -- RDL additions
    rdl_load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rdl_source_system VARCHAR(50) DEFAULT 'olist',
    rdl_is_valid BOOLEAN DEFAULT TRUE,
    rdl_validation_errors TEXT,
    
    -- Enrichment fields
    rdl_category_english VARCHAR(100),
    rdl_category_hierarchy_1 VARCHAR(50), -- Main category
    rdl_category_hierarchy_2 VARCHAR(50), -- Subcategory
    rdl_weight_category VARCHAR(20),
    rdl_size_category VARCHAR(20),
    rdl_volume_cm3 INTEGER,
    
    -- Quality indicators
    rdl_has_complete_dimensions BOOLEAN,
    rdl_has_photos BOOLEAN,
    rdl_description_quality VARCHAR(20) -- short, medium, long
);

-- RDL Order Items (with calculations)
DROP TABLE IF EXISTS rdl.order_items CASCADE;
CREATE TABLE rdl.order_items (
    -- Composite primary key
    order_id VARCHAR(50),
    order_item_id INTEGER,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    
    -- RDL additions
    rdl_load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rdl_source_system VARCHAR(50) DEFAULT 'olist',
    rdl_is_valid BOOLEAN DEFAULT TRUE,
    
    -- Calculated fields
    rdl_total_item_value DECIMAL(10,2),
    rdl_freight_percentage DECIMAL(8,2),
    rdl_item_profit_estimate DECIMAL(10,2),
    
    PRIMARY KEY (order_id, order_item_id)
);

-- RDL Payments (with aggregations)
DROP TABLE IF EXISTS rdl.payments CASCADE;
CREATE TABLE rdl.payments (
    -- Original columns
    order_id VARCHAR(50),
    payment_sequential INTEGER,
    payment_type VARCHAR(50),
    payment_installments INTEGER,
    payment_value DECIMAL(10,2),
    
    -- RDL additions
    rdl_load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rdl_source_system VARCHAR(50) DEFAULT 'olist',
    rdl_is_valid BOOLEAN DEFAULT TRUE,
    
    -- Aggregated at order level
    rdl_total_order_value DECIMAL(10,2),
    rdl_payment_method_count INTEGER,
    rdl_is_multi_payment BOOLEAN,
    
    PRIMARY KEY (order_id, payment_sequential)
);

-- RDL Reviews (with sentiment analysis placeholder)
DROP TABLE IF EXISTS rdl.reviews CASCADE;
CREATE TABLE rdl.reviews (
    review_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    review_score INTEGER,
    review_comment_title VARCHAR(100),
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    
    -- RDL additions
    rdl_load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rdl_source_system VARCHAR(50) DEFAULT 'olist',
    rdl_is_valid BOOLEAN DEFAULT TRUE,
    
    -- Text analysis placeholders
    rdl_has_comment BOOLEAN,
    rdl_comment_length INTEGER,
    rdl_response_time_hours INTEGER,
    rdl_sentiment_score DECIMAL(3,2), -- -1 to 1
    rdl_review_category VARCHAR(50) -- delivery, quality, price, etc.
);

-- RDL Sellers (with business metrics)
DROP TABLE IF EXISTS rdl.sellers CASCADE;
CREATE TABLE rdl.sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix INTEGER,
    seller_city VARCHAR(100),
    seller_state VARCHAR(2),
    
    -- RDL additions
    rdl_load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rdl_source_system VARCHAR(50) DEFAULT 'olist',
    rdl_is_valid BOOLEAN DEFAULT TRUE,
    
    -- Geocoding
    rdl_latitude DECIMAL(10,8),
    rdl_longitude DECIMAL(11,8),
    rdl_geocoding_status VARCHAR(20),
    rdl_geocoding_confidence DECIMAL(3,2),  -- Aggiunta questa colonna
    
    -- Business metrics (to be calculated)
    rdl_total_orders INTEGER,
    rdl_total_revenue DECIMAL(12,2),
    rdl_avg_rating DECIMAL(3,2),
    rdl_active_status VARCHAR(20) -- active, inactive, new
);

-- RDL Geolocation (normalized and cleaned)
DROP TABLE IF EXISTS rdl.geolocation CASCADE;
CREATE TABLE rdl.geolocation (
    geolocation_zip_code_prefix INTEGER,
    geolocation_lat DECIMAL(10,8),
    geolocation_lng DECIMAL(11,8),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(2),
    
    -- RDL additions
    rdl_load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rdl_is_primary BOOLEAN, -- Primary location for this ZIP
    rdl_confidence_score DECIMAL(3,2),
    rdl_region VARCHAR(50),
    
    PRIMARY KEY (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng)
);

-- =====================================================
-- RDL METADATA TABLES
-- =====================================================

-- Data Quality Monitoring
DROP TABLE IF EXISTS rdl.data_quality_log CASCADE;
CREATE TABLE rdl.data_quality_log (
    log_id SERIAL PRIMARY KEY,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name VARCHAR(50),
    check_type VARCHAR(50), -- completeness, accuracy, consistency, timeliness
    total_records INTEGER,
    valid_records INTEGER,
    invalid_records INTEGER,
    quality_score DECIMAL(5,2),
    error_details JSONB
);

-- ETL Process Log
DROP TABLE IF EXISTS rdl.etl_process_log CASCADE;
CREATE TABLE rdl.etl_process_log (
    process_id SERIAL PRIMARY KEY,
    process_name VARCHAR(100),
    process_type VARCHAR(50), -- extract, transform, load
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    status VARCHAR(20), -- running, completed, failed
    records_processed INTEGER,
    records_rejected INTEGER,
    error_message TEXT,
    process_metadata JSONB
);

-- Data Lineage
DROP TABLE IF EXISTS rdl.data_lineage CASCADE;
CREATE TABLE rdl.data_lineage (
    lineage_id SERIAL PRIMARY KEY,
    source_system VARCHAR(50),
    source_table VARCHAR(100),
    source_column VARCHAR(100),
    rdl_table VARCHAR(100),
    rdl_column VARCHAR(100),
    transformation_rule TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================
CREATE INDEX idx_rdl_orders_customer ON rdl.orders(customer_id);
CREATE INDEX idx_rdl_orders_status ON rdl.orders(order_status);
CREATE INDEX idx_rdl_orders_timestamp ON rdl.orders(order_purchase_timestamp);

CREATE INDEX idx_rdl_customers_state ON rdl.customers(customer_state);
CREATE INDEX idx_rdl_customers_valid ON rdl.customers(rdl_is_valid);

CREATE INDEX idx_rdl_products_category ON rdl.products(product_category_name);
CREATE INDEX idx_rdl_products_valid ON rdl.products(rdl_is_valid);

CREATE INDEX idx_rdl_order_items_order ON rdl.order_items(order_id);
CREATE INDEX idx_rdl_order_items_product ON rdl.order_items(product_id);
CREATE INDEX idx_rdl_order_items_seller ON rdl.order_items(seller_id);

-- =====================================================
-- VIEWS FOR DATA QUALITY MONITORING
-- =====================================================

-- Overall data quality dashboard
CREATE OR REPLACE VIEW rdl.v_data_quality_summary AS
SELECT 
    'orders' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN rdl_is_valid THEN 1 ELSE 0 END) as valid_records,
    AVG(CASE WHEN rdl_is_valid THEN 1 ELSE 0 END) * 100 as quality_percentage
FROM rdl.orders
UNION ALL
SELECT 
    'customers' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN rdl_is_valid THEN 1 ELSE 0 END) as valid_records,
    AVG(CASE WHEN rdl_is_valid THEN 1 ELSE 0 END) * 100 as quality_percentage
FROM rdl.customers
UNION ALL
SELECT 
    'products' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN rdl_is_valid THEN 1 ELSE 0 END) as valid_records,
    AVG(CASE WHEN rdl_is_valid THEN 1 ELSE 0 END) * 100 as quality_percentage
FROM rdl.products;

-- Geocoding status view
CREATE OR REPLACE VIEW rdl.v_geocoding_status AS
SELECT 
    'customers' as entity_type,
    rdl_geocoding_status,
    COUNT(*) as count
FROM rdl.customers
WHERE rdl_geocoding_status IS NOT NULL
GROUP BY rdl_geocoding_status
UNION ALL
SELECT 
    'sellers' as entity_type,
    rdl_geocoding_status,
    COUNT(*) as count
FROM rdl.sellers
WHERE rdl_geocoding_status IS NOT NULL
GROUP BY rdl_geocoding_status;

-- =====================================================
-- COMMENTS FOR DOCUMENTATION
-- =====================================================
COMMENT ON SCHEMA rdl IS 'Reconciled Data Layer - Intermediate storage for data cleaning, validation, and enrichment';
COMMENT ON TABLE rdl.orders IS 'Reconciled orders data with validation flags and calculated fields';
COMMENT ON TABLE rdl.customers IS 'Reconciled customer data with geocoding and quality scores';
COMMENT ON TABLE rdl.products IS 'Reconciled product data with translations and categorizations';
COMMENT ON TABLE rdl.data_quality_log IS 'Tracks data quality metrics over time';
COMMENT ON TABLE rdl.etl_process_log IS 'Logs all ETL processes for monitoring and debugging';
COMMENT ON TABLE rdl.data_lineage IS 'Tracks data flow from source to RDL to DW';