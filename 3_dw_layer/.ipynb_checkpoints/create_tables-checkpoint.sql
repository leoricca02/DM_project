-- =====================================================
-- SCHEMA DATABASE
-- =====================================================

DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP TABLE IF EXISTS dim_seller CASCADE;
DROP TABLE IF EXISTS dim_payment CASCADE;
DROP TABLE IF EXISTS dim_geography CASCADE;

-- =====================================================
-- DIMENSIONE GEOGRAFIA
-- =====================================================
CREATE TABLE dim_geography (
    geography_key SERIAL PRIMARY KEY,
    zip_code_prefix VARCHAR(10) UNIQUE NOT NULL,
    city VARCHAR(100),
    state VARCHAR(2),
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    region VARCHAR(50),  -- Nord, Sud, etc.
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- DIMENSIONE CUSTOMER
-- =====================================================
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    customer_city VARCHAR(100),
    customer_state VARCHAR(2),
    customer_zip_code_prefix VARCHAR(10),
    -- Nuovi campi geografici
    customer_latitude DECIMAL(10,8),
    customer_longitude DECIMAL(11,8),
    geography_key INTEGER REFERENCES dim_geography(geography_key),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- DIMENSIONE PRODUCT 
-- =====================================================
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    weight_category VARCHAR(50), -- Light, Medium, Heavy, Very Heavy
    size_category VARCHAR(50),   -- Small, Medium, Large
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- DIMENSIONE SELLER 
-- =====================================================
CREATE TABLE dim_seller (
    seller_key SERIAL PRIMARY KEY,
    seller_id VARCHAR(50) UNIQUE NOT NULL,
    seller_city VARCHAR(100),
    seller_state VARCHAR(2),
    seller_zip_code_prefix VARCHAR(10),
    -- Nuovi campi geografici
    seller_latitude DECIMAL(10,8),
    seller_longitude DECIMAL(11,8),
    geography_key INTEGER REFERENCES dim_geography(geography_key),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- DIMENSIONE TIME
-- =====================================================
CREATE TABLE dim_time (
    time_key SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    day_of_month INTEGER,
    month_num INTEGER,
    month_name VARCHAR(15),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE,
    -- NUOVO: stagionalità Brasile
    season_brazil VARCHAR(20), -- Estate, Inverno, etc.
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- DIMENSIONE PAYMENT
-- =====================================================
CREATE TABLE dim_payment (
    payment_key SERIAL PRIMARY KEY,
    payment_type VARCHAR(50),
    payment_installments INTEGER,
    installment_category VARCHAR(50), -- Cash, Short Term, Medium Term, Long Term
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- FACT TABLE SALES 
-- =====================================================
CREATE TABLE fact_sales (
    sales_key SERIAL PRIMARY KEY,
    
    -- Foreign Keys
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    product_key INTEGER REFERENCES dim_product(product_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    seller_key INTEGER REFERENCES dim_seller(seller_key),
    payment_key INTEGER REFERENCES dim_payment(payment_key),
    
    -- Business Keys
    order_id VARCHAR(50) NOT NULL,
    order_item_id INTEGER NOT NULL,
    
    -- Measures originali
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    quantity INTEGER DEFAULT 1,
    payment_value DECIMAL(10,2),
    review_score INTEGER,
    total_item_value DECIMAL(10,2), -- price + freight
    
    -- NUOVE MISURE GEOGRAFICHE
    customer_seller_distance_km DECIMAL(10,2), -- Distanza calcolata
    is_cross_state_sale BOOLEAN, -- True se stato diverso
    shipping_type VARCHAR(20), -- Local, Interstate
    
    -- NUOVE MISURE DERIVATE
    freight_percentage DECIMAL(8,2), -- freight_value / price * 100
    net_revenue DECIMAL(10,2), -- price - freight_value
    
    -- Audit
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- INDICI PER PERFORMANCE
-- =====================================================
-- Indici esistenti
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_time ON fact_sales(time_key);
CREATE INDEX idx_fact_sales_seller ON fact_sales(seller_key);
CREATE INDEX idx_fact_sales_order ON fact_sales(order_id);

-- Nuovi indici per analisi geografiche
CREATE INDEX idx_fact_sales_cross_state ON fact_sales(is_cross_state_sale);
CREATE INDEX idx_fact_sales_distance ON fact_sales(customer_seller_distance_km);
CREATE INDEX idx_dim_customer_geo ON dim_customer(customer_latitude, customer_longitude);
CREATE INDEX idx_dim_seller_geo ON dim_seller(seller_latitude, seller_longitude);
CREATE INDEX idx_dim_product_category_en ON dim_product(product_category_name_english);

-- =====================================================
-- VISTE MATERIALIZZATE PER PERFORMANCE
-- =====================================================
-- Vista per analisi geografiche rapide
CREATE MATERIALIZED VIEW mv_geographic_sales AS
SELECT 
    dc.customer_state,
    ds.seller_state,
    COUNT(*) as total_sales,
    AVG(fs.customer_seller_distance_km) as avg_distance,
    SUM(CASE WHEN fs.is_cross_state_sale THEN 1 ELSE 0 END) as cross_state_sales,
    SUM(fs.price) as total_revenue,
    AVG(fs.freight_value) as avg_freight
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
JOIN dim_seller ds ON fs.seller_key = ds.seller_key
GROUP BY dc.customer_state, ds.seller_state;

-- Vista per categorie tradotte
CREATE MATERIALIZED VIEW mv_category_performance AS
SELECT 
    dp.product_category_name,
    dp.product_category_name_english,
    COUNT(*) as total_sales,
    SUM(fs.price) as total_revenue,
    AVG(fs.review_score) as avg_rating
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
GROUP BY dp.product_category_name, dp.product_category_name_english;

-- =====================================================
-- COMMENTI 
-- =====================================================
COMMENT ON TABLE fact_sales IS 'Fact table con vendite e-commerce, granularità order item, arricchita con metriche geografiche';
COMMENT ON TABLE dim_customer IS 'Dimensione clienti con coordinate geografiche';
COMMENT ON TABLE dim_product IS 'Dimensione prodotti con traduzioni categorie e classificazioni';
COMMENT ON TABLE dim_seller IS 'Dimensione venditori con coordinate geografiche';
COMMENT ON TABLE dim_geography IS 'Dimensione geografica normalizzata per analisi spaziali';
COMMENT ON COLUMN fact_sales.customer_seller_distance_km IS 'Distanza in km tra cliente e venditore (formula Haversine)';
COMMENT ON COLUMN fact_sales.is_cross_state_sale IS 'True se vendita tra stati diversi';
COMMENT ON COLUMN dim_product.product_category_name_english IS 'Categoria prodotto tradotta in inglese';