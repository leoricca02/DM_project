# =============================================================================
# ETL PROCESS - RECONCILED DATA LAYER (RDL)
# 3-Layer Architecture: Source → RDL → DW
# =============================================================================

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# PHASE 1: EXTRACT - Load raw data into RDL with validation
# =============================================================================

class RDLExtractor:
    """Handles extraction from source files to RDL with validation"""
    
    def __init__(self, engine):
        self.engine = engine
        self.validation_errors = {}
        
    def log_etl_process(self, process_name, process_type, status, records_processed, records_rejected=0, error_message=None):
        """Log ETL process details"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO rdl.etl_process_log 
                (process_name, process_type, start_timestamp, end_timestamp, status, 
                 records_processed, records_rejected, error_message)
                VALUES (:name, :type, :start, :end, :status, :processed, :rejected, :error)
            """), {
                'name': process_name,
                'type': process_type,
                'start': datetime.now(),
                'end': datetime.now(),
                'status': status,
                'processed': records_processed,
                'rejected': records_rejected,
                'error': error_message
            })
            conn.commit()
    
    def validate_orders(self, df):
        """Validate orders data and add quality flags"""
        df = df.copy()
        df['rdl_is_valid'] = True
        df['rdl_validation_errors'] = ''
        
        # Check for missing required fields
        required_fields = ['order_id', 'customer_id', 'order_purchase_timestamp']
        for field in required_fields:
            mask = df[field].isna()
            df.loc[mask, 'rdl_is_valid'] = False
            df.loc[mask, 'rdl_validation_errors'] += f'Missing {field}; '
        
        # Validate dates logic - converte TUTTE le colonne data
        date_columns = [
            'order_purchase_timestamp',
            'order_delivered_customer_date',
            'order_approved_at',
            'order_estimated_delivery_date',
            'order_delivered_carrier_date'
        ]
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Calculate derived fields - solo se entrambe le date esistono
        if 'order_delivered_customer_date' in df.columns and 'order_estimated_delivery_date' in df.columns:
            # Calcola solo dove entrambe le date sono valide
            mask_valid_dates = df['order_delivered_customer_date'].notna() & df['order_estimated_delivery_date'].notna()
            df.loc[mask_valid_dates, 'rdl_delivery_delay_days'] = (
                df.loc[mask_valid_dates, 'order_delivered_customer_date'] - 
                df.loc[mask_valid_dates, 'order_estimated_delivery_date']
            ).dt.days
        else:
            df['rdl_delivery_delay_days'] = None
        
        if 'order_approved_at' in df.columns and 'order_purchase_timestamp' in df.columns:
            mask_valid_times = df['order_approved_at'].notna() & df['order_purchase_timestamp'].notna()
            df.loc[mask_valid_times, 'rdl_approval_delay_hours'] = (
                df.loc[mask_valid_times, 'order_approved_at'] - 
                df.loc[mask_valid_times, 'order_purchase_timestamp']
            ).dt.total_seconds() / 3600
        else:
            df['rdl_approval_delay_hours'] = None
        
        # Check for invalid date sequences
        if 'order_approved_at' in df.columns and 'order_purchase_timestamp' in df.columns:
            mask = (df['order_approved_at'] < df['order_purchase_timestamp']) & df['order_approved_at'].notna()
            df.loc[mask, 'rdl_is_valid'] = False
            df.loc[mask, 'rdl_validation_errors'] += 'Invalid date sequence; '
        
        return df
    
    def validate_customers(self, df, geo_lookup):
        """Validate customers and enrich with geocoding"""
        df = df.copy()
        df['rdl_is_valid'] = True
        df['rdl_validation_errors'] = ''
        
        # Validate state codes
        valid_states = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS',
                       'MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC',
                       'SP','SE','TO']
        
        mask = ~df['customer_state'].isin(valid_states)
        df.loc[mask, 'rdl_is_valid'] = False
        df.loc[mask, 'rdl_validation_errors'] += 'Invalid state code; '
        
        # Geocoding
        df = df.merge(
            geo_lookup[['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']],
            left_on='customer_zip_code_prefix',
            right_on='geolocation_zip_code_prefix',
            how='left'
        )
        
        df['rdl_latitude'] = df['geolocation_lat']
        df['rdl_longitude'] = df['geolocation_lng']
        
        # Set geocoding status
        df['rdl_geocoding_status'] = 'not_found'
        df.loc[df['rdl_latitude'].notna(), 'rdl_geocoding_status'] = 'matched'
        df['rdl_geocoding_confidence'] = df['rdl_latitude'].notna().astype(float)
        
        # Calculate completeness score
        fields_to_check = ['customer_city', 'customer_state', 'customer_zip_code_prefix']
        df['rdl_address_completeness_score'] = (
            df[fields_to_check].notna().sum(axis=1) / len(fields_to_check)
        )
        
        # Overall quality score
        df['rdl_data_quality_score'] = (
            df['rdl_is_valid'].astype(float) * 0.4 +
            df['rdl_address_completeness_score'] * 0.3 +
            df['rdl_geocoding_confidence'] * 0.3
        )
        
        return df
    
    def validate_products(self, df, translations):
        """Validate products and add enrichments"""
        df = df.copy()
        df['rdl_is_valid'] = True
        df['rdl_validation_errors'] = ''
        
        # Merge translations
        df = df.merge(
            translations[['product_category_name', 'product_category_name_english']],
            on='product_category_name',
            how='left'
        )
        df['rdl_category_english'] = df['product_category_name_english'].fillna(df['product_category_name'])
        
        # Validate dimensions
        dimension_cols = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
        for col in dimension_cols:
            mask = (df[col] < 0) | (df[col] > 10000)  # Reasonable limits
            df.loc[mask, 'rdl_is_valid'] = False
            df.loc[mask, 'rdl_validation_errors'] += f'Invalid {col}; '
        
        # Calculate volume
        df['rdl_volume_cm3'] = df['product_length_cm'] * df['product_height_cm'] * df['product_width_cm']
        
        # Categorizations
        df['rdl_weight_category'] = pd.cut(
            df['product_weight_g'],
            bins=[0, 500, 2000, 5000, float('inf')],
            labels=['Light', 'Medium', 'Heavy', 'Very Heavy']
        )
        
        df['rdl_size_category'] = pd.cut(
            df['rdl_volume_cm3'],
            bins=[0, 1000, 8000, float('inf')],
            labels=['Small', 'Medium', 'Large']
        )
        
        # Quality flags
        df['rdl_has_complete_dimensions'] = df[dimension_cols].notna().all(axis=1)
        df['rdl_has_photos'] = df['product_photos_qty'] > 0
        
        # Description quality
        df['rdl_description_quality'] = pd.cut(
            df['product_description_lenght'],
            bins=[0, 100, 500, float('inf')],
            labels=['Short', 'Medium', 'Long']
        )
        
        return df
    
    def load_to_rdl(self, df, table_name):
        """Load validated data to RDL"""
        try:
            # Add RDL metadata
            df['rdl_load_timestamp'] = datetime.now()
            df['rdl_source_system'] = 'olist'
            df['rdl_processing_status'] = 'new'
            
            # Load to database
            df.to_sql(f'rdl.{table_name}', self.engine, if_exists='append', index=False, method='multi')
            
            valid_count = df['rdl_is_valid'].sum() if 'rdl_is_valid' in df.columns else len(df)
            invalid_count = len(df) - valid_count
            
            logger.info(f"Loaded {len(df)} records to rdl.{table_name} ({valid_count} valid, {invalid_count} invalid)")
            
            # Log data quality
            if 'rdl_is_valid' in df.columns:
                quality_score = (valid_count / len(df)) * 100
                with self.engine.connect() as conn:
                    conn.execute(text("""
                        INSERT INTO rdl.data_quality_log
                        (table_name, check_type, total_records, valid_records, invalid_records, quality_score)
                        VALUES (:table, 'validation', :total, :valid, :invalid, :score)
                    """), {
                        'table': table_name,
                        'total': int(len(df)),
                        'valid': int(valid_count),
                        'invalid': int(invalid_count),
                        'score': float(quality_score)
                    })
                    conn.commit()
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading to rdl.{table_name}: {e}")
            self.log_etl_process(f'Load to rdl.{table_name}', 'load', 'failed', 0, 0, str(e))
            return False

# =============================================================================
# PHASE 2: TRANSFORM - Process RDL data for DW
# =============================================================================

class RDLTransformer:
    """Handles transformation from RDL to DW-ready format"""
    
    def __init__(self, engine):
        self.engine = engine
    
    def create_dim_customer_from_rdl(self):
        """Create DIM_CUSTOMER from validated RDL data"""
        query = """
        SELECT DISTINCT
            customer_id,
            customer_city,
            customer_state,
            customer_zip_code_prefix,
            rdl_latitude as customer_latitude,
            rdl_longitude as customer_longitude,
            rdl_data_quality_score
        FROM rdl.customers
        WHERE rdl_is_valid = true
            AND rdl_processing_status = 'new'
        """
        
        df = pd.read_sql(query, self.engine)
        
        # Additional transformations
        df['customer_city'] = df['customer_city'].str.title()
        df['customer_state'] = df['customer_state'].str.upper()
        
        return df
    
    def create_dim_product_from_rdl(self):
        """Create DIM_PRODUCT from validated RDL data"""
        query = """
        SELECT DISTINCT
            product_id,
            product_category_name,
            rdl_category_english as product_category_name_english,
            product_name_lenght,
            product_description_lenght,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm,
            rdl_weight_category as weight_category,
            rdl_size_category as size_category
        FROM rdl.products
        WHERE rdl_is_valid = true
            AND rdl_processing_status = 'new'
        """
        
        return pd.read_sql(query, self.engine)
    
    def create_fact_sales_from_rdl(self):
        """Create FACT_SALES from validated RDL data"""
        query = """
        WITH validated_orders AS (
            SELECT o.*, oi.*, p.*, r.*
            FROM rdl.orders o
            JOIN rdl.order_items oi ON o.order_id = oi.order_id
            JOIN rdl.payments p ON o.order_id = p.order_id
            LEFT JOIN rdl.reviews r ON o.order_id = r.order_id
            WHERE o.rdl_is_valid = true
                AND oi.rdl_is_valid = true
                AND o.order_status = 'delivered'
                AND o.rdl_processing_status = 'new'
        )
        SELECT 
            order_id,
            order_item_id,
            customer_id,
            product_id,
            seller_id,
            order_purchase_timestamp,
            price,
            freight_value,
            payment_value,
            review_score,
            rdl_total_item_value as total_item_value,
            rdl_freight_percentage as freight_percentage
        FROM validated_orders
        """
        
        return pd.read_sql(query, self.engine)
    
    def mark_rdl_as_processed(self, table_name, condition='rdl_processing_status = \'new\''):
        """Mark RDL records as processed after loading to DW"""
        with self.engine.connect() as conn:
            result = conn.execute(text(f"""
                UPDATE rdl.{table_name}
                SET rdl_processing_status = 'processed'
                WHERE {condition}
            """))
            conn.commit()
            logger.info(f"Marked {result.rowcount} records as processed in rdl.{table_name}")

# =============================================================================
# PHASE 3: LOAD - Load transformed data to DW
# =============================================================================

def load_rdl_to_dw(engine):
    """Main process to load from RDL to DW"""
    
    transformer = RDLTransformer(engine)
    
    try:
        # 1. Load dimensions
        logger.info("Loading dimensions from RDL to DW...")
        
        # DIM_CUSTOMER
        dim_customer = transformer.create_dim_customer_from_rdl()
        dim_customer.to_sql('dim_customer', engine, if_exists='append', index=False)
        transformer.mark_rdl_as_processed('customers')
        logger.info(f"Loaded {len(dim_customer)} customers to DW")
        
        # DIM_PRODUCT
        dim_product = transformer.create_dim_product_from_rdl()
        dim_product.to_sql('dim_product', engine, if_exists='append', index=False)
        transformer.mark_rdl_as_processed('products')
        logger.info(f"Loaded {len(dim_product)} products to DW")
        
        # 2. Load fact table
        logger.info("Loading fact table from RDL to DW...")
        fact_sales = transformer.create_fact_sales_from_rdl()
        fact_sales.to_sql('fact_sales', engine, if_exists='append', index=False)
        transformer.mark_rdl_as_processed('orders')
        logger.info(f"Loaded {len(fact_sales)} sales records to DW")
        
        return True
        
    except Exception as e:
        logger.error(f"Error loading RDL to DW: {e}")
        return False

# =============================================================================
# MAIN ETL ORCHESTRATION
# =============================================================================

def run_full_etl_with_rdl(source_data_dict, engine):
    """Run complete ETL: Source → RDL → DW"""
    
    logger.info("Starting 3-layer ETL process...")
    
    # Phase 1: Extract to RDL
    logger.info("PHASE 1: Extracting to RDL...")
    extractor = RDLExtractor(engine)
    
    # Load and validate each dataset
    if 'orders' in source_data_dict:
        orders_validated = extractor.validate_orders(source_data_dict['orders'])
        extractor.load_to_rdl(orders_validated, 'orders')
    
    if 'customers' in source_data_dict and 'geolocation' in source_data_dict:
        customers_validated = extractor.validate_customers(
            source_data_dict['customers'], 
            source_data_dict['geolocation']
        )
        extractor.load_to_rdl(customers_validated, 'customers')
    
    if 'products' in source_data_dict and 'translation' in source_data_dict:
        products_validated = extractor.validate_products(
            source_data_dict['products'],
            source_data_dict['translation']
        )
        extractor.load_to_rdl(products_validated, 'products')
    
    # Load other tables with basic validation
    for table_name in ['order_items', 'payments', 'reviews', 'sellers']:
        if table_name in source_data_dict:
            df = source_data_dict[table_name].copy()
            df['rdl_is_valid'] = True  # Basic validation
            extractor.load_to_rdl(df, table_name)
    
    # Phase 2 & 3: Transform and Load to DW
    logger.info("PHASE 2 & 3: Transforming and loading to DW...")
    success = load_rdl_to_dw(engine)
    
    if success:
        logger.info("ETL process completed successfully!")
        
        # Generate data quality report
        quality_report = pd.read_sql("""
            SELECT * FROM rdl.v_data_quality_summary
            ORDER BY table_name
        """, engine)
        
        logger.info("\nData Quality Report:")
        logger.info(quality_report.to_string())
        
    return success

# =============================================================================
# DATA QUALITY CHECKS
# =============================================================================

class DataQualityChecker:
    """Performs data quality checks on RDL"""
    
    def __init__(self, engine):
        self.engine = engine
        
    def check_completeness(self, table_name, required_columns):
        """Check data completeness for required columns"""
        query = f"""
        SELECT 
            '{table_name}' as table_name,
            COUNT(*) as total_records,
            {', '.join([f"SUM(CASE WHEN {col} IS NOT NULL THEN 1 ELSE 0 END) as {col}_complete" for col in required_columns])}
        FROM rdl.{table_name}
        """
        
        result = pd.read_sql(query, self.engine)
        
        # Calculate completeness scores
        completeness_scores = {}
        for col in required_columns:
            completeness_scores[col] = result[f'{col}_complete'].iloc[0] / result['total_records'].iloc[0] * 100
        
        return completeness_scores
    
    def check_consistency(self):
        """Check referential integrity between RDL tables"""
        consistency_checks = []
        
        # Check 1: Orders without customers
        orphan_orders = pd.read_sql("""
            SELECT COUNT(*) as count
            FROM rdl.orders o
            LEFT JOIN rdl.customers c ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """, self.engine)
        
        consistency_checks.append({
            'check': 'Orders without customers',
            'failed_records': orphan_orders['count'].iloc[0]
        })
        
        # Check 2: Order items without valid products
        orphan_items = pd.read_sql("""
            SELECT COUNT(*) as count
            FROM rdl.order_items oi
            LEFT JOIN rdl.products p ON oi.product_id = p.product_id
            WHERE p.product_id IS NULL
        """, self.engine)
        
        consistency_checks.append({
            'check': 'Order items without products',
            'failed_records': orphan_items['count'].iloc[0]
        })
        
        return consistency_checks
    
    def check_accuracy(self):
        """Check data accuracy with business rules"""
        accuracy_checks = []
        
        # Check 1: Negative prices
        negative_prices = pd.read_sql("""
            SELECT COUNT(*) as count
            FROM rdl.order_items
            WHERE price < 0 OR freight_value < 0
        """, self.engine)
        
        accuracy_checks.append({
            'check': 'Negative prices or freight',
            'failed_records': negative_prices['count'].iloc[0]
        })
        
        # Check 2: Invalid date sequences
        invalid_dates = pd.read_sql("""
            SELECT COUNT(*) as count
            FROM rdl.orders
            WHERE order_delivered_customer_date < order_purchase_timestamp
                OR order_approved_at < order_purchase_timestamp
        """, self.engine)
        
        accuracy_checks.append({
            'check': 'Invalid date sequences',
            'failed_records': invalid_dates['count'].iloc[0]
        })
        
        return accuracy_checks
    
    def generate_quality_report(self):
        """Generate comprehensive data quality report"""
        report = {
            'timestamp': datetime.now(),
            'completeness': {},
            'consistency': [],
            'accuracy': []
        }
        
        # Completeness checks
        tables_to_check = {
            'orders': ['order_id', 'customer_id', 'order_status'],
            'customers': ['customer_id', 'customer_city', 'customer_state'],
            'products': ['product_id', 'product_category_name']
        }
        
        for table, columns in tables_to_check.items():
            report['completeness'][table] = self.check_completeness(table, columns)
        
        # Consistency checks
        report['consistency'] = self.check_consistency()
        
        # Accuracy checks
        report['accuracy'] = self.check_accuracy()
        
        return report

# =============================================================================
# INCREMENTAL LOAD SUPPORT
# =============================================================================

class IncrementalLoader:
    """Handles incremental loads to RDL"""
    
    def __init__(self, engine):
        self.engine = engine
        
    def get_last_load_timestamp(self, table_name):
        """Get timestamp of last successful load"""
        query = f"""
        SELECT MAX(rdl_load_timestamp) as last_load
        FROM rdl.{table_name}
        """
        
        result = pd.read_sql(query, self.engine)
        return result['last_load'].iloc[0]
    
    def load_incremental_data(self, new_data, table_name, timestamp_column):
        """Load only new/changed records"""
        last_load = self.get_last_load_timestamp(table_name)
        
        if last_load is not None:
            # Filter for records newer than last load
            new_data[timestamp_column] = pd.to_datetime(new_data[timestamp_column])
            incremental_data = new_data[new_data[timestamp_column] > last_load]
            
            logger.info(f"Found {len(incremental_data)} new records for {table_name}")
            return incremental_data
        else:
            logger.info(f"No previous load found for {table_name}, loading all data")
            return new_data

# =============================================================================
# USAGE EXAMPLE
# =============================================================================

def example_usage():
    """Example of how to use the RDL ETL process"""
    
    # Create database connection
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/ecommerce')
    
    # Load source data (your existing CSV loading code)
    source_data = {
        'orders': pd.read_csv('path/to/orders.csv'),
        'customers': pd.read_csv('path/to/customers.csv'),
        'products': pd.read_csv('path/to/products.csv'),
        'order_items': pd.read_csv('path/to/order_items.csv'),
        'payments': pd.read_csv('path/to/payments.csv'),
        'reviews': pd.read_csv('path/to/reviews.csv'),
        'sellers': pd.read_csv('path/to/sellers.csv'),
        'geolocation': pd.read_csv('path/to/geolocation.csv'),
        'translation': pd.read_csv('path/to/translation.csv')
    }
    
    # Run full ETL with RDL
    run_full_etl_with_rdl(source_data, engine)
    
    # Generate quality report
    checker = DataQualityChecker(engine)
    quality_report = checker.generate_quality_report()
    
    print("Data Quality Report:")
    print(f"Generated at: {quality_report['timestamp']}")
    print("\nCompleteness Scores:")
    for table, scores in quality_report['completeness'].items():
        print(f"\n{table}:")
        for column, score in scores.items():
            print(f"  {column}: {score:.2f}%")
    
    print("\nConsistency Checks:")
    for check in quality_report['consistency']:
        print(f"  {check['check']}: {check['failed_records']} failed records")
    
    print("\nAccuracy Checks:")
    for check in quality_report['accuracy']:
        print(f"  {check['check']}: {check['failed_records']} failed records")

# =============================================================================
# END OF RDL ETL PROCESS
# =============================================================================