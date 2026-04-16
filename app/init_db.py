import logging
import os
from sqlalchemy import text, create_engine
from database import DB_HOST, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_NAME

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_database_if_not_exists():
    """
    Connect to postgres default DB and create the target DB if it doesn't exist
    """
    # URL to connect to default postgres database
    postgres_url = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/postgres"
    engine_postgres = create_engine(postgres_url, isolation_level="AUTOCOMMIT")
    
    try:
        with engine_postgres.connect() as conn:
            result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname='{DB_NAME}'"))
            exists = result.fetchone()
            
            if not exists:
                logger.info(f"Database '{DB_NAME}' not found. Creating it...")
                conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
                logger.info(f" Database '{DB_NAME}' created successfully.")
            else:
                logger.info(f"Database '{DB_NAME}' already exists.")
        return True
    except Exception as e:
        logger.error(f" Failed to check/create database: {e}")
        return False

def init_database():
    """
    Initialize database by creating necessary tables
    """
    if not create_database_if_not_exists():
        return False

    from database import engine 
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.parsing_results (
        id SERIAL PRIMARY KEY,
        nosert VARCHAR(255),
        noreg VARCHAR(255),
        nmkpl VARCHAR(255),
        jenis_sert VARCHAR(255),
        jenis_survey VARCHAR(255),
        divisi VARCHAR(255),
        lokasi_survey VARCHAR(255),
        mem01 TEXT,
        tgl_sert VARCHAR(100),
        tgl_berlaku VARCHAR(100),
        tgl_survey1 VARCHAR(100),
        tgl_survey2 VARCHAR(100),
        raw_result JSONB,
        nup VARCHAR(100),
        sign_no VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        logger.info(f"Creating tables in '{DB_NAME}'...")
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(create_table_sql))
        logger.info("Table 'parsing_results' initialized successfully!")
        
        add_column_sql = "ALTER TABLE public.parsing_results ADD COLUMN IF NOT EXISTS nup VARCHAR(100);"
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(add_column_sql))
        logger.info("Database migration: 'nup' column ensured.")
        
        add_sign_no_sql = "ALTER TABLE public.parsing_results ADD COLUMN IF NOT EXISTS sign_no VARCHAR(255);"
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(add_sign_no_sql))
        logger.info("Database migration: 'sign_no' column ensured.")

        add_nosert_ocr_sql = "ALTER TABLE public.parsing_results ADD COLUMN IF NOT EXISTS nosert_ocr VARCHAR(255);"
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(add_nosert_ocr_sql))
        logger.info("Database migration: 'nosert_ocr' column ensured.")

        add_nosert_expected_sql = "ALTER TABLE public.parsing_results ADD COLUMN IF NOT EXISTS nosert_expected VARCHAR(255);"
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(add_nosert_expected_sql))
        logger.info("Database migration: 'nosert_expected' column ensured.")

        return True
    except Exception as e:
        logger.error(f"Failed to initialize table: {e}")
        return False

if __name__ == "__main__":
    init_database()
