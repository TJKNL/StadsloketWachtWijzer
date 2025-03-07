import os
import mysql.connector
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# MySQL configuration
mysql_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

# PostgreSQL configuration (from DATABASE_URL)
pg_url = os.getenv('DATABASE_URL')

def migrate_data():
    """Migrate data from MySQL to PostgreSQL"""
    logger.info("Starting migration from MySQL to PostgreSQL")
    
    try:
        # Connect to MySQL
        logger.info("Connecting to MySQL database...")
        mysql_conn = mysql.connector.connect(**mysql_config)
        mysql_cursor = mysql_conn.cursor()

        # Connect to PostgreSQL
        logger.info("Connecting to PostgreSQL database...")
        pg_conn = psycopg2.connect(pg_url)
        pg_cursor = pg_conn.cursor()
        
        # Create PostgreSQL tables
        logger.info("Creating PostgreSQL tables if they don't exist...")
        pg_cursor.execute("""
        CREATE TABLE IF NOT EXISTS wait_times (
            id SERIAL PRIMARY KEY,
            stadsloket_id INTEGER NOT NULL,
            waiting INTEGER,
            waittime VARCHAR(255),
            timestamp TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_stadsloket_id ON wait_times(stadsloket_id);
        
        CREATE TABLE IF NOT EXISTS loket_names (
            stadsloket_id INTEGER PRIMARY KEY,
            loket_name VARCHAR(255)
        );
        """)
        pg_conn.commit()
        
        # Migrate wait_times data
        logger.info("Migrating wait_times data...")
        mysql_cursor.execute("SELECT stadsloket_id, waiting, waittime, timestamp FROM wait_times")
        wait_times_rows = mysql_cursor.fetchall()
        
        if wait_times_rows:
            logger.info(f"Found {len(wait_times_rows)} records in MySQL wait_times table")
            
            # Use batch inserts for better performance
            batch_size = 1000
            for i in range(0, len(wait_times_rows), batch_size):
                batch = wait_times_rows[i:i+batch_size]
                
                # Prepare values for batch insert
                args_str = ','.join(pg_cursor.mogrify("(%s,%s,%s,%s)", row).decode('utf-8') 
                                   for row in batch)
                
                # Execute batch insert
                pg_cursor.execute(f"""
                    INSERT INTO wait_times (stadsloket_id, waiting, waittime, timestamp)
                    VALUES {args_str}
                    ON CONFLICT DO NOTHING
                """)
                
                pg_conn.commit()
                logger.info(f"Inserted batch of {len(batch)} records into PostgreSQL")
        else:
            logger.warning("No wait_times data found in MySQL")
        
        # Migrate loket_names data
        logger.info("Migrating loket_names data...")
        mysql_cursor.execute("SELECT stadsloket_id, loket_name FROM loket_names")
        loket_names_rows = mysql_cursor.fetchall()
        
        if loket_names_rows:
            logger.info(f"Found {len(loket_names_rows)} records in MySQL loket_names table")
            
            # Insert loket_names data
            for stadsloket_id, loket_name in loket_names_rows:
                pg_cursor.execute("""
                INSERT INTO loket_names (stadsloket_id, loket_name)
                VALUES (%s, %s)
                ON CONFLICT (stadsloket_id) DO UPDATE SET loket_name = EXCLUDED.loket_name
                """, (stadsloket_id, loket_name))
            
            pg_conn.commit()
            logger.info(f"Inserted {len(loket_names_rows)} records into PostgreSQL loket_names table")
        else:
            logger.warning("No loket_names data found in MySQL")
        
        logger.info("Migration completed successfully!")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        if 'pg_conn' in locals() and pg_conn:
            pg_conn.rollback()
    finally:
        # Close connections
        if 'mysql_cursor' in locals() and mysql_cursor:
            mysql_cursor.close()
        if 'mysql_conn' in locals() and mysql_conn:
            mysql_conn.close()
        if 'pg_cursor' in locals() and pg_cursor:
            pg_cursor.close()
        if 'pg_conn' in locals() and pg_conn:
            pg_conn.close()

if __name__ == "__main__":
    migrate_data()
