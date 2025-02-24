import schedule
import time
import logging
from datetime import datetime
from wait_time_data import WaitTimeLib, create_database
from dotenv import load_dotenv
import os
from contextlib import contextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

@contextmanager
def wait_time_session():
    """Context manager for handling database connections safely"""
    wait_time = None
    try:
        wait_time = WaitTimeLib(db_config)
        yield wait_time
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if wait_time:
            wait_time.close()

def collect_data():
    """Collect and store data from the API"""
    try:
        logger.info("Starting data collection...")
        
        with wait_time_session() as wait_time:
            # Fetch and store new data
            data = wait_time.fetch_data()
            wait_time.store_data(data)
            
            # Update loket names periodically
            wait_time.fetch_loket_names()
            
            logger.info(f"Successfully collected data for {len(data)} locations")
            
    except Exception as e:
        logger.error(f"Error during data collection: {e}")

def main():
    """Main function to run the data collector"""
    logger.info("Starting data collector service...")
    
    # Ensure database exists
    try:
        create_database(db_config)
    except Exception as e:
        logger.error(f"Failed to create database: {e}")
        return

    # Schedule the job
    schedule.every(15).minutes.do(collect_data)
    
    # Run immediately on startup
    collect_data()
    
    # Keep the script running
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping data collector service...")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            # Wait a bit before retrying
            time.sleep(60)

if __name__ == "__main__":
    main()
