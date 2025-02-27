import schedule
import time
import logging
from datetime import datetime
import pytz
from wait_time_data import WaitTimeLib, create_database
from dotenv import load_dotenv
import os
import requests
from contextlib import contextmanager

# Configure logging with timezone
amsterdam_tz = pytz.timezone('Europe/Amsterdam')
logging.Formatter.converter = lambda *args: datetime.now(amsterdam_tz).timetuple()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z',
    handlers=[
        logging.StreamHandler()  # Only use stream handler in production
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get database URL from environment variable
db_url = os.environ.get('DATABASE_URL')

# Main application URL
APP_URL = 'https://stadsloket-wachtwijzer-amsterdam.nl'
HEALTH_CHECK_PATH = '/health'

@contextmanager
def wait_time_session():
    """Context manager for handling database connections safely"""
    wait_time = None
    try:
        wait_time = WaitTimeLib(db_url)
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

def ping_server():
    """Send a request to the app server to keep it awake"""
    try:
        logger.info(f"Pinging server at {APP_URL}...")
        start_time = time.time()
        
        # Send request to the health check endpoint
        response = requests.get(f"{APP_URL}{HEALTH_CHECK_PATH}", timeout=10)
        
        # Calculate response time
        response_time = time.time() - start_time
        
        if response.status_code == 200:
            logger.info(f"Server ping successful! Response time: {response_time:.2f}s")
            return True
        else:
            logger.warning(f"Server responded with status code {response.status_code}")
            return False
            
    except requests.RequestException as e:
        logger.error(f"Failed to ping server: {e}")
        return False

def backup_ping():
    """Send a request to the main URL if health check fails"""
    try:
        logger.info(f"Attempting backup ping to {APP_URL}...")
        response = requests.get(APP_URL, timeout=10)
        
        if response.status_code == 200:
            logger.info("Backup ping successful!")
            return True
        else:
            logger.warning(f"Backup ping failed with status code {response.status_code}")
            return False
            
    except requests.RequestException as e:
        logger.error(f"Failed to send backup ping: {e}")
        return False

def keep_server_awake():
    """Keep the server awake by pinging it"""
    if not ping_server():
        # If health check fails, try the main URL
        backup_ping()

def main():
    """Main function to run the data collector"""
    logger.info("Starting data collector service...")
    
    # Ensure database exists
    try:
        create_database(db_url)
    except Exception as e:
        logger.error(f"Failed to create database: {e}")
        return

    # Schedule data collection
    schedule.every(15).minutes.do(collect_data)
    
    # Schedule server pings every 8 minutes to prevent sleep
    # (less than the 10-minute inactivity threshold)
    schedule.every(8).minutes.do(keep_server_awake)
    
    # Run immediately on startup
    collect_data()
    keep_server_awake()
    
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
