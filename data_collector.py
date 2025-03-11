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
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Use connection string for database
db_url = os.getenv('DATABASE_URL')

# App URLs
APP_URL = 'https://stadsloket-wachtwijzer-amsterdam.nl'
HEALTH_CHECK_PATH = '/health'

# Active hours (7:00 to 23:00) - for server pinging
ACTIVE_HOURS_START = 7
ACTIVE_HOURS_END = 23

# Data collection hours (collect data between 7:00-22:00)
COLLECT_START = 7
COLLECT_END = 22

@contextmanager
def wait_time_session():
    """Safe database connection context manager"""
    wait_time = None
    try:
        wait_time = WaitTimeLib(db_url)
        yield wait_time
    except Exception as e:
        logger.error(f"DB connection error: {e}")
        raise
    finally:
        if wait_time:
            wait_time.close()

def is_active_hours():
    """Check if current time is within active hours"""
    current_hour = datetime.now(amsterdam_tz).hour
    return ACTIVE_HOURS_START <= current_hour < ACTIVE_HOURS_END

def is_collection_allowed():
    """Check if data collection is currently allowed (between 7:00-22:00)"""
    current_hour = datetime.now(amsterdam_tz).hour
    return COLLECT_START <= current_hour < COLLECT_END

def collect_data():
    """Collect and store wait time data if allowed"""
    try:
        # Check if collection is currently allowed
        if not is_collection_allowed():
            logger.info(f"Data collection skipped - outside collection hours ({COLLECT_START}:00-{COLLECT_END}:00)")
            return
        
        logger.info("Collecting data...")
        
        with wait_time_session() as wait_time:
            data = wait_time.fetch_data()
            wait_time.store_data(data)
            wait_time.fetch_loket_names()
            
            logger.info(f"Collected data for {len(data)} locations")
            
    except Exception as e:
        logger.error(f"Data collection error: {e}")

def ping_server():
    """Ping server to keep it awake"""
    try:
        logger.info(f"Pinging {APP_URL}...")
        start_time = time.time()
        
        response = requests.get(f"{APP_URL}{HEALTH_CHECK_PATH}", timeout=10)
        response_time = time.time() - start_time
        
        if response.status_code == 200:
            logger.info(f"Ping successful ({response_time:.2f}s)")
            return True
        else:
            logger.warning(f"Ping returned status code {response.status_code}")
            return False
            
    except requests.RequestException as e:
        logger.error(f"Ping failed: {e}")
        return False

def backup_ping():
    """Fallback ping to main URL"""
    try:
        logger.info(f"Trying backup ping to {APP_URL}...")
        response = requests.get(APP_URL, timeout=10)
        
        if response.status_code == 200:
            logger.info("Backup ping successful")
            return True
        else:
            logger.warning(f"Backup ping failed: status {response.status_code}")
            return False
            
    except requests.RequestException as e:
        logger.error(f"Backup ping failed: {e}")
        return False

def keep_server_awake():
    """Ping server during active hours to prevent sleep"""
    if not is_active_hours():
        logger.info(f"Outside active hours ({ACTIVE_HOURS_START}-{ACTIVE_HOURS_END}), skipping ping")
        return False
    
    logger.info(f"Within active hours ({ACTIVE_HOURS_START}-{ACTIVE_HOURS_END}), pinging server")
    if not ping_server():
        return backup_ping()
    return True

def main():
    """Main data collector service"""
    logger.info("Starting data collector service")
    logger.info(f"Active hours for server ping: {ACTIVE_HOURS_START}:00-{ACTIVE_HOURS_END}:00 Amsterdam time")
    logger.info(f"Data collection hours: {COLLECT_START}:00-{COLLECT_END}:00 Amsterdam time")
    
    try:
        create_database(db_url)
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        return

    # Schedule tasks
    schedule.every(15).minutes.do(collect_data)
    schedule.every(8).minutes.do(keep_server_awake)
    
    # Initial run - only if collection is allowed
    if is_collection_allowed():
        collect_data()
    else:
        logger.info(f"Initial data collection skipped - outside collection hours ({COLLECT_START}:00-{COLLECT_END}:00)")
        
    if is_active_hours():
        keep_server_awake()
    
    # Main loop
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping data collector")
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
