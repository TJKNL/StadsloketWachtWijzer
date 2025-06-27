import logging
import os
from datetime import datetime
from contextlib import contextmanager

import pytz
from dotenv import load_dotenv

from wait_time_data import WaitTimeLib, create_database

# ---------------------------------------------------------------------------
# Logging configuration with explicit Amsterdam timezone
# ---------------------------------------------------------------------------
amsterdam_tz = pytz.timezone("Europe/Amsterdam")
logging.Formatter.converter = lambda *args: datetime.now(amsterdam_tz).timetuple()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("cron_data_collector")

# ---------------------------------------------------------------------------
# Environment & database setup
# ---------------------------------------------------------------------------
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")

# ---------------------------------------------------------------------------
# Context-manager for safe connection handling
# ---------------------------------------------------------------------------
@contextmanager
def wait_time_session():
    """Provide a WaitTimeLib instance with automatic cleanup."""
    wait_time = None
    try:
        wait_time = WaitTimeLib(DB_URL)
        yield wait_time
    finally:
        if wait_time:
            wait_time.close()

# ---------------------------------------------------------------------------
# Core routine – executed once per cron invocation
# ---------------------------------------------------------------------------

def run():
    """Single-shot data collection suitable for Railway Cron."""
    logger.info("Starting cron data-collector run")

    # Ensure database & tables exist
    try:
        create_database(DB_URL)
    except Exception as exc:
        logger.error(f"Database setup failed: {exc}")
        return

    # Collect & store latest wait-time data
    try:
        with wait_time_session() as wait_time:
            data = wait_time.fetch_data()
            wait_time.store_data(data)
            wait_time.fetch_loket_names()
            logger.info(f"Collected data for {len(data)} locations")
    except Exception as exc:
        logger.error(f"Data collection error: {exc}")

    logger.info("Cron run finished – exiting")


if __name__ == "__main__":
    run() 