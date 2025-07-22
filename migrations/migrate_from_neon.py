import os
import logging
from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

# ----------------------------------------------------------------------------
# Logging setup
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("migrate_from_neon")

# ----------------------------------------------------------------------------
# Environment vars
# ----------------------------------------------------------------------------
load_dotenv()
OLD_DB_URL = os.getenv("DATABASE_URL_NEON")  # Old Neon instance
NEW_DB_URL = os.getenv("DATABASE_URL")       # Target instance

if not OLD_DB_URL:
    raise RuntimeError("DATABASE_URL_NEON environment variable not set – cannot find source DB")
if not NEW_DB_URL:
    raise RuntimeError("DATABASE_URL environment variable not set – cannot find target DB")

# ----------------------------------------------------------------------------
# SQL helpers
# ----------------------------------------------------------------------------
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS wait_times (
    id SERIAL PRIMARY KEY,
    stadsloket_id INTEGER NOT NULL,
    waiting INTEGER,
    waittime VARCHAR(255),
    timestamp TIMESTAMP WITH TIME ZONE
);
CREATE INDEX IF NOT EXISTS idx_stadsloket_id ON wait_times(stadsloket_id);

CREATE TABLE IF NOT EXISTS loket_names (
    stadsloket_id INTEGER PRIMARY KEY,
    loket_name VARCHAR(255)
);
"""

TRUNCATE_SQL = """
TRUNCATE TABLE wait_times RESTART IDENTITY CASCADE;
TRUNCATE TABLE loket_names RESTART IDENTITY CASCADE;
"""

FETCH_WAIT_TIMES_SQL = "SELECT stadsloket_id, waiting, waittime, timestamp FROM wait_times ORDER BY id;"
FETCH_NAMES_SQL = "SELECT stadsloket_id, loket_name FROM loket_names;"

INSERT_WAIT_TIMES_TEMPLATE = (
    "INSERT INTO wait_times (stadsloket_id, waiting, waittime, timestamp) "
    "VALUES %s ON CONFLICT DO NOTHING"
)
INSERT_NAMES_SQL = (
    "INSERT INTO loket_names (stadsloket_id, loket_name) "
    "VALUES (%s, %s) ON CONFLICT (stadsloket_id) "
    "DO UPDATE SET loket_name = EXCLUDED.loket_name"
)

BATCH_SIZE = 1000

# ----------------------------------------------------------------------------
# Main routine
# ----------------------------------------------------------------------------

def migrate():
    logger.info("Starting migration from Neon DB → new PostgreSQL")
    logger.info("Connecting to source (Neon)")
    src_conn = psycopg2.connect(OLD_DB_URL, cursor_factory=DictCursor)
    src_cursor = src_conn.cursor()

    logger.info("Connecting to target")
    tgt_conn = psycopg2.connect(NEW_DB_URL)
    tgt_cursor = tgt_conn.cursor()

    try:
        # Ensure tables and clean slate
        logger.info("Ensuring target tables exist and cleaning old data")
        tgt_cursor.execute(CREATE_TABLES_SQL)
        tgt_cursor.execute(TRUNCATE_SQL)
        tgt_conn.commit()

        # ------------------------------------------------------------------
        # Migrate wait_times
        # ------------------------------------------------------------------
        logger.info("Fetching wait_times from source")
        src_cursor.execute(FETCH_WAIT_TIMES_SQL)
        rows = src_cursor.fetchall()
        total = len(rows)
        logger.info("Found %s wait_times records", total)

        if total:
            for i in range(0, total, BATCH_SIZE):
                batch = rows[i : i + BATCH_SIZE]
                args_str = b",".join(
                    tgt_cursor.mogrify("(%s,%s,%s,%s)", (
                        r["stadsloket_id"], r["waiting"], r["waittime"], r["timestamp"]
                    ))
                    for r in batch
                ).decode("utf-8")
                tgt_cursor.execute(INSERT_WAIT_TIMES_TEMPLATE % args_str)
                tgt_conn.commit()
                logger.info("Inserted %s / %s wait_times", min(i + BATCH_SIZE, total), total)
        else:
            logger.warning("No wait_times records found in source")

        # ------------------------------------------------------------------
        # Migrate loket_names
        # ------------------------------------------------------------------
        logger.info("Fetching loket_names from source")
        src_cursor.execute(FETCH_NAMES_SQL)
        name_rows = src_cursor.fetchall()
        logger.info("Found %s loket_names records", len(name_rows))
        for stadsloket_id, loket_name in name_rows:
            tgt_cursor.execute(INSERT_NAMES_SQL, (stadsloket_id, loket_name))
        tgt_conn.commit()
        logger.info("Completed inserting loket_names")

        logger.info("Migration finished successfully ✅")
    except Exception as exc:
        logger.error("Migration failed – rolling back: %s", exc)
        tgt_conn.rollback()
        raise
    finally:
        src_cursor.close()
        src_conn.close()
        tgt_cursor.close()
        tgt_conn.close()


if __name__ == "__main__":
    migrate() 