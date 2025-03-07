import requests
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
import re
from datetime import datetime
import pytz
from urllib.parse import urlparse

def create_database(config):
    """Connect to PostgreSQL database using dict config or connection string."""
    if isinstance(config, dict):
        # Dictionary configuration
        required_keys = ['host', 'user', 'password', 'database']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required key: {key}")
        return True
    elif isinstance(config, str):
        # Connection string
        try:
            parsed = urlparse(config)
            return {
                'host': parsed.hostname,
                'user': parsed.username,
                'password': parsed.password,
                'database': parsed.path[1:] if parsed.path else None,
                'port': parsed.port or 5432
            }
        except Exception as e:
            raise ValueError(f"Invalid database URL: {e}")
    else:
        raise ValueError("Config must be a dictionary or connection string")

class WaitTimeLib:
    def __init__(self, config):
        """Initialize database connection from config dict or connection string"""
        if isinstance(config, str):
            self.connection_string = config
            self.db = psycopg2.connect(config)
            parsed = urlparse(config)
            self.host = parsed.hostname
            self.user = parsed.username
            self.password = parsed.password
            self.database = parsed.path[1:] if parsed.path else None
        else:
            self.host = config['host']
            self.user = config['user']
            self.password = config['password']
            self.database = config['database']
            self.db_config = config
            conn_params = {
                'host': self.host,
                'user': self.user,
                'password': self.password,
                'dbname': self.database
            }
            if 'port' in config:
                conn_params['port'] = config['port']
            self.db = psycopg2.connect(**conn_params)
        
        self.db.autocommit = False
        self.cursor = self.db.cursor()
        self.timezone = pytz.timezone('Europe/Amsterdam')
        self.create_table()
        
    def create_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS wait_times (
            id SERIAL PRIMARY KEY,
            stadsloket_id INTEGER NOT NULL,
            waiting INTEGER,
            waittime VARCHAR(255),
            timestamp TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_stadsloket_id ON wait_times(stadsloket_id);
        """)
        self.db.commit()

    def create_loket_names_table(self):
        # PostgreSQL version of the table creation
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS loket_names (
            stadsloket_id INTEGER NOT NULL PRIMARY KEY,
            loket_name VARCHAR(255)
        );
        """)
        # Note: In PostgreSQL, we don't need to manually create the foreign key relationship
        # here since we don't have all the records in wait_times yet
        self.db.commit()

    def fetch_data(self):
        response = requests.get('https://wachttijdenamsterdam.nl/data/')
        return response.json()

    def parse_waittime(self, waittime_str):
        if not waittime_str or waittime_str.lower().startswith('geen'):
            return 0
        if 'uur' in waittime_str.lower():
            return 70
        # Remove ' minuten'
        numeric = ''.join([c for c in waittime_str if c.isdigit()])
        if numeric.isdigit():
            val = int(numeric)
            return val if val <= 60 else 70
        return 0

    def store_data(self, data):
        for entry in data:
            parsed_waittime = self.parse_waittime(entry['waittime'])
            # Get current time in Amsterdam timezone
            current_time = datetime.now(self.timezone)
            self.cursor.execute("""
            INSERT INTO wait_times (stadsloket_id, waiting, waittime, timestamp)
            VALUES (%s, %s, %s, %s)
            """, (entry['id'], entry['waiting'], parsed_waittime, current_time))
        self.db.commit()

    def get_mean_wait_times(self):
        self.cursor.execute("""
            SELECT wt.stadsloket_id, ln.loket_name, AVG(wt.waiting) as mean_waiting
            FROM wait_times wt
            LEFT JOIN loket_names ln
            ON wt.stadsloket_id = ln.stadsloket_id
            GROUP BY wt.stadsloket_id, ln.loket_name
        """)
        rows = self.cursor.fetchall()
        results = []
        for stadsloket_id, loket_name, mean_waiting in rows:
            # Convert the decimal or None value to an integer
            results.append((stadsloket_id, loket_name or 'Unknown', int(mean_waiting or 0)))
        return results

    def get_raw_data(self):
        self.cursor.execute("""
            SELECT wt.stadsloket_id, ln.loket_name, wt.waiting, wt.waittime, wt.timestamp
            FROM wait_times wt
            LEFT JOIN loket_names ln
            ON wt.stadsloket_id = ln.stadsloket_id
        """)
        rows = self.cursor.fetchall()
        results = []
        for sid, name, waiting, wtime, ts in rows:
            results.append((sid, name or 'Unknown', waiting, wtime, ts))
        return results

    def fetch_loket_names(self):
        # Retrieve the main page HTML
        page_response = requests.get('https://wachttijdenamsterdam.nl')
        page_html = page_response.text
        # Simple regular expression to capture (stadsloket name) + (id from nfwrtXX)
        # Each row has the pattern: <td data-title="Stadsloket">\s*(.*?)</td> ... id="nfwrtY"
        matches = re.findall(r'<td data-title="Stadsloket">\s*(.*?)</td>.*?id="nfwrt(\d+)"',
                             page_html, flags=re.DOTALL)
        # Create table if needed
        self.create_loket_names_table()
        # Store results - PostgreSQL uses ON CONFLICT instead of ON DUPLICATE KEY
        for (name, loket_id) in matches:
            self.cursor.execute("""
            INSERT INTO loket_names (stadsloket_id, loket_name)
            VALUES (%s, %s)
            ON CONFLICT (stadsloket_id) 
            DO UPDATE SET loket_name = EXCLUDED.loket_name
            """, (loket_id, name.strip()))
        self.db.commit()

    def get_current_waiting(self):
        # PostgreSQL syntax for getting latest records
        self.cursor.execute("""
            WITH latest_times AS (
                SELECT stadsloket_id, MAX(timestamp) as max_timestamp
                FROM wait_times
                GROUP BY stadsloket_id
            )
            SELECT wt.stadsloket_id, ln.loket_name, wt.waittime, wt.waiting
            FROM wait_times wt
            JOIN latest_times lt 
                ON wt.stadsloket_id = lt.stadsloket_id 
                AND wt.timestamp = lt.max_timestamp
            LEFT JOIN loket_names ln ON wt.stadsloket_id = ln.stadsloket_id
        """)
        return [(sid, name or 'Unknown', waittime, waiting) for sid, name, waittime, waiting in self.cursor.fetchall()]

    def get_hourly_averages(self):
        """Get average wait times in minutes by hour of day for each stadsloket"""
        # PostgreSQL uses EXTRACT(HOUR FROM timestamp) instead of HOUR(timestamp)
        self.cursor.execute("""
            SELECT 
                wt.stadsloket_id,
                ln.loket_name,
                EXTRACT(HOUR FROM wt.timestamp) as hour_of_day,
                AVG(wt.waittime::float) as avg_waittime
            FROM wait_times wt
            LEFT JOIN loket_names ln ON wt.stadsloket_id = ln.stadsloket_id
            WHERE EXTRACT(HOUR FROM wt.timestamp) BETWEEN 8 AND 18
            GROUP BY wt.stadsloket_id, ln.loket_name, EXTRACT(HOUR FROM wt.timestamp)
            ORDER BY wt.stadsloket_id, EXTRACT(HOUR FROM wt.timestamp)
        """)
        
        results = {}
        hours = list(range(8, 19))  # 8:00 to 18:00
        
        for stadsloket_id, loket_name, hour, avg_waittime in self.cursor.fetchall():
            hour = int(hour)  # Convert from Decimal to int
            if loket_name not in results:
                results[loket_name or f'Unknown-{stadsloket_id}'] = {
                    'label': loket_name or f'Unknown-{stadsloket_id}',
                    'data': [0] * len(hours)
                }
            try:
                hour_index = hours.index(hour)
                results[loket_name or f'Unknown-{stadsloket_id}']['data'][hour_index] = round(float(avg_waittime or 0), 1)
            except (ValueError, IndexError):
                pass
                
        return {
            'labels': [f"{h}:00" for h in hours],
            'datasets': list(results.values())
        }

    def get_last_update_time(self):
        """Get the timestamp of the most recent data update"""
        self.cursor.execute("""
            SELECT MAX(timestamp)
            FROM wait_times
        """)
        result = self.cursor.fetchone()
        return result[0] if result and result[0] else None

    def close(self):
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'db') and self.db:
            self.db.close()
