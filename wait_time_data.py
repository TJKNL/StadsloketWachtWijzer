import requests
import mysql.connector
import re
from datetime import datetime

def create_database(db_config):
    # Connect without specifying a database
    temp_db = mysql.connector.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password']
    )
    temp_cursor = temp_db.cursor()
    temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_config['database']}")
    temp_cursor.close()
    temp_db.close()

class WaitTimeLib:
    def __init__(self, db_config):
        self.db_config = db_config
        self.db = mysql.connector.connect(**db_config)
        self.cursor = self.db.cursor()
        self.create_table()
        
    def create_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS wait_times (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stadsloket_id INT NOT NULL,
            waiting INT,
            waittime VARCHAR(255),
            timestamp DATETIME,
            INDEX idx_stadsloket_id (stadsloket_id)
        )
        """)

    def create_loket_names_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS loket_names (
            stadsloket_id INT NOT NULL,
            loket_name VARCHAR(255),
            PRIMARY KEY (stadsloket_id),
            CONSTRAINT fk_stadsloket
                FOREIGN KEY (stadsloket_id)
                REFERENCES wait_times(stadsloket_id)
        )
        """)

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
            self.cursor.execute("""
            INSERT INTO wait_times (stadsloket_id, waiting, waittime, timestamp)
            VALUES (%s, %s, %s, %s)
            """, (entry['id'], entry['waiting'], parsed_waittime, datetime.now()))
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
        # Each row has the pattern: <td data-title="Stadsloket"> X </td> ... id="nfwrtY"
        matches = re.findall(r'<td data-title="Stadsloket">\s*(.*?)</td>.*?id="nfwrt(\d+)"',
                             page_html, flags=re.DOTALL)
        # Create table if needed
        self.create_loket_names_table()
        # Store results
        for (name, loket_id) in matches:
            self.cursor.execute("""
            INSERT INTO loket_names (stadsloket_id, loket_name)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                loket_name = VALUES(loket_name)
            """, (loket_id, name.strip()))
        self.db.commit()

    def close(self):
        self.cursor.close()
        self.db.close()
