import requests
import mysql.connector
from datetime import datetime

class WaitTimeData:
    def __init__(self, db_config):
        self.db_config = db_config
        self.db = mysql.connector.connect(**db_config)
        self.cursor = self.db.cursor()
        self.create_table()

    def create_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS wait_times (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stadsloket_id INT,
            waiting INT,
            waittime VARCHAR(255),
            timestamp DATETIME
        )
        """)

    def fetch_data(self):
        response = requests.get('https://wachttijdenamsterdam.nl/data/')
        return response.json()

    def store_data(self, data):
        for entry in data:
            self.cursor.execute("""
            INSERT INTO wait_times (stadsloket_id, waiting, waittime, timestamp)
            VALUES (%s, %s, %s, %s)
            """, (entry['id'], entry['waiting'], entry['waittime'], datetime.now()))
        self.db.commit()

    def get_mean_wait_times(self):
        self.cursor.execute("""
        SELECT stadsloket_id, AVG(waiting) as mean_waiting
        FROM wait_times
        GROUP BY stadsloket_id
        """)
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.db.close()
