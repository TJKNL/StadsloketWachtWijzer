
import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection details from environment variables
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Hardcoded office coordinates to be added
# Correctly mapped to the new stadsloket_id
AMSTERDAM_OFFICES = {
    5: [52.3679375, 4.8969876],  # Centrum
    6: [52.3578312, 4.7999629],  # Nieuw-West
    7: [52.4008393, 4.9279192],  # Noord
    8: [52.356708, 4.9284181],  # Oost
    9: [52.3713686, 4.8349433],  # West
    10: [52.3404624, 4.8913578], # Zuid
    11: [52.3162788, 4.9538333]  # Zuidoost
}

def migrate():
    """
    Adds latitude and longitude columns to the stadsloket table
    and updates the coordinates for each office.
    """
    conn = None
    try:
        # Establish a connection to the database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Database connection established.")
        
        with conn.cursor() as cur:
            # Add latitude and longitude columns if they don't exist
            print("Altering table stadsloket to add latitude and longitude columns...")
            cur.execute("""
                ALTER TABLE stadsloket
                ADD COLUMN IF NOT EXISTS latitude NUMERIC(10, 7),
                ADD COLUMN IF NOT EXISTS longitude NUMERIC(10, 7);
            """)
            print("Table altered successfully or columns already exist.")

            # Update coordinates for each office
            print("Updating coordinates for each stadsloket...")
            for office_id, coords in AMSTERDAM_OFFICES.items():
                lat, lon = coords
                cur.execute(
                    "UPDATE stadsloket SET latitude = %s, longitude = %s WHERE stadsloket_id = %s",
                    (lat, lon, office_id)
                )
            print("Coordinates updated successfully.")
            
        # Commit the changes to the database
        conn.commit()
        print("Migration committed successfully.")
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
            
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    migrate() 