import os
from dotenv import load_dotenv
import psycopg

# Load environment variables from .env
load_dotenv()

# Get database connection string
DATABASE_URL = os.getenv("DATABASE_URL")


def get_connection():
    """
    Returns a new database connection.
    Used by API endpoints.
    """
    return psycopg.connect(DATABASE_URL)


def test_connection():
    """
    Simple test query to verify database connectivity.
    """
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                return cur.fetchone()
    except Exception as e:
        return str(e)