import os
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found in .env")


def get_connection():
    """
    Returns a new database connection.
    Uses dict_row so fetch results are dictionaries.
    """
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def test_connection():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                return cur.fetchone()
    except Exception as e:
        return str(e)