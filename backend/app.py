# Import FastAPI framework
from fastapi import FastAPI

# Import database functions
from db import test_connection, get_connection

# Create FastAPI app instance
app = FastAPI()


# -----------------------------
# ROOT ENDPOINT (Health Check)
# -----------------------------
@app.get("/")
def root():
    """
    Simple health check endpoint.
    Used to confirm the API server is running.
    """
    return {"message": "Golden OPAC API running"}


# -----------------------------
# DATABASE TEST ENDPOINT
# -----------------------------
@app.get("/test-db")
def test_db():
    """
    Tests connection to the Neon database.
    Returns basic response if successful.
    """
    result = test_connection()
    return {"database_response": result}


# -----------------------------
# BOOK SEARCH ENDPOINT (OPAC)
# -----------------------------
@app.get("/api/books")
def search_books(q: str = ""):
    """
    Search books by title.
    Query parameter:
        q -> search keyword
    Example:
        /api/books?q=harry
    """

    # Get database connection
    conn = get_connection()
    cur = conn.cursor()

    # Execute case-insensitive search
    cur.execute(
        """
        SELECT book_id, title, author, section
        FROM book
        WHERE title ILIKE %s
        """,
        (f"%{q}%",)
    )

    # Fetch all matching rows
    books = cur.fetchall()

    # Close connection
    cur.close()
    conn.close()

    # Convert tuples into JSON-friendly format
    return [
        {
            "book_id": b[0],
            "title": b[1],
            "author": b[2],
            "section": b[3]
        }
        for b in books
    ]