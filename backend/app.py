from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from security import verify_password, create_access_token, decode_token
from isbn_lookup import lookup_isbn
from datetime import datetime
from datetime import date, datetime, timedelta
from fastapi.responses import StreamingResponse
import csv
import io
from fastapi import UploadFile, File
import csv
import io
import re
import os
from fastapi import Body

from io import BytesIO
from fastapi.responses import Response

# ReportLab (PDF)
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.platypus import Image
from reportlab.lib.units import inch

from security import hash_password, verify_password

from dotenv import load_dotenv
load_dotenv()

from fastapi import Body, HTTPException
from fastapi.responses import Response
from io import BytesIO

from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, letter, legal

from reportlab.graphics.barcode import code128

from pydantic import BaseModel
from isbn_lookup import normalize_isbn  # <- import your helper
from datetime import datetime
import random
import string

from pydantic import BaseModel

class CheckoutRequest(BaseModel):
    barcode: str
    student_code: str

from typing import Optional

class BookUpdate(BaseModel):
    title: str
    author: str
    publisher: Optional[str] = None
    pub_year: Optional[int] = None
    genre: Optional[str] = None
    subject: Optional[str] = None
    section: Optional[str] = None
    cover_url: Optional[str] = None

import re
from fastapi import HTTPException

def generate_barcode(prefix: str = "GK") -> str:
    """
    Generates a reasonably unique barcode string.
    Example: GK-20260305-082233-483921
    """
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    rand = "".join(random.choices(string.digits, k=6))
    return f"{prefix}-{ts}-{rand}"


class AddCopiesRequest(BaseModel):
    isbn: str
    copies: int = 1

def normalize_grade(raw: str) -> str:
    """
    Converts inputs like:
      'GR. 8', 'Grade 8', '8', 'G8', 'gr 08'  -> '8'
    Returns '1'..'12' as strings.
    """
    if raw is None:
        raise HTTPException(status_code=400, detail="grade is required")

    s = str(raw).strip().upper()
    if not s:
        raise HTTPException(status_code=400, detail="grade is required")

    # extract the first number found
    m = re.search(r'(\d{1,2})', s)
    if not m:
        raise HTTPException(status_code=400, detail=f"Invalid grade: {raw}")

    g = int(m.group(1))
    if g < 1 or g > 12:
        raise HTTPException(status_code=400, detail=f"Grade must be 1-12 (got {g})")

    return str(g)

# Path to logo file 
SCHOOL_LOGO_PATH = "assets/school_logo.png"



# -----------------------------
# REPORT HELPERS
# -----------------------------
def parse_date_yyyy_mm_dd(value: str, field_name: str) -> date:
    """
    Parses a date string in YYYY-MM-DD format.
    Raises HTTP 400 if invalid.
    """
    try:
        return date.fromisoformat(value)
    except Exception:
        raise HTTPException(status_code=400, detail=f"{field_name} must be YYYY-MM-DD")

# Import FastAPI framework
from fastapi import FastAPI

# Import database functions
from db import test_connection, get_connection

# Create FastAPI app instance
app = FastAPI()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # dev only; later set to your GitHub Pages URL
    allow_credentials=True,
    allow_methods=["*"],  # includes OPTIONS, POST, GET
    allow_headers=["*"],  # includes Authorization, Content-Type
)

def get_current_librarian(token: str = Depends(oauth2_scheme)):
    try:
        payload = decode_token(token)
        librarian_id = int(payload["sub"])
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT librarian_id, username, email, first_name, last_name, is_active
        FROM librarian
        WHERE librarian_id = %s
        """,
        (librarian_id,)
    )
    librarian = cur.fetchone()

    cur.close()
    conn.close()

    if not librarian or not librarian["is_active"]:
        raise HTTPException(status_code=401, detail="Account not found or inactive")

    return librarian

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


@app.get("/api/books")
def search_books(q: str = ""):
    """
    Search books by title (Public OPAC).
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT book_id, title, author, section
        FROM book
        WHERE title ILIKE %s
        ORDER BY title ASC
        LIMIT 100
        """,
        (f"%{q}%",),
    )

    books = cur.fetchall()
    cur.close()
    conn.close()

    # ✅ dict-row safe
    return [
        {
            "book_id": b["book_id"],
            "title": b["title"],
            "author": b["author"],
            "section": b["section"],
        }
        for b in books
    ]

# -----------------------------
# LOGIN ENDPOINT 
# -----------------------------

@app.post("/auth/login")
def login(username: str, password: str):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT librarian_id, username, password_hash, is_active
        FROM librarian
        WHERE username = %s
        """,
        (username,)
    )
    user = cur.fetchone()

    cur.close()
    conn.close()

    if not user or not user["is_active"]:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    if not verify_password(password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_access_token(str(user["librarian_id"]))
    return {"access_token": token, "token_type": "bearer"}


@app.get("/auth/me")
def me(current=Depends(get_current_librarian)):
    return current



# -----------------------------
# BOOK DETAILS ENDPOINT (OPAC)
# -----------------------------
@app.get("/api/books/{book_id}")
def get_book_details(book_id: int):
    """
    Get a single book's details + availability counts.

    Returns:
      - book info
      - total copies
      - available copies (status='available')
    """

    conn = get_connection()
    cur = conn.cursor()

    # 1) Get the book row
    cur.execute(
        """
        SELECT
            book_id, title, author, publisher, pub_year,
            genre, subject, section, cover_url
        FROM book
        WHERE book_id = %s
        """,
        (book_id,)
    )
    book = cur.fetchone()

    if not book:
        cur.close()
        conn.close()
        return {"error": "Book not found"}

    # 2) Count copies + available copies (copy-level tracking)
    cur.execute(
        """
        SELECT
            COUNT(*) AS total_copies,
            COALESCE(SUM(CASE WHEN status = 'available' THEN 1 ELSE 0 END), 0) AS available_copies
        FROM book_copy
        WHERE book_id = %s
        """,
        (book_id,)
    )
    counts = cur.fetchone()

    cur.close()
    conn.close()

    # NOTE: If you applied row_factory=dict_row in db.py, `book` and `counts` are dicts.
    # If not, tell me and I'll adapt to tuple-indexing.
    return {
        "book_id": book["book_id"],
        "title": book["title"],
        "author": book["author"],
        "publisher": book["publisher"],
        "pub_year": book["pub_year"],
        "genre": book["genre"],
        "subject": book["subject"],
        "section": book["section"],
        "cover_url": book.get("cover_url"),
        "total_copies": counts["total_copies"],
        "available_copies": counts["available_copies"],
    }

# -----------------------------
# ISBN LOOKUP ENDPOINT (Cataloging helper)
# -----------------------------
@app.get("/api/isbn/{isbn}")
def isbn_lookup(isbn: str):
    """
    Lookup book metadata by ISBN using Google Books + Open Library.

    Librarian can type ISBN only, frontend calls this to autofill fields.
    """
    return lookup_isbn(isbn)


# -----------------------------
# CATALOGING PREVIEW (UX helper)
# -----------------------------
@app.get("/api/cataloging/preview/{isbn}")
def cataloging_preview(isbn: str):
    """
    UX helper:
    Librarian types ISBN -> frontend calls this -> gets autofill + missing fields.
    Frontend then allows librarian to edit missing values before final submit.
    """
    return lookup_isbn(isbn)


@app.get("/api/students/lookup")
def student_lookup(student_code: str, current=Depends(get_current_librarian)):
    code = (student_code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="student_code is required")

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT student_id, student_code, last_name, first_name, grade, section, status
            FROM student
            WHERE student_code = %s
            """,
            (code,),
        )
        s = cur.fetchone()
        if not s:
            raise HTTPException(status_code=404, detail="Student not found")

        return {
            "student_id": s["student_id"],
            "student_code": s["student_code"],
            "last_name": s["last_name"],
            "first_name": s["first_name"],
            "grade": s["grade"],
            "section": s["section"],
            "status": s["status"],
            "grade_section": f"{s['grade']} - {s['section']}" if s["grade"] or s["section"] else "",
        }
    finally:
        cur.close()
        conn.close()

# -----------------------------
# CATALOGING: ADD BOOK BY ISBN (Librarian Only)
# -----------------------------
from datetime import datetime
from uuid import uuid4
from fastapi import HTTPException, Depends

from fastapi import Form
from datetime import datetime

@app.post("/api/cataloging/add-book")
def add_book_by_isbn(
    isbn: str = Form(...),
    copies: int = Form(1),

    # Optional overrides (librarian can edit anything)
    title: str | None = Form(None),
    author: str | None = Form(None),
    publisher: str | None = Form(None),
    pub_year: int | None = Form(None),
    genre: str | None = Form(None),
    subject: str | None = Form(None),
    section: str | None = Form(None),

    # cover can be stored as a URL or a data URL (base64) from the frontend
    cover_url: str | None = Form(None),

    current=Depends(get_current_librarian),
):
    if copies < 1 or copies > 100:
        raise HTTPException(status_code=400, detail="copies must be between 1 and 100")

    meta = lookup_isbn(isbn)
    if not meta.get("found"):
        # still allow manual entry if you want:
        # meta = {"isbn": isbn, "found": True}
        raise HTTPException(status_code=404, detail="ISBN not found in Google Books/Open Library")

    # librarian overrides win, else API meta
    final_title = title or meta.get("title")
    final_author = author or meta.get("author")
    final_publisher = publisher or meta.get("publisher")
    final_pub_year = pub_year or meta.get("pub_year")
    final_genre = genre or meta.get("genre")
    final_subject = subject or meta.get("subject")
    final_section = section

    # cover priority: librarian upload/url override, else API cover
    final_cover = cover_url or meta.get("cover_url")

    missing_required = []
    if not final_title: missing_required.append("title")
    if not final_author: missing_required.append("author")
    if not final_section: missing_required.append("section")

    if missing_required:
        raise HTTPException(
            status_code=400,
            detail={"message": "Missing required fields", "missing": missing_required, "autofill": meta},
        )

    conn = get_connection()
    cur = conn.cursor()
    try:
        # Find existing book by ISBN
        cur.execute(
            """
            SELECT b.book_id
            FROM book b
            JOIN book_identifier bi ON bi.book_id = b.book_id
            WHERE bi.id_type = 'isbn' AND bi.id_value = %s
            ORDER BY bi.is_primary DESC
            LIMIT 1
            """,
            (meta["isbn"],),
        )
        row = cur.fetchone()
        existing_book_id = row["book_id"] if row else None

        book_id = existing_book_id

        if not book_id:
            catalog_key = f"ISBN:{meta['isbn']}"
            cur.execute(
                """
                INSERT INTO book (title, author, publisher, pub_year, genre, subject, section, catalog_key, cover_url, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                RETURNING book_id
                """,
                (
                    final_title, final_author, final_publisher, final_pub_year,
                    final_genre, final_subject, final_section, catalog_key, final_cover
                ),
            )
            book_id = cur.fetchone()["book_id"]

            cur.execute(
                """
                INSERT INTO book_identifier (book_id, id_type, id_value, is_primary, created_at)
                VALUES (%s, 'isbn', %s, TRUE, NOW())
                """,
                (book_id, meta["isbn"]),
            )
        else:
            # IMPORTANT: Update the BOOK record so ALL copies reflect the edited details
            cur.execute(
                """
                UPDATE book
                SET title=%s,
                    author=%s,
                    publisher=%s,
                    pub_year=%s,
                    genre=%s,
                    subject=%s,
                    section=%s,
                    cover_url=%s,
                    updated_at=NOW()
                WHERE book_id=%s
                """,
                (
                    final_title, final_author, final_publisher, final_pub_year,
                    final_genre, final_subject, final_section, final_cover, book_id
                ),
            )

        created = []
        unix_ts = int(datetime.utcnow().timestamp())

        for i in range(1, copies + 1):
            barcode = f"BK{book_id}-TS{unix_ts}-N{i}"
            cur.execute(
                """
                INSERT INTO book_copy (book_id, barcode, status, is_printed, created_at)
                VALUES (%s, %s, 'available', FALSE, NOW())
                RETURNING copy_id, barcode
                """,
                (book_id, barcode),
            )
            c = cur.fetchone()
            created.append({"copy_id": c["copy_id"], "barcode": c["barcode"]})

        conn.commit()

        # total copies after add
        cur.execute("SELECT COUNT(*) AS total FROM book_copy WHERE book_id=%s", (book_id,))
        total = cur.fetchone()["total"]

        return {
            "message": "Book saved successfully",
            "book_id": book_id,
            "isbn": meta["isbn"],
            "copies_created": len(created),
            "total_copies": int(total),
            "barcodes": created,
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to add book: {str(e)}")
    finally:
        cur.close()
        conn.close()

@app.get("/api/cataloging/isbn-exists/{isbn}")
def isbn_exists(isbn: str, current=Depends(get_current_librarian)):
    isbn_n = normalize_isbn(isbn)
    if not isbn_n:
        raise HTTPException(status_code=400, detail="Invalid ISBN")

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT b.book_id, b.title, b.author, b.cover_url,
                   COUNT(c.copy_id) AS total_copies,
                   COALESCE(SUM(CASE WHEN c.status='available' THEN 1 ELSE 0 END), 0) AS available_copies
            FROM book_identifier bi
            JOIN book b ON b.book_id = bi.book_id
            LEFT JOIN book_copy c ON c.book_id = b.book_id
            WHERE UPPER(COALESCE(bi.id_type,'')) = 'ISBN'
              AND bi.id_value = %s
            GROUP BY b.book_id, b.title, b.author, b.cover_url
            LIMIT 1
            """,
            (isbn_n,),
        )
        row = cur.fetchone()
        if not row:
            return {"exists": False}

        return {
            "exists": True,
            "book_id": row["book_id"],
            "title": row["title"],
            "author": row["author"],
            "cover_url": row.get("cover_url"),
            "total_copies": int(row["total_copies"] or 0),
            "available_copies": int(row["available_copies"] or 0),
        }
    finally:
        cur.close()
        conn.close()
from fastapi import HTTPException, Depends
from datetime import datetime

@app.get("/api/cataloging/isbn-info/{isbn}")
def cataloging_isbn_info(
    isbn: str,
    current=Depends(get_current_librarian),
):
    """
    Returns whether ISBN exists in the system.
    If exists: returns book fields + total copies.
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT b.book_id, b.title, b.author, b.publisher, b.pub_year, b.genre, b.subject, b.section, b.cover_url
            FROM book b
            JOIN book_identifier bi ON bi.book_id = b.book_id
            WHERE bi.id_type='isbn' AND bi.id_value=%s
            ORDER BY bi.is_primary DESC
            LIMIT 1
            """,
            (isbn,),
        )
        row = cur.fetchone()
        if not row:
            return {"exists": False}

        cur.execute("SELECT COUNT(*) AS total FROM book_copy WHERE book_id=%s", (row["book_id"],))
        total = cur.fetchone()["total"]

        return {
            "exists": True,
            "book": {
                "book_id": row["book_id"],
                "title": row["title"],
                "author": row["author"],
                "publisher": row["publisher"],
                "pub_year": row["pub_year"],
                "genre": row["genre"],
                "subject": row["subject"],
                "section": row["section"],
                "cover_url": row["cover_url"],
            },
            "total_copies": int(total),
        }
    finally:
        cur.close()
        conn.close()

@app.post("/api/cataloging/add-copies")
def add_copies_only(req: AddCopiesRequest, current=Depends(get_current_librarian)):
    isbn_n = normalize_isbn(req.isbn)
    if not isbn_n:
        raise HTTPException(status_code=400, detail="Invalid ISBN")

    copies = int(req.copies or 1)
    if copies < 1 or copies > 100:
        raise HTTPException(status_code=400, detail="copies must be between 1 and 100")

    conn = get_connection()
    cur = conn.cursor()
    try:
        # Find existing book by ISBN identifier
        cur.execute(
            """
            SELECT bi.book_id
            FROM book_identifier bi
            WHERE UPPER(COALESCE(bi.id_type,'')) = 'ISBN'
              AND bi.id_value = %s
            ORDER BY bi.is_primary DESC, bi.identifier_id ASC
            LIMIT 1
            """,
            (isbn_n,),
        )
        bi = cur.fetchone()
        if not bi:
            raise HTTPException(status_code=404, detail="ISBN not found in system. Use add-book first.")

        book_id = bi["book_id"]

        created = []
        for _ in range(copies):
            # retry a few times in case of barcode collision
            for _try in range(8):
                barcode = generate_barcode("GK")
                try:
                    cur.execute(
                        """
                        INSERT INTO book_copy (book_id, barcode, status)
                        VALUES (%s, %s, 'available')
                        RETURNING copy_id, barcode
                        """,
                        (book_id, barcode),
                    )
                    row = cur.fetchone()
                    created.append({"copy_id": row["copy_id"], "barcode": row["barcode"]})
                    break
                except Exception:
                    # possible UNIQUE collision; retry
                    conn.rollback()
                    continue

        conn.commit()

        return {
            "book_id": book_id,
            "created_copies": len(created),
            "barcodes": created
        }
    finally:
        cur.close()
        conn.close()

# -----------------------------
# LIBRARIAN: SMART SEARCH BOOKS (search any field)
# -----------------------------
@app.get("/api/librarian/books")
def librarian_search_books(
    q: str = "",
    current=Depends(get_current_librarian),
):
    """
    Librarian-only universal search.

    Searches across:
      - title
      - author
      - publisher
      - genre
      - subject
      - section
      - ISBN (book_identifier)
      - catalog_key

    If q is empty:
      - Returns latest 100 books
    """

    conn = get_connection()
    cur = conn.cursor()

    try:
        if q.strip() == "":
            # If no search query, return latest books
            cur.execute(
                """
                SELECT
                    b.book_id,
                    b.title,
                    b.author,
                    b.publisher,
                    b.genre,
                    b.section,
                    b.cover_url,

                    COUNT(bc.copy_id) AS total_copies,
                    COALESCE(SUM(CASE WHEN bc.status = 'available' THEN 1 ELSE 0 END), 0) AS available_copies,
                    COALESCE(SUM(CASE WHEN bc.is_printed IS TRUE THEN 1 ELSE 0 END), 0) AS printed_copies,
                    COALESCE(SUM(CASE WHEN bc.is_printed IS NOT TRUE THEN 1 ELSE 0 END), 0) AS unprinted_copies
                FROM book b
                LEFT JOIN book_copy bc ON bc.book_id = b.book_id
                GROUP BY b.book_id
                ORDER BY b.created_at DESC
                LIMIT 100
                """
            )
        else:
            search = f"%{q}%"

            cur.execute(
                """
                SELECT
                    b.book_id,
                    b.title,
                    b.author,
                    b.publisher,
                    b.genre,
                    b.section,
                    b.cover_url,

                    COUNT(bc.copy_id) AS total_copies,
                    COALESCE(SUM(CASE WHEN bc.status = 'available' THEN 1 ELSE 0 END), 0) AS available_copies,
                    COALESCE(SUM(CASE WHEN bc.is_printed IS TRUE THEN 1 ELSE 0 END), 0) AS printed_copies,
                    COALESCE(SUM(CASE WHEN bc.is_printed IS NOT TRUE THEN 1 ELSE 0 END), 0) AS unprinted_copies
                FROM book b
                LEFT JOIN book_copy bc ON bc.book_id = b.book_id
                LEFT JOIN book_identifier bi ON bi.book_id = b.book_id

                WHERE
                    b.title ILIKE %s OR
                    b.author ILIKE %s OR
                    b.publisher ILIKE %s OR
                    b.genre ILIKE %s OR
                    b.subject ILIKE %s OR
                    b.section ILIKE %s OR
                    b.catalog_key ILIKE %s OR
                    (bi.id_type = 'isbn' AND bi.id_value ILIKE %s)

                GROUP BY b.book_id
                ORDER BY b.title ASC
                LIMIT 100
                """,
                (
                    search, search, search, search,
                    search, search, search, search
                ),
            )

        return cur.fetchall()

    finally:
        cur.close()
        conn.close()

# -----------------------------
# LIBRARIAN: FILTER COPIES (only_unprinted toggle support)
# -----------------------------
@app.get("/api/librarian/books/{book_id}/copies")
def librarian_get_copies(
    book_id: int,
    only_unprinted: bool = False,
    current=Depends(get_current_librarian),
):
    """
    Librarian-only copy list.
    Query:
      - only_unprinted=true => returns only copies where is_printed = false
    """

    conn = get_connection()
    cur = conn.cursor()

    try:
        if only_unprinted:
            cur.execute(
                """
                SELECT copy_id, barcode, status, is_printed, printed_at, reprint_count
                FROM book_copy
                WHERE book_id = %s AND is_printed = FALSE
                ORDER BY copy_id ASC
                """,
                (book_id,),
            )
        else:
            cur.execute(
                """
                SELECT copy_id, barcode, status, is_printed, printed_at, reprint_count
                FROM book_copy
                WHERE book_id = %s
                ORDER BY copy_id ASC
                """,
                (book_id,),
            )

        return cur.fetchall()

    finally:
        cur.close()
        conn.close()

# -----------------------------
# PRINTING: MARK SELECTED COPIES AS PRINTED (Librarian Only)
# -----------------------------
@app.post("/api/printing/mark-printed")
def mark_copies_as_printed(
    copy_ids: str,
    batch_id: str | None = None,
    current=Depends(get_current_librarian),
):
    """
    Librarian-only.
    Used by the "Mark Selected as Printed" button.

    Inputs (query params for now):
      - copy_ids: comma-separated list of copy_id values, e.g. "12,13,14"
      - batch_id: optional string to group a print batch (frontend can generate)

    Behavior:
      - Updates book_copy.is_printed = TRUE
      - Sets printed_at = NOW()
      - Sets printed_by = current librarian
      - If already printed, increments reprint_count
      - Inserts print_log rows for auditing
    """

    # --- Validate + parse copy_ids ---
    raw = [x.strip() for x in copy_ids.split(",") if x.strip()]
    if not raw:
        raise HTTPException(status_code=400, detail="copy_ids is required")

    try:
        ids = [int(x) for x in raw]
    except ValueError:
        raise HTTPException(status_code=400, detail="copy_ids must be integers separated by commas")

    # --- DB ---
    conn = get_connection()
    cur = conn.cursor()

    try:
        librarian_id = current["librarian_id"]

        updated = []
        for cid in ids:
            # 1) Read current print status
            cur.execute(
                """
                SELECT copy_id, is_printed, reprint_count
                FROM book_copy
                WHERE copy_id = %s
                """,
                (cid,),
            )
            copy_row = cur.fetchone()
            if not copy_row:
                # Skip missing copy_id instead of failing everything
                continue

            already_printed = bool(copy_row["is_printed"])
            current_reprint = copy_row["reprint_count"] or 0

            # 2) Update book_copy
            if already_printed:
                # Reprint: increment reprint_count
                cur.execute(
                    """
                    UPDATE book_copy
                    SET
                        is_printed = TRUE,
                        printed_at = NOW(),
                        printed_by = %s,
                        reprint_count = %s
                    WHERE copy_id = %s
                    """,
                    (librarian_id, current_reprint + 1, cid),
                )
                action = "reprint"
            else:
                # First time print
                cur.execute(
                    """
                    UPDATE book_copy
                    SET
                        is_printed = TRUE,
                        printed_at = NOW(),
                        printed_by = %s
                    WHERE copy_id = %s
                    """,
                    (librarian_id, cid),
                )
                action = "print"

            # 3) Insert print_log row (audit trail)
            cur.execute(
                """
                INSERT INTO print_log (copy_id, printed_by, printed_at, action, batch_id)
                VALUES (%s, %s, NOW(), %s, %s)
                """,
                (cid, librarian_id, action, batch_id),
            )

            updated.append({"copy_id": cid, "action": action})

        conn.commit()

        return {
            "message": "Marked selected copies as printed",
            "batch_id": batch_id,
            "updated_count": len(updated),
            "updated": updated,
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to mark printed: {str(e)}")
    finally:
        cur.close()
        conn.close()

# -----------------------------
# SETTINGS: GRADE RULES (Librarian Only)
# -----------------------------
@app.get("/api/settings/grade-rules")
def list_grade_rules(current=Depends(get_current_librarian)):
    """
    Lists all grade rules.
    The frontend can display this in Settings -> Circulation Settings.
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT rule_id, grade, loan_period_days, max_borrow_limit, fine_per_day,
                   max_renewals, block_renew_if_overdue, updated_at
            FROM grade_rule
            ORDER BY grade ASC, updated_at DESC NULLS LAST
            """
        )
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


@app.post("/api/settings/grade-rules")
def upsert_grade_rule(
    grade: str,
    loan_period_days: int,
    max_borrow_limit: int,
    fine_per_day: float,
    max_renewals: int = 1,
    block_renew_if_overdue: bool = True,
    current=Depends(get_current_librarian),
):
    """
    Creates or updates a grade rule.

    NOTE:
    - We keep rule history by inserting new rows (simple audit trail).
    - Checkout will always pick the latest updated rule for the grade.
    """
    # Basic validation
    if not grade.strip():
        raise HTTPException(status_code=400, detail="grade is required")
    if loan_period_days < 1:
        raise HTTPException(status_code=400, detail="loan_period_days must be >= 1")
    if max_borrow_limit < 1:
        raise HTTPException(status_code=400, detail="max_borrow_limit must be >= 1")
    if fine_per_day < 0:
        raise HTTPException(status_code=400, detail="fine_per_day must be >= 0")
    if max_renewals < 0:
        raise HTTPException(status_code=400, detail="max_renewals must be >= 0")

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO grade_rule (grade, loan_period_days, max_borrow_limit, fine_per_day,
                                   max_renewals, block_renew_if_overdue, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            RETURNING rule_id
            """,
            (grade.strip(), loan_period_days, max_borrow_limit, fine_per_day, max_renewals, block_renew_if_overdue),
        )
        rule_id = cur.fetchone()["rule_id"]
        conn.commit()
        return {"message": "Grade rule saved", "rule_id": rule_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to save grade rule: {str(e)}")
    finally:
        cur.close()
        conn.close()

# -----------------------------
# STUDENTS: ADD + SEARCH + VIEW (Librarian Only)
# -----------------------------
@app.post("/api/students")
def add_student(
    student_code: str,
    last_name: str,
    first_name: str,
    grade: str,
    section: str,
    status: str = "active",
    current=Depends(get_current_librarian),
):
    """
    Adds a student.
    student_code is the barcode value that will be scanned during checkout.
    """
    # Validation
    if not student_code.strip():
        raise HTTPException(status_code=400, detail="student_code is required")
    if not last_name.strip() or not first_name.strip():
        raise HTTPException(status_code=400, detail="student name is required")
    if not grade.strip() or not section.strip():
        raise HTTPException(status_code=400, detail="grade and section are required")

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO student (student_code, last_name, first_name, grade, section, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            RETURNING student_id
            """,
            (student_code.strip(), last_name.strip(), first_name.strip(), grade.strip(), section.strip(), status.strip()),
        )
        student_id = cur.fetchone()["student_id"]
        conn.commit()
        return {"message": "Student added", "student_id": student_id}
    except Exception as e:
        conn.rollback()
        # Most common: duplicate student_code (unique constraint)
        raise HTTPException(status_code=500, detail=f"Failed to add student: {str(e)}")
    finally:
        cur.close()
        conn.close()


@app.get("/api/students")
def search_students(q: str = "", current=Depends(get_current_librarian)):
    """
    Search students by:
      - last name
      - first name
      - student_code
      - grade
      - section
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        if q.strip() == "":
            cur.execute(
                """
                SELECT student_id, student_code, last_name, first_name, grade, section, status
                FROM student
                ORDER BY last_name ASC
                LIMIT 50
                """
            )
        else:
            s = f"%{q}%"
            cur.execute(
                """
                SELECT student_id, student_code, last_name, first_name, grade, section, status
                FROM student
                WHERE
                    student_code ILIKE %s OR
                    last_name ILIKE %s OR
                    first_name ILIKE %s OR
                    grade ILIKE %s OR
                    section ILIKE %s
                ORDER BY last_name ASC
                LIMIT 50
                """,
                (s, s, s, s, s),
            )

        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


@app.get("/api/students/{student_id}")
def get_student(student_id: int, current=Depends(get_current_librarian)):
    """
    Student profile basic info (we’ll add loans/fines/history next).
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT student_id, student_code, last_name, first_name, grade, section, status, created_at
            FROM student
            WHERE student_id = %s
            """,
            (student_id,),
        )
        s = cur.fetchone()
        if not s:
            raise HTTPException(status_code=404, detail="Student not found")
        return s
    finally:
        cur.close()
        conn.close()

# -----------------------------
# CIRCULATION: CHECK OUT (Librarian Only)
# -----------------------------
@app.post("/api/circulation/checkout")
def checkout_book(req: CheckoutRequest, current=Depends(get_current_librarian)):
    barcode = req.barcode
    student_code = req.student_code
    """
    Librarian-only checkout.

    Inputs (query params for now):
      - barcode: scanned book_copy.barcode
      - student_code: scanned student.student_code

    Behavior:
      1) Find copy by barcode; must be 'available'
      2) Find student by student_code; must be 'active'
      3) Load grade_rule for student's grade (loan period, limit, fine, max renewals)
      4) Validate:
          - no unpaid fines
          - no overdue loans
          - not exceeded borrow limit
      5) Create loan with due_at
      6) Update book_copy.status = 'borrowed'
    """

    conn = get_connection()
    cur = conn.cursor()

    try:
        librarian_id = current["librarian_id"]

        # --- 1) Find copy by barcode ---
        cur.execute(
            """
            SELECT copy_id, book_id, status
            FROM book_copy
            WHERE barcode = %s
            """,
            (barcode,),
        )
        copy_row = cur.fetchone()
        if not copy_row:
            raise HTTPException(status_code=404, detail="Copy barcode not found")

        if copy_row["status"] != "available":
            raise HTTPException(status_code=400, detail=f"Copy is not available (status={copy_row['status']})")

        copy_id = copy_row["copy_id"]
        book_id = copy_row["book_id"]

        # --- 2) Find student by student_code ---
        cur.execute(
            """
            SELECT student_id, student_code, grade, section, status, last_name, first_name
            FROM student
            WHERE student_code = %s
            """,
            (student_code,),
        )
        student = cur.fetchone()
        if not student:
            raise HTTPException(status_code=404, detail="Student not found")

        if (student["status"] or "").lower() != "active":
            raise HTTPException(status_code=400, detail=f"Student is not active (status={student['status']})")

        student_id = student["student_id"]
        grade_raw = student["grade"]
        grade = normalize_grade(grade_raw)   # <— normalize to "12"

        cur.execute("""
        SELECT loan_period_days, max_borrow_limit, fine_per_day, max_renewals, block_renew_if_overdue
        FROM grade_rule
        WHERE grade = %s
        ORDER BY updated_at DESC NULLS LAST
        LIMIT 1
        """, (grade,))
        rule = cur.fetchone()

        # Fallback to global defaults if no per-grade rule exists
        if not rule:
            cur.execute("""
            SELECT loan_period_days, max_borrow_limit, fine_per_day, max_renewals, block_renew_if_overdue
            FROM system_settings
            ORDER BY updated_at DESC, settings_id DESC
            LIMIT 1
            """)
            rule = cur.fetchone()

        if not rule:
            raise HTTPException(status_code=400, detail=f"No grade_rule for grade={grade} and no system_settings found")

        loan_period_days = rule["loan_period_days"]
        max_borrow_limit = rule["max_borrow_limit"]

        if not loan_period_days or loan_period_days < 1:
            raise HTTPException(status_code=400, detail="Invalid loan_period_days in grade_rule")
        if not max_borrow_limit or max_borrow_limit < 1:
            raise HTTPException(status_code=400, detail="Invalid max_borrow_limit in grade_rule")

        # --- 4A) Block if student has unpaid fines ---
        cur.execute(
            """
            SELECT COALESCE(SUM(amount - amount_paid), 0) AS outstanding
            FROM fine
            WHERE student_id = %s AND status = 'unpaid'
            """,
            (student_id,),
        )
        fine_row = cur.fetchone()
        outstanding = float(fine_row["outstanding"] or 0)
        if outstanding > 0:
            raise HTTPException(
                status_code=400,
                detail={"message": "Student has unpaid fines", "outstanding": outstanding},
            )

        # --- 4B) Block if student has overdue loans ---
        cur.execute(
            """
            SELECT COUNT(*) AS overdue_count
            FROM loan
            WHERE student_id = %s
              AND returned_at IS NULL
              AND due_at < NOW()
            """,
            (student_id,),
        )
        overdue_row = cur.fetchone()
        if int(overdue_row["overdue_count"]) > 0:
            raise HTTPException(status_code=400, detail="Student has overdue books")

        # --- 4C) Block if reached max borrow limit ---
        cur.execute(
            """
            SELECT COUNT(*) AS active_loans
            FROM loan
            WHERE student_id = %s AND returned_at IS NULL
            """,
            (student_id,),
        )
        active_row = cur.fetchone()
        active_loans = int(active_row["active_loans"])
        if active_loans >= int(max_borrow_limit):
            raise HTTPException(
                status_code=400,
                detail={"message": "Max borrow limit reached", "active_loans": active_loans, "limit": max_borrow_limit},
            )

        # --- 5) Create loan with due_at based on grade rule ---
        cur.execute(
            """
            INSERT INTO loan (student_id, copy_id, processed_by, borrowed_at, due_at, status, renew_count)
            VALUES (%s, %s, %s, NOW(), NOW() + (%s || ' days')::interval, 'borrowed', 0)
            RETURNING loan_id, borrowed_at, due_at
            """,
            (student_id, copy_id, librarian_id, loan_period_days),
        )
        loan_row = cur.fetchone()

        # --- 6) Update copy status to borrowed ---
        cur.execute(
            """
            UPDATE book_copy
            SET status = 'borrowed'
            WHERE copy_id = %s
            """,
            (copy_id,),
        )

        conn.commit()

        return {
            "message": "Checkout successful",
            "loan": {
                "loan_id": loan_row["loan_id"],
                "student_id": student_id,
                "copy_id": copy_id,
                "book_id": book_id,
                "borrowed_at": str(loan_row["borrowed_at"]),
                "due_at": str(loan_row["due_at"]),
                "status": "borrowed",
            },
            "student": {
                "student_code": student["student_code"],
                "last_name": student["last_name"],
                "first_name": student["first_name"],
                "grade": student["grade"],
                "section": student["section"],
            },
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Checkout failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

# -----------------------------
# CIRCULATION: RENEW LOAN (Librarian Only)
# -----------------------------
@app.post("/api/circulation/renew")
def renew_loan(
    loan_id: int,
    current=Depends(get_current_librarian),
):
    """
    Renews a borrowed book.

    Rules (from grade_rule):
      - max_renewals: max times a loan can be renewed
      - block_renew_if_overdue: if TRUE, disallow renew when due_at < NOW()

    Behavior:
      - loan must exist and not returned
      - extend due_at by loan_period_days
      - increment renew_count
      - update last_renewed_at
    """

    conn = get_connection()
    cur = conn.cursor()

    try:
        # 1) Get the loan + student grade (needed for grade_rule)
        cur.execute(
            """
            SELECT
                l.loan_id, l.student_id, l.copy_id, l.due_at, l.returned_at,
                l.renew_count,
                s.grade
            FROM loan l
            JOIN student s ON s.student_id = l.student_id
            WHERE l.loan_id = %s
            """,
            (loan_id,),
        )
        loan = cur.fetchone()
        if not loan:
            raise HTTPException(status_code=404, detail="Loan not found")

        if loan["returned_at"] is not None:
            raise HTTPException(status_code=400, detail="Cannot renew: book already returned")

        grade = (loan["grade"] or "").strip()

        # 2) Load grade rule
        cur.execute(
            """
            SELECT loan_period_days, max_renewals, block_renew_if_overdue
            FROM grade_rule
            WHERE grade = %s
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
            """,
            (grade,),
        )
        rule = cur.fetchone()
        if not rule:
            cur.execute("""
            SELECT loan_period_days, max_borrow_limit, fine_per_day, max_renewals, block_renew_if_overdue
            FROM system_settings
            ORDER BY updated_at DESC, settings_id DESC
            LIMIT 1
            """)
            rule = cur.fetchone()
        if not rule:
            raise HTTPException(status_code=400, detail="No grade_rule and no system_settings found")

        loan_period_days = int(rule["loan_period_days"])
        max_renewals = int(rule["max_renewals"] or 0)
        block_if_overdue = bool(rule["block_renew_if_overdue"])

        # 3) Enforce renew limits
        current_renew_count = int(loan["renew_count"] or 0)
        if current_renew_count >= max_renewals:
            raise HTTPException(
                status_code=400,
                detail={"message": "Max renewals reached", "renew_count": current_renew_count, "max_renewals": max_renewals},
            )

        # 4) Block if overdue (configurable)
        if block_if_overdue:
            cur.execute("SELECT (NOW() > %s) AS is_overdue", (loan["due_at"],))
            overdue_flag = cur.fetchone()
            if overdue_flag and overdue_flag["is_overdue"]:
                raise HTTPException(status_code=400, detail="Cannot renew: loan is already overdue")

        # 5) Renew: extend due_at by loan_period_days
        cur.execute(
            """
            UPDATE loan
            SET
                due_at = due_at + (%s || ' days')::interval,
                renew_count = renew_count + 1,
                last_renewed_at = NOW()
            WHERE loan_id = %s
            RETURNING loan_id, due_at, renew_count, last_renewed_at
            """,
            (loan_period_days, loan_id),
        )
        updated = cur.fetchone()

        conn.commit()

        return {
            "message": "Renew successful",
            "loan_id": updated["loan_id"],
            "new_due_at": str(updated["due_at"]),
            "renew_count": updated["renew_count"],
            "last_renewed_at": str(updated["last_renewed_at"]),
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Renew failed: {str(e)}")
    finally:
        cur.close()
        conn.close()


# -----------------------------
# CIRCULATION: CHECK IN (Librarian Only)
# -----------------------------
@app.get("/api/circulation/checkin/lookup")
def checkin_lookup(
    barcode: str,
    current=Depends(get_current_librarian),
):
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Copy + Book (REMOVED ISBN)
        cur.execute(
            """
            SELECT
              bc.copy_id, bc.barcode, bc.status AS copy_status,
              b.book_id, b.title, b.author
            FROM book_copy bc
            JOIN book b ON b.book_id = bc.book_id
            WHERE bc.barcode = %s
            """,
            (barcode,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Copy barcode not found")

        # Active loan (if borrowed)
        cur.execute(
            """
            SELECT
              l.loan_id, l.borrowed_at, l.due_at,
              s.student_id, s.student_code, s.last_name, s.first_name, s.grade, s.section
            FROM loan l
            JOIN student s ON s.student_id = l.student_id
            WHERE l.copy_id = %s AND l.returned_at IS NULL
            ORDER BY l.borrowed_at DESC
            LIMIT 1
            """,
            (row["copy_id"],),
        )
        loan = cur.fetchone()

        # Base book payload
        book_payload = {
            "book_id": row["book_id"],
            "title": row["title"],
            "author": row["author"],
            # IMPORTANT: always include this key so frontend won't crash
            "student": None,
        }

        if not loan:
            return {
                "barcode": row["barcode"],
                "copy_status": row["copy_status"],
                "book": book_payload,
                "has_active_loan": False,
                "message": "No active loan for this barcode (already returned or never borrowed).",
            }

        # Overdue calc + projected fine
        cur.execute("SELECT (NOW() > %s) AS is_overdue", (loan["due_at"],))
        is_overdue = bool(cur.fetchone()["is_overdue"])

        cur.execute(
            "SELECT GREATEST(0, (DATE(NOW()) - DATE(%s))) AS overdue_days",
            (loan["due_at"],),
        )
        overdue_days = int(cur.fetchone()["overdue_days"])

        cur.execute(
            """
            SELECT fine_per_day
            FROM grade_rule
            WHERE grade = %s
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
            """,
            (loan["grade"],),
        )
        rule = cur.fetchone()

        # Use grade_rule if present, otherwise fallback to system_settings
        if rule and rule.get("fine_per_day") is not None and float(rule["fine_per_day"]) > 0:
            fine_per_day = float(rule["fine_per_day"])
        else:
            cur.execute(
                """
                SELECT fine_per_day
                FROM system_settings
                ORDER BY updated_at DESC
                LIMIT 1
                """
            )
            settings = cur.fetchone()
            fine_per_day = float(settings["fine_per_day"]) if settings and settings.get("fine_per_day") is not None else 0.0

        projected_fine = round(overdue_days * fine_per_day, 2) if overdue_days > 0 else 0.0

        # Unpaid fines list
        cur.execute(
            """
            SELECT
              fine_id, amount, amount_paid,
              (amount - amount_paid) AS outstanding,
              status, reason, assessed_at
            FROM fine
            WHERE student_id = %s AND status = 'unpaid'
            ORDER BY assessed_at DESC
            """,
            (loan["student_id"],),
        )
        unpaid = cur.fetchall()

        full_name = f"{(loan.get('last_name') or '').strip()}, {(loan.get('first_name') or '').strip()}".strip(", ").strip()

        # Student payload
        student_payload = {
            "student_id": loan["student_id"],
            "student_code": loan["student_code"],
            "name": full_name,
            "last_name": loan["last_name"],
            "first_name": loan["first_name"],
            "grade": loan["grade"],
            "section": loan["section"],
        }

        # IMPORTANT: also embed inside book for frontend compatibility
        book_payload["student"] = {"name": full_name}

        return {
            "barcode": row["barcode"],
            "copy_status": row["copy_status"],
            "book": book_payload,
            "has_active_loan": True,
            "loan": {
                "loan_id": loan["loan_id"],
                "borrowed_at": loan["borrowed_at"].isoformat() if loan.get("borrowed_at") else None,
                "due_at": loan["due_at"].isoformat() if loan.get("due_at") else None,
                "is_overdue": is_overdue,
                "overdue_days": overdue_days,
                "fine_per_day": fine_per_day,
                "projected_fine": projected_fine,
            },
            "student": student_payload,
            "unpaid_fines": unpaid,
        }

    finally:
        cur.close()
        conn.close()

# -----------------------------
# STUDENTS: PROFILE (Librarian Only)
# -----------------------------
@app.get("/api/students/{student_id}/profile")
def student_profile(student_id: int, current=Depends(get_current_librarian)):
    """
    Returns a student profile view:
      - basic info
      - current borrowed books
      - borrow history
      - outstanding fine summary
      - fine history
      - damage records
      - overdue count display
    """

    conn = get_connection()
    cur = conn.cursor()

    try:
        # --- Basic student info ---
        cur.execute(
            """
            SELECT student_id, student_code, last_name, first_name, grade, section, status, created_at, updated_at
            FROM student
            WHERE student_id = %s
            """,
            (student_id,),
        )
        student = cur.fetchone()
        if not student:
            raise HTTPException(status_code=404, detail="Student not found")

        # --- Current borrowed (active loans) ---
        cur.execute(
            """
            SELECT
                l.loan_id,
                l.borrowed_at,
                l.due_at,
                l.renew_count,
                bc.copy_id,
                bc.barcode,
                b.book_id,
                b.title,
                b.author,
                (l.due_at < NOW()) AS is_overdue
            FROM loan l
            JOIN book_copy bc ON bc.copy_id = l.copy_id
            JOIN book b ON b.book_id = bc.book_id
            WHERE l.student_id = %s AND l.returned_at IS NULL
            ORDER BY l.borrowed_at DESC
            """,
            (student_id,),
        )
        current_loans = cur.fetchall()

        # --- Overdue count display ---
        overdue_count = sum(1 for x in current_loans if x["is_overdue"])

        # --- Borrow history ---
        cur.execute(
            """
            SELECT
                l.loan_id,
                l.borrowed_at,
                l.due_at,
                l.returned_at,
                l.status,
                l.renew_count,
                bc.barcode,
                b.title,
                b.author
            FROM loan l
            JOIN book_copy bc ON bc.copy_id = l.copy_id
            JOIN book b ON b.book_id = bc.book_id
            WHERE l.student_id = %s
            ORDER BY l.borrowed_at DESC
            LIMIT 200
            """,
            (student_id,),
        )
        borrow_history = cur.fetchall()

        # --- Outstanding fine summary ---
        cur.execute(
            """
            SELECT COALESCE(SUM(amount - amount_paid), 0) AS outstanding
            FROM fine
            WHERE student_id = %s AND status = 'unpaid'
            """,
            (student_id,),
        )
        outstanding = float(cur.fetchone()["outstanding"] or 0)

        # --- Fine history ---
        cur.execute(
            """
            SELECT fine_id, loan_id, amount, amount_paid, status, reason, assessed_at
            FROM fine
            WHERE student_id = %s
            ORDER BY assessed_at DESC
            LIMIT 200
            """,
            (student_id,),
        )
        fine_history = cur.fetchall()

        # --- Damage records ---
        cur.execute(
            """
            SELECT
                d.damage_id,
                d.copy_id,
                d.severity,
                d.notes,
                d.reported_at,
                bc.barcode,
                b.title
            FROM damage_report d
            JOIN book_copy bc ON bc.copy_id = d.copy_id
            JOIN book b ON b.book_id = bc.book_id
            WHERE d.student_id = %s
            ORDER BY d.reported_at DESC
            LIMIT 200
            """,
            (student_id,),
        )
        damage_records = cur.fetchall()

        return {
            "student": student,
            "summary": {
                "current_borrowed_count": len(current_loans),
                "overdue_count": overdue_count,
                "outstanding_fines": outstanding,
            },
            "current_loans": current_loans,
            "borrow_history": borrow_history,
            "fine_history": fine_history,
            "damage_records": damage_records,
        }

    finally:
        cur.close()
        conn.close()


# -----------------------------
# REPORTS: DASHBOARD (Librarian Only) - Upgraded
# -----------------------------
@app.get("/api/reports/dashboard")
def reports_dashboard(
    date_from: str | None = None,
    date_to: str | None = None,
    current=Depends(get_current_librarian),
):
    """
    Dashboard analytics for Reports Module (Upgraded).

    Optional filters:
      - date_from (YYYY-MM-DD)
      - date_to (YYYY-MM-DD)

    Adds:
      - copy_status_distribution
      - barcode_print_status
      - fine_analytics (outstanding + collected this month)
      - overdue_by_grade / overdue_by_section
      - daily_borrow_trend (last 30 days)
    """

    conn = get_connection()
    cur = conn.cursor()

    try:
        # --------- Date filter handling (optional) ----------
        # Used for TOP lists and trends (not for total books/copies).
        date_where = "TRUE"
        date_params: list = []

        if date_from and date_to:
            date_where = "borrowed_at::date BETWEEN %s::date AND %s::date"
            date_params = [date_from, date_to]
        elif date_from:
            date_where = "borrowed_at::date >= %s::date"
            date_params = [date_from]
        elif date_to:
            date_where = "borrowed_at::date <= %s::date"
            date_params = [date_to]

        # --- Total students (users) ---
        cur.execute("SELECT COUNT(*) AS total_students FROM student;")
        total_students = int(cur.fetchone()["total_students"])

        # --- Total books (bibliographic records) ---
        cur.execute("SELECT COUNT(*) AS total_books FROM book;")
        total_books = int(cur.fetchone()["total_books"])

        # --- Total copies (physical copies) ---
        cur.execute("SELECT COUNT(*) AS total_copies FROM book_copy;")
        total_copies = int(cur.fetchone()["total_copies"])

        # --- Active loans (not returned) ---
        cur.execute("SELECT COUNT(*) AS active_loans FROM loan WHERE returned_at IS NULL;")
        active_loans = int(cur.fetchone()["active_loans"])

        # --- Overdue loans (not returned AND due_at < now) ---
        cur.execute(
            """
            SELECT COUNT(*) AS overdue_loans
            FROM loan
            WHERE returned_at IS NULL AND due_at < NOW();
            """
        )
        overdue_loans = int(cur.fetchone()["overdue_loans"])

        # -----------------------------
        # NEW: Copy status distribution
        # -----------------------------
        cur.execute(
            """
            SELECT
                COALESCE(NULLIF(TRIM(status), ''), 'unknown') AS status,
                COUNT(*)::int AS count
            FROM book_copy
            GROUP BY COALESCE(NULLIF(TRIM(status), ''), 'unknown')
            ORDER BY count DESC;
            """
        )
        copy_status_distribution = cur.fetchall()

        # -----------------------------
        # NEW: Barcode printed status
        # -----------------------------
        cur.execute(
            """
            SELECT
                CASE WHEN is_printed THEN 'printed' ELSE 'unprinted' END AS printed_status,
                COUNT(*)::int AS count
            FROM book_copy
            GROUP BY CASE WHEN is_printed THEN 'printed' ELSE 'unprinted' END
            ORDER BY count DESC;
            """
        )
        barcode_print_status = cur.fetchall()

        # -----------------------------
        # Most borrowed books (Top 5) - filtered by date range
        # + include cover + availability
        # -----------------------------
        cur.execute(
            f"""
            SELECT
                b.book_id,
                b.title,
                b.author,
                b.cover_url,
                COUNT(*)::int AS borrow_count,
                COUNT(bc_all.copy_id)::int AS total_copies,
                COUNT(CASE WHEN bc_all.status='available' THEN 1 END)::int AS available_copies
            FROM loan l
            JOIN book_copy bc ON bc.copy_id = l.copy_id
            JOIN book b ON b.book_id = bc.book_id
            LEFT JOIN book_copy bc_all ON bc_all.book_id = b.book_id
            WHERE {date_where}
            GROUP BY b.book_id
            ORDER BY borrow_count DESC
            LIMIT 5;
            """,
            tuple(date_params),
        )
        most_borrowed_books = cur.fetchall()

        # -----------------------------
        # Most active students (Top 5) - filtered by date range
        # -----------------------------
        cur.execute(
            f"""
            SELECT
                s.student_id,
                s.student_code,
                s.last_name,
                s.first_name,
                s.grade,
                s.section,
                COUNT(*)::int AS borrow_count
            FROM loan l
            JOIN student s ON s.student_id = l.student_id
            WHERE {date_where}
            GROUP BY s.student_id
            ORDER BY borrow_count DESC
            LIMIT 5;
            """,
            tuple(date_params),
        )
        most_active_students = cur.fetchall()

        # -----------------------------
        # Monthly borrowing trend (last 12 months) - always last 12 months
        # -----------------------------
        cur.execute(
            """
            SELECT
                TO_CHAR(DATE_TRUNC('month', borrowed_at), 'YYYY-MM') AS month,
                COUNT(*)::int AS borrow_count
            FROM loan
            WHERE borrowed_at >= (DATE_TRUNC('month', NOW()) - INTERVAL '11 months')
            GROUP BY DATE_TRUNC('month', borrowed_at)
            ORDER BY month ASC;
            """
        )
        monthly_borrow_trend = cur.fetchall()

        # -----------------------------
        # NEW: Daily borrowing trend (last 30 days)
        # -----------------------------
        cur.execute(
            """
            SELECT
                TO_CHAR(borrowed_at::date, 'YYYY-MM-DD') AS day,
                COUNT(*)::int AS borrow_count
            FROM loan
            WHERE borrowed_at >= (NOW() - INTERVAL '30 days')
            GROUP BY borrowed_at::date
            ORDER BY day ASC;
            """
        )
        daily_borrow_trend = cur.fetchall()

        # -----------------------------
        # Books by genre distribution (top 10)
        # -----------------------------
        cur.execute(
            """
            SELECT
                COALESCE(NULLIF(TRIM(genre), ''), 'Unknown') AS genre,
                COUNT(*)::int AS book_count
            FROM book
            GROUP BY COALESCE(NULLIF(TRIM(genre), ''), 'Unknown')
            ORDER BY book_count DESC
            LIMIT 10;
            """
        )
        genre_distribution = cur.fetchall()

        # -----------------------------
        # NEW: Overdue breakdown by grade / section
        # -----------------------------
        cur.execute(
            """
            SELECT
                COALESCE(s.grade, 'Unknown') AS grade,
                COUNT(*)::int AS overdue_count
            FROM loan l
            JOIN student s ON s.student_id = l.student_id
            WHERE l.returned_at IS NULL AND l.due_at < NOW()
            GROUP BY COALESCE(s.grade, 'Unknown')
            ORDER BY overdue_count DESC;
            """
        )
        overdue_by_grade = cur.fetchall()

        cur.execute(
            """
            SELECT
                COALESCE(s.section, 'Unknown') AS section,
                COUNT(*)::int AS overdue_count
            FROM loan l
            JOIN student s ON s.student_id = l.student_id
            WHERE l.returned_at IS NULL AND l.due_at < NOW()
            GROUP BY COALESCE(s.section, 'Unknown')
            ORDER BY overdue_count DESC
            LIMIT 15;
            """
        )
        overdue_by_section = cur.fetchall()

        # -----------------------------
        # NEW: Fine analytics (outstanding + collected this month)
        # -----------------------------
        cur.execute(
            """
            SELECT
                COALESCE(SUM(amount - amount_paid), 0)::numeric(10,2) AS outstanding_total,
                COUNT(*) FILTER (WHERE status='unpaid')::int AS unpaid_fine_count
            FROM fine;
            """
        )
        fine_outstanding = cur.fetchone()

        cur.execute(
            """
            SELECT
                COALESCE(SUM(amount), 0)::numeric(10,2) AS collected_this_month
            FROM fine_payment
            WHERE DATE_TRUNC('month', paid_at) = DATE_TRUNC('month', NOW());
            """
        )
        fine_collected_month = cur.fetchone()

        # inside /api/reports/dashboard before return {...}

        cur.execute("""
            SELECT COALESCE(TRIM(s.grade), 'Unknown') AS grade,
                COUNT(*)::int AS borrow_count
            FROM loan l
            JOIN student s ON s.student_id = l.student_id
            GROUP BY COALESCE(TRIM(s.grade), 'Unknown')
            ORDER BY borrow_count DESC, grade ASC
        """)
        borrowed_by_grade = cur.fetchall()

        # then include in the response:
        # "borrowed_by_grade": borrowed_by_grade,

        return {
            "totals": {
                "total_books": total_books,
                "total_copies": total_copies,
                "active_loans": active_loans,
                "overdue_loans": overdue_loans,
                "total_students": total_students, 
            },
            "inventory": {
                "copy_status_distribution": copy_status_distribution,
                "barcode_print_status": barcode_print_status,
            },
            "fines": {
                "outstanding_total": str(fine_outstanding["outstanding_total"]),
                "unpaid_fine_count": int(fine_outstanding["unpaid_fine_count"]),
                "collected_this_month": str(fine_collected_month["collected_this_month"]),
            },
            "most_borrowed_books": most_borrowed_books,
            "most_active_students": most_active_students,
            "monthly_borrow_trend": monthly_borrow_trend,
            "daily_borrow_trend": daily_borrow_trend,
            "genre_distribution": genre_distribution,
            
            "borrowed_by_grade": borrowed_by_grade,
            "overdue_breakdown": {
                "by_grade": overdue_by_grade,
                "by_section": overdue_by_section,
            },
            "filters_used": {"date_from": date_from, "date_to": date_to},
        }
        
    finally:
        cur.close()
        conn.close()


# -----------------------------
# REPORTS: GENERATE REPORT (Preview JSON) (Librarian Only)
# -----------------------------
@app.get("/api/reports/generate")
def generate_report(
    date_from: str | None = None,
    date_to: str | None = None,
    grade: str | None = None,
    section: str | None = None,
    genre: str | None = None,
    book_section: str | None = None,  # shelf/location section from book.section
    current=Depends(get_current_librarian),
):
    """
    Returns a report preview based on filters (for UI preview before export).

    Filters:
      - date_from/date_to (YYYY-MM-DD): borrowed_at date range
      - grade, section (student filters)
      - genre, book_section (book filters)

    Output includes:
      - generated metadata (librarian + timestamp)
      - fine summary
      - rows of loans (joined with student + book + copy)
    """

    # --- Default date range if not provided: last 30 days ---
    if date_to is None:
        dt_to = date.today()
    else:
        dt_to = parse_date_yyyy_mm_dd(date_to, "date_to")

    if date_from is None:
        dt_from = dt_to - timedelta(days=30)
    else:
        dt_from = parse_date_yyyy_mm_dd(date_from, "date_from")

    if dt_from > dt_to:
        raise HTTPException(status_code=400, detail="date_from must be <= date_to")

    # --- Build dynamic WHERE conditions safely ---
    where_clauses = []
    params = []

    # Borrowed_at date range (inclusive)
    where_clauses.append("DATE(l.borrowed_at) >= %s")
    params.append(dt_from.isoformat())

    where_clauses.append("DATE(l.borrowed_at) <= %s")
    params.append(dt_to.isoformat())

    if grade and grade.strip():
        where_clauses.append("s.grade = %s")
        params.append(grade.strip())

    if section and section.strip():
        where_clauses.append("s.section = %s")
        params.append(section.strip())

    if genre and genre.strip():
        where_clauses.append("b.genre ILIKE %s")
        params.append(f"%{genre.strip()}%")

    if book_section and book_section.strip():
        where_clauses.append("b.section ILIKE %s")
        params.append(f"%{book_section.strip()}%")

    where_sql = " AND ".join(where_clauses)

    conn = get_connection()
    cur = conn.cursor()

    try:
        # --- Main rows: loans with student + book + copy + fine total per loan ---
        # Fine may have multiple rows per loan (ex: separate assessments) so we SUM it.
        cur.execute(
            f"""
            SELECT
                l.loan_id,
                l.borrowed_at,
                l.due_at,
                l.returned_at,
                l.status AS loan_status,
                l.renew_count,

                -- Student
                s.student_id,
                s.student_code,
                s.last_name,
                s.first_name,
                s.grade,
                s.section AS student_section,

                -- Book + copy
                b.book_id,
                b.title,
                b.author,
                b.genre,
                b.section AS book_section,
                bc.copy_id,
                bc.barcode,
                bc.status AS copy_status,

                -- Overdue flag (only meaningful if not returned)
                (l.returned_at IS NULL AND l.due_at < NOW()) AS is_overdue,

                -- Fine totals (if any)
                COALESCE(fsum.total_fine, 0) AS total_fine,
                COALESCE(fsum.total_paid, 0) AS total_paid,
                COALESCE(fsum.total_fine - fsum.total_paid, 0) AS outstanding_fine

            FROM loan l
            JOIN student s ON s.student_id = l.student_id
            JOIN book_copy bc ON bc.copy_id = l.copy_id
            JOIN book b ON b.book_id = bc.book_id

            LEFT JOIN (
                SELECT
                    loan_id,
                    SUM(amount) AS total_fine,
                    SUM(amount_paid) AS total_paid
                FROM fine
                GROUP BY loan_id
            ) fsum ON fsum.loan_id = l.loan_id

            WHERE {where_sql}
            ORDER BY l.borrowed_at DESC
            LIMIT 2000
            """,
            tuple(params),
        )
        rows = cur.fetchall()

        # --- Fine summary for this report result set ---
        # We compute summary from returned rows (simple + consistent with filters).
        total_fine = float(sum(r["total_fine"] for r in rows))
        total_paid = float(sum(r["total_paid"] for r in rows))
        outstanding = float(sum(r["outstanding_fine"] for r in rows))

        return {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "generated_by": {
                "librarian_id": current["librarian_id"],
                "username": current["username"],
                "email": current["email"],
            },
            "filters": {
                "date_from": dt_from.isoformat(),
                "date_to": dt_to.isoformat(),
                "grade": grade,
                "section": section,
                "genre": genre,
                "book_section": book_section,
            },
            "summary": {
                "row_count": len(rows),
                "fine_total": round(total_fine, 2),
                "fine_paid_total": round(total_paid, 2),
                "fine_outstanding_total": round(outstanding, 2),
            },
            "rows": rows,
        }

    finally:
        cur.close()
        conn.close()

# -----------------------------
# REPORTS: EXPORT CSV (Librarian Only)
# -----------------------------
@app.get("/api/reports/export/csv")
def export_report_csv(
    date_from: str | None = None,
    date_to: str | None = None,
    grade: str | None = None,
    section: str | None = None,
    genre: str | None = None,
    book_section: str | None = None,
    current=Depends(get_current_librarian),
):
    """
    Exports the same report data as CSV.
    Uses the same filters as /api/reports/generate.
    """

    # Reuse the preview generator logic so filters match exactly
    report = generate_report(
        date_from=date_from,
        date_to=date_to,
        grade=grade,
        section=section,
        genre=genre,
        book_section=book_section,
        current=current,
    )

    rows = report["rows"]

    # Define CSV columns (order matters)
    columns = [
        "loan_id", "borrowed_at", "due_at", "returned_at", "loan_status", "renew_count",
        "student_code", "last_name", "first_name", "grade", "student_section",
        "title", "author", "genre", "book_section",
        "barcode", "copy_status", "is_overdue",
        "total_fine", "total_paid", "outstanding_fine",
    ]

    def stream():
        """
        Stream CSV output so it downloads instantly even with large reports.
        """
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=columns)
        writer.writeheader()
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        for r in rows:
            # Each row is a dict_row; we map only the columns we want
            writer.writerow({c: r.get(c) for c in columns})
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

    filename = f"opac_report_{report['filters']['date_from']}_to_{report['filters']['date_to']}.csv"

    return StreamingResponse(
        stream(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

# -----------------------------
# REPORTS: PDF BUILDER (ReportLab) - Formal + Logo + Readable Table
# -----------------------------
def build_report_pdf(report: dict) -> bytes:
    """
    Build a formal, readable PDF (bytes) from the report dict.

    Features:
      - Landscape page for better table readability
      - School logo + formal title header
      - Filters + Summary as a formal document section
      - Readable 5-column table (not cramped)
      - Repeated table header on every page
      - Footer: page numbers + official-use note
    """

    buffer = BytesIO()

    # Use landscape for reports (more horizontal space)
    page_size = landscape(letter)

    doc = SimpleDocTemplate(
        buffer,
        pagesize=page_size,
        leftMargin=28,
        rightMargin=28,
        topMargin=28,
        bottomMargin=28,
        title="Golden Key OPAC Report",
        author="Golden Key OPAC",
    )

    styles = getSampleStyleSheet()

    # Small paragraph style for table cells
    cell_style = ParagraphStyle(
        "Cell",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8,
        leading=10,
        spaceAfter=0,
        spaceBefore=0,
    )

    story = []

    # Pull blocks from report dict
    gen = report.get("generated_by", {}) or {}
    filt = report.get("filters", {}) or {}
    summ = report.get("summary", {}) or {}

    # --- Formal Header: Logo + Title ---
    header_table_data = []

    # Build logo (if exists). If missing, we omit gracefully.
    try:
        logo_flowable = Image(SCHOOL_LOGO_PATH)
        logo_flowable.drawHeight = 0.85 * inch
        logo_flowable.drawWidth = 0.85 * inch
    except Exception:
        logo_flowable = Paragraph("", styles["Normal"])

    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=20,
        alignment=2,  # right align
        spaceAfter=0,
    )

    subtitle_style = ParagraphStyle(
        "ReportSubTitle",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=10,
        leading=12,
        alignment=2,  # right align
        textColor=colors.grey,
    )

    title_block = [
        Paragraph("Golden Key OPAC Report", title_style),
        Paragraph("Official Library Circulation & Catalog Report", subtitle_style),
    ]

    header_table_data.append([logo_flowable, title_block])

    header_tbl = Table(
        header_table_data,
        colWidths=[0.95 * inch, 6.55 * inch],
    )
    header_tbl.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (0, 0), "LEFT"),
        ("ALIGN", (1, 0), (1, 0), "RIGHT"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))

    story.append(header_tbl)
    story.append(Spacer(1, 6))

    # Divider line under header
    divider = Table([[""]], colWidths=[7.5 * inch])
    divider.setStyle(TableStyle([("LINEBELOW", (0, 0), (-1, -1), 1, colors.black)]))
    story.append(divider)
    story.append(Spacer(1, 10))

    # --- Metadata block ---
    story.append(Paragraph(f"<b>Generated at:</b> {report.get('generated_at', '')}", styles["Normal"]))
    story.append(Paragraph(
        f"<b>Generated by:</b> {gen.get('username', '')} ({gen.get('email', '')})",
        styles["Normal"]
    ))
    story.append(Spacer(1, 10))

    # --- Filters ---
    story.append(Paragraph("<b>Filters</b>", styles["Heading2"]))
    story.append(Paragraph(f"Date range: {filt.get('date_from')} to {filt.get('date_to')}", styles["Normal"]))
    story.append(Paragraph(f"Grade: {filt.get('grade') or 'All'}", styles["Normal"]))
    story.append(Paragraph(f"Student Section: {filt.get('section') or 'All'}", styles["Normal"]))
    story.append(Paragraph(f"Genre: {filt.get('genre') or 'All'}", styles["Normal"]))
    story.append(Paragraph(f"Book Section (Shelf/Location): {filt.get('book_section') or 'All'}", styles["Normal"]))
    story.append(Spacer(1, 10))

    # --- Summary ---
    story.append(Paragraph("<b>Summary</b>", styles["Heading2"]))
    story.append(Paragraph(f"Rows: {summ.get('row_count', 0)}", styles["Normal"]))
    story.append(Paragraph(f"Fine total: {summ.get('fine_total', 0)}", styles["Normal"]))
    story.append(Paragraph(f"Fine paid total: {summ.get('fine_paid_total', 0)}", styles["Normal"]))
    story.append(Paragraph(f"Fine outstanding total: {summ.get('fine_outstanding_total', 0)}", styles["Normal"]))
    story.append(Spacer(1, 12))

    rows = report.get("rows", []) or []

    # Cap rows shown in PDF (keeps PDF readable)
    MAX_ROWS = 300
    shown = rows[:MAX_ROWS]
    if len(rows) > MAX_ROWS:
        story.append(Paragraph(
            f"<i>Note: Showing first {MAX_ROWS} rows. Use CSV export for full data.</i>",
            styles["Italic"]
        ))
        story.append(Spacer(1, 8))

    def safe(val):
        return "" if val is None else str(val)

    def dt2(val):
        """Return YYYY-MM-DD<br/>HH:MM for compact display."""
        if not val:
            return ""
        s = str(val)
        d = s[:10]
        t = s[11:16] if len(s) >= 16 else ""
        return f"{d}<br/>{t}".strip()

    # --- Readable table (fewer columns, richer cells) ---
    header = [
        Paragraph("<b>Loan</b>", cell_style),
        Paragraph("<b>Dates</b>", cell_style),
        Paragraph("<b>Student</b>", cell_style),
        Paragraph("<b>Book</b>", cell_style),
        Paragraph("<b>Copy / Status</b>", cell_style),
    ]
    table_data = [header]

    for r in shown:
        loan_id = safe(r.get("loan_id"))

        dates_cell = Paragraph(
            f"<b>B:</b> {dt2(r.get('borrowed_at'))}<br/>"
            f"<b>D:</b> {dt2(r.get('due_at'))}<br/>"
            f"<b>R:</b> {dt2(r.get('returned_at'))}",
            cell_style
        )

        student_cell = Paragraph(
            f"{safe(r.get('last_name'))}, {safe(r.get('first_name'))}<br/>"
            f"<b>Grade:</b> {safe(r.get('grade'))} &nbsp;&nbsp; "
            f"<b>Sec:</b> {safe(r.get('student_section'))}",
            cell_style
        )

        book_cell = Paragraph(
            f"<b>{safe(r.get('title'))}</b><br/>"
            f"{safe(r.get('author'))}",
            cell_style
        )

        overdue_txt = "YES" if r.get("is_overdue") else "NO"
        copy_cell = Paragraph(
            f"<b>Barcode:</b> {safe(r.get('barcode'))}<br/>"
            f"<b>Overdue:</b> {overdue_txt} &nbsp;&nbsp; "
            f"<b>Fine:</b> {safe(r.get('outstanding_fine'))}",
            cell_style
        )

        table_data.append([
            Paragraph(loan_id, cell_style),
            dates_cell,
            student_cell,
            book_cell,
            copy_cell
        ])

    # Column widths tuned to fit landscape letter cleanly
    col_widths = [
        45,   # Loan
        115,  # Dates (stacked)
        165,  # Student
        260,  # Book (wide + wraps)
        165,  # Copy / Status
    ]

    tbl = Table(table_data, colWidths=col_widths, repeatRows=1)

    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E6E6E6")),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("ALIGN", (0, 0), (-1, 0), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),

        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 8),

        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),

        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
    ]))

    story.append(tbl)

    # --- Footer: Page number + official note ---
    def add_page_number(canvas, doc_):
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        canvas.drawString(28, 18, "System-generated report. For official use only.")
        canvas.drawRightString(page_size[0] - 28, 18, f"Page {doc_.page}")
        canvas.restoreState()

    doc.build(story, onFirstPage=add_page_number, onLaterPages=add_page_number)

    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes

@app.get("/api/books")
def search_books(q: str = ""):
    """
    Search books by ANY keyword (title, author, publisher, genre, subject, section, catalog_key, identifiers).
    Used by OPAC + Librarian search.
    """
    q = (q or "").strip()
    if not q:
        return []

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT DISTINCT
            b.book_id,
            b.title,
            b.author,
            b.section,
            b.cover_url
        FROM book b
        LEFT JOIN book_identifier bi ON bi.book_id = b.book_id
        WHERE
            b.title ILIKE %(q)s
            OR b.author ILIKE %(q)s
            OR COALESCE(b.publisher,'') ILIKE %(q)s
            OR COALESCE(b.genre,'') ILIKE %(q)s
            OR COALESCE(b.subject,'') ILIKE %(q)s
            OR COALESCE(b.section,'') ILIKE %(q)s
            OR b.catalog_key ILIKE %(q)s
            OR COALESCE(bi.id_value,'') ILIKE %(q)s
        ORDER BY b.title ASC
        LIMIT 100
        """,
        {"q": f"%{q}%"},
    )

    books = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "book_id": b["book_id"],
            "title": b["title"],
            "author": b["author"],
            "section": b["section"],
            "cover_url": b["cover_url"],
        }
        for b in books
    ]
# -----------------------------
# REPORTS: EXPORT PDF (Librarian Only)
# -----------------------------
@app.get("/api/reports/export/pdf")
def export_report_pdf(
    date_from: str | None = None,
    date_to: str | None = None,
    grade: str | None = None,
    section: str | None = None,
    genre: str | None = None,
    book_section: str | None = None,
    current=Depends(get_current_librarian),
):
    """
    Exports a PDF version of the report.
    Uses the same filters as /api/reports/generate.

    Important:
      - PDF preview is capped to the first 200 rows for readability.
      - CSV export remains the best option for full dataset export.
    """

    # Reuse the report generator so filters match exactly
    report = generate_report(
        date_from=date_from,
        date_to=date_to,
        grade=grade,
        section=section,
        genre=genre,
        book_section=book_section,
        current=current,
    )

    pdf_bytes = build_report_pdf(report)

    filename = f"opac_report_{report['filters']['date_from']}_to_{report['filters']['date_to']}.pdf"

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# -----------------------------
# TEMP TEST: PDF EXPORT WITH TOKEN IN QUERY (REMOVE LATER)
# -----------------------------
@app.get("/api/reports/export/pdf-test")
def export_report_pdf_test(
    token: str,
    date_from: str | None = None,
    date_to: str | None = None,
    grade: str | None = None,
    section: str | None = None,
    genre: str | None = None,
    book_section: str | None = None,
):
    """
    TEMPORARY: Use token in query so browser can download PDF easily.
    REMOVE THIS ENDPOINT BEFORE DEPLOYMENT.
    """
    # Decode token using your existing function
    try:
        payload = decode_token(token)
        librarian_id = int(payload["sub"])
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    # Load librarian (reuse your existing DB logic)
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT librarian_id, username, email, is_active
            FROM librarian
            WHERE librarian_id = %s
            """,
            (librarian_id,),
        )
        librarian = cur.fetchone()
        if not librarian or not librarian["is_active"]:
            raise HTTPException(status_code=401, detail="Account not found or inactive")
    finally:
        cur.close()
        conn.close()

    # Generate report + PDF
    report = generate_report(
        date_from=date_from,
        date_to=date_to,
        grade=grade,
        section=section,
        genre=genre,
        book_section=book_section,
        current=librarian,
    )
    pdf_bytes = build_report_pdf(report)

    filename = f"opac_report_{report['filters']['date_from']}_to_{report['filters']['date_to']}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

# -----------------------------
# FINES: LIST UNPAID FINES (Librarian Only)
# -----------------------------
@app.get("/api/fines/unpaid")
def list_unpaid_fines(
    student_id: int,
    current=Depends(get_current_librarian),
):
    """
    Lists all unpaid fines for a student, including outstanding amount.
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                fine_id,
                loan_id,
                student_id,
                amount,
                amount_paid,
                (amount - amount_paid) AS outstanding,
                status,
                reason,
                assessed_at
            FROM fine
            WHERE student_id = %s AND status = 'unpaid'
            ORDER BY assessed_at DESC
            """,
            (student_id,),
        )
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

# -----------------------------
# FINES: PAY (Partial or Full) (Librarian Only)
# -----------------------------
@app.post("/api/fines/pay")
def pay_fine(
    fine_id: int,
    amount: float,
    method: str = "cash",
    note: str | None = None,
    current=Depends(get_current_librarian),
):
    """
    Records a fine payment and updates fine totals.

    Rules:
      - amount must be > 0
      - cannot pay more than outstanding (we block overpayment)
      - updates fine.amount_paid
      - sets fine.status = 'paid' when fully settled
      - inserts fine_payment row (audit history)
    """
    if amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be greater than 0")

    conn = get_connection()
    cur = conn.cursor()

    try:
        librarian_id = current["librarian_id"]

        # 1) Load fine
        cur.execute(
            """
            SELECT fine_id, student_id, amount, amount_paid, status
            FROM fine
            WHERE fine_id = %s
            """,
            (fine_id,),
        )
        fine = cur.fetchone()
        if not fine:
            raise HTTPException(status_code=404, detail="Fine not found")

        if fine["status"] == "paid":
            raise HTTPException(status_code=400, detail="Fine is already fully paid")

        total_amount = float(fine["amount"] or 0)
        paid_amount = float(fine["amount_paid"] or 0)
        outstanding = round(total_amount - paid_amount, 2)

        if outstanding <= 0:
            # Safety: if data already settled but status not updated
            raise HTTPException(status_code=400, detail="No outstanding balance for this fine")

        # 2) Prevent overpayment
        if amount > outstanding:
            raise HTTPException(
                status_code=400,
                detail={"message": "Payment exceeds outstanding balance", "outstanding": outstanding},
            )

        # 3) Insert payment record
        cur.execute(
            """
            INSERT INTO fine_payment (fine_id, student_id, received_by, amount, method, paid_at, note)
            VALUES (%s, %s, %s, %s, %s, NOW(), %s)
            RETURNING payment_id, paid_at
            """,
            (fine_id, fine["student_id"], librarian_id, amount, method, note),
        )
        payment_row = cur.fetchone()

        # 4) Update fine totals
        new_paid = round(paid_amount + amount, 2)
        new_outstanding = round(total_amount - new_paid, 2)

        new_status = "paid" if new_outstanding <= 0 else "unpaid"

        cur.execute(
            """
            UPDATE fine
            SET amount_paid = %s, status = %s
            WHERE fine_id = %s
            """,
            (new_paid, new_status, fine_id),
        )

        conn.commit()

        return {
            "message": "Payment recorded",
            "fine_id": fine_id,
            "payment_id": payment_row["payment_id"],
            "paid_at": str(payment_row["paid_at"]),
            "amount_paid_now": amount,
            "fine_total": total_amount,
            "fine_amount_paid_total": new_paid,
            "fine_outstanding": new_outstanding,
            "fine_status": new_status,
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Payment failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

# -----------------------------
# FINES: PAYMENT HISTORY (Librarian Only)
# -----------------------------
@app.get("/api/fines/{fine_id}/payments")
def fine_payment_history(
    fine_id: int,
    current=Depends(get_current_librarian),
):
    """
    Shows payment history for a fine (audit trail).
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                fp.payment_id,
                fp.fine_id,
                fp.student_id,
                fp.received_by,
                l.username AS received_by_username,
                fp.amount,
                fp.method,
                fp.paid_at,
                fp.note
            FROM fine_payment fp
            LEFT JOIN librarian l ON l.librarian_id = fp.received_by
            WHERE fp.fine_id = %s
            ORDER BY fp.paid_at DESC
            """,
            (fine_id,),
        )
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


# -----------------------------
# CIRCULATION: MARK BOOK AS LOST (Librarian Only)
# -----------------------------
@app.post("/api/circulation/mark-lost")
def mark_book_lost(
    barcode: str,
    student_code: str,
    note: str | None = None,
    current=Depends(get_current_librarian),
):
    """
    Marks a borrowed copy as LOST and creates a fine.

    Behavior:
      1) Find student by student_code
      2) Find copy by barcode
      3) Find active loan for (student_id, copy_id) where returned_at IS NULL
      4) Update loan: returned_at = NOW(), status = 'lost'
      5) Update copy: status = 'lost'
      6) Create fine (unpaid) using system_settings.lost_fee
      7) Return summary

    Notes:
      - Uses system_settings.lost_fee (no client-provided lost_fee to avoid tampering)
      - Prevents duplicate "lost" fines for the same loan
    """
    conn = get_connection()
    cur = conn.cursor()

    try:
        # --- 0) Load lost fee from system_settings ---
        cur.execute(
            """
            SELECT COALESCE(lost_fee, 0) AS lost_fee
            FROM system_settings
            ORDER BY updated_at DESC, settings_id DESC
            LIMIT 1
            """
        )
        settings = cur.fetchone()
        lost_fee = float(settings["lost_fee"] or 0) if settings else 0.0

        if lost_fee <= 0:
            raise HTTPException(status_code=400, detail="Lost fee is not configured in system_settings")

        # --- 1) Student ---
        cur.execute(
            """
            SELECT student_id
            FROM student
            WHERE student_code = %s
            """,
            (student_code,),
        )
        student = cur.fetchone()
        if not student:
            raise HTTPException(status_code=404, detail="Student not found")
        student_id = student["student_id"]

        # --- 2) Copy ---
        cur.execute(
            """
            SELECT copy_id, status
            FROM book_copy
            WHERE barcode = %s
            """,
            (barcode,),
        )
        copy_row = cur.fetchone()
        if not copy_row:
            raise HTTPException(status_code=404, detail="Copy barcode not found")
        copy_id = copy_row["copy_id"]

        # --- 3) Active loan for this student + copy ---
        cur.execute(
            """
            SELECT loan_id, due_at, status
            FROM loan
            WHERE student_id = %s
              AND copy_id = %s
              AND returned_at IS NULL
            ORDER BY borrowed_at DESC
            LIMIT 1
            """,
            (student_id, copy_id),
        )
        loan = cur.fetchone()
        if not loan:
            raise HTTPException(status_code=400, detail="No active loan found for this student and copy")

        loan_id = loan["loan_id"]

        # Guard: If someone already marked it lost via some other flow, prevent duplication
        if (loan.get("status") or "").lower() == "lost":
            raise HTTPException(status_code=400, detail="This loan is already marked as lost")

        # --- 4) Update loan to 'lost' and CLOSE it so it disappears from Current Borrowed ---
        cur.execute(
            """
            UPDATE loan
            SET returned_at = NOW(),
                status = 'lost'
            WHERE loan_id = %s
            RETURNING returned_at
            """,
            (loan_id,),
        )
        returned_row = cur.fetchone()

        # --- 5) Update copy to 'lost' (not borrowable) ---
        cur.execute(
            """
            UPDATE book_copy
            SET status = 'lost'
            WHERE copy_id = %s
            """,
            (copy_id,),
        )

        # --- 6) Prevent duplicate lost fine for this loan ---
        cur.execute(
            """
            SELECT fine_id
            FROM fine
            WHERE loan_id = %s
              AND status = 'unpaid'
              AND (reason ILIKE 'lost%%' OR reason ILIKE '%%lost%%')
            ORDER BY assessed_at DESC
            LIMIT 1
            """,
            (loan_id,),
        )
        existing_fine = cur.fetchone()

        reason = "Lost book fee"
        if note and note.strip():
            reason = f"Lost book fee - {note.strip()}"

        if existing_fine:
            # Fine already exists; do not create another one
            fine_row = {"fine_id": existing_fine["fine_id"], "amount": lost_fee, "status": "unpaid"}
        else:
            cur.execute(
                """
                INSERT INTO fine (loan_id, student_id, amount, amount_paid, status, reason, assessed_at)
                VALUES (%s, %s, %s, 0, 'unpaid', %s, NOW())
                RETURNING fine_id, amount, status
                """,
                (loan_id, student_id, round(lost_fee, 2), reason),
            )
            fine_row = cur.fetchone()

        conn.commit()

        return {
            "message": "Book marked as lost",
            "loan_id": loan_id,
            "copy_id": copy_id,
            "barcode": barcode,
            "student_code": student_code,
            "loan_returned_at": returned_row["returned_at"].isoformat() if returned_row and returned_row.get("returned_at") else None,
            "copy_status": "lost",
            "fine": {
                "fine_id": fine_row["fine_id"],
                "amount": float(fine_row["amount"]),
                "status": fine_row["status"],
                "reason": reason,
            },
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Mark lost failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

from fastapi import Body

@app.put("/api/students/{student_id}")
def update_student(
    student_id: int,
    payload: dict = Body(...),
    current=Depends(get_current_librarian),
):
    """
    Updates a student.
    Expected JSON:
      student_code,last_name,first_name,grade,section,status
    """
    student_code = (payload.get("student_code") or "").strip()
    last_name = (payload.get("last_name") or "").strip()
    first_name = (payload.get("first_name") or "").strip()
    grade_raw = (payload.get("grade") or "").strip()
    section = (payload.get("section") or "").strip()
    status = (payload.get("status") or "active").strip().lower()

    if not student_code or not last_name or not first_name or not grade_raw or not section:
        raise HTTPException(status_code=400, detail="All required fields must be provided.")

    grade = normalize_grade(grade_raw)
    if status not in ["active", "suspended", "graduated"]:
        raise HTTPException(status_code=400, detail="status must be active, suspended, or graduated")

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE student
            SET student_code=%s, last_name=%s, first_name=%s, grade=%s, section=%s, status=%s, updated_at=NOW()
            WHERE student_id=%s
            RETURNING student_id
            """,
            (student_code, last_name, first_name, grade, section, status, student_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Student not found")

        conn.commit()
        return {"message": "Student updated", "student_id": student_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Update student failed: {str(e)}")
    finally:
        cur.close()
        conn.close()
# -----------------------------
# STUDENTS: BULK IMPORT (CSV) (Librarian Only)
# -----------------------------
@app.post("/api/students/import-csv")
async def import_students_csv(
    file: UploadFile = File(...),
    default_status: str = "active",
    current=Depends(get_current_librarian),
):
    """
    Imports students from a CSV file.

    Expected headers:
      student_code,last_name,first_name,grade,section,status

    Behavior:
      - Validates required fields
      - Normalizes grade to '1'..'12'
      - If status is blank, uses default_status
      - Skips duplicates (same student_code) and records them as errors
      - Returns summary: inserted + failed rows with reasons
    """

    # Basic file validation
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    content = await file.read()

    # Try decoding as UTF-8 (common). If your CSV is Excel-exported, this usually works.
    try:
        text = content.decode("utf-8-sig")  # utf-8-sig handles BOM
    except Exception:
        raise HTTPException(status_code=400, detail="CSV must be UTF-8 encoded")

    reader = csv.DictReader(io.StringIO(text))

    required_headers = {"student_code", "last_name", "first_name", "grade", "section"}
    missing = required_headers - set((h or "").strip() for h in (reader.fieldnames or []))
    if missing:
        raise HTTPException(
            status_code=400,
            detail={"message": "Missing required CSV headers", "missing_headers": sorted(list(missing))},
        )

    conn = get_connection()
    cur = conn.cursor()

    inserted = 0
    failed = []

    try:
        for idx, row in enumerate(reader, start=2):  # line 1 is header
            try:
                student_code = (row.get("student_code") or "").strip()
                last_name = (row.get("last_name") or "").strip()
                first_name = (row.get("first_name") or "").strip()
                grade_raw = (row.get("grade") or "").strip()
                section = (row.get("section") or "").strip()
                status = (row.get("status") or "").strip() or default_status

                # Validate required fields
                if not student_code:
                    raise ValueError("student_code is required")
                if not last_name or not first_name:
                    raise ValueError("last_name and first_name are required")
                if not grade_raw:
                    raise ValueError("grade is required")
                if not section:
                    raise ValueError("section is required")

                # Normalize grade to '1'..'12' (use your helper)
                grade = normalize_grade(grade_raw)

                # Normalize status
                status_norm = status.lower()
                if status_norm not in ["active", "suspended", "graduated"]:
                    raise ValueError("status must be active, suspended, or graduated")

                # Insert student
                cur.execute(
                    """
                    INSERT INTO student (student_code, last_name, first_name, grade, section, status, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (student_code, last_name, first_name, grade, section, status_norm),
                )
                inserted += 1

            except Exception as row_err:
                # Record which row failed and why (do not stop entire import)
                failed.append({"line": idx, "student_code": row.get("student_code"), "error": str(row_err)})

        conn.commit()

        return {
            "message": "Import finished",
            "inserted": inserted,
            "failed_count": len(failed),
            "failed": failed[:200],  # cap response size
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"CSV import failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

# -----------------------------
# CIRCULATION: LOOKUP BY COPY BARCODE (Librarian Only)
# -----------------------------
@app.get("/api/circulation/lookup")
def circulation_lookup(barcode: str, current=Depends(get_current_librarian)):
    barcode_clean = (barcode or "").strip()
    if not barcode_clean:
        raise HTTPException(status_code=400, detail="barcode is required")

    conn = get_connection()
    cur = conn.cursor()

    try:
        # 1) Find the copy + its book (barcode is from book_copy.barcode)
        cur.execute(
            """
            SELECT
                bc.copy_id,
                bc.barcode,
                bc.status AS copy_status,

                b.book_id,
                b.title,
                b.author,
                b.publisher,
                b.pub_year,
                b.genre,
                b.subject,
                b.section,
                b.cover_url,

                (
                    SELECT bi.id_value
                    FROM book_identifier bi
                    WHERE bi.book_id = b.book_id
                      AND bi.id_type = 'isbn'
                    ORDER BY bi.is_primary DESC, bi.created_at ASC
                    LIMIT 1
                ) AS isbn
            FROM book_copy bc
            JOIN book b ON b.book_id = bc.book_id
            WHERE bc.barcode = %s
            """,
            (barcode_clean,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Barcode not found")

        book_id = row["book_id"]

        # 2) Availability counts for that book_id
        # (same idea you already use elsewhere: total + available counts) :contentReference[oaicite:2]{index=2}
        cur.execute(
            """
            SELECT
                COUNT(*)::int AS total_copies,
                COALESCE(SUM(CASE WHEN status = 'available' THEN 1 ELSE 0 END), 0)::int AS available_copies
            FROM book_copy
            WHERE book_id = %s
            """,
            (book_id,),
        )
        counts = cur.fetchone()

        return {
            "copy_id": row["copy_id"],
            "barcode": row["barcode"],
            "copy_status": row["copy_status"],

            "book_id": row["book_id"],
            "title": row["title"],
            "author": row["author"],
            "publisher": row["publisher"],
            "pub_year": row["pub_year"],
            "genre": row["genre"],
            "subject": row["subject"],
            "section": row["section"],
            "cover_url": row.get("cover_url"),
            "isbn": row.get("isbn"),

            "total_copies": counts["total_copies"],
            "available_copies": counts["available_copies"],
        }

    finally:
        cur.close()
        conn.close()

# -----------------------------
# STUDENTS: BULK PROMOTE GRADE (Librarian Only)
# -----------------------------
@app.post("/api/students/bulk-promote")
def bulk_promote_students(
    from_grade: str,
    to_grade: str,
    only_status: str | None = "active",
    current=Depends(get_current_librarian),
):
    """
    Mass change students from one grade to another.

    Example:
      from_grade=8 -> to_grade=9 (only active students)

    Params:
      - only_status: if provided, only update students with this status
                    (active/suspended/graduated). Use None to update all.
    """
    g_from = normalize_grade(from_grade)
    g_to = normalize_grade(to_grade)

    if g_from == g_to:
        raise HTTPException(status_code=400, detail="from_grade and to_grade must be different")

    if only_status is not None:
        st = only_status.strip().lower()
        if st not in ["active", "suspended", "graduated"]:
            raise HTTPException(status_code=400, detail="only_status must be active, suspended, or graduated")
    else:
        st = None

    conn = get_connection()
    cur = conn.cursor()

    try:
        if st is None:
            cur.execute(
                """
                UPDATE student
                SET grade = %s, updated_at = NOW()
                WHERE grade = %s
                """,
                (g_to, g_from),
            )
        else:
            cur.execute(
                """
                UPDATE student
                SET grade = %s, updated_at = NOW()
                WHERE grade = %s AND status = %s
                """,
                (g_to, g_from, st),
            )

        updated = cur.rowcount
        conn.commit()

        return {
            "message": "Bulk grade update complete",
            "from_grade": g_from,
            "to_grade": g_to,
            "only_status": st,
            "updated_count": updated,
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Bulk promote failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

# -----------------------------
# STUDENTS: BULK STATUS CHANGE (Librarian Only)
# -----------------------------
@app.post("/api/students/bulk-status")
def bulk_change_status(
    from_status: str,
    to_status: str,
    grade: str | None = None,
    section: str | None = None,
    current=Depends(get_current_librarian),
):
    """
    Mass change student statuses.

    Examples:
      - active -> graduated for grade=12
      - active -> suspended for section="Amethyst"
      - suspended -> active (re-activate)

    Optional filters:
      - grade (1..12)
      - section (exact match)
    """

    fs = from_status.strip().lower()
    ts = to_status.strip().lower()

    allowed = ["active", "suspended", "graduated"]
    if fs not in allowed or ts not in allowed:
        raise HTTPException(status_code=400, detail="from_status and to_status must be active, suspended, or graduated")

    g = normalize_grade(grade) if grade and grade.strip() else None
    sec = section.strip() if section and section.strip() else None

    where = ["status = %s"]
    params = [fs]

    if g is not None:
        where.append("grade = %s")
        params.append(g)

    if sec is not None:
        where.append("section = %s")
        params.append(sec)

    where_sql = " AND ".join(where)

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            f"""
            UPDATE student
            SET status = %s, updated_at = NOW()
            WHERE {where_sql}
            """,
            tuple([ts] + params),
        )

        updated = cur.rowcount
        conn.commit()

        return {
            "message": "Bulk status update complete",
            "from_status": fs,
            "to_status": ts,
            "grade": g,
            "section": sec,
            "updated_count": updated,
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Bulk status update failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

# -----------------------------
# SETTINGS: PERSONAL INFO (Librarian Only)
# -----------------------------
@app.get("/api/settings/me")
def get_my_profile(current=Depends(get_current_librarian)):
    """
    Returns the logged-in librarian profile.
    Used by Settings -> Personal Information.
    """
    # current already contains librarian row (based on your auth dependency)
    return {
        "librarian_id": current["librarian_id"],
        "username": current["username"],
        "email": current["email"],
        "last_name": current.get("last_name"),
        "first_name": current.get("first_name"),
        "is_active": current.get("is_active"),
        "created_at": str(current.get("created_at")) if current.get("created_at") else None,
        "last_login_at": str(current.get("last_login_at")) if current.get("last_login_at") else None,
    }


@app.put("/api/settings/me")
def update_my_profile(
    first_name: str | None = None,
    last_name: str | None = None,
    email: str | None = None,
    current=Depends(get_current_librarian),
):
    """
    Updates the logged-in librarian profile fields.
    Used by Settings -> Personal Information.
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        updates = []
        params = []

        if first_name is not None:
            updates.append("first_name = %s")
            params.append(first_name.strip())

        if last_name is not None:
            updates.append("last_name = %s")
            params.append(last_name.strip())

        if email is not None:
            updates.append("email = %s")
            params.append(email.strip())

        if not updates:
            raise HTTPException(status_code=400, detail="No fields provided to update")

        params.append(current["librarian_id"])

        cur.execute(
            f"""
            UPDATE librarian
            SET {", ".join(updates)}
            WHERE librarian_id = %s
            """,
            tuple(params),
        )

        conn.commit()
        return {"message": "Profile updated"}

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Profile update failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

# -----------------------------
# SETTINGS: CIRCULATION DEFAULTS (Librarian Only)
# -----------------------------
@app.get("/api/settings/circulation")
def get_circulation_settings(current=Depends(get_current_librarian)):
    """
    Returns global circulation defaults from system_settings (row id=1).
    Frontend uses this for Settings -> Circulation Settings.
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT settings_id, loan_period_days, fine_per_day, max_borrow_limit,
                   max_renewals, block_renew_if_overdue, updated_at, updated_by
            FROM system_settings
            WHERE settings_id = 1
            """
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=500, detail="system_settings row missing (settings_id=1)")
        return row
    finally:
        cur.close()
        conn.close()
from fastapi import HTTPException, Depends


from datetime import date, timedelta


def get_system_settings(cur):
    cur.execute("""
        SELECT loan_period_days, fine_per_day, max_borrow_limit, max_renewals, block_renew_if_overdue,
               COALESCE(damage_fine_minor, 0) AS damage_fine_minor,
               COALESCE(damage_fine_major, 0) AS damage_fine_major,
               COALESCE(lost_book_fine, 0) AS lost_book_fine
        FROM system_settings
        ORDER BY settings_id ASC
        LIMIT 1
    """)
    s = cur.fetchone()
    return s or {
        "fine_per_day": 0,
        "damage_fine_minor": 0,
        "damage_fine_major": 0,
        "lost_book_fine": 0,
    }
def _get_settings_for_grade(cur, grade: str | None):
    # Try grade_rule first
    if grade:
        cur.execute(
            """
            SELECT loan_period_days, max_borrow_limit, fine_per_day, max_renewals, block_renew_if_overdue
            FROM grade_rule
            WHERE grade = %s
            ORDER BY updated_at DESC NULLS LAST, rule_id DESC
            LIMIT 1
            """,
            (grade,),
        )
        r = cur.fetchone()
        if r:
            return r

    # Fallback to system_settings (latest row)
    cur.execute(
        """
        SELECT loan_period_days, max_borrow_limit, fine_per_day, max_renewals, block_renew_if_overdue
        FROM system_settings
        ORDER BY updated_at DESC, settings_id DESC
        LIMIT 1
        """
    )
    s = cur.fetchone()
    if not s:
        # Absolute fallback if table is empty
        return {
            "loan_period_days": 7,
            "max_borrow_limit": 3,
            "fine_per_day": 5.00,
            "max_renewals": 1,
            "block_renew_if_overdue": True,
        }
    return s

@app.post("/api/circulation/lost")
def mark_loan_lost(
    loan_id: int,
    note: str | None = None,
    current=Depends(get_current_librarian),
):
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Load settings
        cur.execute("""
            SELECT COALESCE(lost_book_fine, 0) AS lost_book_fine
            FROM system_settings
            ORDER BY settings_id ASC
            LIMIT 1
        """)
        settings = cur.fetchone()
        lost_fee = float((settings and settings["lost_book_fine"]) or 0)
        if lost_fee <= 0:
            raise HTTPException(status_code=400, detail="Lost book fine is not configured")

        # Load loan (must be active)
        cur.execute("""
            SELECT loan_id, student_id, copy_id, returned_at, status
            FROM loan
            WHERE loan_id = %s
        """, (loan_id,))
        loan = cur.fetchone()
        if not loan:
            raise HTTPException(status_code=404, detail="Loan not found")
        if loan["returned_at"] is not None:
            raise HTTPException(status_code=400, detail="Loan is already returned")
        if (loan.get("status") or "").lower() == "lost":
            # Already lost; still allow return info but no duplicate fine
            pass

        # Prevent duplicate lost fine for this loan
        cur.execute("""
            SELECT fine_id
            FROM fine
            WHERE loan_id = %s AND reason = 'lost'
            ORDER BY assessed_at DESC
            LIMIT 1
        """, (loan_id,))
        existing = cur.fetchone()
        if existing:
            raise HTTPException(status_code=400, detail="Lost fine already exists for this loan")

        # Update loan + copy status
        cur.execute("UPDATE loan SET status = 'lost' WHERE loan_id = %s", (loan_id,))
        cur.execute("UPDATE book_copy SET status = 'lost' WHERE copy_id = %s", (loan["copy_id"],))

        # Create fine
        cur.execute("""
            INSERT INTO fine (loan_id, student_id, amount, amount_paid, status, reason, assessed_at)
            VALUES (%s, %s, %s, 0, 'unpaid', 'lost', NOW())
            RETURNING fine_id
        """, (loan_id, loan["student_id"], lost_fee))
        fine_row = cur.fetchone()

        conn.commit()

        return {
            "message": "Marked as lost and fine created",
            "loan_id": loan_id,
            "copy_id": loan["copy_id"],
            "student_id": loan["student_id"],
            "fine_id": fine_row["fine_id"],
            "lost_fee": lost_fee,
            "note": note,
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Mark lost failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

@app.post("/api/circulation/damage")
def mark_damage_and_fine(
    loan_id: int,
    severity: str,
    notes: str | None = None,
    current=Depends(get_current_librarian),
):
    sev = (severity or "").strip().lower()
    if sev not in ("minor", "major"):
        raise HTTPException(status_code=400, detail="severity must be 'minor' or 'major'")

    conn = get_connection()
    cur = conn.cursor()
    try:
        # Load settings
        cur.execute("""
            SELECT
              COALESCE(damage_fine_minor, 0) AS damage_fine_minor,
              COALESCE(damage_fine_major, 0) AS damage_fine_major
            FROM system_settings
            ORDER BY settings_id ASC
            LIMIT 1
        """)
        settings = cur.fetchone()
        if not settings:
            raise HTTPException(status_code=404, detail="system_settings not configured")

        fee = float(settings["damage_fine_minor"] if sev == "minor" else settings["damage_fine_major"])
        if fee <= 0:
            raise HTTPException(status_code=400, detail="Damage fine is not configured")

        librarian_id = current["librarian_id"]

        # Load loan
        cur.execute("""
            SELECT loan_id, student_id, copy_id, returned_at
            FROM loan
            WHERE loan_id = %s
        """, (loan_id,))
        loan = cur.fetchone()
        if not loan:
            raise HTTPException(status_code=404, detail="Loan not found")

        # Prevent duplicate damage fine for this loan & severity
        reason = f"damage_{sev}"
        cur.execute("""
            SELECT fine_id
            FROM fine
            WHERE loan_id = %s AND reason = %s
            ORDER BY assessed_at DESC
            LIMIT 1
        """, (loan_id, reason))
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Damage fine already exists for this loan")

        # Insert damage report
        cur.execute("""
            INSERT INTO damage_report (copy_id, student_id, recorded_by, severity, notes, reported_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            RETURNING damage_id
        """, (loan["copy_id"], loan["student_id"], librarian_id, sev, notes))
        damage_row = cur.fetchone()

        # Mark copy status (optional but recommended)
        cur.execute("UPDATE book_copy SET status = 'damaged' WHERE copy_id = %s", (loan["copy_id"],))

        # Insert fine
        cur.execute("""
            INSERT INTO fine (loan_id, student_id, amount, amount_paid, status, reason, assessed_at)
            VALUES (%s, %s, %s, 0, 'unpaid', %s, NOW())
            RETURNING fine_id
        """, (loan_id, loan["student_id"], fee, reason))
        fine_row = cur.fetchone()

        conn.commit()

        return {
            "message": "Damage recorded and fine created",
            "loan_id": loan_id,
            "copy_id": loan["copy_id"],
            "student_id": loan["student_id"],
            "damage_id": damage_row["damage_id"],
            "fine_id": fine_row["fine_id"],
            "severity": sev,
            "fee": fee,
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Damage fine failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

@app.get("/api/circulation/precheck")
def circulation_precheck(student_code: str, current=Depends(get_current_librarian)):
    code = (student_code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="student_code is required")

    conn = get_connection()
    cur = conn.cursor()
    try:
        # 1) Student
        cur.execute(
            """
            SELECT student_id, student_code, last_name, first_name, grade, section, status
            FROM student
            WHERE student_code = %s
            """,
            (code,),
        )
        st = cur.fetchone()
        if not st:
            raise HTTPException(status_code=404, detail="Student not found")

        # 2) Get settings (grade_rule -> system_settings)
        settings = _get_settings_for_grade(cur, st["grade"])
        loan_days = int(settings["loan_period_days"] or 7)
        borrow_limit = int(settings["max_borrow_limit"] or 3)

        today = date.today()
        due = today + timedelta(days=loan_days)

        student_id = st["student_id"]

        # 3) Unpaid fines (balance)
        cur.execute(
            """
            SELECT COALESCE(SUM((amount - amount_paid)), 0) AS balance
            FROM fine
            WHERE student_id = %s
              AND COALESCE(status,'') NOT IN ('paid','void','waived')
              AND (amount - amount_paid) > 0
            """,
            (student_id,),
        )
        fine_row = cur.fetchone()
        fine_balance = float(fine_row["balance"] or 0)

        # 4) Overdue loans count (not returned and due_at < now)
        cur.execute(
            """
            SELECT COUNT(*)::int AS overdue_count
            FROM loan
            WHERE student_id = %s
              AND returned_at IS NULL
              AND due_at IS NOT NULL
              AND due_at < NOW()
            """,
            (student_id,),
        )
        overdue_count = int(cur.fetchone()["overdue_count"] or 0)

        # 5) Active borrowed count
        cur.execute(
            """
            SELECT COUNT(*)::int AS borrowed_count
            FROM loan
            WHERE student_id = %s
              AND returned_at IS NULL
            """,
            (student_id,),
        )
        borrowed_count = int(cur.fetchone()["borrowed_count"] or 0)

        # 6) Damage history (optional: treat as warning)
        cur.execute(
            """
            SELECT COUNT(*)::int AS damage_count
            FROM damage_report
            WHERE student_id = %s
            """,
            (student_id,),
        )
        damage_count = int(cur.fetchone()["damage_count"] or 0)

        # 7) Build reasons list (frontend displays this)
        reasons = []

        # Student inactive blocks checkout
        if (st["status"] or "active").lower() != "active":
            reasons.append({
                "type": "student_inactive",
                "label": "Student account is not active",
                "blocking": True
            })

        if fine_balance > 0:
            reasons.append({
                "type": "unpaid_fines",
                "label": "Unpaid fines",
                "amount": fine_balance,
                "blocking": True
            })

        if overdue_count > 0:
            reasons.append({
                "type": "overdue_books",
                "label": "Overdue books",
                "count": overdue_count,
                "blocking": True
            })

        if borrowed_count >= borrow_limit:
            reasons.append({
                "type": "borrow_limit",
                "label": "Borrow limit reached",
                "current": borrowed_count,
                "limit": borrow_limit,
                "blocking": True
            })

        if damage_count > 0:
            reasons.append({
                "type": "damage_history",
                "label": "Has damage history",
                "count": damage_count,
                "blocking": False
            })

        ok = not any(r.get("blocking") for r in reasons)

        return {
            "ok": ok,
            "student": {
                "student_id": st["student_id"],
                "student_code": st["student_code"],
                "last_name": st["last_name"],
                "first_name": st["first_name"],
                "grade": st["grade"],
                "section": st["section"],
                "status": st["status"],
                "grade_section": f"{st['grade']} - {st['section']}" if st["grade"] or st["section"] else "",
            },
            "rules": {
                "loan_period_days": loan_days,
                "max_borrow_limit": borrow_limit,
                "fine_per_day": float(settings["fine_per_day"] or 0),
                "max_renewals": int(settings["max_renewals"] or 0),
                "block_renew_if_overdue": bool(settings["block_renew_if_overdue"]),
            },
            "stats": {
                "borrowed_count": borrowed_count,
                "overdue_count": overdue_count,
                "fine_balance": fine_balance,
                "damage_count": damage_count,
            },
            "date_borrowed": today.isoformat(),
            "due_date": due.isoformat(),
            "reasons": reasons,
        }
    finally:
        cur.close()
        conn.close()

@app.get("/api/circulation/overdue")
def overdue_list(student_code: str, current=Depends(get_current_librarian)):
    code = (student_code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="student_code is required")

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT student_id FROM student WHERE student_code=%s", (code,))
        st = cur.fetchone()
        if not st:
            raise HTTPException(status_code=404, detail="Student not found")

        cur.execute(
            """
            SELECT
              l.loan_id,
              l.borrowed_at,
              l.due_at,
              bc.barcode,
              b.title
            FROM loan l
            JOIN book_copy bc ON bc.copy_id = l.copy_id
            JOIN book b ON b.book_id = bc.book_id
            WHERE l.student_id = %s
              AND l.returned_at IS NULL
              AND l.due_at IS NOT NULL
              AND l.due_at < NOW()
            ORDER BY l.due_at ASC
            """,
            (st["student_id"],),
        )
        return {"items": cur.fetchall()}
    finally:
        cur.close()
        conn.close()

@app.put("/api/settings/circulation")
def update_circulation_settings(
    loan_period_days: int | None = None,
    fine_per_day: float | None = None,
    max_borrow_limit: int | None = None,
    max_renewals: int | None = None,
    block_renew_if_overdue: bool | None = None,
    current=Depends(get_current_librarian),
):
    """
    Updates global circulation defaults.
    Only fields provided will be updated.
    """
    conn = get_connection()
    cur = conn.cursor()

    try:
        updates = []
        params = []

        if loan_period_days is not None:
            if loan_period_days < 1:
                raise HTTPException(status_code=400, detail="loan_period_days must be >= 1")
            updates.append("loan_period_days = %s")
            params.append(loan_period_days)

        if fine_per_day is not None:
            if fine_per_day < 0:
                raise HTTPException(status_code=400, detail="fine_per_day must be >= 0")
            updates.append("fine_per_day = %s")
            params.append(fine_per_day)

        if max_borrow_limit is not None:
            if max_borrow_limit < 1:
                raise HTTPException(status_code=400, detail="max_borrow_limit must be >= 1")
            updates.append("max_borrow_limit = %s")
            params.append(max_borrow_limit)

        if max_renewals is not None:
            if max_renewals < 0:
                raise HTTPException(status_code=400, detail="max_renewals must be >= 0")
            updates.append("max_renewals = %s")
            params.append(max_renewals)

        if block_renew_if_overdue is not None:
            updates.append("block_renew_if_overdue = %s")
            params.append(block_renew_if_overdue)

        if not updates:
            raise HTTPException(status_code=400, detail="No fields provided to update")

        # Always update audit fields
        updates.append("updated_at = NOW()")
        updates.append("updated_by = %s")
        params.append(current["librarian_id"])

        # WHERE clause param
        params.append(1)

        cur.execute(
            f"""
            UPDATE system_settings
            SET {", ".join(updates)}
            WHERE settings_id = %s
            """,
            tuple(params),
        )

        conn.commit()
        return {"message": "Circulation settings updated"}

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Settings update failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

# -----------------------------
# PASSWORD RULES
# -----------------------------
def validate_new_password(password: str) -> None:
    """
    Basic password validation.
    Adjust as you want (length/complexity).

    NOTE (bcrypt): passwords longer than ~72 bytes can cause errors or truncation.
    We'll keep it simple and safe by limiting length.
    """
    if password is None or not password.strip():
        raise HTTPException(status_code=400, detail="Password is required")

    pw = password.strip()

    if len(pw) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    # If you use bcrypt, keep under 72 chars (ASCII) to avoid backend issues.
    if len(pw.encode("utf-8")) > 72:
        raise HTTPException(status_code=400, detail="Password too long (max 72 bytes)")
    
# -----------------------------
# SETTINGS: CHANGE PASSWORD (Librarian Only)
# -----------------------------
@app.post("/api/settings/change-password")
def change_password(
    current_password: str,
    new_password: str,
    current=Depends(get_current_librarian),
):
    """
    Changes the password for the logged-in librarian.

    Flow:
      1) Verify current password
      2) Validate new password
      3) Hash and update password_hash
    """
    validate_new_password(new_password)

    if current_password.strip() == new_password.strip():
        raise HTTPException(status_code=400, detail="New password must be different from current password")

    conn = get_connection()
    cur = conn.cursor()

    try:
        # 1) Load current hash from DB (do not trust token payload for hash)
        cur.execute(
            """
            SELECT password_hash
            FROM librarian
            WHERE librarian_id = %s
            """,
            (current["librarian_id"],),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Librarian not found")

        # 2) Verify current password
        if not verify_password(current_password, row["password_hash"]):
            raise HTTPException(status_code=400, detail="Current password is incorrect")

        # 3) Update password hash
        new_hash = hash_password(new_password)

        cur.execute(
            """
            UPDATE librarian
            SET password_hash = %s
            WHERE librarian_id = %s
            """,
            (new_hash, current["librarian_id"]),
        )

        conn.commit()
        return {"message": "Password changed successfully"}

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Change password failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

# -----------------------------
# SETTINGS: ENROLL NEW LIBRARIAN (New Profile) (Librarian Only)
# -----------------------------
@app.post("/api/settings/librarians/enroll")
def enroll_librarian(
    enroll_key: str,
    username: str,
    email: str,
    password: str,
    first_name: str | None = None,
    last_name: str | None = None,
    current=Depends(get_current_librarian),
):
    """
    Creates a new librarian account.

    Security:
      - Requires enroll_key stored in .env (LIBRARIAN_ENROLL_KEY)
      - This prevents any random logged-in librarian from creating accounts.

    Behavior:
      - Validates password
      - Hashes password
      - Inserts into librarian table
    """
    expected_key = os.getenv("LIBRARIAN_ENROLL_KEY", "")
    if not expected_key or enroll_key != expected_key:
        raise HTTPException(status_code=403, detail="Invalid enroll key")

    if not username.strip():
        raise HTTPException(status_code=400, detail="username is required")
    if not email.strip():
        raise HTTPException(status_code=400, detail="email is required")

    validate_new_password(password)

    pw_hash = hash_password(password)

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO librarian (username, email, password_hash, first_name, last_name, is_active, created_at)
            VALUES (%s, %s, %s, %s, %s, TRUE, NOW())
            RETURNING librarian_id, username, email, first_name, last_name, is_active, created_at
            """,
            (
                username.strip(),
                email.strip(),
                pw_hash,
                first_name.strip() if first_name else None,
                last_name.strip() if last_name else None,
            ),
        )
        new_lib = cur.fetchone()
        conn.commit()

        return {
            "message": "Librarian enrolled successfully",
            "librarian": new_lib,
        }

    except Exception as e:
        conn.rollback()
        # Most common: unique constraint on username/email
        raise HTTPException(status_code=500, detail=f"Enroll librarian failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

# -----------------------------
# SETTINGS: LIST LIBRARIANS (Librarian Only)
# -----------------------------
@app.get("/api/settings/librarians")
def list_librarians(current=Depends(get_current_librarian)):
    """
    Lists librarian profiles (no password hash).
    Useful for Settings -> New Profile page to show existing accounts.
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT librarian_id, username, email, first_name, last_name, is_active, created_at, last_login_at
            FROM librarian
            ORDER BY created_at DESC
            LIMIT 200
            """
        )
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()
    

# -----------------------------
# LIBRARIAN: ADVANCED BOOK SEARCH (Librarian Only) - FIXED
# -----------------------------
@app.get("/api/librarian/books/search")
def librarian_advanced_search(
    q: str = "",
    sort: str = "relevance",   # relevance|title_asc|title_desc|newest|most_borrowed
    page: int = 1,
    page_size: int = 20,
    current=Depends(get_current_librarian),
):
    """
    Advanced search for librarians:
      - Searches title/author/publisher/genre/subject/section/catalog_key
      - Searches identifiers (ISBN etc.) via book_identifier
      - Searches barcode via book_copy
      - Returns availability counts (available / total copies)
      - Supports sorting + pagination

    Fix note:
      - Uses EXISTS-based relevance scoring (safe with GROUP BY).
    """

    if page < 1:
        page = 1
    if page_size < 1 or page_size > 100:
        page_size = 20

    q_clean = (q or "").strip()
    like = f"%{q_clean}%"
    offset = (page - 1) * page_size

    # Sorting
    # We define an ORDER BY block that does not reference ungrouped joined columns.
    if sort == "title_asc":
        order_sql = "b.title ASC"
        order_params = []
    elif sort == "title_desc":
        order_sql = "b.title DESC"
        order_params = []
    elif sort == "newest":
        order_sql = "b.created_at DESC"
        order_params = []
    elif sort == "most_borrowed":
        order_sql = "borrow_count DESC NULLS LAST, b.title ASC"
        order_params = []
    else:
        # Default = relevance
        # Lower score means higher priority.
        # Uses EXISTS subqueries so it works with GROUP BY.
        order_sql = """
        (
          CASE
            WHEN b.title ILIKE %s THEN 1
            WHEN b.author ILIKE %s THEN 2
            WHEN EXISTS (
              SELECT 1 FROM book_identifier bi2
              WHERE bi2.book_id = b.book_id AND bi2.id_value ILIKE %s
            ) THEN 3
            WHEN EXISTS (
              SELECT 1 FROM book_copy bc3
              WHERE bc3.book_id = b.book_id AND bc3.barcode ILIKE %s
            ) THEN 4
            ELSE 99
          END
        ) ASC,
        b.title ASC
        """
        order_params = [like, like, like, like]

    conn = get_connection()
    cur = conn.cursor()
    try:
        # Main query:
        # - aggregates copy counts + availability
        # - computes borrow_count via CTE
        # - WHERE uses EXISTS for identifier/barcode matching (no duplicate explosion)
        cur.execute(
            f"""
            WITH borrow AS (
                SELECT
                    bc.book_id,
                    COUNT(*)::int AS borrow_count
                FROM loan l
                JOIN book_copy bc ON bc.copy_id = l.copy_id
                GROUP BY bc.book_id
            )
            SELECT
                b.book_id,
                b.title,
                b.author,
                b.publisher,
                b.pub_year,
                b.genre,
                b.subject,
                b.section,
                b.cover_url,
                b.created_at,

                COALESCE(borrow.borrow_count, 0) AS borrow_count,

                COUNT(bc2.copy_id)::int AS total_copies,
                COUNT(CASE WHEN bc2.status = 'available' THEN 1 END)::int AS available_copies
            FROM book b
            LEFT JOIN book_copy bc2 ON bc2.book_id = b.book_id
            LEFT JOIN borrow ON borrow.book_id = b.book_id
            WHERE
                (%s = '' OR (
                    b.title ILIKE %s OR
                    b.author ILIKE %s OR
                    COALESCE(b.publisher,'') ILIKE %s OR
                    COALESCE(b.genre,'') ILIKE %s OR
                    COALESCE(b.subject,'') ILIKE %s OR
                    COALESCE(b.section,'') ILIKE %s OR
                    COALESCE(b.catalog_key,'') ILIKE %s OR

                    EXISTS (
                        SELECT 1 FROM book_identifier bi
                        WHERE bi.book_id = b.book_id AND bi.id_value ILIKE %s
                    ) OR

                    EXISTS (
                        SELECT 1 FROM book_copy bc
                        WHERE bc.book_id = b.book_id AND bc.barcode ILIKE %s
                    )
                ))
            GROUP BY b.book_id, borrow.borrow_count
            ORDER BY {order_sql}
            LIMIT %s OFFSET %s
            """,
            tuple(
                # WHERE params
                [q_clean, like, like, like, like, like, like, like, like, like]
                # ORDER params (only for relevance)
                + order_params
                # paging
                + [page_size, offset]
            ),
        )

        rows = cur.fetchall()

        return {
            "page": page,
            "page_size": page_size,
            "sort": sort,
            "q": q_clean,
            "results": rows,
        }

    finally:
        cur.close()
        conn.close()

# -----------------------------
# PUBLIC OPAC: SEARCH/LIST BOOKS (No Login)
# -----------------------------
@app.get("/api/opac/books")
def opac_search(
    q: str = "",
    genre: str | None = None,
    subject: str | None = None,
    section: str | None = None,
    sort: str = "title_asc",   # title_asc|title_desc|newest|most_borrowed
    page: int = 1,
    page_size: int = 24,
):
    """
    Public OPAC list/search:
      - filters: genre/subject/section
      - sorts: A–Z, Newest, Most Borrowed
      - includes availability: available/total
    """

    if page < 1:
        page = 1
    if page_size < 1 or page_size > 60:
        page_size = 24

    q_clean = (q or "").strip()
    like = f"%{q_clean}%"
    offset = (page - 1) * page_size

    where = []
    params = []

    if q_clean:
    # Normalize possible ISBN (remove hyphens/spaces/etc; keep digits and X)
        isbn_norm = re.sub(r"[^0-9Xx]", "", q_clean).upper()

    where.append("""
      (
        b.title ILIKE %s
        OR b.author ILIKE %s
        OR COALESCE(b.subject,'') ILIKE %s
        OR EXISTS (
          SELECT 1
          FROM book_identifier bi
          WHERE bi.book_id = b.book_id
            AND (
              bi.id_value ILIKE %s
              OR regexp_replace(upper(bi.id_value), '[^0-9X]', '', 'g') = %s
            )
        )
      )
    """)
    params += [like, like, like, like, isbn_norm]

    if genre and genre.strip():
        where.append("COALESCE(b.genre,'') ILIKE %s")
        params.append(f"%{genre.strip()}%")

    if subject and subject.strip():
        where.append("COALESCE(b.subject,'') ILIKE %s")
        params.append(f"%{subject.strip()}%")

    if section and section.strip():
        where.append("COALESCE(b.section,'') ILIKE %s")
        params.append(f"%{section.strip()}%")

    where_sql = " AND ".join(where) if where else "TRUE"

    order_sql = "b.title ASC"
    if sort == "title_desc":
        order_sql = "b.title DESC"
    elif sort == "newest":
        order_sql = "b.created_at DESC"
    elif sort == "most_borrowed":
        order_sql = "borrow_count DESC NULLS LAST, b.title ASC"

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            WITH borrow AS (
                SELECT bc.book_id, COUNT(*)::int AS borrow_count
                FROM loan l
                JOIN book_copy bc ON bc.copy_id = l.copy_id
                GROUP BY bc.book_id
            )
            SELECT
                b.book_id,
                b.title,
                b.author,
                b.genre,
                b.subject,
                b.section,
                b.cover_url,
                COALESCE(borrow.borrow_count, 0) AS borrow_count,
                COUNT(bc.copy_id)::int AS total_copies,
                COUNT(CASE WHEN bc.status='available' THEN 1 END)::int AS available_copies
            FROM book b
            LEFT JOIN book_copy bc ON bc.book_id=b.book_id
            LEFT JOIN borrow ON borrow.book_id=b.book_id
            WHERE {where_sql}
            GROUP BY b.book_id, borrow.borrow_count
            ORDER BY {order_sql}
            LIMIT %s OFFSET %s
            """,
            tuple(params + [page_size, offset]),
        )
        rows = cur.fetchall()

        return {"page": page, "page_size": page_size, "sort": sort, "results": rows}

    finally:
        cur.close()
        conn.close()

# -----------------------------
# PUBLIC OPAC: BOOK DETAILS (No Login)
# -----------------------------
@app.get("/api/opac/books/{book_id}")
def opac_book_details(book_id: int):
    """
    Public OPAC book detail:
      - full bibliographic details
      - availability counts
      - shelf/location section
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT book_id, title, author, publisher, pub_year, genre, subject, section, cover_url, catalog_key
            FROM book
            WHERE book_id = %s
            """,
            (book_id,),
        )
        book = cur.fetchone()
        if not book:
            raise HTTPException(status_code=404, detail="Book not found")

        cur.execute(
            """
            SELECT
                COUNT(*)::int AS total_copies,
                COUNT(CASE WHEN status='available' THEN 1 END)::int AS available_copies
            FROM book_copy
            WHERE book_id = %s
            """,
            (book_id,),
        )
        counts = cur.fetchone()

        return {"book": book, "availability": counts}

    finally:
        cur.close()
        conn.close()
# -----------------------------
# PUBLIC OPAC: FILTER OPTIONS (No Login)
# -----------------------------
@app.get("/api/opac/filters")
def opac_filters():
    """
    Returns distinct filter options for genre/subject/section.
    Useful for dropdowns in the public OPAC.
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT DISTINCT COALESCE(NULLIF(TRIM(genre), ''), NULL) AS v
            FROM book
            WHERE genre IS NOT NULL AND TRIM(genre) <> ''
            ORDER BY v ASC
        """)
        genres = [r["v"] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT COALESCE(NULLIF(TRIM(subject), ''), NULL) AS v
            FROM book
            WHERE subject IS NOT NULL AND TRIM(subject) <> ''
            ORDER BY v ASC
        """)
        subjects = [r["v"] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT COALESCE(NULLIF(TRIM(section), ''), NULL) AS v
            FROM book
            WHERE section IS NOT NULL AND TRIM(section) <> ''
            ORDER BY v ASC
        """)
        sections = [r["v"] for r in cur.fetchall()]

        return {"genres": genres, "subjects": subjects, "sections": sections}

    finally:
        cur.close()
        conn.close()

from io import BytesIO
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, letter, legal
from reportlab.graphics.barcode import code128
from fastapi import HTTPException


def build_title_barcode_labels_pdf(
    labels: list[dict],
    paper: str = "A4",     # A4 | LEGAL | SHORT
    columns: int = 3,      # 2 or 3
) -> bytes:
    """
    labels: list of dict with keys:
      - title: str
      - barcode: str  (Copy ID / barcode value)

    Output per label:
      [ Barcode graphic ]
      Title
      Copy ID

    Layout:
      - 2 or 3 columns
      - border outline around each label
    """

    # --- Paper size mapping ---
    paper_key = (paper or "A4").strip().upper()
    if paper_key in ["SHORT", "LETTER", "SHORT_BOND", "SHORTBOND"]:
        page_size = letter  # short bond paper
    elif paper_key in ["LEGAL", "LONG", "LONG_BOND", "LONGBOND"]:
        page_size = legal
    else:
        page_size = A4

    # --- Column validation ---
    if columns not in [2, 3]:
        raise HTTPException(status_code=400, detail="columns must be 2 or 3")

    buffer = BytesIO()

    doc = SimpleDocTemplate(
        buffer,
        pagesize=page_size,
        leftMargin=0.35 * inch,
        rightMargin=0.35 * inch,
        topMargin=0.45 * inch,
        bottomMargin=0.45 * inch,
        title="Barcode Labels",
        author="Golden Key OPAC",
    )

    styles = getSampleStyleSheet()

    # Smaller, cleaner label text
    title_style = ParagraphStyle(
        "LabelTitle",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=7,
        leading=8,
        alignment=1,  # center
        spaceAfter=1,
    )
    id_style = ParagraphStyle(
        "LabelID",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=7,
        leading=8,
        alignment=1,  # center
    )

    usable_w = page_size[0] - doc.leftMargin - doc.rightMargin
    col_w = usable_w / columns

    # Label height tuning (smaller than before)
    # 3 columns = tighter labels
    label_h = 1.05 * inch if columns == 3 else 1.20 * inch

    # Barcode sizing (smaller)
    bar_height = 0.42 * inch if columns == 3 else 0.50 * inch
    bar_width = 0.010 * inch  # thinner bars

    def make_cell(item: dict):
        raw_title = (item.get("title") or "").strip()
        raw_code = (item.get("barcode") or "").strip()

        # Title truncation to avoid overflow
        title = raw_title
        if len(title) > 48:
            title = title[:48].rstrip() + "…"

        # Barcode value is the Copy ID
        barcode_value = raw_code if raw_code else "N/A"

        bc = code128.Code128(
            barcode_value,
            barHeight=bar_height,
            barWidth=bar_width,
            humanReadable=False,  # you want text under barcode, not embedded
        )

        # Cell content: barcode first, then title + copy id under
        inner = Table(
            [
                [bc],
                [Spacer(1, 2)],
                [Paragraph(title, title_style)],
                [Paragraph(f"Copy ID: {barcode_value}", id_style)],
            ],
            colWidths=[col_w - 10],
        )

        inner.setStyle(TableStyle([
            ("LEFTPADDING", (0, 0), (-1, -1), 3),
            ("RIGHTPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ]))

        return inner

    # Build label grid
    grid = []
    row = []
    for item in labels:
        row.append(make_cell(item))
        if len(row) == columns:
            grid.append(row)
            row = []
    if row:
        while len(row) < columns:
            row.append("")
        grid.append(row)

    sheet = Table(
        grid,
        colWidths=[col_w] * columns,
        rowHeights=[label_h] * len(grid),
    )

    # Outlines between labels (cut guides)
    sheet.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.6, colors.black),  # strong outline border
        ("LEFTPADDING", (0, 0), (-1, -1), 2),
        ("RIGHTPADDING", (0, 0), (-1, -1), 2),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))

    doc.build([sheet])

    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes

# -----------------------------
# CIRCULATION: CHECK IN (Librarian Only)  ✅ NEW ENDPOINT
# -----------------------------
@app.post("/api/circulation/checkin")
def checkin_book(
    barcode: str,
    student_code: str,
    is_damaged: bool = False,
    severity: str | None = None,
    notes: str | None = None,
    current=Depends(get_current_librarian),
):
    conn = get_connection()
    cur = conn.cursor()
    try:
        # 1) Find copy + book
        cur.execute(
            """
            SELECT
              bc.copy_id, bc.barcode, bc.status AS copy_status,
              b.book_id, b.title, b.author
            FROM book_copy bc
            JOIN book b ON b.book_id = bc.book_id
            WHERE bc.barcode = %s
            """,
            (barcode,),
        )
        copy_row = cur.fetchone()
        if not copy_row:
            raise HTTPException(status_code=404, detail="Copy barcode not found")

        # 2) Find active loan for this copy (not yet returned)
        cur.execute(
            """
            SELECT
              l.loan_id, l.student_id, l.borrowed_at, l.due_at, l.returned_at, l.status,
              s.student_code, s.last_name, s.first_name, s.grade, s.section
            FROM loan l
            JOIN student s ON s.student_id = l.student_id
            WHERE l.copy_id = %s AND l.returned_at IS NULL
            ORDER BY l.borrowed_at DESC
            LIMIT 1
            """,
            (copy_row["copy_id"],),
        )
        loan = cur.fetchone()
        if not loan:
            # Copy exists, but there is no active loan to close
            raise HTTPException(status_code=404, detail="No active loan found for this barcode")

        # 3) Safety check: student_code must match the borrower of the active loan
        if (loan["student_code"] or "").strip() != (student_code or "").strip():
            raise HTTPException(status_code=400, detail="Student code does not match the active borrower")

        # 4) Overdue + fine calculation
        cur.execute(
            "SELECT GREATEST(0, (DATE(NOW()) - DATE(%s))) AS overdue_days",
            (loan["due_at"],),
        )
        overdue_days = int(cur.fetchone()["overdue_days"])

        # Try grade_rule first
        cur.execute(
            """
            SELECT fine_per_day
            FROM grade_rule
            WHERE grade = %s
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
            """,
            (loan["grade"],),
        )
        rule = cur.fetchone()

        fine_per_day = None
        if rule and rule.get("fine_per_day") is not None:
            fine_per_day = float(rule["fine_per_day"])
        else:
            # fallback to system_settings
            cur.execute(
                """
                SELECT fine_per_day
                FROM system_settings
                ORDER BY updated_at DESC
                LIMIT 1
                """
            )
            settings = cur.fetchone()
            fine_per_day = float(settings["fine_per_day"]) if settings and settings.get("fine_per_day") is not None else 0.0

        projected_fine = round(overdue_days * fine_per_day, 2) if (overdue_days > 0 and fine_per_day > 0) else 0.0

        fine_created = None
        if projected_fine > 0:
            # Create a fine record as unpaid (you can change reason text as you like)
            cur.execute(
                """
                INSERT INTO fine (loan_id, student_id, amount, amount_paid, status, reason, assessed_at)
                VALUES (%s, %s, %s, 0, 'unpaid', %s, NOW())
                RETURNING fine_id, amount, status
                """,
                (loan["loan_id"], loan["student_id"], projected_fine, f"Overdue ({overdue_days} day/s)"),
            )
            fine_created = cur.fetchone()

        # 5) Close the loan
        cur.execute(
            """
            UPDATE loan
            SET returned_at = NOW(),
                status = 'returned'
            WHERE loan_id = %s
            """,
            (loan["loan_id"],),
        )

        # 6) Update copy status
        new_copy_status = "available"
        if is_damaged:
            new_copy_status = "damaged"

            # Optional validation
            if severity is None or str(severity).strip() == "":
                severity_val = "unspecified"
            else:
                severity_val = str(severity).strip()

            cur.execute(
                """
                INSERT INTO damage_report (copy_id, student_id, recorded_by, severity, notes, reported_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                """,
                (copy_row["copy_id"], loan["student_id"], int(current["librarian_id"]), severity_val, notes),
            )

        cur.execute(
            """
            UPDATE book_copy
            SET status = %s
            WHERE copy_id = %s
            """,
            (new_copy_status, copy_row["copy_id"]),
        )

        conn.commit()

        full_name = f"{(loan.get('last_name') or '').strip()}, {(loan.get('first_name') or '').strip()}".strip(", ").strip()

        return {
            "message": "Check-in successful",
            "barcode": copy_row["barcode"],
            "copy_status": new_copy_status,
            "book": {
                "book_id": copy_row["book_id"],
                "title": copy_row["title"],
                "author": copy_row["author"],
            },
            "student": {
                "student_id": loan["student_id"],
                "student_code": loan["student_code"],
                "name": full_name,
                "grade": loan["grade"],
                "section": loan["section"],
            },
            "loan": {
                "loan_id": loan["loan_id"],
                "borrowed_at": loan["borrowed_at"].isoformat() if loan.get("borrowed_at") else None,
                "due_at": loan["due_at"].isoformat() if loan.get("due_at") else None,
                "returned_at": datetime.utcnow().isoformat(),
                "overdue_days": overdue_days,
                "fine_per_day": fine_per_day,
                "projected_fine": projected_fine,
            },
            "fine_created": fine_created,
            "damage_logged": bool(is_damaged),
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Check-in failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

@app.get("/api/circulation/checkin/lookup")
def checkin_lookup(
    barcode: str,
    current=Depends(get_current_librarian),
):
    """
    Lookup for Check-in screen.
    Scan ONE book barcode -> returns:
      - copy + book info
      - active loan (if any)
      - borrower student info
      - overdue status + projected fine (based on grade_rule)
      - current unpaid fines list (for quick payment)
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        # 1) Copy + Book
        cur.execute("""
            SELECT
                b.title,
                b.author,
                bi.id_value AS isbn,
                s.first_name,
                s.last_name,
                l.loan_id
            FROM book_copy bc
            JOIN book b ON b.book_id = bc.book_id
            LEFT JOIN book_identifier bi ON bi.book_id = b.book_id AND bi.is_primary = TRUE
            LEFT JOIN loan l ON l.copy_id = bc.copy_id AND l.status = 'borrowed'
            LEFT JOIN student s ON s.student_id = l.student_id
            WHERE bc.barcode = %s
        """, (barcode,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Copy barcode not found")

        # 2) Active loan (if borrowed)
        cur.execute(
            """
            SELECT
              l.loan_id, l.borrowed_at, l.due_at,
              s.student_id, s.student_code, s.last_name, s.first_name, s.grade, s.section
            FROM loan l
            JOIN student s ON s.student_id = l.student_id
            WHERE l.copy_id = %s AND l.returned_at IS NULL
            ORDER BY l.borrowed_at DESC
            LIMIT 1
            """,
            (row["copy_id"],),
        )
        loan = cur.fetchone()

        if not loan:
            return {
                "barcode": row["barcode"],
                "copy_status": row["copy_status"],
                "book": {
                    "book_id": row["book_id"],
                    "title": row["title"],
                    "author": row["author"],
                    "isbn": row["isbn"],
                },
                "has_active_loan": False,
                "message": "No active loan for this barcode (already returned or never borrowed).",
            }

        # 3) Overdue calc + projected fine
        cur.execute("SELECT (NOW() > %s) AS is_overdue", (loan["due_at"],))
        is_overdue = bool(cur.fetchone()["is_overdue"])

        cur.execute(
            "SELECT GREATEST(0, (DATE(NOW()) - DATE(%s))) AS overdue_days",
            (loan["due_at"],),
        )
        overdue_days = int(cur.fetchone()["overdue_days"])

        cur.execute(
            """
            SELECT fine_per_day
            FROM grade_rule
            WHERE grade = %s
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
            """,
            (loan["grade"],),
        )
        
        rule = cur.fetchone()

# If grade_rule exists and fine_per_day is set, use it
        if rule and rule.get("fine_per_day") is not None and float(rule["fine_per_day"]) > 0:
            fine_per_day = float(rule["fine_per_day"])
        else:
            # Fallback to global system_settings
            cur.execute(
                """
                SELECT fine_per_day
                FROM system_settings
                ORDER BY updated_at DESC
                LIMIT 1
                """
            )
            settings = cur.fetchone()
            fine_per_day = float(settings["fine_per_day"]) if settings and settings.get("fine_per_day") is not None else 0.0

        projected_fine = round(overdue_days * fine_per_day, 2) if overdue_days > 0 else 0.0

        # 4) Unpaid fines list (for quick pay)
        cur.execute(
            """
            SELECT
              fine_id, amount, amount_paid,
              (amount - amount_paid) AS outstanding,
              status, reason, assessed_at
            FROM fine
            WHERE student_id = %s AND status = 'unpaid'
            ORDER BY assessed_at DESC
            """,
            (loan["student_id"],),
        )
        unpaid = cur.fetchall()

        return {
            "barcode": row["barcode"],
            "copy_status": row["copy_status"],
            "book": {
                "book_id": row["book_id"],
                "title": row["title"],
                "author": row["author"],
                "isbn": row["isbn"],
            },
            "has_active_loan": True,
            "loan": {
                "loan_id": loan["loan_id"],
                "borrowed_at": str(loan["borrowed_at"]),
                "due_at": str(loan["due_at"]),
                "is_overdue": is_overdue,
                "overdue_days": overdue_days,
                "fine_per_day": fine_per_day,
                "projected_fine": projected_fine,
            },
            "student": {
                "student_id": loan["student_id"],
                "student_code": loan["student_code"],
                "last_name": loan["last_name"],
                "first_name": loan["first_name"],
                "grade": loan["grade"],
                "section": loan["section"],
            },
            "unpaid_fines": unpaid,
        }

    finally:
        cur.close()
        conn.close()
# -----------------------------
# BARCODE LABELS: TITLE + BARCODE PDF (Librarian Only)
# -----------------------------
@app.post("/api/barcodes/labels/pdf")
def barcode_labels_pdf(
    copy_ids: list[int] = Body(..., embed=True),
    paper: str = "A4",      # A4 | LEGAL | SHORT
    columns: int = 3,       # 2 or 3
    current=Depends(get_current_librarian),
):
    """
    Generates a label PDF containing ONLY:
      - Book Title
      - Barcode graphic

    Layout:
      - 2 or 3 columns
    Paper:
      - A4 / LEGAL / SHORT (Short bond = Letter)
    """
    if not copy_ids:
        raise HTTPException(status_code=400, detail="copy_ids is required")

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                bc.copy_id,
                bc.barcode,
                b.title
            FROM book_copy bc
            JOIN book b ON b.book_id = bc.book_id
            WHERE bc.copy_id = ANY(%s)
            ORDER BY b.title ASC, bc.copy_id ASC
            """,
            (copy_ids,),
        )
        rows = cur.fetchall()

        labels = [{"title": r["title"], "barcode": r["barcode"]} for r in rows]
        pdf_bytes = build_title_barcode_labels_pdf(labels, paper=paper, columns=columns)

        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": 'attachment; filename="barcode_labels.pdf"'},
        )
    finally:
        cur.close()
        conn.close()

@app.put("/api/books/{book_id}")
def update_book(
    book_id: int,
    payload: BookUpdate,
    current=Depends(get_current_librarian),  # ✅ librarian-only
):
    # Basic validation
    if not payload.title or not payload.title.strip():
        raise HTTPException(status_code=400, detail="Title is required")
    if not payload.author or not payload.author.strip():
        raise HTTPException(status_code=400, detail="Author is required")
    if payload.pub_year is not None and (payload.pub_year < 0 or payload.pub_year > 3000):
        raise HTTPException(status_code=400, detail="Invalid publication year")

    conn = get_connection()
    cur = conn.cursor()

    # Make sure book exists
    cur.execute("SELECT book_id FROM book WHERE book_id = %s", (book_id,))
    exists = cur.fetchone()
    if not exists:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Book not found")

    # Update
    cur.execute(
        """
        UPDATE book
        SET
            title = %s,
            author = %s,
            publisher = %s,
            pub_year = %s,
            genre = %s,
            subject = %s,
            section = %s,
            cover_url = %s,
            updated_at = NOW()
        WHERE book_id = %s
        RETURNING
            book_id, title, author, publisher, pub_year, genre, subject, section, cover_url, updated_at
        """,
        (
            payload.title.strip(),
            payload.author.strip(),
            payload.publisher.strip() if payload.publisher else None,
            payload.pub_year,
            payload.genre.strip() if payload.genre else None,
            payload.subject.strip() if payload.subject else None,
            payload.section.strip() if payload.section else None,
            payload.cover_url.strip() if payload.cover_url else None,
            book_id,
        ),
    )

    updated = cur.fetchone()
    conn.commit()

    cur.close()
    conn.close()

    return {
        "book_id": updated["book_id"],
        "title": updated["title"],
        "author": updated["author"],
        "publisher": updated["publisher"],
        "pub_year": updated["pub_year"],
        "genre": updated["genre"],
        "subject": updated["subject"],
        "section": updated["section"],
        "cover_url": updated.get("cover_url"),
        "updated_at": updated.get("updated_at"),
    }

from fastapi import HTTPException, Depends
from typing import Optional

@app.get("/api/books/{book_id}/damaged-copies")
def list_damaged_copies(
    book_id: int,
    current=Depends(get_current_librarian),
):
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Ensure book exists
        cur.execute("SELECT book_id FROM book WHERE book_id = %s", (book_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Book not found")

        cur.execute(
            """
            SELECT
                bc.copy_id,
                bc.barcode,
                bc.status,
                dr.damage_id,
                dr.severity,
                dr.notes,
                dr.reported_at,
                s.student_code,
                s.last_name,
                s.first_name
            FROM book_copy bc
            LEFT JOIN LATERAL (
                SELECT damage_id, severity, notes, reported_at, student_id
                FROM damage_report
                WHERE copy_id = bc.copy_id
                ORDER BY reported_at DESC
                LIMIT 1
            ) dr ON TRUE
            LEFT JOIN student s ON s.student_id = dr.student_id
            WHERE bc.book_id = %s
              AND bc.status = 'damaged'
            ORDER BY bc.copy_id DESC
            """,
            (book_id,),
        )
        rows = cur.fetchall()

        return {
            "book_id": book_id,
            "damaged": [
                {
                    "copy_id": r["copy_id"],
                    "barcode": r["barcode"],
                    "status": r["status"],
                    "last_damage": {
                        "damage_id": r["damage_id"],
                        "severity": r["severity"],
                        "notes": r["notes"],
                        "reported_at": r["reported_at"].isoformat() if r["reported_at"] else None,
                        "student": {
                            "student_code": r["student_code"],
                            "last_name": r["last_name"],
                            "first_name": r["first_name"],
                        } if r["student_code"] else None,
                    } if r["damage_id"] else None,
                }
                for r in rows
            ],
        }
    finally:
        cur.close()
        conn.close()


@app.post("/api/book-copies/{copy_id}/restore")
def restore_damaged_copy(
    copy_id: int,
    note: Optional[str] = None,
    current=Depends(get_current_librarian),
):
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Only restore if currently damaged
        cur.execute(
            """
            UPDATE book_copy
            SET status = 'available'
            WHERE copy_id = %s AND status = 'damaged'
            RETURNING copy_id, book_id, barcode, status
            """,
            (copy_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Damaged copy not found (or not damaged anymore)")

        conn.commit()

        return {
            "message": "Copy restored to available",
            "copy_id": row["copy_id"],
            "book_id": row["book_id"],
            "barcode": row["barcode"],
            "status": row["status"],
            "note": note,
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Restore failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

from fastapi import HTTPException, Depends

@app.delete("/api/books/{book_id}")
def delete_book(
    book_id: int,
    current=Depends(get_current_librarian),
):
    conn = get_connection()
    cur = conn.cursor()
    try:
        # confirm exists
        cur.execute("SELECT book_id FROM book WHERE book_id = %s", (book_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Book not found")

        cur.execute("DELETE FROM book WHERE book_id = %s RETURNING book_id", (book_id,))
        conn.commit()
        return {"message": "Book record deleted", "book_id": book_id}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")
    finally:
        cur.close()
        conn.close()

from pydantic import BaseModel
from typing import List, Optional
from fastapi import Depends, HTTPException

class MarkPrintedRequest(BaseModel):
    copy_ids: List[int]
    batch_id: Optional[str] = None
    reprint_reason: Optional[str] = None

@app.post("/api/printing/mark-printed")
def mark_printed_json(
    payload: MarkPrintedRequest,
    current=Depends(get_current_librarian),
):
    copy_ids = payload.copy_ids or []
    if not copy_ids:
        raise HTTPException(status_code=400, detail="copy_ids is required")

    batch_id = payload.batch_id or f"BATCH-{int(datetime.utcnow().timestamp())}"
    reprint_reason = (payload.reprint_reason or "").strip() or None

    conn = get_connection()
    cur = conn.cursor()
    try:
        # Load copy states
        cur.execute(
            """
            SELECT copy_id, is_printed
            FROM book_copy
            WHERE copy_id = ANY(%s)
            """,
            (copy_ids,),
        )
        rows = cur.fetchall()
        found_ids = {r["copy_id"] for r in rows}
        missing = [cid for cid in copy_ids if cid not in found_ids]
        if missing:
            raise HTTPException(status_code=404, detail=f"Copies not found: {missing}")

        printed_ids = [r["copy_id"] for r in rows if r["is_printed"]]
        unprinted_ids = [r["copy_id"] for r in rows if not r["is_printed"]]

        # If any already printed => must provide reason
        if printed_ids and not reprint_reason:
            raise HTTPException(status_code=400, detail="reprint_reason is required when reprinting printed copies")

        librarian_id = int(current["librarian_id"])

        # Update copies (mark printed + timestamps)
        cur.execute(
            """
            UPDATE book_copy
            SET is_printed = TRUE,
                printed_at = NOW(),
                printed_by = %s,
                updated_at = NOW()
            WHERE copy_id = ANY(%s)
            """,
            (librarian_id, copy_ids),
        )

        # Increment reprint_count for already-printed copies
        if printed_ids:
            cur.execute(
                """
                UPDATE book_copy
                SET reprint_count = COALESCE(reprint_count, 0) + 1,
                    updated_at = NOW()
                WHERE copy_id = ANY(%s)
                """,
                (printed_ids,),
            )

        # Insert print logs
        def insert_logs(ids, action):
            if not ids:
                return
            cur.execute(
                """
                INSERT INTO print_log (copy_id, printed_by, printed_at, action, batch_id)
                SELECT x, %s, NOW(), %s, %s
                FROM unnest(%s::int[]) AS x
                """,
                (librarian_id, action, batch_id, ids),
            )

        insert_logs(unprinted_ids, "print")
        insert_logs(printed_ids, "reprint")

        # Optional: store reason inside print_log.action or add a note column.
        # Since your print_log has no note field, we can:
        # - either keep just action='reprint'
        # - or add a new column print_log.note (recommended)
        # For now we keep it simple (no schema change).

        conn.commit()

        return {
            "updated_count": len(copy_ids),
            "printed_count": len(unprinted_ids),
            "reprinted_count": len(printed_ids),
            "batch_id": batch_id,
        }
    finally:
        cur.close()
        conn.close()

from fastapi import Query

# -----------------------------
# REPORTS: FILTER OPTIONS (Librarian Only)
# -----------------------------
@app.get("/api/reports/filters")
def reports_filter_options(
    grade: str | None = Query(default=None),
    current=Depends(get_current_librarian),
):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT DISTINCT TRIM(grade) AS grade
            FROM student
            WHERE grade IS NOT NULL AND TRIM(grade) <> ''
            ORDER BY TRIM(grade) ASC
        """)
        grades = [r["grade"] for r in cur.fetchall() if r.get("grade")]

        if grade and grade.strip():
            cur.execute("""
                SELECT DISTINCT TRIM(section) AS section
                FROM student
                WHERE section IS NOT NULL AND TRIM(section) <> ''
                  AND TRIM(grade) = TRIM(%s)
                ORDER BY TRIM(section) ASC
            """, (grade.strip(),))
        else:
            cur.execute("""
                SELECT DISTINCT TRIM(section) AS section
                FROM student
                WHERE section IS NOT NULL AND TRIM(section) <> ''
                ORDER BY TRIM(section) ASC
            """)

        sections = [r["section"] for r in cur.fetchall() if r.get("section")]
        return {"grades": grades, "sections": sections}
    finally:
        cur.close()
        conn.close()

@app.get("/api/reports/activity")
def reports_activity(limit: int = 30, current=Depends(get_current_librarian)):
    """
    Returns recent activity logs for the Reports dashboard.

    Includes:
      - Checkouts (loan borrowed_at)
      - Returns (loan returned_at)
      - Fine payments (fine_payment)
      - Barcode print / reprint (print_log)
      - Damage reports (damage_report)
    """
    limit = max(5, min(int(limit or 30), 200))

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT *
            FROM (
                -- Checkout
                SELECT
                    l.borrowed_at AS ts,
                    'checkout' AS type,
                    ('Checked out: ' || b.title || ' — ' || s.last_name || ', ' || s.first_name) AS message,
                    b.book_id,
                    s.student_id
                FROM loan l
                JOIN student s ON s.student_id = l.student_id
                JOIN book_copy bc ON bc.copy_id = l.copy_id
                JOIN book b ON b.book_id = bc.book_id

                UNION ALL

                -- Return
                SELECT
                    l.returned_at AS ts,
                    'return' AS type,
                    ('Returned: ' || b.title || ' — ' || s.last_name || ', ' || s.first_name) AS message,
                    b.book_id,
                    s.student_id
                FROM loan l
                JOIN student s ON s.student_id = l.student_id
                JOIN book_copy bc ON bc.copy_id = l.copy_id
                JOIN book b ON b.book_id = bc.book_id
                WHERE l.returned_at IS NOT NULL

                UNION ALL

                -- Fine payment
                SELECT
                    fp.paid_at AS ts,
                    'fine_payment' AS type,
                    ('Fine payment: ₱' || fp.amount || ' — ' || s.last_name || ', ' || s.first_name) AS message,
                    NULL::int AS book_id,
                    s.student_id
                FROM fine_payment fp
                JOIN student s ON s.student_id = fp.student_id

                UNION ALL

                -- Print / Reprint
                SELECT
                    pl.printed_at AS ts,
                    'barcode_print' AS type,
                    ('Barcode ' || pl.action || ': ' || bc.barcode || ' — ' || b.title) AS message,
                    b.book_id,
                    NULL::int AS student_id
                FROM print_log pl
                JOIN book_copy bc ON bc.copy_id = pl.copy_id
                JOIN book b ON b.book_id = bc.book_id

                UNION ALL

                -- Damage report
                SELECT
                    d.reported_at AS ts,
                    'damage' AS type,
                    ('Damage report (' || d.severity || '): ' || b.title || ' — ' || s.last_name || ', ' || s.first_name) AS message,
                    b.book_id,
                    s.student_id
                FROM damage_report d
                JOIN book_copy bc ON bc.copy_id = d.copy_id
                JOIN book b ON b.book_id = bc.book_id
                JOIN student s ON s.student_id = d.student_id
            ) x
            WHERE x.ts IS NOT NULL
            ORDER BY x.ts DESC
            LIMIT %s
            """,
            (limit,),
        )

        return cur.fetchall()
    finally:
        cur.close()
        conn.close()