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

# -----------------------------
# CATALOGING: ADD BOOK BY ISBN (Librarian Only)
# -----------------------------
@app.post("/api/cataloging/add-book")
def add_book_by_isbn(
    isbn: str,
    copies: int = 1,
    # Optional overrides (librarian can edit anything)
    title: str | None = None,
    author: str | None = None,
    publisher: str | None = None,
    pub_year: int | None = None,
    genre: str | None = None,
    subject: str | None = None,
    section: str | None = None,
    current=Depends(get_current_librarian),
):
    """
    Librarian-only endpoint.
    Workflow:
      1) Lookup ISBN via Google Books + Open Library (best effort)
      2) Merge: librarian inputs override API data
      3) If ISBN exists in book_identifier -> use existing book
         else -> create new book + identifier
      4) Create N book_copy rows with unique barcodes
      5) Return created barcodes

    Notes:
      - 'section' is usually NOT available from APIs, librarian must fill it.
      - Your schema requires book.title, book.author, book.catalog_key (unique).
    """

    # --- Basic validation ---
    if copies < 1 or copies > 100:
        # 100 is an arbitrary safety cap to prevent accidental massive inserts
        raise HTTPException(status_code=400, detail="copies must be between 1 and 100")

    # --- 1) Fetch from APIs ---
    meta = lookup_isbn(isbn)
    if not meta.get("found"):
        raise HTTPException(status_code=404, detail="ISBN not found in Google Books/Open Library")

    # --- 2) Merge strategy: librarian overrides win ---
    final_title = title or meta.get("title")
    final_author = author or meta.get("author")
    final_publisher = publisher or meta.get("publisher")
    final_pub_year = pub_year or meta.get("pub_year")
    final_genre = genre or meta.get("genre")
    final_subject = subject or meta.get("subject")
    final_section = section  # section is librarian-defined; don't auto-fill unless you want to.

    # --- 3) Required-field enforcement ---
    missing_required = []
    if not final_title:
        missing_required.append("title")
    if not final_author:
        missing_required.append("author")
    if not final_section:
        missing_required.append("section")

    if missing_required:
        # UX-friendly: tell frontend exactly what to ask librarian to fill
        raise HTTPException(
            status_code=400,
            detail={"message": "Missing required fields", "missing": missing_required, "autofill": meta},
        )

    # --- DB work ---
    conn = get_connection()
    cur = conn.cursor()

    try:
        # Use a transaction so we don't end up with half-created rows
        # psycopg opens a transaction automatically; we commit at the end.

        # --- 4) Check if book already exists by ISBN (primary identifier) ---
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

        # --- 5) Create book if new ---
        if not book_id:
            # catalog_key must be unique and not null; we standardize it as ISBN:{isbn}
            catalog_key = f"ISBN:{meta['isbn']}"

            cur.execute(
                """
                INSERT INTO book (title, author, publisher, pub_year, genre, subject, section, catalog_key, cover_url, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING book_id
                """,
                (
                    final_title,
                    final_author,
                    final_publisher,
                    final_pub_year,
                    final_genre,
                    final_subject,
                    final_section,
                    catalog_key,
                    meta.get("cover_url"),
                ),
            )
            book_id = cur.fetchone()["book_id"]

            # Insert primary ISBN identifier
            cur.execute(
                """
                INSERT INTO book_identifier (book_id, id_type, id_value, is_primary, created_at)
                VALUES (%s, 'isbn', %s, TRUE, NOW())
                """,
                (book_id, meta["isbn"]),
            )

        # --- 6) Create copies + barcodes ---
        created = []
        # Simple unique barcode format: BK{book_id}-TS{unix}-N{counter}
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

        return {
            "message": "Book saved successfully",
            "book_id": book_id,
            "isbn": meta["isbn"],
            "copies_created": len(created),
            "barcodes": created,
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to add book: {str(e)}")
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
                    COALESCE(SUM(CASE WHEN bc.status = 'available' THEN 1 ELSE 0 END), 0) AS available_copies

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
                    COALESCE(SUM(CASE WHEN bc.status = 'available' THEN 1 ELSE 0 END), 0) AS available_copies

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
def checkout_book(
    barcode: str,
    student_code: str,
    current=Depends(get_current_librarian),
):
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
        grade = student["grade"]

        # --- 3) Load grade rule (due date & limits) ---
        cur.execute(
            """
            SELECT loan_period_days, max_borrow_limit, fine_per_day, max_renewals, block_renew_if_overdue
            FROM grade_rule
            WHERE grade = %s
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
            """,
            (grade,),
        )
        rule = cur.fetchone()
        if not rule:
            raise HTTPException(status_code=400, detail=f"No grade rule configured for grade={grade}")

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
            raise HTTPException(status_code=400, detail=f"No grade rule configured for grade={grade}")

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
@app.post("/api/circulation/checkin")
def checkin_book(
    barcode: str,
    student_code: str,
    is_damaged: bool = False,
    severity: str | None = None,
    notes: str | None = None,
    current=Depends(get_current_librarian),
):
    """
    Checks in a borrowed copy.

    Behavior:
      1) Find student by student_code
      2) Find copy by barcode
      3) Find active loan for (student_id, copy_id) where returned_at IS NULL
      4) Set returned_at = NOW()
      5) If overdue:
          - compute overdue_days
          - compute fine amount from grade_rule.fine_per_day
          - create fine record (status = 'unpaid')
      6) If damaged:
          - create damage_report (severity + notes)
          - set copy status to 'damaged'
        Else:
          - set copy status to 'available'
    """

    conn = get_connection()
    cur = conn.cursor()

    try:
        librarian_id = current["librarian_id"]

        # --- 1) Find student ---
        cur.execute(
            """
            SELECT student_id, grade, status
            FROM student
            WHERE student_code = %s
            """,
            (student_code,),
        )
        student = cur.fetchone()
        if not student:
            raise HTTPException(status_code=404, detail="Student not found")

        student_id = student["student_id"]
        grade = student["grade"]

        # --- 2) Find copy ---
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

        # --- 3) Find active loan for this student and copy ---
        cur.execute(
            """
            SELECT loan_id, borrowed_at, due_at, returned_at
            FROM loan
            WHERE student_id = %s AND copy_id = %s AND returned_at IS NULL
            ORDER BY borrowed_at DESC
            LIMIT 1
            """,
            (student_id, copy_id),
        )
        loan = cur.fetchone()
        if not loan:
            raise HTTPException(status_code=400, detail="No active loan found for this student and copy")

        loan_id = loan["loan_id"]

        # --- 4) Mark loan returned ---
        cur.execute(
            """
            UPDATE loan
            SET returned_at = NOW(), status = 'returned'
            WHERE loan_id = %s
            RETURNING returned_at
            """,
            (loan_id,),
        )
        returned = cur.fetchone()

        # --- 5) Load grade rule for fine computation ---
        cur.execute(
            """
            SELECT fine_per_day
            FROM grade_rule
            WHERE grade = %s
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
            """,
            (grade,),
        )
        rule = cur.fetchone()
        fine_per_day = float(rule["fine_per_day"] or 0) if rule else 0.0

        # Compute overdue days safely (Postgres date math)
        # overdue_days = max(0, (returned_date - due_date))
        cur.execute(
            """
            SELECT GREATEST(0, (DATE(NOW()) - DATE(%s))) AS overdue_days
            """,
            (loan["due_at"],),
        )
        overdue_days = int(cur.fetchone()["overdue_days"])

        fine_created = None
        if overdue_days > 0 and fine_per_day > 0:
            amount = round(overdue_days * fine_per_day, 2)

            # Create fine record (unpaid by default)
            cur.execute(
                """
                INSERT INTO fine (loan_id, student_id, amount, amount_paid, status, reason, assessed_at)
                VALUES (%s, %s, %s, 0, 'unpaid', %s, NOW())
                RETURNING fine_id, amount, status
                """,
                (loan_id, student_id, amount, f"Overdue {overdue_days} day(s)"),
            )
            fine_row = cur.fetchone()
            fine_created = {
                "fine_id": fine_row["fine_id"],
                "amount": float(fine_row["amount"]),
                "status": fine_row["status"],
                "overdue_days": overdue_days,
                "fine_per_day": fine_per_day,
            }

        # --- 6) Damage reporting + copy status update ---
        if is_damaged:
            # Validate severity if damaged
            if severity is None or severity.strip() not in ["Minor", "Major"]:
                raise HTTPException(status_code=400, detail="severity must be 'Minor' or 'Major' when is_damaged=true")

            cur.execute(
                """
                INSERT INTO damage_report (copy_id, student_id, recorded_by, severity, notes, reported_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                RETURNING damage_id
                """,
                (copy_id, student_id, librarian_id, severity.strip(), notes),
            )
            damage_id = cur.fetchone()["damage_id"]

            cur.execute(
                """
                UPDATE book_copy
                SET status = 'damaged'
                WHERE copy_id = %s
                """,
                (copy_id,),
            )

            copy_new_status = "damaged"
        else:
            # Normal return sets copy back to available
            cur.execute(
                """
                UPDATE book_copy
                SET status = 'available'
                WHERE copy_id = %s
                """,
                (copy_id,),
            )
            damage_id = None
            copy_new_status = "available"

        conn.commit()

        return {
            "message": "Check-in successful",
            "loan_id": loan_id,
            "returned_at": str(returned["returned_at"]),
            "copy_status": copy_new_status,
            "fine": fine_created,
            "damage_report_id": damage_id,
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
# REPORTS: DASHBOARD (Librarian Only)
# -----------------------------
@app.get("/api/reports/dashboard")
def reports_dashboard(current=Depends(get_current_librarian)):
    """
    Dashboard analytics for Reports Module.

    Returns:
      - total_books (bibliographic records)
      - total_copies (physical copies)
      - active_loans (currently borrowed)
      - overdue_loans (currently overdue)
      - most_borrowed_books (top 5)
      - most_active_students (top 5)
      - monthly_borrow_trend (last 12 months)
      - genre_distribution (top 10)
    """

    conn = get_connection()
    cur = conn.cursor()

    try:
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

        # --- Most borrowed books (top 5 by total loans) ---
        cur.execute(
            """
            SELECT
                b.book_id,
                b.title,
                b.author,
                COUNT(*) AS borrow_count
            FROM loan l
            JOIN book_copy bc ON bc.copy_id = l.copy_id
            JOIN book b ON b.book_id = bc.book_id
            GROUP BY b.book_id
            ORDER BY borrow_count DESC
            LIMIT 5;
            """
        )
        most_borrowed_books = cur.fetchall()

        # --- Most active students (top 5 by total loans) ---
        cur.execute(
            """
            SELECT
                s.student_id,
                s.student_code,
                s.last_name,
                s.first_name,
                s.grade,
                s.section,
                COUNT(*) AS borrow_count
            FROM loan l
            JOIN student s ON s.student_id = l.student_id
            GROUP BY s.student_id
            ORDER BY borrow_count DESC
            LIMIT 5;
            """
        )
        most_active_students = cur.fetchall()

        # --- Monthly borrowing trend (last 12 months) ---
        cur.execute(
            """
            SELECT
                TO_CHAR(DATE_TRUNC('month', borrowed_at), 'YYYY-MM') AS month,
                COUNT(*) AS borrow_count
            FROM loan
            WHERE borrowed_at >= (DATE_TRUNC('month', NOW()) - INTERVAL '11 months')
            GROUP BY DATE_TRUNC('month', borrowed_at)
            ORDER BY month ASC;
            """
        )
        monthly_borrow_trend = cur.fetchall()

        # --- Books by genre distribution (top 10) ---
        cur.execute(
            """
            SELECT
                COALESCE(NULLIF(TRIM(genre), ''), 'Unknown') AS genre,
                COUNT(*) AS book_count
            FROM book
            GROUP BY COALESCE(NULLIF(TRIM(genre), ''), 'Unknown')
            ORDER BY book_count DESC
            LIMIT 10;
            """
        )
        genre_distribution = cur.fetchall()

        return {
            "totals": {
                "total_books": total_books,
                "total_copies": total_copies,
                "active_loans": active_loans,
                "overdue_loans": overdue_loans,
            },
            "most_borrowed_books": most_borrowed_books,
            "most_active_students": most_active_students,
            "monthly_borrow_trend": monthly_borrow_trend,
            "genre_distribution": genre_distribution,
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
    lost_fee: float,
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
      6) Create fine (unpaid) for lost_fee
      7) Return summary

    Notes:
      - lost_fee must be > 0
      - This is separate from overdue fines (this is a replacement/lost fee)
    """
    if lost_fee <= 0:
        raise HTTPException(status_code=400, detail="lost_fee must be greater than 0")

    conn = get_connection()
    cur = conn.cursor()

    try:
        librarian_id = current["librarian_id"]

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
            SELECT loan_id, due_at
            FROM loan
            WHERE student_id = %s AND copy_id = %s AND returned_at IS NULL
            ORDER BY borrowed_at DESC
            LIMIT 1
            """,
            (student_id, copy_id),
        )
        loan = cur.fetchone()
        if not loan:
            raise HTTPException(status_code=400, detail="No active loan found for this student and copy")

        loan_id = loan["loan_id"]

        # --- 4) Update loan to 'lost' ---
        cur.execute(
            """
            UPDATE loan
            SET returned_at = NOW(), status = 'lost'
            WHERE loan_id = %s
            RETURNING returned_at
            """,
            (loan_id,),
        )
        returned_row = cur.fetchone()

        # --- 5) Update copy to 'lost' ---
        cur.execute(
            """
            UPDATE book_copy
            SET status = 'lost'
            WHERE copy_id = %s
            """,
            (copy_id,),
        )

        # --- 6) Create lost fee fine (unpaid) ---
        reason = "Lost book fee"
        if note and note.strip():
            reason = f"Lost book fee - {note.strip()}"

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
            "loan_returned_at": str(returned_row["returned_at"]),
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

    