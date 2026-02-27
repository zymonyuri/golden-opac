"""
isbn_lookup.py
Fetch bibliographic metadata by ISBN using:
  1) Google Books
  2) Open Library
Returns best-effort merged data (title/author/publisher/year/subjects/cover_url).
"""

import re
import requests


def normalize_isbn(isbn: str) -> str:
    """Remove spaces/hyphens and keep digits/X only."""
    return re.sub(r"[^0-9Xx]", "", isbn).upper()


import os

def fetch_google_books(isbn: str) -> dict | None:
    """
    Google Books lookup with API key + graceful fallback.
    """

    api_key = os.getenv("GOOGLE_BOOKS_API_KEY")

    url = "https://www.googleapis.com/books/v1/volumes"
    params = {
        "q": f"isbn:{isbn}",
        "key": api_key  # use key to avoid rate limiting
    }

    try:
        r = requests.get(url, params=params, timeout=15)

        # If rate-limited or any error, return None (don't crash backend)
        if r.status_code != 200:
            return None

        data = r.json()

        if not data.get("items"):
            return None

        v = data["items"][0].get("volumeInfo", {})

        image_links = v.get("imageLinks", {}) or {}
        cover = image_links.get("thumbnail") or image_links.get("smallThumbnail")

        authors = v.get("authors") or []
        author = ", ".join(authors) if authors else None

        published_date = v.get("publishedDate")
        pub_year = None
        if isinstance(published_date, str) and len(published_date) >= 4 and published_date[:4].isdigit():
            pub_year = int(published_date[:4])

        categories = v.get("categories") or []
        genre = categories[0] if categories else None
        subject = ", ".join(categories) if categories else None

        return {
            "title": v.get("title"),
            "author": author,
            "publisher": v.get("publisher"),
            "pub_year": pub_year,
            "genre": genre,
            "subject": subject,
            "cover_url": cover,
            "source": "google_books",
        }

    except Exception:
        # Never crash the API
        return None

def fetch_open_library(isbn: str) -> dict | None:
    """
    Open Library Books API:
      https://openlibrary.org/isbn/{isbn}.json
    Cover:
      https://covers.openlibrary.org/b/isbn/{isbn}-L.jpg
    Returns a normalized dict or None.
    """
    url = f"https://openlibrary.org/isbn/{isbn}.json"
    r = requests.get(url, timeout=15)

    if r.status_code == 404:
        return None

    r.raise_for_status()
    data = r.json()

    title = data.get("title")
    publisher = None
    pubs = data.get("publishers") or []
    if pubs:
        publisher = pubs[0]

    pub_year = None
    publish_date = data.get("publish_date")  # e.g. "October 2005"
    if isinstance(publish_date, str):
        # Grab first 4-digit year if present
        m = re.search(r"\b(\d{4})\b", publish_date)
        if m:
            pub_year = int(m.group(1))

    # Authors in Open Library are keys; resolving names requires extra calls.
    # We'll keep author as None here (Google usually covers authors better).
    cover = f"https://covers.openlibrary.org/b/isbn/{isbn}-L.jpg"

    # Subjects sometimes exist in OL, but not always
    subjects = data.get("subjects") or []
    subject = ", ".join(subjects[:10]) if subjects else None

    return {
        "title": title,
        "author": None,
        "publisher": publisher,
        "pub_year": pub_year,
        "genre": None,
        "subject": subject,
        "cover_url": cover,
        "source": "open_library",
    }


def lookup_isbn(isbn_raw: str) -> dict:
    """
    Merge strategy:
      - Prefer Google Books for author + richer metadata
      - Fill missing fields from Open Library
      - Prefer Google cover if present; else Open Library cover

    UX helper:
      - Return missing_fields so frontend knows what librarian must fill in.
    """
    isbn = normalize_isbn(isbn_raw)

    g = fetch_google_books(isbn)
    o = fetch_open_library(isbn)

    if not g and not o:
        return {"isbn": isbn, "found": False, "message": "No data found from Google Books or Open Library."}

    merged = {"isbn": isbn, "found": True}

    primary = g or o
    secondary = o if g else None

    for key in ["title", "author", "publisher", "pub_year", "genre", "subject", "cover_url"]:
        merged[key] = (primary.get(key) if primary else None) or (secondary.get(key) if secondary else None)

    merged["sources"] = [x["source"] for x in [g, o] if x]

    # Fields librarian might need to fill in (or confirm)
    required_for_your_schema = ["title", "author", "section"]  # section is not in APIs usually
    optional_fields = ["publisher", "pub_year", "genre", "subject", "cover_url"]

    missing = []
    for f in required_for_your_schema:
        if f == "section":
            # APIs usually don't provide section; librarian sets shelf/location.
            missing.append("section")
        elif not merged.get(f):
            missing.append(f)

    # Optional missing fields (helpful for librarian)
    optional_missing = [f for f in optional_fields if not merged.get(f)]

    merged["missing_fields"] = missing
    merged["optional_missing_fields"] = optional_missing

    # Simple confidence signal for UX (not “AI”, just rule-based)
    merged["confidence"] = "high" if merged.get("title") and merged.get("author") else "medium"

    return merged