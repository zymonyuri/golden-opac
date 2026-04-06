
# Golden OPAC

Golden OPAC is a web-based library management and OPAC system built for school use. It provides librarian tools for cataloging, circulation, student management, reports, settings, and a public-facing book search experience.

## Features

- Public OPAC for searching available books
- Librarian login and protected admin pages
- Book cataloging and metadata management
- Barcode/copy management
- Student management
- Borrowing, returning, and circulation workflows
- Reports and summaries
- Branding and display settings
- Grade and section-based student handling


## Tech Stack

- Backend: FastAPI
- Database: PostgreSQL
- Frontend: HTML, CSS, JavaScript
- Auth: Token-based librarian authentication

## Setup

### 1. Clone the project

```bash
git clone <your-repo-url>
cd golden-opac
```

### 2. Create and activate a virtual environment

Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 3. Install backend dependencies

```powershell
pip install -r backend\requirements.txt
```

### 4. Configure environment variables

Create or update your `.env` file with your database and app settings.

Example:

```env
DATABASE_URL=your_database_url
SECRET_KEY=your_secret_key
ACCESS_TOKEN_EXPIRE_MINUTES=60
```

Adjust the values to match your environment.

## Run the Backend

From the project root:

```powershell
uvicorn backend.app:app --reload
```

The API will typically run at:

```text
http://127.0.0.1:8000
```

## Run the Frontend

The frontend pages are in the `docs/` folder. You can open them with a local static server.

Example using Python:

```powershell
python -m http.server 5500 -d docs
```

Then open:

```text
http://127.0.0.1:5500
```

## Main Pages

- `docs/index.html` - public OPAC landing page
- `docs/login.html` - librarian login
- `docs/home.html` - librarian dashboard
- `docs/search-admin.html` - librarian book search and editing
- `docs/cataloging.html` - add and manage books
- `docs/circulation.html` - borrowing/returning workflow
- `docs/students.html` - student list
- `docs/settings.html` - system settings and maintenance

## Notes

- The backend must be running for librarian and OPAC features to work.
- The frontend expects the API base URL configured in the page scripts.
- Some features depend on valid database tables and seed data already existing.



1. a shorter professional README
2. a more polished GitHub README
3. a school-project style README with screenshots section placeholders
