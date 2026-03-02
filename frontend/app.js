const BASE_URL = "http://127.0.0.1:8000";

const searchInput = document.getElementById("searchInput");
const searchBtn = document.getElementById("searchBtn");
const resultsDiv = document.getElementById("results");

function debounce(fn, delay = 300) {
  let t;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), delay);
  };
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function searchBooks(q) {
  const query = (q ?? searchInput.value).trim();
  if (!query) {
    resultsDiv.innerHTML = "";
    return;
  }

  resultsDiv.innerHTML = `<div style="opacity:.9">Searching…</div>`;

  try {
    const res = await fetch(`${BASE_URL}/api/opac/books?q=${encodeURIComponent(query)}&page_size=10`);
    const data = await res.json();

    const list = data?.results ?? [];
    if (!list.length) {
      resultsDiv.innerHTML = `<div style="opacity:.9">No books found.</div>`;
      return;
    }

    resultsDiv.innerHTML = list.map(book => {
      const title = escapeHtml(book.title);
      const author = escapeHtml(book.author ?? "");
      const genre = escapeHtml(book.genre ?? "Unknown");
      const avail = `${book.available_copies ?? 0} of ${book.total_copies ?? 0} available`;
      return `
        <div class="result-card">
          <div class="result-title">${title}</div>
          <div class="result-meta">${author} • ${genre} • ${avail}</div>
        </div>
      `;
    }).join("");

  } catch (err) {
    resultsDiv.innerHTML = `<div style="opacity:.9">Error connecting to server.</div>`;
  }
}

const searchBooksDebounced = debounce(() => searchBooks(), 320);

searchBtn?.addEventListener("click", () => searchBooks());
searchInput?.addEventListener("input", searchBooksDebounced);
searchInput?.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    e.preventDefault();
    searchBooks();
  }
});

/* Header glass only when scrolled */
const header = document.getElementById("siteHeader");
function updateHeaderGlass(){
  if (!header) return;
  header.classList.toggle("is-scrolled", window.scrollY > 8);
}
window.addEventListener("scroll", updateHeaderGlass, { passive: true });
updateHeaderGlass();

/* NAV active underline */
const navLinks = Array.from(document.querySelectorAll(".nav__link"));

function setActive(target) {
  navLinks.forEach(a => a.classList.toggle("is-active", a.dataset.target === target));
}

/*
Fix: underline not appearing on first click
Reason: scroll-handler immediately flips it back to Home before footer is reached.
Solution: lock active state briefly after click.
*/
let activeLockUntil = 0;

function lockActive(target, ms = 900) {
  activeLockUntil = Date.now() + ms;
  setActive(target);
}

function isLocked() {
  return Date.now() < activeLockUntil;
}

/* Smooth scroll with fixed-header offset + active underline immediately */
const contactLink = document.getElementById("contactLink");
contactLink?.addEventListener("click", (e) => {
  e.preventDefault();

  const footer = document.getElementById("footer");
  if (!footer) return;

  lockActive("footer", 1100);

  const headerH = header?.getBoundingClientRect().height ?? 0;
  const y = footer.getBoundingClientRect().top + window.scrollY - headerH - 12;

  window.scrollTo({ top: y, behavior: "smooth" });
});

/* Home click should also activate immediately */
const homeLink = navLinks.find(a => a.dataset.target === "top");
homeLink?.addEventListener("click", (e) => {
  // allow default jump, but set active instantly
  lockActive("top", 600);
});

/* Scroll-based auto switching (when not locked) */
window.addEventListener("scroll", () => {
  if (isLocked()) return;

  const footer = document.getElementById("footer");
  if (!footer) return;

  const headerH = header?.getBoundingClientRect().height ?? 0;
  const footerTop = footer.getBoundingClientRect().top;

  if (footerTop <= headerH + 80) {
    setActive("footer");
  } else {
    setActive("top");
  }
}, { passive: true });

/* Footer year */
const yEl = document.getElementById("footerYear");
if (yEl) yEl.textContent = String(new Date().getFullYear());
