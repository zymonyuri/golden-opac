const BASE_URL = "https://golden-opac-backend.onrender.com";

const searchInput = document.getElementById("searchInput");
const searchBtn = document.getElementById("searchBtn");
const resultsDiv = document.getElementById("results");

// ---------------------------
// Helpers
// ---------------------------
function debounce(fn, delay = 300) {
  let t;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), delay);
  };
}

function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

// ---------------------------
// Progress Bar (Green track + Yellow fill) - in preview area
// ---------------------------
let progressInterval = null;
let currentProgress = 0;

function showProgressBar() {
  resultsDiv.style.opacity = "1";
  resultsDiv.style.transition = "";

  resultsDiv.innerHTML = `
    <div class="search-progress" role="progressbar" aria-label="Searching">
      <div class="search-progress__bar" id="searchProgressBar"></div>
    </div>
  `;

  const bar = document.getElementById("searchProgressBar");
  currentProgress = 0;
  bar.style.width = "0%";

  if (progressInterval) clearInterval(progressInterval);

  progressInterval = setInterval(() => {
    if (!bar) return;
    if (currentProgress < 85) {
      currentProgress += 3;
      bar.style.width = `${currentProgress}%`;
    }
  }, 120);
}

function completeProgressBar() {
  const bar = document.getElementById("searchProgressBar");
  if (!bar) return;

  if (progressInterval) clearInterval(progressInterval);
  progressInterval = null;

  bar.style.width = "100%";
}

// ---------------------------
// Cover rendering (preview list cards)
// ---------------------------
function getCoverHtml(coverUrl, title) {
  if (!coverUrl) {
    return `<div class="result-cover" aria-label="No cover available"></div>`;
  }

  const safeUrl = escapeHtml(coverUrl);
  const safeTitle = escapeHtml(title || "Book");

  return `
    <div class="result-cover">
      <img
        src="${safeUrl}"
        alt="${safeTitle} cover"
        loading="lazy"
        onerror="this.style.display='none';"
      />
    </div>
  `;
}

// ---------------------------
// Redirect (Enter / Search button)
// ---------------------------
function goToResults() {
  const query = (searchInput?.value ?? "").trim();
  if (!query) return;

  // Optional: tiny UI feedback before leaving
  showProgressBar();

  const params = new URLSearchParams();
  params.set("q", query);

  // Redirect to results page in same folder
  window.location.href = `results.html?${params.toString()}`;
}

// ---------------------------
// Preview search (typing only)
// ---------------------------
async function previewSearch(q) {
  const query = (q ?? searchInput?.value ?? "").trim();

  // If empty, clear preview box
  if (!query) {
    resultsDiv.innerHTML = "";
    return;
  }

  showProgressBar();

  try {
    const res = await fetch(
      `${BASE_URL}/api/opac/books?q=${encodeURIComponent(query)}&page_size=10`
    );

    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const data = await res.json();

    completeProgressBar();

    const list = data?.results ?? [];

    // Give 120ms so users see bar fill to 100%
    setTimeout(() => {
      // Empty state (styled card)
      if (!list.length) {
        const searched = escapeHtml(query);
        resultsDiv.innerHTML = `
          <div class="result-empty">
            <div class="result-empty-title">No books found for "${searched}"</div>
            <div class="result-empty-sub">Try searching by title, author, or ISBN.</div>
          </div>
        `;
        return;
      }

      // Preview cards
      resultsDiv.innerHTML = list
        .map((book) => {
          const title = escapeHtml(book.title ?? "");
          const author = escapeHtml(book.author ?? "");
          const genre = escapeHtml(book.genre ?? "Unknown");
          const avail = `${book.available_copies ?? 0} of ${book.total_copies ?? 0} available`;

          const coverUrl = book.cover_url ?? book.coverUrl ?? book.cover ?? null;

         const bookId = book.book_id ?? book.id;
        const q = encodeURIComponent(query);

        return `
        <div class="result-card" role="link" tabindex="0"
            data-book-id="${bookId ?? ""}"
            style="cursor:pointer;">
            ${getCoverHtml(coverUrl, book.title)}
            <div class="result-info">
            <div class="result-title">${title}</div>
            <div class="result-meta">${author} • ${genre} • ${avail}</div>
            </div>
        </div>
        `;
        })
        .join("");
        // Attach click listeners AFTER rendering
resultsDiv.querySelectorAll(".result-card").forEach(card => {
  card.addEventListener("click", () => {
    const id = card.dataset.bookId;
    if (!id) return;

    window.location.href =
      `details.html?book_id=${encodeURIComponent(id)}&q=${encodeURIComponent(query)}`;
  });

  card.addEventListener("keydown", (e) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      card.click();
    }
  });
});
    }, 120);
  } catch (err) {
    if (progressInterval) clearInterval(progressInterval);
    progressInterval = null;

    resultsDiv.innerHTML = `
      <div class="result-empty">
        <div class="result-empty-title">Unable to fetch results</div>
        <div class="result-empty-sub">Please check your connection and try again.</div>
      </div>
    `;
  }
}

const previewSearchDebounced = debounce(() => previewSearch(), 320);

// ✅ Typing = preview
searchInput?.addEventListener("input", previewSearchDebounced);

// ✅ Click / Enter = redirect
searchBtn?.addEventListener("click", goToResults);
searchInput?.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    e.preventDefault();
    goToResults();
  }
});

// ---------------------------
// Header glass only when scrolled
// ---------------------------
const header = document.getElementById("siteHeader");

function updateHeaderGlass() {
  if (!header) return;
  header.classList.toggle("is-scrolled", window.scrollY > 8);
}

window.addEventListener("scroll", updateHeaderGlass, { passive: true });
updateHeaderGlass();

// ---------------------------
// NAV active underline + Contact scroll
// ---------------------------
const navLinks = Array.from(document.querySelectorAll(".nav__link"));

function setActive(target) {
  navLinks.forEach((a) => a.classList.toggle("is-active", a.dataset.target === target));
}

let activeLockUntil = 0;
function lockActive(target, ms = 900) {
  activeLockUntil = Date.now() + ms;
  setActive(target);
}
function isLocked() {
  return Date.now() < activeLockUntil;
}

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

const homeLink = navLinks.find((a) => a.dataset.target === "top");
homeLink?.addEventListener("click", () => {
  lockActive("top", 600);
});

window.addEventListener(
  "scroll",
  () => {
    if (isLocked()) return;

    const footer = document.getElementById("footer");
    if (!footer) return;

    const headerH = header?.getBoundingClientRect().height ?? 0;
    const footerTop = footer.getBoundingClientRect().top;

    if (footerTop <= headerH + 80) setActive("footer");
    else setActive("top");
  },
  { passive: true }
);

// ---------------------------
// Footer year
// ---------------------------
const yEl = document.getElementById("footerYear");
if (yEl) yEl.textContent = String(new Date().getFullYear());
