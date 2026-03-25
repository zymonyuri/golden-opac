// ===============================
// FAST BRANDING LOADER (ENHANCED)
// ===============================

const DISPLAY_BASE_URL =
  window.CONFIG?.API_BASE || "http://127.0.0.1:8000";

const DISPLAY_CACHE_KEY = "displaySettings";
const DEFAULT_SCHOOL_NAME = "Your School Name";
const DEFAULT_SUBTITLE = "Library System";

// -------------------------------
// Normalize URL
// -------------------------------
function normalizeURL(url) {
  if (!url || typeof url !== "string") return null;

  const trimmed = url.trim();
  if (!trimmed) return null;

  if (/^https?:\/\//i.test(trimmed) || /^data:/i.test(trimmed) || /^blob:/i.test(trimmed)) {
    return trimmed;
  }

  return DISPLAY_BASE_URL.replace(/\/$/, "") + "/" + trimmed.replace(/^\//, "");
}

// -------------------------------
// Preload image for faster swap
// -------------------------------
function preloadImage(url) {
  if (!url) return;
  const img = new Image();
  img.decoding = "async";
  img.src = url;
}

// -------------------------------
// Apply logo safely
// -------------------------------
function applyLogo(logoUrl) {
  const resolved = normalizeURL(logoUrl);
  if (!resolved) return;

  preloadImage(resolved);

  document.querySelectorAll("[data-brand-logo], .brand__logo").forEach((img) => {
    if (!(img instanceof HTMLImageElement)) return;

    if (img.dataset.brandLogoApplied === resolved && img.src === resolved) return;

    img.loading = "eager";
    img.decoding = "async";
    try {
      img.fetchPriority = "high";
    } catch (e) {}

    img.onerror = function () {
      const fallback = img.getAttribute("data-brand-logo-fallback") || "assets/default-school-logo.png";
      if (img.src !== fallback) img.src = fallback;
    };

    img.src = resolved;
    img.dataset.brandLogoApplied = resolved;
  });
}

// -------------------------------
// Apply text safely
// -------------------------------
function setText(selector, value, fallback) {
  document.querySelectorAll(selector).forEach((el) => {
    el.textContent = value || fallback;
  });
}

// -------------------------------
// Apply colors safely
// -------------------------------
function applyColors(data) {
  if (data?.brand_primary) {
    document.documentElement.style.setProperty("--brand-primary", data.brand_primary);
  }
  if (data?.brand_secondary) {
    document.documentElement.style.setProperty("--brand-secondary", data.brand_secondary);
  }
}

// -------------------------------
// Apply branding to DOM
// -------------------------------
function applyBranding(data) {
  if (!data || typeof data !== "object") return;

  applyLogo(data.logo_url);

  setText("[data-brand-school-name]", data.school_name, DEFAULT_SCHOOL_NAME);
  setText("[data-brand-header-subtitle]", data.header_subtitle, DEFAULT_SUBTITLE);
  setText("[data-brand-system-name]", data.system_name, DEFAULT_SUBTITLE);

  document.querySelectorAll("[data-brand-footer-title]").forEach((el) => {
    el.textContent = data.footer_title || data.school_name || DEFAULT_SCHOOL_NAME;
  });

  document.querySelectorAll("[data-brand-footer-description]").forEach((el) => {
    el.textContent = data.footer_description || "";
  });

  document.querySelectorAll("[data-brand-footer-address]").forEach((el) => {
    el.textContent = data.footer_address || "";
  });

  document.querySelectorAll("[data-brand-footer-phone]").forEach((el) => {
    el.textContent = data.footer_phone || "";
  });

  document.querySelectorAll("[data-brand-footer-email]").forEach((el) => {
    el.textContent = data.footer_email || "";
  });

  document.querySelectorAll("[data-brand-footer-website]").forEach((el) => {
    el.textContent = data.footer_website || "";
  });

  document.querySelectorAll("[data-brand-footer-facebook]").forEach((el) => {
    el.textContent = data.footer_facebook || "";
  });

  applyColors(data);
}

// -------------------------------
// Read cached branding
// -------------------------------
function getCachedBranding() {
  try {
    const raw = localStorage.getItem(DISPLAY_CACHE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch (e) {
    console.warn("Cache branding error:", e);
    return null;
  }
}

// -------------------------------
// Save cached branding
// -------------------------------
function cacheBranding(data) {
  try {
    localStorage.setItem(DISPLAY_CACHE_KEY, JSON.stringify(data));
  } catch (e) {
    console.warn("Unable to cache branding:", e);
  }
}

// -------------------------------
// Apply cached first (instant)
// -------------------------------
(function applyCachedBrandingEarly() {
  const cached = getCachedBranding();
  if (cached) {
    applyBranding(cached);
  }
})();

// -------------------------------
// Reapply after DOM load
// -------------------------------
function reapplyCachedBranding() {
  const cached = getCachedBranding();
  if (cached) {
    applyBranding(cached);
  }
}

// -------------------------------
// Fetch fresh branding
// -------------------------------
async function loadBranding() {
  try {
    const res = await fetch(`${DISPLAY_BASE_URL.replace(/\/$/, "")}/api/public/display`, {
      cache: "no-store"
    });

    if (!res.ok) throw new Error(`Failed fetch: ${res.status}`);

    const data = await res.json();

    cacheBranding(data);
    applyBranding(data);
  } catch (err) {
    console.warn("Branding fetch failed, using cache only.", err);
    reapplyCachedBranding();
  }
}

// -------------------------------
// Run logic
// -------------------------------
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => {
    reapplyCachedBranding();
    loadBranding();
  });
} else {
  reapplyCachedBranding();
  loadBranding();
}

// -------------------------------
// Manual refresh hook (VERY IMPORTANT)
// Call this after saving settings
// -------------------------------
window.refreshBranding = loadBranding;