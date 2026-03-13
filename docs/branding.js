const DISPLAY_BASE_URL = "http://127.0.0.1:8000";
const DISPLAY_CACHE_KEY = "displaySettings";

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function setText(selector, value) {
  document.querySelectorAll(selector).forEach((el) => {
    el.textContent = value ?? "";
  });
}

function setImage(selector, src, alt) {
  document.querySelectorAll(selector).forEach((el) => {
    el.onload = null;
    el.onerror = null;

    el.classList.remove("is-loaded");

    if (alt) el.alt = alt;

    if (!src) {
      el.removeAttribute("src");
      return;
    }

    el.onload = () => {
      el.classList.add("is-loaded");
    };

    el.onerror = () => {
      el.classList.remove("is-loaded");
    };

    el.src = src;
  });
}

function setAnchorLink(elementId, rawValue) {
  const el = document.getElementById(elementId);
  if (!el) return;

  const value = String(rawValue ?? "").trim();

  if (!value) {
    el.removeAttribute("href");
    el.style.display = "none";
    return;
  }

  el.href = /^https?:\/\//i.test(value) ? value : `https://${value}`;
  el.style.display = "";
}

function applyDocumentTitle({
  schoolName,
  systemName,
  headerSubtitle,
  portalTitle
}) {
  const currentTitle = document.title || "";
  const lower = currentTitle.toLowerCase();

  if (lower.includes("login")) {
    document.title = `${schoolName} — Librarian Login`;
    return;
  }

  if (lower.includes("home")) {
    document.title = `${schoolName} — ${portalTitle}`;
    return;
  }

  if (lower.includes("settings")) {
    document.title = `${schoolName} — Settings`;
    return;
  }

  if (lower.includes("results")) {
    document.title = `${systemName} — Results`;
    return;
  }

  if (lower.includes("book details")) {
    document.title = `${systemName} — Book Details`;
    return;
  }

  if (
    currentTitle === "Library Management System" ||
    currentTitle.includes("Golden Key") ||
    currentTitle.includes("OPAC")
  ) {
    document.title = systemName || `${schoolName} — ${headerSubtitle}`;
  }
}

function saveBrandingCache(data) {
  try {
    if (!data || typeof data !== "object") return;

    const cacheData = {
      school_name: data.school_name || "",
      system_name: data.system_name || "",
      header_subtitle: data.header_subtitle || "",
      librarian_portal_title: data.librarian_portal_title || "",
      brand_primary: data.brand_primary || "",
      brand_secondary: data.brand_secondary || "",
      footer_title: data.footer_title || "",
      footer_description: data.footer_description || "",
      footer_address: data.footer_address || "",
      footer_phone: data.footer_phone || "",
      footer_email: data.footer_email || "",
      footer_website: data.footer_website || "",
      footer_facebook: data.footer_facebook || "",
      footer_copyright: data.footer_copyright || ""
    };

    const logo = String(data.logo_url || "").trim();

if (logo) {
  cacheData.logo_url = logo;
}

    localStorage.setItem(DISPLAY_CACHE_KEY, JSON.stringify(cacheData));
  } catch (err) {
    console.warn("Failed to save branding cache:", err);
  }
}

function readBrandingCache() {
  try {
    const raw = localStorage.getItem(DISPLAY_CACHE_KEY);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch (err) {
    console.warn("Failed to read branding cache:", err);
    return null;
  }
}

function applyBranding(data = {}) {
  const primary = data.brand_primary || "#2563eb";
  const secondary = data.brand_secondary || "#94a3b8";
  const logoUrl = String(data.logo_url || "").trim();

  const schoolName = data.school_name || "Your School Name";
  const systemName = data.system_name || "Library Management System";
  const headerSubtitle =
    data.header_subtitle || "Online Public Access Catalogue";
  const portalTitle = data.librarian_portal_title || "Librarian Portal";

  const footerTitle = data.footer_title || systemName;
  const footerDescription =
    data.footer_description ||
    "Customize your school branding, contact details, and interface appearance in Display Settings.";
  const footerAddress = data.footer_address || "School Address";
  const footerPhone = data.footer_phone || "School Contact Number";
  const footerEmail = data.footer_email || "library@school.edu";
  const footerWebsite = data.footer_website || "";
  const footerFacebook = data.footer_facebook || "";
  const footerCopyright =
    data.footer_copyright || `${schoolName} — All Rights Reserved.`;

  const root = document.documentElement;

  root.style.setProperty("--brand-yellow", primary);
  root.style.setProperty("--brand-green", secondary);
  root.style.setProperty("--brand-primary", primary);
  root.style.setProperty("--brand-secondary", secondary);

  if (logoUrl) {
  setImage(".brand__logo", logoUrl, `${schoolName} Logo`);
} else {
  setImage(".brand__logo", "", `${schoolName} Logo`);
}

  document.querySelectorAll(".brand__name").forEach((el) => {
    const schoolSpan = el.querySelector("[data-brand-school-name]");
    if (schoolSpan) {
      schoolSpan.textContent = schoolName;
    } else {
      el.textContent = schoolName;
    }
  });

  document.querySelectorAll(".brand__sub").forEach((el) => {
    const role = (el.dataset.brandRole || "").trim().toLowerCase();
    const nextText = role === "portal" ? portalTitle : headerSubtitle;

    const subtitleSpan = el.querySelector("[data-brand-header-subtitle]");
    if (subtitleSpan) {
      subtitleSpan.textContent = nextText;
    } else {
      el.textContent = nextText;
    }
  });

  const heroSystemTitle = document.getElementById("heroSystemTitle");
  if (heroSystemTitle) {
    heroSystemTitle.innerHTML = `
      <span class="accent">${escapeHtml(schoolName)}</span>
      <span>${escapeHtml(headerSubtitle)}</span>
    `;
  }

  setText("[data-brand-school-name]", schoolName);
  setText("[data-brand-header-subtitle]", headerSubtitle);
  setText("[data-brand-portal-title]", portalTitle);
  setText("[data-brand-system-name]", systemName);

  document.querySelectorAll(".portal-title").forEach((el) => {
    el.textContent = portalTitle;
  });

  document.querySelectorAll(".school-name").forEach((el) => {
    el.textContent = schoolName;
  });

  document.querySelectorAll(".system-name").forEach((el) => {
    el.textContent = systemName;
  });

  // Footer fields
  const footerBrandTitle = document.getElementById("footerBrandTitle");
  if (footerBrandTitle) footerBrandTitle.textContent = footerTitle;

  const footerBrandDescription = document.getElementById("footerBrandDescription");
  if (footerBrandDescription) footerBrandDescription.textContent = footerDescription;

  const footerBrandAddress = document.getElementById("footerBrandAddress");
  if (footerBrandAddress) footerBrandAddress.textContent = footerAddress;

  const footerBrandPhone = document.getElementById("footerBrandPhone");
  if (footerBrandPhone) footerBrandPhone.textContent = footerPhone;

  const footerBrandEmail = document.getElementById("footerBrandEmail");
  if (footerBrandEmail) footerBrandEmail.textContent = footerEmail;

  const footerBrandCopyright = document.getElementById("footerBrandCopyright");
  if (footerBrandCopyright) footerBrandCopyright.textContent = footerCopyright;

  setAnchorLink("footerBrandWebsite", footerWebsite);
  setAnchorLink("footerBrandFacebook", footerFacebook);

  const footerBrandDivider = document.getElementById("footerBrandDivider");
  if (footerBrandDivider) {
    const hasFacebook = !!String(footerFacebook).trim();
    const hasWebsite = !!String(footerWebsite).trim();
    footerBrandDivider.style.display = hasFacebook && hasWebsite ? "" : "none";
  }

  applyDocumentTitle({
    schoolName,
    systemName,
    headerSubtitle,
    portalTitle
  });

  root.classList.remove("branding-loading");
  root.classList.add("branding-ready");
}

function applyCachedBranding() {
  const cached = readBrandingCache();
  if (!cached) return false;
  applyBranding(cached);
  return true;
}

async function loadDisplayBranding() {
  applyCachedBranding();

  try {
    const res = await fetch(`${DISPLAY_BASE_URL}/api/public/display`);

    if (!res.ok) {
      throw new Error(`Failed to load display settings: HTTP ${res.status}`);
    }

    const data = await res.json();
    applyBranding(data);
    saveBrandingCache(data);
  } catch (err) {
    console.warn("Failed to load display branding from API:", err);
  } finally {
    document.documentElement.classList.remove("branding-loading");
    document.documentElement.classList.add("branding-ready");
  }
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", loadDisplayBranding);
} else {
  loadDisplayBranding();
}


window.addEventListener("message", (event) => {
  const msg = event.data;
  if (!msg || msg.type !== "DISPLAY_PREVIEW_UPDATE" || !msg.data) return;

  applyBranding(msg.data);
});
window.applyBranding = applyBranding;
window.saveBrandingCache = saveBrandingCache;