(function () {
  function setupMobileNav() {
    const header = document.querySelector(".topbar");
    const nav = header?.querySelector(".nav");
    if (!header || !nav || header.querySelector(".mobile-nav-toggle")) return;

    const toggle = document.createElement("button");
    toggle.type = "button";
    toggle.className = "mobile-nav-toggle";
    toggle.setAttribute("aria-label", "Toggle navigation menu");
    toggle.setAttribute("aria-expanded", "false");
    toggle.innerHTML = '<span class="mobile-nav-toggle__bars" aria-hidden="true"></span>';

    function isMobile() {
      return window.matchMedia("(max-width: 980px)").matches;
    }

    function closeNav() {
      header.classList.remove("is-nav-open");
      toggle.setAttribute("aria-expanded", "false");
    }

    function syncForViewport() {
      if (!isMobile()) {
        nav.style.removeProperty("display");
        header.classList.remove("is-nav-open");
        toggle.hidden = true;
        toggle.setAttribute("aria-expanded", "false");
        return;
      }
      toggle.hidden = false;
    }

    toggle.addEventListener("click", function () {
      if (!isMobile()) return;
      const willOpen = !header.classList.contains("is-nav-open");
      header.classList.toggle("is-nav-open", willOpen);
      toggle.setAttribute("aria-expanded", willOpen ? "true" : "false");
    });

    nav.querySelectorAll("a").forEach(function (link) {
      link.addEventListener("click", function () {
        closeNav();
      });
    });

    document.addEventListener("click", function (event) {
      if (!isMobile()) return;
      if (!header.classList.contains("is-nav-open")) return;
      if (header.contains(event.target)) return;
      closeNav();
    });

    window.addEventListener("resize", syncForViewport, { passive: true });
    window.addEventListener("orientationchange", syncForViewport, { passive: true });

    header.appendChild(toggle);
    syncForViewport();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", setupMobileNav, { once: true });
  } else {
    setupMobileNav();
  }
})();
