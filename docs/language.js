(function () {
  const storageKey = "spanfold-language";
  const supported = new Set(["csharp", "python"]);

  function resolveLanguage(value) {
    return supported.has(value) ? value : "csharp";
  }

  function setLanguage(language) {
    const resolved = resolveLanguage(language);
    document.body.dataset.language = resolved;
    document.querySelectorAll("[data-language-toggle]").forEach((button) => {
      button.setAttribute("aria-pressed", String(button.dataset.languageToggle === resolved));
    });
    localStorage.setItem(storageKey, resolved);
  }

  document.addEventListener("DOMContentLoaded", () => {
    setLanguage(localStorage.getItem(storageKey) || document.body.dataset.language);

    document.querySelectorAll("[data-language-toggle]").forEach((button) => {
      button.addEventListener("click", () => {
        setLanguage(button.dataset.languageToggle);
      });
    });

    const navToggle = document.querySelector(".nav-toggle");
    const siteHeader = document.querySelector(".site-header");
    if (navToggle && siteHeader) {
      function closeNav() {
        siteHeader.classList.remove("nav-open");
        navToggle.setAttribute("aria-expanded", "false");
      }

      navToggle.addEventListener("click", () => {
        const isOpen = siteHeader.classList.toggle("nav-open");
        navToggle.setAttribute("aria-expanded", String(isOpen));
      });
      document.querySelectorAll(".nav a").forEach((link) => {
        link.addEventListener("click", closeNav);
      });
      document.addEventListener("click", (event) => {
        if (!siteHeader.classList.contains("nav-open")) {
          return;
        }
        if (!siteHeader.contains(event.target)) {
          closeNav();
        }
      });
      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
          closeNav();
        }
      });
    }
  });
})();
