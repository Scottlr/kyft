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
  });
})();
