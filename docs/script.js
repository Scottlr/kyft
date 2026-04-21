const internalLinks = document.querySelectorAll('a[href^="#"]');

for (const link of internalLinks) {
  link.addEventListener("click", () => {
    const nav = document.querySelector(".nav");
    if (nav) {
      nav.dataset.lastNavigation = link.getAttribute("href") ?? "";
    }
  });
}
