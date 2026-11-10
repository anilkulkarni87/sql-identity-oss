function renderAllMath() {
  if (typeof renderMathInElement === "function") {
    renderMathInElement(document.body, {
      delimiters: [
        { left: "$$",  right: "$$",  display: true },
        { left: "$",   right: "$",   display: false },
        { left: "\\(", right: "\\)", display: false },
        { left: "\\[", right: "\\]", display: true }
      ],
      throwOnError: false
    });
  }
}

function renderAllMermaid() {
  if (typeof mermaid === "undefined") return;
  const blocks = document.querySelectorAll("pre.mermaid, .mermaid");
  if (!blocks.length) return;
  mermaid.initialize({
    startOnLoad: false,
    theme: document.body.getAttribute("data-md-color-scheme") === "slate" ? "dark" : "default",
    securityLevel: "loose"
  });
  blocks.forEach(async (el, idx) => {
    if (el.tagName.toLowerCase() === "pre") {
      const code = el.textContent || "";
      try {
        const id = "mermaid_svg_" + idx + "_" + Math.floor(Math.random() * 10000);
        const { svg } = await mermaid.render(id, code);
        const div = document.createElement("div");
        div.className = "mermaid";
        div.innerHTML = svg;
        el.replaceWith(div);
      } catch (err) {
        console.warn("Mermaid render error:", err);
      }
    }
  });
}

function initDocs() {
  renderAllMath();
  renderAllMermaid();
}

if (typeof document$ !== "undefined") {
  document$.subscribe(() => {
    initDocs();
  });
} else {
  document.addEventListener("DOMContentLoaded", initDocs);
}
