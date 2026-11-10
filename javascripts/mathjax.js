window.MathJax = {
  tex: {
    inlineMath: [["\\(", "\\)"], ["$", "$"]],
    displayMath: [["\\[", "\\]"], ["$$", "$$"]],
    processEscapes: true,
    processEnvironments: true
  },
  options: {
    ignoreHtmlClass: ".*|",
    processHtmlClass: "arithmatex"
  }
};

function renderFallbackMermaid() {
  const blocks = document.querySelectorAll("pre.mermaid");
  if (!blocks.length) return;

  const runMermaid = () => {
    if (typeof window.mermaid === "undefined") return;
    const isDark = document.body.getAttribute("data-md-color-scheme") === "slate";
    window.mermaid.initialize({
      startOnLoad: false,
      theme: isDark ? "dark" : "default",
      securityLevel: "loose"
    });
    blocks.forEach(async (pre, idx) => {
      if (!pre.isConnected) return;
      const code = pre.textContent || "";
      try {
        const id = "idr_mermaid_" + Date.now() + "_" + idx;
        const { svg } = await window.mermaid.render(id, code);
        const container = document.createElement("div");
        container.className = "mermaid";
        container.innerHTML = svg;
        pre.replaceWith(container);
      } catch (err) {
        console.warn("Mermaid fallback render warning:", err);
      }
    });
  };

  if (typeof window.mermaid !== "undefined") {
    runMermaid();
  } else {
    const script = document.createElement("script");
    script.src = "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js";
    script.onload = runMermaid;
    document.head.appendChild(script);
  }
}

if (typeof document$ !== "undefined") {
  document$.subscribe(() => {
    if (typeof MathJax !== "undefined" && MathJax.startup && MathJax.startup.promise) {
      MathJax.startup.promise
        .then(() => {
          if (MathJax.typesetClear) MathJax.typesetClear();
          if (MathJax.texReset) MathJax.texReset();
          if (MathJax.typesetPromise) return MathJax.typesetPromise();
        })
        .catch((err) => console.warn("MathJax typeset warning:", err));
    }
    setTimeout(renderFallbackMermaid, 800);
  });
}
