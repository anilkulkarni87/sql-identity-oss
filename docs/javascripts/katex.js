function renderAllMath() {
  if (typeof renderMathInElement !== "function") return false;
  try {
    renderMathInElement(document.body, {
      delimiters: [
        { left: "$$", right: "$$", display: true },
        { left: "$", right: "$", display: false },
        { left: "\\(", right: "\\)", display: false },
        { left: "\\[", right: "\\]", display: true }
      ],
      throwOnError: false
    });
    return true;
  } catch (err) {
    console.warn("KaTeX render error:", err);
    return false;
  }
}

function renderAllMermaid() {
  if (typeof mermaid === "undefined") return false;
  const blocks = document.querySelectorAll("pre.mermaid, .mermaid");
  if (!blocks.length) return true;

  try {
    mermaid.initialize({
      startOnLoad: false,
      theme: document.body.getAttribute("data-md-color-scheme") === "slate" ? "dark" : "default",
      securityLevel: "loose"
    });
  } catch (err) {
    console.warn("Mermaid init error:", err);
  }

  blocks.forEach(async (el, idx) => {
    if (el.tagName.toLowerCase() === "pre") {
      const code = el.textContent || "";
      try {
        const id = "mermaid_svg_" + idx + "_" + Math.floor(Math.random() * 100000);
        const { svg } = await mermaid.render(id, code);
        const div = document.createElement("div");
        div.className = "mermaid";
        div.innerHTML = svg;
        el.replaceWith(div);
      } catch (err) {
        console.warn("Mermaid render error for block " + idx + ":", err);
      }
    }
  });
  return true;
}

function initDocsWithRetry(retries = 20, delay = 100) {
  const mathDone = renderAllMath();
  const mermaidDone = renderAllMermaid();

  if ((!mathDone || !mermaidDone) && retries > 0) {
    setTimeout(() => initDocsWithRetry(retries - 1, delay), delay);
  }
}

if (typeof document$ !== "undefined") {
  document$.subscribe(() => {
    initDocsWithRetry();
  });
} else {
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => initDocsWithRetry());
  } else {
    initDocsWithRetry();
  }
}
