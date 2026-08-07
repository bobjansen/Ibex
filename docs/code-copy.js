document.querySelectorAll("pre > code").forEach((code) => {
  const pre = code.parentElement;
  const button = document.createElement("button");
  button.className = "copy-code";
  button.type = "button";
  button.textContent = "Copy";
  button.setAttribute("aria-label", "Copy code to clipboard");

  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(code.textContent);
      button.textContent = "Copied";
      button.classList.add("copied");
      setTimeout(() => {
        button.textContent = "Copy";
        button.classList.remove("copied");
      }, 1600);
    } catch {
      button.textContent = "Copy failed";
      setTimeout(() => { button.textContent = "Copy"; }, 1600);
    }
  });

  pre.append(button);
});
