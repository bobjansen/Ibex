// Interactive Ibex playground — the real interpreter, compiled to WebAssembly
// (see wasm/ in the repo), running entirely in the browser. No server.
(() => {
  "use strict";

  const root = document.querySelector("[data-playground]");
  if (!root) return;

  const editor = root.querySelector("[data-pg-editor]");
  const runButton = root.querySelector("[data-pg-run]");
  const resetButton = root.querySelector("[data-pg-reset]");
  const status = root.querySelector("[data-pg-status]");
  const output = root.querySelector("[data-pg-output]");
  const envBar = root.querySelector("[data-pg-env]");
  const exampleBar = root.querySelector("[data-pg-examples]");

  const SEED = `import data_gen;
seed_rng(20240115);
let trades = gen_ticks(50000, "AAPL,MSFT,GOOG,AMZN,NVDA");
let reference = gen_reference("AAPL,MSFT,GOOG,AMZN,NVDA");
let prices = gen_walk(2000, 100.0, 1.0);`;

  const EXAMPLES = [
    ["Aggregate by group", 'trades[select { avg_price = mean(price), total_volume = sum(volume) }, by symbol];'],
    ["Filter and sort", "trades[filter price > 100, order price desc, head 20];"],
    ["Join reference data", "trades[select { trades = count() }, by symbol]\n  join reference on symbol;"],
    ["High / low by symbol", "trades[by symbol, select { hi = max(price), lo = min(price), spread = max(price) - min(price) }];"],
    ["Distinct symbols", "trades[distinct symbol];"],
  ];

  const FIRST_QUERY = EXAMPLES[0][1];

  const MAX_ROWS_SHOWN = 100;
  const ROW_LIMIT_NOTE = 1000; // bridge.cpp caps marshalled rows here

  let ibex = null;

  const setStatus = (text, kind = "") => {
    status.textContent = text;
    status.className = "pg-status" + (kind ? " pg-status-" + kind : "");
  };

  const renderEnv = (environment) => {
    const tables = (environment && environment.tables) || [];
    envBar.replaceChildren();
    if (!tables.length) return;
    const label = document.createElement("span");
    label.className = "pg-env-label";
    label.textContent = "session:";
    envBar.append(label);
    for (const t of tables) {
      const chip = document.createElement("button");
      chip.type = "button";
      chip.className = "pg-chip";
      chip.textContent = `${t.name} · ${t.rows.toLocaleString()} rows`;
      chip.title = t.columns.map((c) => `${c.name} ${c.type}`).join("\n");
      chip.addEventListener("click", () => {
        editor.value = `${t.name}[head 10];`;
        editor.focus();
      });
      envBar.append(chip);
    }
  };

  const renderTable = (table) => {
    const wrap = document.createElement("div");
    wrap.className = "pg-table-wrap";
    const el = document.createElement("table");
    el.className = "pg-table";

    const thead = el.createTHead().insertRow();
    for (const col of table.columns) {
      const th = document.createElement("th");
      th.innerHTML = `${escapeHtml(col.name)}<span>${col.type}</span>`;
      thead.append(th);
    }

    const body = el.createTBody();
    const shown = table.rows.slice(0, MAX_ROWS_SHOWN);
    for (const row of shown) {
      const tr = body.insertRow();
      for (const value of row) {
        const td = tr.insertCell();
        if (value === null) {
          td.className = "pg-null";
          td.textContent = "·";
        } else if (typeof value === "number") {
          td.className = "pg-num";
          td.textContent = formatNumber(value);
        } else {
          td.textContent = String(value);
        }
      }
    }
    wrap.append(el);

    const notes = [];
    if (table.total_rows > shown.length) {
      notes.push(`showing ${shown.length.toLocaleString()} of ${table.total_rows.toLocaleString()} rows`);
      if (table.total_rows >= ROW_LIMIT_NOTE) {
        notes.push(`the playground marshals the first ${ROW_LIMIT_NOTE.toLocaleString()} — summarise to see more`);
      }
    }
    if (notes.length) {
      const p = document.createElement("p");
      p.className = "pg-note";
      p.textContent = notes.join(" · ");
      wrap.append(p);
    }
    return wrap;
  };

  const render = (result) => {
    output.replaceChildren();

    if (result.error) {
      const box = document.createElement("pre");
      box.className = "pg-error";
      const where = result.error_line ? ` (line ${result.error_line})` : "";
      box.textContent = result.error + where;
      output.append(box);
    }

    for (const table of result.results) output.append(renderTable(table));

    if (result.scalar !== undefined) {
      const p = document.createElement("p");
      p.className = "pg-scalar";
      p.textContent =
        "= " + (typeof result.scalar === "number" ? formatNumber(result.scalar) : String(result.scalar));
      output.append(p);
    }

    if (!result.error && !result.results.length && result.scalar === undefined) {
      const p = document.createElement("p");
      p.className = "pg-note";
      p.textContent = "No table or scalar returned.";
      output.append(p);
    }

    renderEnv(result.environment);
  };

  const run = () => {
    if (!ibex) return;
    const source = editor.value.trim();
    if (!source) return;
    runButton.disabled = true;
    try {
      const start = performance.now();
      const result = JSON.parse(ibex.execute(source));
      const ms = performance.now() - start;
      render(result);
      if (result.error) {
        setStatus(`Error in ${ms.toFixed(1)} ms`, "error");
      } else {
        setStatus(`Ran in ${ms.toFixed(1)} ms — in your browser, no server`, "ok");
      }
    } catch (err) {
      setStatus("Interpreter crashed — reload the page", "error");
      console.error(err);
    } finally {
      runButton.disabled = false;
    }
  };

  const resetSession = () => {
    output.replaceChildren();
    envBar.replaceChildren();
    editor.value = FIRST_QUERY;
    boot(true);
  };

  const boot = async (reset) => {
    setStatus("Loading the interpreter (~1.4 MB, cached after first load)…");
    runButton.disabled = true;
    try {
      if (reset || !window.__createIbex) {
        const factory = (await import("./playground/ibex.mjs")).default;
        window.__createIbex = factory;
      }
      ibex = await window.__createIbex();
      // Seed the sample tables so the first query has something to hit.
      const seeded = JSON.parse(ibex.execute(SEED));
      renderEnv(seeded.environment);
      if (seeded.error) {
        setStatus("Sample data failed to load: " + seeded.error, "error");
      } else {
        setStatus("Ready — sample tables loaded. Run a query with ⌘/Ctrl + Enter.");
      }
      runButton.disabled = false;
    } catch (err) {
      setStatus("Could not load the interpreter — check the console", "error");
      console.error(err);
    }
  };

  // ── helpers ──────────────────────────────────────────────────────────────
  function escapeHtml(text) {
    return String(text).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
  }
  function formatNumber(value) {
    if (Number.isInteger(value)) return value.toLocaleString();
    if (Math.abs(value) >= 1e-4 && Math.abs(value) < 1e12) {
      return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
    }
    return value.toExponential(4);
  }

  // ── wire up ──────────────────────────────────────────────────────────────
  editor.value = FIRST_QUERY;
  for (const [label, code] of EXAMPLES) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "pg-example";
    button.textContent = label;
    button.addEventListener("click", () => {
      editor.value = code;
      editor.focus();
      run();
    });
    exampleBar.append(button);
  }

  runButton.addEventListener("click", run);
  resetButton.addEventListener("click", resetSession);
  editor.addEventListener("keydown", (event) => {
    if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
      event.preventDefault();
      run();
    }
  });

  boot(false);
})();
