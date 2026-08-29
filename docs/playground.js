// Interactive Ibex playground — the real interpreter, compiled to WebAssembly
// (see wasm/ in the repo), running entirely in the browser. No server.
(() => {
  "use strict";

  const root = document.querySelector("[data-playground]");
  if (!root) return;

  const editor = root.querySelector("[data-pg-editor]");
  const runButton = root.querySelector("[data-pg-run]");
  const resetButton = root.querySelector("[data-pg-reset]");
  const regenButton = root.querySelector("[data-pg-regen]");
  const status = root.querySelector("[data-pg-status]");
  const output = root.querySelector("[data-pg-output]");
  const envBar = root.querySelector("[data-pg-env]");
  const exampleBar = root.querySelector("[data-pg-examples]");
  const sizeBar = root.querySelector("[data-pg-size]");

  const SYMBOLS = "AAPL,MSFT,GOOG,AMZN,NVDA";

  // Selectable trades-table size, so the timing means something: at 50k a query
  // is mostly call overhead; at a few million the interpreter's actual work
  // shows.
  const SIZES = [
    [50_000, "50K"],
    [500_000, "500K"],
    [2_000_000, "2M"],
    [10_000_000, "10M"],
  ];
  const DEFAULT_ROWS = 50_000;
  let currentRows = DEFAULT_ROWS;

  const tradesScript = (rows, seed) =>
    `import data_gen;\nseed_rng(${seed});\nlet trades = gen_ticks(${rows}, "${SYMBOLS}");`;

  const seedScript = (rows) =>
    `${tradesScript(rows, 42)}\nlet reference = gen_reference("${SYMBOLS}");\nlet prices = gen_walk(2000, 100.0, 1.0);`;

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
    label.textContent = "Available tables";
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

  const showEmpty = () => {
    output.replaceChildren();
    const p = document.createElement("p");
    p.className = "pg-note pg-empty";
    p.textContent = "Run a query to see results.";
    output.append(p);
  };

  const resetSession = () => {
    envBar.replaceChildren();
    editor.value = FIRST_QUERY;
    setSizeButtons(DEFAULT_ROWS);
    currentRows = DEFAULT_ROWS;
    showEmpty();
    boot(true);
  };

  const setSizeButtons = (rows) => {
    for (const button of sizeBar.querySelectorAll(".pg-size-option")) {
      button.classList.toggle("active", Number(button.dataset.rows) === rows);
    }
  };

  // Rebuild `trades` and report how long generating it took. `describe` names
  // the action in the status line. Generation is synchronous and can take most
  // of a second at 10M rows, so paint a pending state first (double rAF) before
  // the blocking call.
  const rebuildTrades = (rows, seed, describe) => {
    if (!ibex) return;
    const busy = (on) => {
      runButton.disabled = on;
      for (const b of sizeBar.querySelectorAll(".pg-size-option")) b.disabled = on;
      regenButton.disabled = on;
    };
    busy(true);
    setStatus(`Generating ${rows.toLocaleString()} rows…`);
    requestAnimationFrame(() =>
      requestAnimationFrame(() => {
        const start = performance.now();
        const result = JSON.parse(ibex.execute(tradesScript(rows, seed)));
        const ms = performance.now() - start;
        renderEnv(result.environment);
        if (result.error) {
          setStatus("Could not build trades: " + result.error, "error");
        } else {
          setStatus(`${describe}: ${rows.toLocaleString()} rows in ${ms.toFixed(0)} ms`);
        }
        busy(false);
      }),
    );
  };

  // Session action: same size, fresh random data.
  const regenerate = () => rebuildTrades(currentRows, Math.floor(Math.random() * 1e9), "Regenerated trades");

  const setSize = (rows) => {
    if (rows === currentRows && ibex) return;
    currentRows = rows;
    setSizeButtons(rows);
    rebuildTrades(rows, 42, "Resized trades");
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
      const seeded = JSON.parse(ibex.execute(seedScript(currentRows)));
      renderEnv(seeded.environment);
      if (seeded.error) {
        setStatus("Sample data failed to load: " + seeded.error, "error");
      } else {
        setStatus("Ready. Run a query with ⌘/Ctrl + Enter.");
        if (!output.childElementCount) showEmpty();
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

  for (const [rows, label] of SIZES) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "pg-size-option" + (rows === currentRows ? " active" : "");
    button.dataset.rows = String(rows);
    button.textContent = label;
    button.addEventListener("click", () => setSize(rows));
    sizeBar.append(button);
  }

  runButton.addEventListener("click", run);
  resetButton.addEventListener("click", resetSession);
  regenButton.addEventListener("click", regenerate);
  editor.addEventListener("keydown", (event) => {
    if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
      event.preventDefault();
      run();
    }
  });

  boot(false);
})();
