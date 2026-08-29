// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import Editor, { type OnMount } from "@monaco-editor/react";
import type {
  editor as MonacoEditor,
  Position as MonacoPosition,
} from "monaco-editor";
import { useVirtualizer } from "@tanstack/react-virtual";
import "./styles.css";

type Column = { name: string; type: string };
type Page = {
  kind: "table";
  columns: Column[];
  rows: unknown[][];
  offset: number;
  limit: number;
  total_rows: number;
};
type EnvironmentTable = {
  name: string;
  rows: number;
  lazy: boolean;
  columns: Column[];
};
type Environment = { tables: EnvironmentTable[] };
type FileEntry = {
  name: string;
  path: string;
  relative_path: string;
  directory: boolean;
};
type FileListing = { path: string; entries: FileEntry[] };
type TableResult = { result_id: string; result: Page };
type ExecuteResponse = {
  ok: boolean;
  result?: Page | { kind: "scalar"; value: unknown } | { kind: "none" };
  result_id?: string;
  results?: TableResult[];
  elapsed_ms: number;
  environment: Environment;
  error?: { message: string; line?: number; column?: number };
};

const initialQuery = "trades[select { price, symbol }];";

type Example = { label: string; source: string };
const examples: Example[] = [
  {
    label: "Select columns",
    source: "trades[select { symbol, price, volume }];",
  },
  {
    label: "Filter rows",
    source: "trades[filter price > 100, select { timestamp, symbol, price }];",
  },
  {
    label: "Aggregate by group",
    source:
      "trades[select { ticks = count(), avg_price = mean(price), total_volume = sum(volume) }, by symbol];",
  },
  {
    label: "Top 10 by price",
    source: "trades[order { price desc }, head 10];",
  },
  {
    label: "Join reference data",
    source:
      "trades[select { avg_price = mean(price), total_volume = sum(volume) }, by symbol]\n  join reference on symbol;",
  },
  {
    label: "Rolling 1-minute mean",
    source:
      'let tf = as_timeframe(trades, "timestamp");\ntf[window 1m, update { avg_1m = rolling_mean(price) }];',
  },
  {
    label: "Summary stats",
    source:
      "samples[select { n = count(), mean = mean(value), min = min(value), max = max(value) }];",
  },
];

// Surface vocabulary the interpreter understands so the editor can complete it.
const KEYWORDS = [
  "let",
  "fn",
  "extern",
  "from",
  "import",
  "filter",
  "select",
  "update",
  "distinct",
  "order",
  "head",
  "tail",
  "by",
  "window",
  "resample",
  "rename",
  "join",
  "left",
  "right",
  "outer",
  "semi",
  "anti",
  "cross",
  "asof",
  "on",
  "suffix",
  "asc",
  "desc",
  "as",
  "case",
  "else",
  "map",
  "in",
];

type Builtin = { name: string; signature: string; summary: string };
const BUILTINS: Builtin[] = [
  {
    name: "mean",
    signature: "mean(col) -> Float64",
    summary: "Aggregate mean.",
  },
  { name: "sum", signature: "sum(col)", summary: "Aggregate sum." },
  {
    name: "count",
    signature: "count() | count(col) -> Int64",
    summary: "Rows in the group, or non-null values of col.",
  },
  { name: "min", signature: "min(col)", summary: "Aggregate minimum." },
  { name: "max", signature: "max(col)", summary: "Aggregate maximum." },
  {
    name: "first",
    signature: "first(col)",
    summary: "First value in group order.",
  },
  {
    name: "last",
    signature: "last(col)",
    summary: "Last value in group order.",
  },
  {
    name: "median",
    signature: "median(col) -> Float64",
    summary: "Aggregate median.",
  },
  {
    name: "std",
    signature: "std(col) -> Float64",
    summary: "Sample standard deviation.",
  },
  {
    name: "quantile",
    signature: "quantile(col, p) -> Float64",
    summary: "Interpolated aggregate quantile.",
  },
  {
    name: "as_timeframe",
    signature: 'as_timeframe(table, "ts_col") -> TimeFrame',
    summary: "Mark a table as time-indexed and sorted.",
  },
  {
    name: "lag",
    signature: "lag(col, n)",
    summary: "Value n rows before the current row.",
  },
  {
    name: "lead",
    signature: "lead(col, n)",
    summary: "Value n rows after the current row.",
  },
  {
    name: "rank",
    signature: "rank(expr, method = dense, ascending = true)",
    summary: "Row rank, optionally partitioned by a by clause.",
  },
  {
    name: "round",
    signature: "round(value, mode)",
    summary: "nearest / bankers / floor / ceil / trunc.",
  },
  {
    name: "like",
    signature: "like(value, pattern) -> Bool",
    summary: "SQL-LIKE match: % any run, _ one char.",
  },
  {
    name: "columns",
    signature: "columns(table) -> DataFrame",
    summary: "Metadata table of column names.",
  },
  {
    name: "scalar",
    signature: "scalar(table, column)",
    summary: "Extract a scalar from a one-row table.",
  },
  { name: "print", signature: "print(value)", summary: "Print a value." },
  {
    name: "rolling_mean",
    signature: "rolling_mean(col)",
    summary: "Mean over the surrounding window clause.",
  },
  {
    name: "rolling_sum",
    signature: "rolling_sum(col)",
    summary: "Sum over the surrounding window clause.",
  },
  {
    name: "rolling_count",
    signature: "rolling_count()",
    summary: "Row count over the surrounding window clause.",
  },
  {
    name: "rolling_min",
    signature: "rolling_min(col)",
    summary: "Minimum over the surrounding window clause.",
  },
  {
    name: "rolling_max",
    signature: "rolling_max(col)",
    summary: "Maximum over the surrounding window clause.",
  },
  {
    name: "rolling_std",
    signature: "rolling_std(col)",
    summary: "Std deviation over the surrounding window clause.",
  },
];

type CheatEntry = { title: string; code: string; note: string };
const cheatsheet: CheatEntry[] = [
  {
    title: "Select columns",
    code: "trades[select { symbol, price }]",
    note: "Pick or compute columns. Bare `trades[{ symbol, price }]` is shorthand.",
  },
  {
    title: "Filter rows",
    code: "trades[filter price > 100]",
    note: "Keep rows where the predicate holds. Combine with `&&`, `||`, `!`.",
  },
  {
    title: "Computed columns",
    code: "trades[select { notional = price * volume }]",
    note: "`select` replaces the schema; `update` adds to it.",
  },
  {
    title: "Group and aggregate",
    code: "trades[select { avg = mean(price), n = count() }, by symbol]",
    note: "`by` needs a `select` or `update`. Group keys come through automatically.",
  },
  {
    title: "Order and limit",
    code: "trades[order { price desc }, head 10]",
    note: "`order { col asc/desc, ... }`, then `head n` / `tail n`.",
  },
  {
    title: "Distinct",
    code: "trades[distinct { symbol }]",
    note: "One row per distinct combination of the listed columns.",
  },
  {
    title: "Join",
    code: "trades join reference on symbol",
    note: '`left/right/outer/semi/anti/cross join`. Add `suffix { "", "_r" }` when non-key names collide.',
  },
  {
    title: "Time series",
    code: 'let tf = as_timeframe(trades, "timestamp");\ntf[window 5m, update { avg = rolling_mean(price) }]',
    note: "`window` for trailing windows, `resample 1m` for fixed bars. Needs a TimeFrame.",
  },
  {
    title: "Lag / lead",
    code: "trades[update { prev = lag(price, 1) }, by symbol]",
    note: "Row-relative access within each group.",
  },
];

// Monaco language providers are global; register the completion provider once
// even if the editor component remounts.
let completionRegistered = false;

const WELCOME_KEY = "ibex-ui-welcome";
function welcomeDismissed(): boolean {
  try {
    return localStorage.getItem(WELCOME_KEY) === "dismissed";
  } catch {
    return false;
  }
}
function dismissWelcome(): void {
  try {
    localStorage.setItem(WELCOME_KEY, "dismissed");
  } catch {
    // Ignore storage failures (private mode, blocked site data).
  }
}

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
  });
  if (!response.ok) throw new Error(await response.text());
  return response.json() as Promise<T>;
}

const integerFormatter = new Intl.NumberFormat(undefined, {
  maximumFractionDigits: 0,
});
const floatFormatter = new Intl.NumberFormat(undefined, {
  maximumSignificantDigits: 6,
});

function rawCellValue(value: unknown): string {
  if (typeof value === "string") return value;
  if (value === null) return "NULL";
  return String(value);
}

function displayCellValue(value: unknown, type: string): string {
  if (typeof value !== "number" || !Number.isFinite(value))
    return rawCellValue(value);
  return type === "Int64"
    ? integerFormatter.format(value)
    : floatFormatter.format(value);
}

function Grid({ page }: { page: Page }) {
  const parentRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: page.rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 30,
    overscan: 12,
  });
  const columns = {
    gridTemplateColumns: `repeat(${page.columns.length}, minmax(140px, 1fr))`,
  };
  // Execute responses are deliberately capped at 200 rows. Rendering that
  // bounded first page directly avoids relying on a scroll-container size
  // measurement before the result pane is laid out; accumulated pages remain
  // virtualized.
  const isBoundedPage = page.rows.length <= 200;
  const bodyStyle = isBoundedPage
    ? undefined
    : { height: virtualizer.getTotalSize() };
  const renderRow = (
    row: unknown[],
    key: string | number,
    transform?: string,
  ) => (
    <div
      className={`grid-row${isBoundedPage ? " direct-row" : ""}`}
      key={key}
      style={{ ...columns, transform }}
    >
      {row.map((value, index) => {
        const column = page.columns[index];
        const raw = rawCellValue(value);
        return (
          <span
            className="cell-value"
            key={column.name}
            title={raw}
            onCopy={(event) => {
              event.clipboardData.setData("text/plain", raw);
              event.preventDefault();
            }}
          >
            {value === null ? (
              <em>NULL</em>
            ) : (
              displayCellValue(value, column.type)
            )}
          </span>
        );
      })}
    </div>
  );
  return (
    <div className="result-grid" ref={parentRef}>
      <div className="grid-header" style={columns}>
        {page.columns.map((column) => (
          <strong key={column.name}>
            {column.name} <small>{column.type}</small>
          </strong>
        ))}
      </div>
      <div className="grid-body" style={bodyStyle}>
        {isBoundedPage
          ? page.rows.map((row, index) => renderRow(row, index))
          : virtualizer
              .getVirtualItems()
              .map((item) =>
                renderRow(
                  page.rows[item.index],
                  String(item.key),
                  `translateY(${item.start}px)`,
                ),
              )}
      </div>
    </div>
  );
}

function App() {
  const [source, setSource] = useState(initialQuery);
  const [tableResults, setTableResults] = useState<TableResult[]>([]);
  const [activeResultId, setActiveResultId] = useState<string>();
  const [environment, setEnvironment] = useState<EnvironmentTable[]>([]);
  const [error, setError] = useState<string>();
  const [errorAt, setErrorAt] = useState<{ line: number; column?: number }>();
  const [scalar, setScalar] = useState<unknown>();
  const [running, setRunning] = useState(false);
  const [elapsedMs, setElapsedMs] = useState<number>();
  const [files, setFiles] = useState<FileListing>();
  const [filesError, setFilesError] = useState<string>();
  const [demo, setDemo] = useState(false);
  const [showWelcome, setShowWelcome] = useState(() => !welcomeDismissed());
  const [showCheatsheet, setShowCheatsheet] = useState(false);
  const runRef = useRef<() => void>(() => {});
  const editorRef = useRef<MonacoEditor.IStandaloneCodeEditor | null>(null);
  const monacoRef = useRef<Parameters<OnMount>[1] | null>(null);
  const environmentRef = useRef<EnvironmentTable[]>([]);

  const refreshEnvironment = useCallback(async () => {
    const response = await api<Environment>("/api/v1/environment");
    setEnvironment(response.tables);
  }, []);
  useEffect(() => {
    void refreshEnvironment();
  }, [refreshEnvironment]);

  useEffect(() => {
    api<{ demo: boolean }>("/api/v1/config")
      .then((config) => setDemo(config.demo))
      .catch(() => setDemo(false));
  }, []);

  const loadExample = useCallback((label: string) => {
    const example = examples.find((entry) => entry.label === label);
    if (!example) return;
    setSource(example.source);
    editorRef.current?.focus();
  }, []);

  const refreshFiles = useCallback(async (path = "") => {
    try {
      const response = await api<FileListing>(
        `/api/v1/files?path=${encodeURIComponent(path)}`,
      );
      setFiles(response);
      setFilesError(undefined);
    } catch (cause) {
      setFilesError(
        cause instanceof Error ? cause.message : "Could not list files",
      );
    }
  }, []);
  useEffect(() => {
    void refreshFiles();
  }, [refreshFiles]);

  const setEditorMarkers = useCallback(
    (err?: { message: string; line?: number; column?: number }) => {
      const monaco = monacoRef.current;
      const model = editorRef.current?.getModel();
      if (!monaco || !model) return;
      const markers =
        err && err.line
          ? [
              {
                message: err.message,
                severity: monaco.MarkerSeverity.Error,
                startLineNumber: err.line,
                startColumn: err.column ?? 1,
                endLineNumber: err.line,
                endColumn: (err.column ?? 1) + 1,
              },
            ]
          : [];
      monaco.editor.setModelMarkers(model, "ibex", markers);
    },
    [],
  );

  const run = useCallback(async () => {
    setRunning(true);
    setError(undefined);
    setErrorAt(undefined);
    setEditorMarkers(undefined);
    setScalar(undefined);
    setElapsedMs(undefined);
    setTableResults([]);
    setActiveResultId(undefined);
    try {
      const response = await api<ExecuteResponse>("/api/v1/execute", {
        method: "POST",
        body: JSON.stringify({ source, limit: 200 }),
      });
      setEnvironment(response.environment.tables);
      setElapsedMs(response.elapsed_ms);
      if (!response.ok) {
        setError(response.error?.message ?? "Query failed");
        if (response.error?.line !== undefined) {
          setErrorAt({
            line: response.error.line,
            column: response.error.column,
          });
        }
        setEditorMarkers(response.error);
        return;
      }
      const tables =
        response.results ??
        (response.result?.kind === "table" && response.result_id
          ? [{ result_id: response.result_id, result: response.result }]
          : []);
      setTableResults(tables);
      setActiveResultId(tables[0]?.result_id);
      if (response.result?.kind === "scalar") setScalar(response.result.value);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Request failed");
    } finally {
      setRunning(false);
    }
  }, [source, setEditorMarkers]);
  runRef.current = () => {
    void run();
  };
  environmentRef.current = environment;

  const remove = useCallback(async (name: string) => {
    const response = await api<Environment>(
      `/api/v1/environment/${encodeURIComponent(name)}`,
      { method: "DELETE" },
    );
    setEnvironment(response.tables);
  }, []);

  const loadMore = useCallback(async () => {
    const current = tableResults.find(
      (result) => result.result_id === activeResultId,
    );
    if (!current || current.result.rows.length >= current.result.total_rows)
      return;
    try {
      const next = await api<Page>(
        `/api/v1/results/${current.result_id}?offset=${current.result.rows.length}&limit=200`,
      );
      setTableResults((results) =>
        results.map((result) =>
          result.result_id === current.result_id
            ? {
                ...result,
                result: {
                  ...next,
                  rows: [...result.result.rows, ...next.rows],
                  offset: 0,
                },
              }
            : result,
        ),
      );
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "Could not load more rows",
      );
    }
  }, [activeResultId, tableResults]);

  const onMount: OnMount = (editor, monaco) => {
    editorRef.current = editor;
    monacoRef.current = monaco;
    monaco.languages.register({ id: "ibex" });
    monaco.languages.setMonarchTokensProvider("ibex", {
      keywords: KEYWORDS,
      tokenizer: {
        root: [
          [/\/\/.*$/, "comment"],
          [/"([^"\\]|\\.)*"/, "string"],
          [
            /\b(Int|Int32|Int64|Float32|Float64|String|Bool|Date|Timestamp|Series|DataFrame|TimeFrame|Stream)\b/,
            "type",
          ],
          [/\d+(\.\d+)?[smhd]?/, "number"],
          [
            /[a-zA-Z_]\w*/,
            { cases: { "@keywords": "keyword", "@default": "identifier" } },
          ],
        ],
      },
    });
    monaco.editor.setModelLanguage(editor.getModel()!, "ibex");
    editor.addAction({
      id: "run-query",
      label: "Run query",
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
      run: () => runRef.current(),
    });

    if (!completionRegistered) {
      completionRegistered = true;
      monaco.languages.registerCompletionItemProvider("ibex", {
        provideCompletionItems: (
          model: MonacoEditor.ITextModel,
          position: MonacoPosition,
        ) => {
          const word = model.getWordUntilPosition(position);
          const range = {
            startLineNumber: position.lineNumber,
            endLineNumber: position.lineNumber,
            startColumn: word.startColumn,
            endColumn: word.endColumn,
          };
          const K = monaco.languages.CompletionItemKind;
          const suggestions = [
            ...KEYWORDS.map((label) => ({
              label,
              kind: K.Keyword,
              insertText: label,
              range,
            })),
            ...BUILTINS.map((fn) => ({
              label: fn.name,
              kind: K.Function,
              detail: fn.signature,
              documentation: fn.summary,
              insertText: fn.name,
              range,
            })),
            ...environmentRef.current.map((table) => ({
              label: table.name,
              kind: K.Struct,
              detail: `${table.lazy ? "lazy source" : `${table.rows} rows`}`,
              insertText: table.name,
              range,
            })),
            ...[
              ...new Map(
                environmentRef.current
                  .flatMap((table) => table.columns)
                  .map((column) => [column.name, column]),
              ).values(),
            ].map((column) => ({
              label: column.name,
              kind: K.Field,
              detail: column.type,
              insertText: column.name,
              range,
            })),
          ];
          return { suggestions };
        },
      });
    }
  };

  // Drop a starter query onto the current line if it is blank, otherwise onto
  // the next blank line, otherwise appended on a fresh line at the end. Never
  // overwrites what the user has already typed.
  const setStarterQuery = useCallback((table: EnvironmentTable) => {
    const columns = table.columns
      .slice(0, 4)
      .map((column) => column.name)
      .join(", ");
    const snippet = `${table.name}[select { ${columns} }];`;
    const editor = editorRef.current;
    const model = editor?.getModel();
    if (!editor || !model) {
      setSource((current) =>
        current.trim()
          ? `${current.replace(/\n*$/, "")}\n${snippet}\n`
          : snippet,
      );
      return;
    }
    const lineCount = model.getLineCount();
    const cursorLine = editor.getPosition()?.lineNumber ?? lineCount;
    const isBlank = (line: number) => model.getLineContent(line).trim() === "";

    let target = isBlank(cursorLine) ? cursorLine : 0;
    for (
      let line = cursorLine + 1;
      target === 0 && line <= lineCount;
      line += 1
    ) {
      if (isBlank(line)) target = line;
    }

    if (target !== 0) {
      editor.executeEdits("environment", [
        {
          range: {
            startLineNumber: target,
            startColumn: 1,
            endLineNumber: target,
            endColumn: model.getLineMaxColumn(target),
          },
          text: snippet,
          forceMoveMarkers: true,
        },
      ]);
      editor.setPosition({ lineNumber: target, column: snippet.length + 1 });
    } else {
      const endColumn = model.getLineMaxColumn(lineCount);
      editor.executeEdits("environment", [
        {
          range: {
            startLineNumber: lineCount,
            startColumn: endColumn,
            endLineNumber: lineCount,
            endColumn,
          },
          text: `\n${snippet}`,
          forceMoveMarkers: true,
        },
      ]);
      editor.setPosition({
        lineNumber: lineCount + 1,
        column: snippet.length + 1,
      });
    }
    editor.focus();
  }, []);

  const insertSnippet = useCallback((text: string) => {
    const editor = editorRef.current;
    if (!editor) {
      setSource((current) => (current ? `${current}\n${text}` : text));
      return;
    }
    const selection = editor.getSelection();
    if (selection)
      editor.executeEdits("environment", [
        { range: selection, text, forceMoveMarkers: true },
      ]);
    editor.focus();
  }, []);
  const insertFilePath = useCallback(
    (path: string) => insertSnippet(JSON.stringify(path)),
    [insertSnippet],
  );

  const jumpToError = useCallback(() => {
    const editor = editorRef.current;
    if (!editor || !errorAt) return;
    editor.revealLineInCenter(errorAt.line);
    editor.setPosition({
      lineNumber: errorAt.line,
      column: errorAt.column ?? 1,
    });
    editor.focus();
  }, [errorAt]);

  const page = tableResults.find(
    (result) => result.result_id === activeResultId,
  )?.result;
  const resultSummary = useMemo(
    () =>
      page
        ? `${page.total_rows.toLocaleString()} rows`
        : scalar !== undefined
          ? "Scalar result"
          : "No result",
    [page, scalar],
  );
  return (
    <main>
      <aside>
        <header>
          <strong>Environment</strong>
          <button onClick={() => void refreshEnvironment()}>↻</button>
        </header>
        {environment.map((table) => (
          <section className="binding" key={table.name}>
            <div>
              <button
                className="binding-name"
                title={`Load a starter query for ${table.name}`}
                onClick={() => setStarterQuery(table)}
              >
                {table.name}
              </button>
              <button
                title={`Remove ${table.name}`}
                onClick={() => void remove(table.name)}
              >
                ×
              </button>
            </div>
            <small>
              {table.lazy
                ? "lazy source"
                : `${table.rows.toLocaleString()} rows`}
            </small>
            {table.columns.map((column) => (
              <button
                className="binding-column"
                key={column.name}
                title={`Insert ${column.name}`}
                onClick={() => insertSnippet(column.name)}
              >
                {column.name} <i>{column.type}</i>
              </button>
            ))}
          </section>
        ))}
        <section className="file-explorer">
          <header>
            <strong>Files</strong>
            <button onClick={() => void refreshFiles(files?.path)}>↻</button>
          </header>
          {files?.path && (
            <button
              className="file-entry parent"
              onClick={() =>
                void refreshFiles(files.path.split("/").slice(0, -1).join("/"))
              }
            >
              ..
            </button>
          )}
          {files?.entries.map((entry) => (
            <button
              className="file-entry"
              key={entry.relative_path}
              onClick={() =>
                entry.directory && void refreshFiles(entry.relative_path)
              }
              onDoubleClick={() =>
                !entry.directory && insertFilePath(entry.path)
              }
              title={entry.path}
            >
              {entry.directory ? "▸ " : ""}
              {entry.name}
            </button>
          ))}
          {filesError && <small className="files-error">{filesError}</small>}
        </section>
      </aside>
      <section className="workspace">
        <header className="toolbar">
          <div className="brand">
            <strong>Ibex</strong>
            <small>Typed columnar DataFrame &amp; TimeFrame DSL</small>
            <nav className="brand-links">
              <a
                href="https://ibexlang.org/docs.html"
                target="_blank"
                rel="noreferrer"
              >
                Docs
              </a>
              <a
                href="https://ibexlang.org/cheatsheet.html"
                target="_blank"
                rel="noreferrer"
              >
                Cheatsheet
              </a>
              <a
                href="https://github.com/bobjansen/Ibex"
                target="_blank"
                rel="noreferrer"
              >
                GitHub
              </a>
            </nav>
          </div>
          <div className="toolbar-actions">
            <select
              className="examples"
              value=""
              onChange={(event) => loadExample(event.target.value)}
            >
              <option value="">Examples…</option>
              {examples.map((example) => (
                <option key={example.label} value={example.label}>
                  {example.label}
                </option>
              ))}
            </select>
            <button onClick={() => setShowCheatsheet(true)}>Cheatsheet</button>
            <button
              className="run"
              disabled={running}
              onClick={() => void run()}
            >
              {running ? "Running…" : "Run"}
              <kbd>⌘↵</kbd>
            </button>
          </div>
        </header>
        <div className="editor">
          <Editor
            height="100%"
            theme="vs-dark"
            defaultLanguage="ibex"
            value={source}
            onChange={(value) => setSource(value ?? "")}
            onMount={onMount}
            options={{
              minimap: { enabled: false },
              fontSize: 14,
              automaticLayout: true,
            }}
          />
        </div>
        <section className="output-pane">
          {error && (
            <div className="error">
              <div className="error-head">
                <strong>Query error</strong>
                {errorAt && (
                  <button onClick={jumpToError}>
                    Line {errorAt.line}
                    {errorAt.column ? `, col ${errorAt.column}` : ""}
                  </button>
                )}
              </div>
              <pre>{error}</pre>
            </div>
          )}
          <section className="results">
            <header>
              <strong>Results</strong>
              <span>{resultSummary}</span>
              {elapsedMs !== undefined && (
                <span className="query-timing">
                  {elapsedMs.toFixed(elapsedMs < 10 ? 2 : 1)} ms
                </span>
              )}
              {page && page.rows.length < page.total_rows && (
                <button onClick={() => void loadMore()}>Load more</button>
              )}
            </header>
            {tableResults.length > 1 && (
              <nav className="result-tabs">
                {tableResults.map((result, index) => (
                  <button
                    className={
                      result.result_id === activeResultId ? "active" : ""
                    }
                    key={result.result_id}
                    onClick={() => setActiveResultId(result.result_id)}
                  >
                    Table {index + 1}
                  </button>
                ))}
              </nav>
            )}
            {page && <Grid page={page} />}
            {scalar !== undefined && <output>{String(scalar)}</output>}
          </section>
        </section>
      </section>
      {showCheatsheet && (
        <div
          className="welcome-backdrop"
          onClick={() => setShowCheatsheet(false)}
        >
          <div
            className="cheatsheet-card"
            onClick={(event) => event.stopPropagation()}
          >
            <header>
              <strong>Ibex bracket syntax</strong>
              <button onClick={() => setShowCheatsheet(false)}>Close</button>
            </header>
            <p className="cheatsheet-intro">
              A table expression is <code>name[clause, clause, …]</code>.
              Clauses run left to right. Click an entry to load it into the
              editor.
            </p>
            <dl>
              {cheatsheet.map((entry) => (
                <div
                  className="cheatsheet-entry"
                  key={entry.title}
                  onClick={() => {
                    setSource(entry.code);
                    setShowCheatsheet(false);
                    editorRef.current?.focus();
                  }}
                >
                  <dt>{entry.title}</dt>
                  <dd>
                    <pre>{entry.code}</pre>
                    <span>{entry.note}</span>
                  </dd>
                </div>
              ))}
            </dl>
            <p className="cheatsheet-more">
              Full reference:{" "}
              <a
                href="https://ibexlang.org/cheatsheet.html"
                target="_blank"
                rel="noreferrer"
              >
                cheatsheet
              </a>{" "}
              ·{" "}
              <a
                href="https://ibexlang.org/docs.html"
                target="_blank"
                rel="noreferrer"
              >
                docs
              </a>
            </p>
          </div>
        </div>
      )}
      {showWelcome && (
        <div className="welcome-backdrop">
          <div className="welcome-card">
            <strong>Welcome to Ibex</strong>
            <p>
              Ibex is a statically typed DSL for columnar DataFrame and
              time-series work, with a fast parallel interpreter. Write a query
              on the left, press <kbd>Run</kbd>, and inspect the result grid
              below.
            </p>
            <p>
              {demo ? (
                <>
                  Four tables are preloaded: <code>trades</code>,{" "}
                  <code>reference</code> (symbol master data),{" "}
                  <code>prices</code>, and <code>samples</code>. Press Run to
                  execute the sample query, or pick one from the{" "}
                  <em>Examples</em> menu.
                </>
              ) : (
                <>
                  A tiny built-in <code>trades</code> table is available. Open a
                  data file from the Files panel, or restart with{" "}
                  <code>ibex ui --demo</code> for larger sample tables that the{" "}
                  <em>Examples</em> menu is written against.
                </>
              )}
            </p>
            <button
              className="run"
              onClick={() => {
                dismissWelcome();
                setShowWelcome(false);
              }}
            >
              Get started
            </button>
          </div>
        </div>
      )}
    </main>
  );
}

export default App;

createRoot(document.getElementById("root")!).render(<App />);
