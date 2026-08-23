// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import Editor, { type OnMount } from "@monaco-editor/react";
import type { editor as MonacoEditor } from "monaco-editor";
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
  const [scalar, setScalar] = useState<unknown>();
  const [running, setRunning] = useState(false);
  const [elapsedMs, setElapsedMs] = useState<number>();
  const [files, setFiles] = useState<FileListing>();
  const [filesError, setFilesError] = useState<string>();
  const runRef = useRef<() => void>(() => {});
  const editorRef = useRef<MonacoEditor.IStandaloneCodeEditor | null>(null);

  const refreshEnvironment = useCallback(async () => {
    const response = await api<Environment>("/api/v1/environment");
    setEnvironment(response.tables);
  }, []);
  useEffect(() => {
    void refreshEnvironment();
  }, [refreshEnvironment]);

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

  const run = useCallback(async () => {
    setRunning(true);
    setError(undefined);
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
  }, [source]);
  runRef.current = () => {
    void run();
  };

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
    monaco.languages.register({ id: "ibex" });
    monaco.languages.setMonarchTokensProvider("ibex", {
      tokenizer: {
        root: [
          [
            /\b(let|fn|filter|select|update|by|window|import|extern|from)\b/,
            "keyword",
          ],
          [/\b(Int|Int64|Float64|String|Bool|DataFrame|TimeFrame)\b/, "type"],
          [/\/\/.*$/, "comment"],
          [/"([^"\\]|\\.)*"/, "string"],
          [/\d+(\.\d+)?/, "number"],
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
  };

  const insertFilePath = useCallback((path: string) => {
    const editor = editorRef.current;
    const text = JSON.stringify(path);
    if (!editor) {
      setSource((current) => current + text);
      return;
    }
    const selection = editor.getSelection();
    if (selection)
      editor.executeEdits("file-explorer", [
        { range: selection, text, forceMoveMarkers: true },
      ]);
    editor.focus();
  }, []);

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
              <strong>{table.name}</strong>
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
              <span key={column.name}>
                {column.name} <i>{column.type}</i>
              </span>
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
          <strong>Ibex</strong>
          <button className="run" disabled={running} onClick={() => void run()}>
            {running ? "Running…" : "Run"}
            <kbd>⌘↵</kbd>
          </button>
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
              <strong>Query error</strong>
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
    </main>
  );
}

export default App;

createRoot(document.getElementById("root")!).render(<App />);
