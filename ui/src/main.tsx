// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import Editor, { type OnMount } from "@monaco-editor/react";
import { flexRender, getCoreRowModel, useReactTable } from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import "./styles.css";

type Column = { name: string; type: string };
type Page = { kind: "table"; columns: Column[]; rows: unknown[][]; offset: number; limit: number; total_rows: number };
type EnvironmentTable = { name: string; rows: number; lazy: boolean; columns: Column[] };
type Environment = { tables: EnvironmentTable[] };
type ExecuteResponse = {
  ok: boolean;
  result?: Page | { kind: "scalar"; value: unknown } | { kind: "none" };
  result_id?: string;
  environment: Environment;
  error?: { message: string; line?: number; column?: number };
};

const initialQuery = "trades[select { price, symbol }];";

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, { ...init, headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) } });
  if (!response.ok) throw new Error(await response.text());
  return response.json() as Promise<T>;
}

function Grid({ page }: { page: Page }) {
  const parentRef = useRef<HTMLDivElement>(null);
  const table = useReactTable({
    data: page.rows,
    columns: page.columns.map((column, index) => ({ id: column.name, header: `${column.name} · ${column.type}`, accessorFn: (row: unknown[]) => row[index] })),
    getCoreRowModel: getCoreRowModel(),
  });
  const rows = table.getRowModel().rows;
  const virtualizer = useVirtualizer({ count: rows.length, getScrollElement: () => parentRef.current, estimateSize: () => 30, overscan: 12 });
  return <div className="result-grid" ref={parentRef}>
    <table>
      <thead>{table.getHeaderGroups().map(group => <tr key={group.id}>{group.headers.map(header => <th key={header.id}>{flexRender(header.column.columnDef.header, header.getContext())}</th>)}</tr>)}</thead>
      <tbody style={{ height: virtualizer.getTotalSize() }}>
        {virtualizer.getVirtualItems().map(item => {
          const row = rows[item.index];
          return <tr key={row.id} style={{ transform: `translateY(${item.start}px)` }}>
            {row.getVisibleCells().map(cell => <td key={cell.id}>{cell.getValue() === null ? <em>NULL</em> : String(cell.getValue())}</td>)}
          </tr>;
        })}
      </tbody>
    </table>
  </div>;
}

function App() {
  const [source, setSource] = useState(initialQuery);
  const [page, setPage] = useState<Page>();
  const [resultId, setResultId] = useState<string>();
  const [environment, setEnvironment] = useState<EnvironmentTable[]>([]);
  const [error, setError] = useState<string>();
  const [scalar, setScalar] = useState<unknown>();
  const [running, setRunning] = useState(false);

  const refreshEnvironment = useCallback(async () => {
    const response = await api<Environment>("/api/v1/environment");
    setEnvironment(response.tables);
  }, []);
  useEffect(() => { void refreshEnvironment(); }, [refreshEnvironment]);

  const run = useCallback(async () => {
    setRunning(true); setError(undefined); setScalar(undefined);
    try {
      const response = await api<ExecuteResponse>("/api/v1/execute", { method: "POST", body: JSON.stringify({ source, limit: 200 }) });
      setEnvironment(response.environment.tables);
      if (!response.ok) { setError(response.error?.message ?? "Query failed"); return; }
      setResultId(response.result_id);
      if (response.result?.kind === "table") setPage(response.result);
      else { setPage(undefined); if (response.result?.kind === "scalar") setScalar(response.result.value); }
    } catch (cause) { setError(cause instanceof Error ? cause.message : "Request failed"); }
    finally { setRunning(false); }
  }, [source]);

  const remove = useCallback(async (name: string) => {
    const response = await api<Environment>(`/api/v1/environment/${encodeURIComponent(name)}`, { method: "DELETE" });
    setEnvironment(response.tables);
  }, []);

  const loadMore = useCallback(async () => {
    if (!page || !resultId || page.rows.length >= page.total_rows) return;
    try {
      const next = await api<Page>(`/api/v1/results/${resultId}?offset=${page.rows.length}&limit=200`);
      setPage(current => current ? { ...next, rows: [...current.rows, ...next.rows], offset: 0 } : next);
    } catch (cause) { setError(cause instanceof Error ? cause.message : "Could not load more rows"); }
  }, [page, resultId]);

  const onMount: OnMount = (editor, monaco) => {
    monaco.languages.register({ id: "ibex" });
    monaco.languages.setMonarchTokensProvider("ibex", { tokenizer: { root: [[/\b(let|fn|filter|select|update|by|window|import|extern|from)\b/, "keyword"], [/\b(Int|Int64|Float64|String|Bool|DataFrame|TimeFrame)\b/, "type"], [/\/\/.*$/, "comment"], [/"([^"\\]|\\.)*"/, "string"], [/\d+(\.\d+)?/, "number"]] } });
    monaco.editor.setModelLanguage(editor.getModel()!, "ibex");
    editor.addAction({ id: "run-query", label: "Run query", keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter], run });
  };

  const resultSummary = useMemo(() => page ? `${page.total_rows.toLocaleString()} rows` : scalar !== undefined ? "Scalar result" : "No result", [page, scalar]);
  return <main>
    <aside><header><strong>Environment</strong><button onClick={() => void refreshEnvironment()}>↻</button></header>
      {environment.map(table => <section className="binding" key={table.name}><div><strong>{table.name}</strong><button title={`Remove ${table.name}`} onClick={() => void remove(table.name)}>×</button></div><small>{table.lazy ? "lazy source" : `${table.rows.toLocaleString()} rows`}</small>{table.columns.map(column => <span key={column.name}>{column.name} <i>{column.type}</i></span>)}</section>)}
    </aside>
    <section className="workspace"><header className="toolbar"><strong>Ibex</strong><button className="run" disabled={running} onClick={() => void run()}>{running ? "Running…" : "Run"}<kbd>⌘↵</kbd></button></header>
      <div className="editor"><Editor height="100%" theme="vs-dark" defaultLanguage="ibex" value={source} onChange={value => setSource(value ?? "")} onMount={onMount} options={{ minimap: { enabled: false }, fontSize: 14, automaticLayout: true }} /></div>
      {error && <div className="error"><strong>Query error</strong><pre>{error}</pre></div>}
      <section className="results"><header><strong>Results</strong><span>{resultSummary}</span>{resultId && <span className="muted">paged result {resultId}</span>}{page && page.rows.length < page.total_rows && <button onClick={() => void loadMore()}>Load more</button>}</header>{page && <Grid page={page} />}{scalar !== undefined && <output>{String(scalar)}</output>}</section>
    </section>
  </main>;
}

export default App;

createRoot(document.getElementById("root")!).render(<App />);
