#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Publish immutable benchmark/methodology pairs and a browsable history index.

Seed an existing page: python3 benchmarking/website_history.py docs/benchmarks.html
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

CONTROLS = '''<!-- benchmark-history -->
    <div id="benchmark-history" class="bench-controls" style="margin-top:1rem" aria-label="Benchmark publication">
      <label for="benchmark-run">History</label>
      <select id="benchmark-run" style="font:inherit;max-width:100%" disabled><option>Loading history…</option></select>
      <a id="benchmark-permalink" hidden>Permalink to this publication</a>
      <span id="benchmark-history-status" role="status"></span>
    </div>
    <script src="./benchmark-history.js" defer></script>
    <!-- /benchmark-history -->'''


def strip_controls(page: str) -> str:
    return re.sub(r'<!-- benchmark-history -->.*?<!-- /benchmark-history -->\s*', '', page, flags=re.S)


def decorate(page: str, run_id: str) -> str:
    controls = CONTROLS.replace('id="benchmark-history"', f'id="benchmark-history" data-run="{run_id}"')
    return strip_controls(page).replace('<div class="headline"', controls + '\n    <div class="headline"', 1)


def archive(out: Path, page: str, methodology: str) -> tuple[str, dict]:
    """Content-derived ids keep repeated publication safe, even within a minute."""
    page = strip_controls(page)
    marker = 'const PAYLOAD = '
    payload, _ = json.JSONDecoder().raw_decode(page.split(marker, 1)[1])
    meta = payload['meta']
    generated = meta['generated']
    stamp = re.sub(r'[^0-9]', '', generated)
    style_path = out.parent / 'styles.css'
    if not style_path.exists():
        style_path = Path(__file__).resolve().parents[1] / 'docs' / 'styles.css'
    styles = style_path.read_text(encoding='utf-8')
    digest = hashlib.sha256((page + '\0' + methodology + '\0' + styles).encode()).hexdigest()[:16]
    run_id = f'{stamp}-{digest}'
    directory = out.parent / 'benchmark-history' / run_id
    # Base keeps navigation and images relative to the docs root. The code page
    # and stylesheet explicitly point to this publication's frozen copies.
    def snapshot(source: str) -> str:
        return (source.replace('<head>', '<head>\n  <base href="../../" />', 1)
                .replace('./methodology.html', f'./benchmark-history/{run_id}/methodology.html')
                .replace('./styles.css', f'./benchmark-history/{run_id}/styles.css'))
    files = {'index.html': snapshot(decorate(page, run_id)),
             'methodology.html': snapshot(methodology).replace(
                 './benchmarks.html', f'./benchmark-history/{run_id}/index.html'),
             'styles.css': styles}
    directory.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        path = directory / name
        if path.exists():
            if path.read_text(encoding='utf-8') != content:
                raise ValueError(f'refusing to overwrite benchmark snapshot: {path}')
        else:
            path.write_text(content, encoding='utf-8')
    entry = {'id': run_id, 'generated': generated, 'commit': meta.get('commit', 'unknown'),
             'url': f'benchmark-history/{run_id}/index.html'}
    return run_id, entry


def publish(out: Path, page: str, methodology: str) -> None:
    """Preserve the outgoing page before replacing the current publication."""
    index = out.parent / 'benchmark-history' / 'index.json'
    history = json.loads(index.read_text(encoding='utf-8')) if index.exists() else {'runs': []}
    entries = {entry['id']: entry for entry in history['runs']}
    code_out = out.parent / 'methodology.html'
    if out.exists() and code_out.exists():
        old_id, old_entry = archive(out, out.read_text(encoding='utf-8'), code_out.read_text(encoding='utf-8'))
        entries[old_id] = old_entry
    run_id, entry = archive(out, page, methodology)
    entries[run_id] = entry
    history = {'latest': run_id, 'runs': sorted(entries.values(), key=lambda e: (e['generated'], e['id']), reverse=True)}
    index.write_text(json.dumps(history, indent=2) + '\n', encoding='utf-8')
    # A script works for file:// previews too, where JSON fetch is blocked.
    index.with_suffix('.js').write_text(
        'window.IBEX_BENCHMARK_HISTORY = ' + json.dumps(history) + ';\n', encoding='utf-8')
    out.write_text(decorate(page, run_id), encoding='utf-8')
    code_out.write_text(methodology, encoding='utf-8')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('page', type=Path)
    args = parser.parse_args()
    publish(args.page, args.page.read_text(encoding='utf-8'),
            args.page.with_name('methodology.html').read_text(encoding='utf-8'))
