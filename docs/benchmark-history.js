// SPDX-License-Identifier: AGPL-3.0-only
(() => {
  const script = document.currentScript;
  const root = new URL('./', script.src);
  const container = document.getElementById('benchmark-history');
  if (!container) return;
  const select = document.getElementById('benchmark-run');
  const link = document.getElementById('benchmark-permalink');
  const current = container.dataset.run;
  // The permalink remains usable even when loading the history fails.
  link.href = new URL(`benchmark-history/${current}/index.html`, root);
  link.hidden = false;
  new Promise((resolve, reject) => {
    const dataScript = document.createElement('script');
    dataScript.src = new URL('benchmark-history/index.js', root).href;
    dataScript.onload = () => {
      if (window.IBEX_BENCHMARK_HISTORY) resolve(window.IBEX_BENCHMARK_HISTORY);
      else reject(new Error('History unavailable'));
    };
    dataScript.onerror = reject;
    document.head.append(dataScript);
  })
    .then(history => {
      select.replaceChildren();
      const latest = new Option('Latest publication', new URL('benchmarks.html', root).href);
      select.add(latest);
      const isLatestPage = new URL(location.href).pathname === new URL('benchmarks.html', root).pathname;
      for (const run of history.runs) {
        const label = `${run.generated} · ${run.commit}${run.id === history.latest ? ' (latest)' : ''}`;
        const option = new Option(label, new URL(run.url, root).href);
        option.selected = !isLatestPage && run.id === current;
        select.add(option);
      }
      if (isLatestPage) latest.selected = true;
      select.disabled = false;
      select.addEventListener('change', () => {
        const target = new URL(select.value);
        target.hash = location.hash;
        location.assign(target.href);
      });
    })
    .catch(() => {
      select.replaceChildren(new Option('History unavailable', ''));
      document.getElementById('benchmark-history-status').textContent = 'Could not load publication history.';
    });
})();
