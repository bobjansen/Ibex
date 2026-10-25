// SPDX-License-Identifier: AGPL-3.0-only
// Run: node benchmarking/test_website_history.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const docs = path.resolve(__dirname, '../docs');

async function check(root, archived, fail = false) {
  const manifest = JSON.parse(fs.readFileSync(path.join(docs, 'benchmark-history/index.json')));
  const nodes = {
    'benchmark-history': {dataset: {run: manifest.latest}},
    'benchmark-run': {options: [], replaceChildren(...xs) {this.options = xs;}, add(x) {this.options.push(x);}, addEventListener(_, fn) {this.change = fn;}},
    'benchmark-permalink': {}, 'benchmark-history-status': {}
  };
  const location = {href: root + (archived ? manifest.runs[0].url : 'benchmarks.html'), hash: '#h2o', assign(value) {this.target = value;}};
  const context = {URL, window: {}, location,
    Option: function(text, value) {this.text = text; this.value = value;},
    document: {
      currentScript: {src: root + 'benchmark-history.js'},
      getElementById(id) {return nodes[id];},
      createElement(tag) {assert.equal(tag, 'script'); return {};},
      head: {append(script) {
        assert.equal(script.src, root + 'benchmark-history/index.js');
        if (fail) script.onerror(new Error('Missing script'));
        else {
          vm.runInNewContext(fs.readFileSync(path.join(docs, 'benchmark-history/index.js'), 'utf8'), context);
          script.onload();
        }
      }}
    }
  };
  vm.runInNewContext(fs.readFileSync(path.join(docs, 'benchmark-history.js'), 'utf8'), context);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(nodes['benchmark-permalink'].hidden, false);
  const select = nodes['benchmark-run'];
  if (fail) assert.ok(nodes['benchmark-history-status'].textContent);
  else {
    assert.equal(select.options[archived ? 1 : 0].selected, true);
    assert.equal(select.disabled, false);
    select.value = select.options[1].value;
    select.change();
    assert.equal(location.target, root + manifest.runs[0].url + '#h2o');
  }
}
(async () => {
  for (const root of ['file:///tmp/docs/', 'https://example.org/project/']) {
    await check(root, false);
    await check(root, true);
    await check(root, false, true);
  }
  console.log('History viewer checks passed for local files and HTTP.');
})().catch(error => {console.error(error); process.exitCode = 1;});
