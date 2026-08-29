// Node smoke test for the WASM bridge. Run: node wasm/demo/smoke.mjs
import createIbex from "../../build-wasm/wasm/ibex.mjs";

const mod = await createIbex();

const run = (label, src) => {
  console.log(`\n=== ${label} ===\n${src}`);
  const out = JSON.parse(mod.execute(src));
  console.dir(out, { depth: 4 });
};

run("arithmetic", "2 + 40;");
run("seed demo tables", 'import data_gen;\nseed_rng(20240115);\nlet trades = gen_ticks(2000, "AAPL,MSFT,GOOG");\nlet reference = gen_reference("AAPL,MSFT,GOOG");');
run("aggregate by group", "trades[select { avg_price = mean(price), n = count() }, by symbol];");
run("filter + head", "trades[filter price > 100, head 3];");
run("semantic error", "trades[select { nope }];");
