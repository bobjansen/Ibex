// Node smoke test for the WASM bridge. Run: node wasm/demo/smoke.mjs
import createIbex from "../../build-wasm/wasm/ibex.mjs";
import assert from "node:assert/strict";

const mod = await createIbex();

const run = (label, src) => {
  console.log(`\n=== ${label} ===\n${src}`);
  const out = JSON.parse(mod.execute(src));
  console.dir(out, { depth: 4 });
  assert.equal(out.ok, label !== "semantic error", out.error);
  return out;
};

run("arithmetic", "2 + 40;");
run("seed demo tables", 'import data_gen;\nseed_rng(20240115);\nlet trades = gen_ticks(2000, "AAPL,MSFT,GOOG");');
run("aggregate by group", "trades[select { avg_price = mean(price), n = count() }, by symbol];");
run("filter + head", "trades[filter price > 100, head 3];");
run("semantic error", "trades[select { nope }];");

const scalar = run("decimal scalar", 'decimal"-12.30";');
assert.equal(scalar.scalar, "-12.30");
const decimals = run("decimal table", `Table {
  amount = [decimal"123456789012345678901234567890123456.78", decimal"-12.30"]
};`);
assert.deepEqual(decimals.results[0].columns, [{ name: "amount", type: "Decimal(38, 2)" }]);
assert.deepEqual(decimals.results[0].rows, [["123456789012345678901234567890123456.78"], ["-12.30"]]);
