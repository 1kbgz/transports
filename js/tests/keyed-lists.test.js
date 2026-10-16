import fs from "node:fs";
import { test, expect } from "@playwright/test";
import { initSync } from "../dist/pkg/transports";
import { diff, apply, toValue, Store } from "../src/ts/index";

const cases = JSON.parse(
  fs.readFileSync("../rust/tests/fixtures/keyed_lists.json", "utf8"),
);
const rules = JSON.stringify([
  { path: [{ Key: "rows" }], key: [{ Key: "id" }] },
]);
test.beforeAll(() =>
  initSync({ module: fs.readFileSync("dist/pkg/transports_bg.wasm") }),
);
for (const fixture of cases) {
  test(fixture.name, () => {
    const old = JSON.stringify(toValue(fixture.old));
    const next = JSON.stringify(toValue(fixture.new));
    const patch = diff(old, next, rules);
    expect(JSON.parse(apply(old, patch))).toEqual(JSON.parse(next));
    if (fixture.fallback) expect(patch).toBe(diff(old, next));
    else
      expect(JSON.parse(patch).ops.map((op) => Object.keys(op)[0])).toEqual(
        fixture.kinds,
      );
    const store = new Store();
    try {
      const id = store.host("Rows", old, rules);
      expect(JSON.parse(store.mutate(id, next)).ops).toEqual(
        JSON.parse(patch).ops,
      );
    } finally {
      store.free();
    }
  });
}
test("duplicate policy paths fail before hosting", () => {
  const invalid = JSON.stringify([...JSON.parse(rules), ...JSON.parse(rules)]);
  expect(() => diff('"Null"', '"Null"', invalid)).toThrow(
    "duplicate list_keys path",
  );
});
