import { test, expect } from "@playwright/test";

test("anywidget frontend mirrors, edits without wasm, and bubbles events", async ({
  page,
}) => {
  await page.goto("http://127.0.0.1:3000/examples/index.html");
  const result = await page.evaluate(async () => {
    const mod = await import("/js/dist/cdn/widget.js");
    const handlers = {};
    const sent = [];
    const model = {
      on: (event, cb) => (handlers[event] = cb),
      send: (content) => sent.push(content),
    };
    const el = document.createElement("div");
    document.body.appendChild(el);

    mod.default.initialize({ model });
    mod.default.render({ model, el });

    const changes = [];
    const rejects = [];
    el.addEventListener("transports-change", (e) => changes.push(e.detail.t));
    el.addEventListener("transports-reject", (e) =>
      rejects.push(e.detail.error),
    );

    handlers["msg:custom"]({
      wire: JSON.stringify({
        t: "snapshot",
        id: 1,
        type: "Device",
        rev: 0,
        value: { Map: { name: { Str: "lamp" } } },
      }),
    });
    handlers["msg:custom"]({
      wire: JSON.stringify({
        t: "patch",
        id: 1,
        patch: {
          rev: 1,
          ops: [{ Set: { path: [{ Key: "name" }], value: { Str: "beacon" } } }],
        },
      }),
    });
    el.transports.edit(1, ["name"], "manual"); // wasm-free single-Set proposal
    handlers["msg:custom"]({
      wire: JSON.stringify({ t: "reject", id: 1, rev: 1, error: "nope" }),
    });

    return {
      sent,
      text: el.querySelector("pre").textContent,
      error: el.children[1].textContent,
      changes,
      rejects,
    };
  });

  expect(result.sent[0]).toEqual({ ready: true }); // the handshake that triggers snapshots
  expect(result.text).toContain("beacon"); // live view reflects the patched mirror
  expect(result.changes).toEqual(["snapshot", "patch"]);
  const edit = JSON.parse(result.sent[1].wire);
  expect(edit.t).toBe("patch");
  expect(edit.patch.ops[0].Set.value).toEqual({ Str: "manual" });
  expect(result.rejects).toEqual(["nope"]);
  expect(result.error).toContain("nope"); // inline error surface
});
