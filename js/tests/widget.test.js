import { test, expect } from "@playwright/test";

test("anywidget frontend mirrors through shared wasm state and bubbles events", async ({
  page,
}) => {
  await page.goto("http://127.0.0.1:3000/examples/index.html");
  const result = await page.evaluate(async () => {
    const source = await fetch("/js/dist/cdn/widget.js").then((response) =>
      response.text(),
    );
    const url = URL.createObjectURL(
      new Blob([source], { type: "text/javascript" }),
    );
    const mod = await import(url);
    URL.revokeObjectURL(url);
    const handlers = {};
    const sent = [];
    const model = {
      on: (event, cb) => (handlers[event] = cb),
      send: (content) => sent.push(content),
    };
    const el = document.createElement("div");
    document.body.appendChild(el);

    await mod.default.initialize({ model });
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

  expect(result.sent[0]).toMatchObject({
    ready: true,
    ownership: true,
    view: expect.any(String),
  }); // the handshake that triggers snapshots
  expect(result.text).toContain("beacon"); // live view reflects the patched mirror
  expect(result.changes).toEqual(["snapshot", "patch"]);
  const edit = JSON.parse(result.sent[1].wire);
  expect(edit.t).toBe("patch");
  expect(edit.patch.ops[0].Set.value).toEqual({ Str: "manual" });
  expect(result.rejects).toEqual(["nope"]);
  expect(result.error).toContain("nope"); // inline error surface
});

test("anywidget heartbeat recovers expired ownership and stops on disposal", async ({
  page,
}) => {
  await page.goto("http://127.0.0.1:3000/examples/index.html");
  const result = await page.evaluate(async () => {
    const source = await fetch("/js/dist/cdn/widget.js").then((r) => r.text());
    const url = URL.createObjectURL(
      new Blob([source], { type: "text/javascript" }),
    );
    const mod = await import(url);
    URL.revokeObjectURL(url);
    const sent = [];
    const handlers = {};
    const model = {
      on: (name, cb) => (handlers[name] = cb),
      off: (name) => delete handlers[name],
      send: (message) => sent.push(message),
    };
    const dispose = await mod.default.initialize({ model });
    const view = sent[0].view;
    handlers["msg:custom"]({ view, heartbeat: 0.01 });
    await new Promise((resolve) => setTimeout(resolve, 35));
    const heartbeats = sent.filter((m) => m.heartbeat && !m.ready).length;
    handlers["msg:custom"]({ view, expired: true });
    const ready = sent.filter((m) => m.ready).length;
    dispose();
    const count = sent.length;
    await new Promise((resolve) => setTimeout(resolve, 35));
    return { heartbeats, ready, count, after: sent.length, last: sent.at(-1) };
  });
  expect(result.heartbeats).toBeGreaterThan(0);
  expect(result.ready).toBe(2);
  expect(result.after).toBe(result.count);
  expect(result.last.closed).toBe(true);
});
