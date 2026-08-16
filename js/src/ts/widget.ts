/** The anywidget frontend for `transports.widget(server)` — the widget *is* the connection.
 *
 * anywidget loads this module (`_esm`); `initialize` opens the wire — a `Client` mirroring the
 * kernel's models over the widget's custom-message channel — and `render` shows a live view of every
 * mirrored model. The anywidget wire is JSON-only and mirroring is pure TS, so no wasm is fetched;
 * even edits skip `diff` by sending a single-`Set` proposal built from plain path segments.
 *
 * Consumers hook in through DOM events bubbled from the widget element — `transports-change`
 * (detail: the accepted `ReceiveChange`) and `transports-reject` (detail: the server's reject) — or
 * through `el.transports = { client, edit }` for direct access.
 */
import { fromValue, toValue } from "./bridge";
import { Client } from "./client";
import type { PathSeg, ReceiveChange, RejectMsg } from "./client";

type AnyModel = {
  on(event: string, callback: (...args: unknown[]) => void): void;
  send(content: unknown): void;
};

const clients = new WeakMap<AnyModel, Client>();

function clientFor(model: AnyModel): Client {
  let client = clients.get(model);
  if (!client) {
    const created = new Client(); // the anywidget wire carries JSON only
    clients.set(model, created);
    model.on("msg:custom", (content: unknown) => {
      const wire = (content as { wire?: string } | undefined)?.wire;
      if (wire !== undefined) created.recv(wire);
    });
    model.send({ ready: true }); // the kernel sends the opening snapshots once a frontend listens
    client = created;
  }
  return client;
}

/** A single-`Set` proposal from plain path segments — no wasm `diff` needed in the frontend. */
function sendEdit(
  model: AnyModel,
  id: number,
  path: (string | number)[],
  value: unknown,
): void {
  const segments: PathSeg[] = path.map((p) =>
    typeof p === "number" ? { Index: p } : { Key: p },
  );
  const patch = {
    rev: 0, // a proposal's rev is ignored; the server owns rev
    ops: [{ Set: { path: segments, value: toValue(value) } }],
  };
  model.send({ wire: JSON.stringify({ t: "patch", id, patch }) });
}

export default {
  initialize({ model }: { model: AnyModel }) {
    clientFor(model);
  },

  render({ model, el }: { model: AnyModel; el: HTMLElement }) {
    const client = clientFor(model);
    const container = document.createElement("div");
    const error = document.createElement("div");
    error.setAttribute(
      "style",
      "color: #b91c1c; font-size: 0.85em; min-height: 1em;",
    );
    el.appendChild(container);
    el.appendChild(error);

    const views = new Map<number, HTMLElement>();
    const update = (id: number) => {
      let pre = views.get(id);
      if (!pre) {
        pre = document.createElement("pre");
        pre.setAttribute("style", "margin: 0.25em 0;");
        views.set(id, pre);
        container.appendChild(pre);
      }
      pre.textContent = JSON.stringify(
        fromValue(client.value(id) as never),
        null,
        2,
      );
    };
    for (const id of client.ids()) update(id);

    const offChange = client.onChange((change: ReceiveChange) => {
      error.textContent = "";
      update(change.id);
      el.dispatchEvent(
        new CustomEvent("transports-change", { detail: change, bubbles: true }),
      );
    });
    const offReject = client.onReject((reject: RejectMsg) => {
      error.textContent = `rejected: ${reject.error}`;
      el.dispatchEvent(
        new CustomEvent("transports-reject", { detail: reject, bubbles: true }),
      );
    });

    (el as HTMLElement & { transports?: unknown }).transports = {
      client,
      edit: (id: number, path: (string | number)[], value: unknown) =>
        sendEdit(model, id, path, value),
    };
    return () => {
      offChange();
      offReject();
    };
  },
};
