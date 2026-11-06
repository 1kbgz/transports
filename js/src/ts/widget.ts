/** The anywidget frontend for `transports.widget(server)` — the widget *is* the connection.
 *
 * anywidget loads this module (`_esm`); `initialize` opens the wire — a `Client` mirroring the
 * kernel's models over the widget's custom-message channel — and `render` shows a live view of every
 * mirrored model. The anywidget wire uses JSON headers and separate binary buffers; it loads the same Rust client state as the main
 * browser package. Edits skip `diff` by sending a single-`Set` proposal from plain path segments.
 *
 * Consumers hook in through DOM events bubbled from the widget element — `transports-change`
 * (detail: the accepted `ReceiveChange`) and `transports-reject` (detail: the server's reject) — or
 * through `el.transports = { client, edit }` for direct access.
 */
import { fromValue, toValue } from "./bridge";
import { Client } from "./client";
import { type BufferHeader } from "./buffers";
import { BufferFrame } from "../../dist/pkg/transports";
import type { PathSeg, ReceiveChange, RejectMsg } from "./client";
import init from "../../dist/pkg/transports";
import wasm from "../../dist/pkg/transports_bg.wasm";

type AnyModel = {
  on(event: string, callback: (...args: unknown[]) => void): void;
  off?(event: string, callback: (...args: unknown[]) => void): void;
  send(
    content: unknown,
    callbacks?: unknown,
    buffers?: ArrayBufferView[],
  ): void;
};

const clients = new WeakMap<AnyModel, Client>();
const disposers = new WeakMap<AnyModel, () => void>();

function clientFor(model: AnyModel): Client {
  let client = clients.get(model);
  if (!client) {
    const created = new Client();
    clients.set(model, created);
    const view = crypto.randomUUID();
    const sender = (wire: string | Uint8Array) => {
      const frame =
        typeof wire === "string"
          ? BufferFrame.from_json(wire)
          : BufferFrame.decode(wire, "msgpack");
      try {
        const buffers = Array.from({ length: frame.count() }, (_, index) =>
          frame.buffer(index),
        );
        if (buffers.length)
          model.send(
            { wire: JSON.parse(frame.header), view },
            undefined,
            buffers,
          );
        else
          model.send({
            wire: typeof wire === "string" ? wire : frame.to_json(),
            view,
          });
      } finally {
        frame.free();
      }
    };
    let heartbeat: ReturnType<typeof setInterval> | undefined;
    const ready = () =>
      model.send({ ready: true, ownership: true, heartbeat: true, view });
    const receive = (content: unknown, buffers: unknown) => {
      const message = content as
        | {
            wire?: string | BufferHeader;
            view?: string;
            heartbeat?: number;
            expired?: boolean;
          }
        | undefined;
      if (message?.view && message.view !== view) return;
      if (message?.expired) {
        clearInterval(heartbeat);
        created.detach(sender);
        created.attach(sender);
        ready();
        return;
      }
      if (typeof message?.heartbeat === "number" && message.heartbeat > 0) {
        clearInterval(heartbeat);
        heartbeat = setInterval(
          () => model.send({ heartbeat: true, view }),
          message.heartbeat * 1000,
        );
      }
      const wire = message?.wire;
      if (typeof wire === "string") created.recv(wire);
      else if (wire)
        created.recvBuffers(wire, (buffers || []) as ArrayBufferView[]);
    };
    model.on("msg:custom", receive);
    created.attach(sender);
    disposers.set(model, () => {
      clearInterval(heartbeat);
      created.detach(sender);
      model.send({ closed: true, view });
      model.off?.("msg:custom", receive);
      clients.delete(model);
      disposers.delete(model);
    });
    ready();
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
  clientFor(model).proposeOps(id, [
    { Set: { path: segments, value: toValue(value) } },
  ]);
}

export default {
  async initialize({ model }: { model: AnyModel }) {
    await init({ module_or_path: wasm });
    clientFor(model);
    return disposers.get(model);
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
