import { apply, msgpackToJson } from "./index";

type SnapshotMsg = {
  t: "snapshot";
  id: number;
  type: string;
  rev: number;
  value: unknown;
};
type PatchMsg = {
  t: "patch";
  id: number;
  patch: { rev: number; ops: unknown[] };
};

/** Mirrors a remote transports `Session` from connection messages.
 *
 * Inbound frames are decoded by type — text frames are JSON, binary frames are MessagePack — so a
 * client transparently mirrors a server regardless of the negotiated codec. Requires the wasm core
 * to be initialized before applying patches.
 */
export class Client {
  private values = new Map<number, unknown>();
  private revs = new Map<number, number>();

  constructor(private codec: string = "json") {}

  /** Apply an inbound snapshot or patch frame (string JSON or binary msgpack) to the mirror. */
  recv(data: string | Uint8Array): void {
    const text = typeof data === "string" ? data : msgpackToJson(data);
    const msg = JSON.parse(text) as SnapshotMsg | PatchMsg;
    if (msg.t === "snapshot") {
      this.values.set(msg.id, msg.value);
      this.revs.set(msg.id, msg.rev);
    } else if (msg.t === "patch") {
      const cur = JSON.stringify(this.values.get(msg.id));
      this.values.set(
        msg.id,
        JSON.parse(apply(cur, JSON.stringify(msg.patch))),
      );
      this.revs.set(msg.id, msg.patch.rev);
    }
  }

  /** The current mirrored core `Value` of a model. */
  value(id: number): unknown {
    return this.values.get(id);
  }

  ids(): number[] {
    return [...this.values.keys()];
  }

  /** Connect to a transports server and mirror it. Returns the `WebSocket`. */
  connect(url: string): WebSocket {
    const sep = url.includes("?") ? "&" : "?";
    const ws = new WebSocket(`${url}${sep}codec=${this.codec}`);
    ws.binaryType = "arraybuffer";
    ws.addEventListener("message", (e) => {
      const data = (e as MessageEvent).data;
      this.recv(
        typeof data === "string" ? data : new Uint8Array(data as ArrayBuffer),
      );
    });
    return ws;
  }
}
