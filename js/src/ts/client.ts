import { apply } from "./index";

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
 * Requires the wasm core to be initialized before applying patches.
 */
export class Client {
  private values = new Map<number, unknown>();
  private revs = new Map<number, number>();

  /** Apply an inbound snapshot or patch message to the local mirror. */
  recv(text: string): void {
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
    const ws = new WebSocket(url);
    ws.addEventListener("message", (e) =>
      this.recv(String((e as MessageEvent).data)),
    );
    return ws;
  }
}
