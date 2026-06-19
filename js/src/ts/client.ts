import { codecFor } from "./codecs";
import { apply, diff, jsonToMsgpack, msgpackToJson } from "./index";

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

  /** Apply an inbound snapshot or patch frame to the mirror.
   *
   * Decodes by the client's codec: a registered custom codec, else built-in JSON (text) / msgpack
   * (binary).
   */
  recv(data: string | Uint8Array): void {
    const custom = codecFor(this.codec);
    const msg = (
      custom
        ? custom.decode(data)
        : JSON.parse(typeof data === "string" ? data : msgpackToJson(data))
    ) as SnapshotMsg | PatchMsg;
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

  /** Propose an edit to a mirrored model; returns the patch frame to send (encoded in this codec).
   *
   * Server-authoritative: the local mirror updates when the server echoes the authoritative patch
   * back via `recv`, not optimistically.
   */
  edit(id: number, value: unknown): string | Uint8Array {
    const patch = JSON.parse(
      diff(JSON.stringify(this.values.get(id)), JSON.stringify(value)),
    );
    const msg = { t: "patch", id, patch };
    const custom = codecFor(this.codec);
    if (custom) return custom.encode(msg);
    const s = JSON.stringify(msg);
    return this.codec === "msgpack" ? jsonToMsgpack(s) : s;
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

  /** Mirror a server over Server-Sent Events (receive-only, JSON). Returns the `EventSource`. */
  connectSSE(url: string): EventSource {
    const es = new EventSource(url);
    es.addEventListener("message", (e) =>
      this.recv((e as MessageEvent).data as string),
    );
    return es;
  }
}
