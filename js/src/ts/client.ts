import { codecFor } from "./codecs";
import {
  apply,
  cborToJson,
  diff,
  jsonToCbor,
  jsonToMsgpack,
  msgpackToJson,
} from "./index";

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
    let msg: SnapshotMsg | PatchMsg;
    if (custom) {
      msg = custom.decode(data) as SnapshotMsg | PatchMsg;
    } else if (typeof data === "string") {
      msg = JSON.parse(data);
    } else {
      // binary frame: disambiguate by the connection's codec (msgpack vs cbor)
      msg = JSON.parse(
        this.codec === "cbor" ? cborToJson(data) : msgpackToJson(data),
      );
    }
    if (msg.t === "snapshot") {
      this.values.set(msg.id, msg.value);
      this.revs.set(msg.id, msg.rev);
    } else if (msg.t === "patch") {
      // rev is the model's sequence number; ignore a patch already reflected in the mirror (e.g. one
      // the opening snapshot already captured, which the server then also broadcasts).
      const seen = this.revs.get(msg.id);
      if (seen !== undefined && msg.patch.rev <= seen) return;
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
    if (this.codec === "msgpack") return jsonToMsgpack(s);
    if (this.codec === "cbor") return jsonToCbor(s);
    return s;
  }

  /** Connect to a transports server and mirror it. Returns the `WebSocket`.
   *
   * On a reconnect (this client already mirrors models) it appends `?since=` with its last-seen rev per
   * model, so the server replays only the delta instead of re-sending each whole model.
   */
  connect(url: string): WebSocket {
    const sep = url.includes("?") ? "&" : "?";
    let params = `codec=${this.codec}`;
    if (this.revs.size) {
      const since = encodeURIComponent(
        JSON.stringify(Object.fromEntries(this.revs)),
      );
      params += `&since=${since}`;
    }
    const ws = new WebSocket(`${url}${sep}${params}`);
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
