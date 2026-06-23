/** Custom wire codec registry (the JS analog of `transports.register_codec` in Python).
 *
 * A codec turns a JSON-able object (a protocol message, or a model `Value`) into a wire frame
 * (`string` or `Uint8Array`) and back. Register a matching implementation here for any content type
 * you also register on the server, then use it via `new Client(contentType)` / `?codec=`.
 */

export type CodecEncode = (obj: unknown) => string | Uint8Array;
export type CodecDecode = (data: string | Uint8Array) => unknown;

const BUILTIN = new Set([
  "",
  "json",
  "application/json",
  "msgpack",
  "application/msgpack",
  "x-msgpack",
  "application/x-msgpack",
  "cbor",
  "application/cbor",
]);

const registry = new Map<
  string,
  { encode: CodecEncode; decode: CodecDecode }
>();

/** Register a custom codec under `contentType`. The built-in json/msgpack codecs cannot be overridden. */
export function registerCodec(
  contentType: string,
  encode: CodecEncode,
  decode: CodecDecode,
): void {
  if (BUILTIN.has(contentType)) {
    throw new Error(`cannot override built-in codec: ${contentType}`);
  }
  registry.set(contentType, { encode, decode });
}

/** Remove a previously registered custom codec. */
export function unregisterCodec(contentType: string): void {
  registry.delete(contentType);
}

/** The currently registered custom codec for `contentType`, if any. */
export function codecFor(
  contentType: string,
): { encode: CodecEncode; decode: CodecDecode } | undefined {
  return registry.get(contentType);
}
