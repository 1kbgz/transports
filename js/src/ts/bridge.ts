/** The core `Value` wire form: the externally-tagged enum the Rust core speaks. */
export type Value =
  | "Null"
  | { Bool: boolean }
  | { Int: number }
  | { Float: number }
  | { Str: string }
  | { List: Value[] }
  | { Map: { [k: string]: Value } }
  | { Submodel: number };

/**
 * Convert a plain JS value to the core `Value` wire form.
 *
 * Note: JS has a single number type, so integers and whole-valued floats both encode as `Int`.
 */
export function toValue(v: unknown): Value {
  if (v === null || v === undefined) return "Null";
  if (typeof v === "boolean") return { Bool: v };
  if (typeof v === "number")
    return Number.isInteger(v) ? { Int: v } : { Float: v };
  if (typeof v === "string") return { Str: v };
  if (Array.isArray(v)) return { List: v.map(toValue) };
  if (typeof v === "object") {
    const m: { [k: string]: Value } = {};
    for (const [k, x] of Object.entries(v as object)) m[k] = toValue(x);
    return { Map: m };
  }
  throw new TypeError(`unsupported value for transports: ${typeof v}`);
}

/** Convert a core `Value` back to a plain JS value. */
export function fromValue(value: Value): unknown {
  if (value === "Null") return null;
  const [tag, inner] = Object.entries(value)[0];
  switch (tag) {
    case "Bool":
    case "Int":
    case "Float":
    case "Str":
    case "Submodel":
      return inner;
    case "List":
      return (inner as Value[]).map(fromValue);
    case "Map": {
      const o: { [k: string]: unknown } = {};
      for (const [k, x] of Object.entries(inner as { [k: string]: Value }))
        o[k] = fromValue(x);
      return o;
    }
    default:
      throw new Error(`unrecognized tagged value: ${JSON.stringify(value)}`);
  }
}
