import { BufferFrame } from "../../dist/pkg/transports";

export type BufferHeader = {
  message: unknown;
  buffer_paths: (string | number)[][];
};

/** Detach native views without copying their underlying bytes. */
export function splitBuffers(message: unknown): {
  header: BufferHeader;
  buffers: Uint8Array[];
} {
  const buffers: Uint8Array[] = [];
  const paths: (string | number)[][] = [];
  function visit(value: unknown, path: (string | number)[]): unknown {
    if (ArrayBuffer.isView(value)) {
      const index = buffers.length;
      buffers.push(
        new Uint8Array(value.buffer, value.byteOffset, value.byteLength),
      );
      paths.push(path);
      return { buffer: index };
    }
    if (Array.isArray(value))
      return value.map((v, i) => visit(v, [...path, i]));
    if (value && typeof value === "object")
      return Object.fromEntries(
        Object.entries(value).map(([key, v]) => [
          key,
          visit(v, [...path, key]),
        ]),
      );
    return value;
  }
  return {
    header: { message: visit(message, []), buffer_paths: paths },
    buffers,
  };
}

/** Reattach buffers as Uint8Array views. Only explicitly listed paths are interpreted. */
export function joinBuffers(
  header: BufferHeader,
  buffers: ArrayBufferView[],
): unknown {
  if (
    !Array.isArray(header.buffer_paths) ||
    header.buffer_paths.length < buffers.length ||
    (header.buffer_paths.length === 0) !== (buffers.length === 0)
  )
    throw new Error("buffer count does not match buffer_paths");
  // Clone the small control object so a malformed frame cannot partially mutate its caller.
  const message = structuredClone(header.message);
  const referenced = new Set<number>();
  for (const path of header.buffer_paths) {
    if (!Array.isArray(path) || !path.length)
      throw new Error("invalid buffer path");
    let parent = message as Record<string | number, unknown>;
    for (const key of path.slice(0, -1)) {
      if (
        !parent ||
        typeof parent !== "object" ||
        !Object.prototype.hasOwnProperty.call(parent, key)
      )
        throw new Error("buffer path not found");
      parent = parent[key] as Record<string | number, unknown>;
    }
    const key = path[path.length - 1];
    const target =
      parent && Object.prototype.hasOwnProperty.call(parent, key)
        ? (parent[key] as { buffer?: unknown })
        : undefined;
    if (
      !target ||
      Object.keys(target).length !== 1 ||
      !Number.isSafeInteger(target.buffer)
    )
      throw new Error("invalid or repeated buffer placeholder");
    const index = target.buffer as number;
    referenced.add(index);
    const buffer = buffers[index];
    if (!ArrayBuffer.isView(buffer))
      throw new Error("expected a binary buffer view");
    Object.defineProperty(parent, key, {
      value: new Uint8Array(
        buffer.buffer,
        buffer.byteOffset,
        buffer.byteLength,
      ),
      enumerable: true,
      configurable: true,
      writable: true,
    });
  }
  if (referenced.size !== buffers.length)
    throw new Error("unreferenced buffers");
  return message;
}

export function decodeBuffers(data: Uint8Array, codec: string): unknown {
  const frame = BufferFrame.decode(data, codec);
  try {
    const buffers = Array.from({ length: frame.count() }, (_, i) =>
      frame.buffer(i),
    );
    return joinBuffers(JSON.parse(frame.header), buffers);
  } finally {
    frame.free();
  }
}

export function encodeBuffers(message: unknown, codec: string): Uint8Array {
  const { header, buffers } = splitBuffers(message);
  const frame = new BufferFrame(JSON.stringify(header));
  try {
    for (const buffer of buffers) frame.push(buffer);
    return frame.encode(codec);
  } finally {
    frame.free();
  }
}
