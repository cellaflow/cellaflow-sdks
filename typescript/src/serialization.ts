import { encode, decode } from "@msgpack/msgpack";

/**
 * Serializes a JavaScript object to a MessagePack byte array.
 * This ensures we never use JSON for state payloads, mitigating RCE risks.
 *
 * @throws {TypeError} If `data` is not a plain, non-null object.
 */
export function serialize(data: Record<string, any>): Uint8Array {
  // Explicit null check first — `typeof null === "object"` would otherwise
  // produce a misleading error message reporting "got object" instead of "got null".
  if (data === null) {
    throw new TypeError("Expected an object for serialization, got null");
  }
  if (typeof data !== "object" || Array.isArray(data)) {
    throw new TypeError(
      `Expected an object for serialization, got ${Array.isArray(data) ? "array" : typeof data}`
    );
  }
  return encode(data);
}

/**
 * Deserializes a MessagePack byte array back into a JavaScript object.
 *
 * Uses `ArrayBuffer.isView()` instead of `instanceof Uint8Array` to work
 * correctly across JS realms (worker threads, vm contexts, etc.).
 *
 * @throws {TypeError} If `data` is not a Uint8Array / ArrayBufferView.
 * @throws {TypeError} If the deserialized value is not a plain object.
 */
export function deserialize(data: Uint8Array): Record<string, any> {
  // ArrayBuffer.isView is cross-realm safe; instanceof Uint8Array is not.
  if (!ArrayBuffer.isView(data)) {
    throw new TypeError(
      `Expected Uint8Array for deserialization, got ${data === null ? "null" : typeof data}`
    );
  }

  const unpacked = decode(data);

  if (
    typeof unpacked !== "object" ||
    unpacked === null ||
    Array.isArray(unpacked)
  ) {
    throw new TypeError(
      `Deserialized data is not an object, got ${unpacked === null ? "null" : typeof unpacked}`
    );
  }

  return unpacked as Record<string, any>;
}
