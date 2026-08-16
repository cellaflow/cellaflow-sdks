import msgpack
from typing import Any, Dict


def serialize(data: Dict[str, Any]) -> bytes:
    """
    Serializes a Python dictionary to a MessagePack byte string.
    This ensures we never use JSON for state payloads, mitigating RCE risks.
    """
    if not isinstance(data, dict):
        raise TypeError(f"Expected a dictionary for serialization, got {type(data)}")
    # We use strict_types=True for tighter encoding and use_bin_type=True
    import typing

    return typing.cast(bytes, msgpack.packb(data, use_bin_type=True, strict_types=True))


def deserialize(data: bytes) -> Dict[str, Any]:
    """
    Deserializes a MessagePack byte string back into a Python dictionary.
    """
    if not isinstance(data, bytes):
        raise TypeError(f"Expected bytes for deserialization, got {type(data)}")

    # We use strict_map_key=False to allow robust unpacking, but raw=False to
    # decode strings as utf-8 strings instead of bytes.
    unpacked = msgpack.unpackb(data, raw=False, strict_map_key=False)

    if not isinstance(unpacked, dict):
        raise ValueError(f"Deserialized data is not a dictionary, got {type(unpacked)}")
    return unpacked
