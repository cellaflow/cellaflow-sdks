import pytest
from cellaflow.serialization import serialize, deserialize


def test_serialization_roundtrip_simple() -> None:
    data = {"key": "value", "count": 42}
    serialized = serialize(data)

    assert isinstance(serialized, bytes)

    deserialized = deserialize(serialized)
    assert deserialized == data


def test_serialization_roundtrip_complex() -> None:
    data = {
        "string": "hello",
        "int": 12345,
        "float": 3.14159,
        "boolean": True,
        "null": None,
        "list": [1, 2, "three", {"nested": "dict"}],
        "dict": {"a": 1, "b": [True, False, None]},
    }
    serialized = serialize(data)
    deserialized = deserialize(serialized)
    assert deserialized == data


def test_serialize_rejects_non_dict() -> None:
    with pytest.raises(TypeError):
        serialize([1, 2, 3])  # type: ignore


def test_deserialize_rejects_non_bytes() -> None:
    with pytest.raises(TypeError):
        deserialize("not bytes")  # type: ignore


def test_deserialize_rejects_non_dict_result() -> None:
    # Serialize a list
    import msgpack

    data = msgpack.packb([1, 2, 3])

    with pytest.raises(ValueError, match="Deserialized data is not a dictionary"):
        deserialize(data)
