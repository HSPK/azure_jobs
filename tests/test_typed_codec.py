"""Tagged payload encoding and defensive decoding."""

from __future__ import annotations

from collections import namedtuple
from dataclasses import dataclass

from azure_jobs.shared.contract import typed


def test_registered_dataclass_round_trip() -> None:
    @dataclass
    class Example:
        name: str
        count: int

    typed.register_dataclass(Example)
    decoded = typed.decode(typed.encode(Example("x", 2)))
    assert decoded == Example("x", 2)


def test_nested_collections_are_encoded() -> None:
    encoded = typed.encode({"rows": (1, {2, 3})})
    assert encoded["rows"][0] == 1
    assert sorted(encoded["rows"][1]) == [2, 3]


def test_namedtuple_and_public_object_fields_encode() -> None:
    Row = namedtuple("CodecRow", "name value")

    class Object:
        def __init__(self) -> None:
            self.visible = 1
            self._private = 2

    assert typed.encode(Row("a", 3)) == {"name": "a", "value": 3}
    encoded = typed.encode(Object())
    assert encoded["visible"] == 1
    assert "_private" not in encoded


def test_unhandled_value_falls_back_to_string() -> None:
    class Slots:
        __slots__ = ()

        def __str__(self) -> str:
            return "slots-value"

    assert typed.encode(Slots()) == "slots-value"


def test_unknown_and_failing_decoders_keep_plain_data() -> None:
    assert typed.decode({typed.TAG: "Missing", "value": 1}) == {"value": 1}

    def fail(data):
        raise ValueError("bad decoder")

    typed.register("FailingDecoder", fail)
    assert typed.decode({typed.TAG: "FailingDecoder", "value": 2}) == {"value": 2}
