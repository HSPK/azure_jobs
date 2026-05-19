"""Utilities for working with dataclasses, particularly serialization/deserialization."""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from typing import Any, get_args, get_origin, get_type_hints


def remove_empty_values(d: dict[str, Any]) -> dict[str, Any]:
    """Recursively remove empty/default values from dict for cleaner JSON output.

    Removes None, empty strings, and zero integers. Nested dicts are recursively
    cleaned, and empty nested dicts are themselves removed.

    Args:
        d: Dictionary to clean.

    Returns:
        Cleaned dictionary with empty/default values removed.
    """
    result = {}
    for k, v in d.items():
        if isinstance(v, dict):
            cleaned = remove_empty_values(v)
            if cleaned:
                result[k] = cleaned
        elif v is None or v == "" or (isinstance(v, int) and v == 0):
            continue  # Skip None, empty strings, and zero
        else:
            result[k] = v
    return result


def dataclass_from_dict(dc_type: type, data: dict[str, Any]) -> Any:
    """Recursively construct a dataclass from a dict, handling nested dataclasses.

    Automatically resolves field types using type hints and handles nested
    dataclasses by recursively constructing them from dict values. Supports
    Optional and Union types by unwrapping them to find the actual type.

    Args:
        dc_type: The dataclass type to construct.
        data: Dictionary with field values.

    Returns:
        Instance of dc_type constructed from data, or data itself if dc_type
        is not a dataclass.

    Example:
        >>> @dataclass
        ... class Inner:
        ...     value: str = ""
        ...
        >>> @dataclass
        ... class Outer:
        ...     inner: Inner = field(default_factory=Inner)
        ...
        >>> result = dataclass_from_dict(Outer, {"inner": {"value": "hello"}})
        >>> result.inner.value
        'hello'
    """
    if not is_dataclass(dc_type):
        return data

    # Get type hints for better type resolution (handles Optional, Union, etc.)
    hints = get_type_hints(dc_type)
    field_values = {}

    for f in fields(dc_type):
        value = data.get(f.name)
        if value is None:
            continue

        # Get the actual field type from hints
        field_type = hints.get(f.name, f.type)

        # Try to unwrap Optional, Union, and other generic types
        origin_type = get_origin(field_type)
        args = get_args(field_type)

        if (
            origin_type is list
            and args
            and isinstance(args[0], type)
            and is_dataclass(args[0])
        ):
            # list[SomeDataclass]: recurse into each element
            item_type = args[0]
            field_values[f.name] = [
                dataclass_from_dict(item_type, item) if isinstance(item, dict) else item
                for item in value
            ]
        elif origin_type is not None:
            # Unwrap Optional/Union to get first concrete type
            if args and isinstance(args[0], type):
                field_type = args[0]
            # If the unwrapped type is a dataclass and value is dict, recurse
            if is_dataclass(field_type) and isinstance(value, dict):
                field_values[f.name] = dataclass_from_dict(field_type, value)
            else:
                field_values[f.name] = value
        elif (
            isinstance(field_type, type)
            and is_dataclass(field_type)
            and isinstance(value, dict)
        ):
            # Plain (non-generic) nested dataclass
            field_values[f.name] = dataclass_from_dict(field_type, value)
        else:
            field_values[f.name] = value

    return dc_type(**field_values)
