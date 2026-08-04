"""Utilities for working with dataclasses."""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from typing import Any, get_args, get_origin, get_type_hints

def remove_empty_values(d: dict[str, Any]) -> dict[str, Any]:
    """Recursively remove empty/default values from dict for cleaner JSON output."""
    result = {}
    for k, v in d.items():
        if isinstance(v, dict):
            cleaned = remove_empty_values(v)
            if cleaned:
                result[k] = cleaned
        elif v is None or v == "" or (isinstance(v, int) and v == 0):
            continue
        else:
            result[k] = v
    return result

def dataclass_from_dict(dc_type: type, data: dict[str, Any]) -> Any:
    """Recursively construct a dataclass from a dict, handling nested dataclasses."""
    if not is_dataclass(dc_type):
        return data

    hints = get_type_hints(dc_type)
    field_values = {}

    for f in fields(dc_type):
        value = data.get(f.name)
        if value is None:
            continue

        field_type = hints.get(f.name, f.type)

        origin_type = get_origin(field_type)
        args = get_args(field_type)

        if (
            origin_type is list
            and args
            and isinstance(args[0], type)
            and is_dataclass(args[0])
        ):
            item_type = args[0]
            field_values[f.name] = [
                dataclass_from_dict(item_type, item) if isinstance(item, dict) else item
                for item in value
            ]
        elif origin_type is not None:
            if args and isinstance(args[0], type):
                field_type = args[0]
            if is_dataclass(field_type) and isinstance(value, dict):
                field_values[f.name] = dataclass_from_dict(field_type, value)
            else:
                field_values[f.name] = value
        elif (
            isinstance(field_type, type)
            and is_dataclass(field_type)
            and isinstance(value, dict)
        ):
            field_values[f.name] = dataclass_from_dict(field_type, value)
        else:
            field_values[f.name] = value

    return dc_type(**field_values)
