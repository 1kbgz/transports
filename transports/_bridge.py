"""Bridge Python models <-> the core `Value` wire form, derive core schemas, and emit TS types.

Supports three model kinds with one contract — **pydantic** models, stdlib **dataclasses**, and
**msgspec.Struct**s:

- `to_value` / `from_value` convert an instance ⇄ the core `Value` (the externally-tagged enum the
  Rust core speaks: `{"Bool": true}`, `{"Int": 3}`, `{"Str": "x"}`, `"Null"`, `{"List": [...]}`,
  `{"Map": {...}}`). We obtain JSON-native python (pydantic `model_dump(mode="json")`, else via
  `msgspec`) and tag it; the inverse strips tags and lets the model library validate/coerce.
- `schema_of` derives a core `Schema` (type_name + fields) from the model class.
- `schema_to_ts` renders that schema as a TypeScript interface — one schema, both languages.

Nested models are inlined as `Map`s; `Submodel`-by-id references are a later refinement.
"""

import dataclasses
from types import UnionType
from typing import Any, Type, TypeVar, Union, cast, get_args, get_origin, get_type_hints

import msgspec
from pydantic import BaseModel

M = TypeVar("M")


def _value_of(v: Any) -> Any:
    if v is None:
        return "Null"
    if isinstance(v, bool):
        return {"Bool": v}
    if isinstance(v, int):
        return {"Int": v}
    if isinstance(v, float):
        return {"Float": v}
    if isinstance(v, str):
        return {"Str": v}
    if isinstance(v, list):
        return {"List": [_value_of(x) for x in v]}
    if isinstance(v, dict):
        return {"Map": {str(k): _value_of(x) for k, x in v.items()}}
    raise TypeError(f"unsupported value type for transports: {type(v)!r}")


def _py_of(value: Any) -> Any:
    if value == "Null":
        return None
    ((tag, inner),) = value.items()
    if tag in ("Bool", "Int", "Float", "Str", "Submodel"):
        return inner
    if tag == "List":
        return [_py_of(x) for x in inner]
    if tag == "Map":
        return {k: _py_of(x) for k, x in inner.items()}
    raise ValueError(f"unrecognized tagged value: {value!r}")


def to_value(model: Any) -> Any:
    """A pydantic / dataclass / msgspec model as the core `Value` (a tagged `Map`)."""
    if isinstance(model, BaseModel):
        plain = model.model_dump(mode="json")
    else:
        # dataclasses and msgspec.Structs both encode through msgspec (JSON-native: datetimes,
        # enums, etc. are normalized).
        plain = msgspec.json.decode(msgspec.json.encode(model))
    return _value_of(plain)


def from_value(value: Any, cls: Type[M]) -> M:
    """Reconstruct a model of type `cls` from a core `Value`."""
    plain = _py_of(value)
    if isinstance(cls, type) and issubclass(cls, BaseModel):
        return cast(M, cls.model_validate(plain))
    return msgspec.convert(plain, cls)  # dataclass or msgspec.Struct


def _annotations(cls: type) -> dict:
    if issubclass(cls, BaseModel):
        return {name: field.annotation for name, field in cls.model_fields.items()}
    if issubclass(cls, msgspec.Struct):
        return {f.name: f.type for f in msgspec.structs.fields(cls)}
    if dataclasses.is_dataclass(cls):
        hints = get_type_hints(cls)
        return {f.name: hints.get(f.name, f.type) for f in dataclasses.fields(cls)}
    raise TypeError(f"not a supported model class: {cls!r}")


def _is_nested_model(ann: Any) -> bool:
    return isinstance(ann, type) and (issubclass(ann, BaseModel) or dataclasses.is_dataclass(ann) or issubclass(ann, msgspec.Struct))


def _field_type(ann: Any) -> str:
    origin = get_origin(ann)
    if origin is Union or origin is UnionType:
        members = [a for a in get_args(ann) if a is not type(None)]
        if members:
            return _field_type(members[0])
    if ann is bool:
        return "Bool"
    if ann is int:
        return "Int"
    if ann is float:
        return "Float"
    if ann is str:
        return "Str"
    if ann in (list, tuple, set, frozenset) or origin in (list, tuple, set, frozenset):
        return "List"
    if ann is dict or origin is dict:
        return "Map"
    if _is_nested_model(ann):
        return "Map"  # nested models inlined as Maps; Submodel-by-id is a later refinement
    return "Any"


def schema_of(cls: type) -> dict:
    """Derive a core `Schema` (type_name + fields) from a pydantic / dataclass / msgspec class."""
    return {
        "type_name": cls.__name__,
        "fields": [{"name": name, "ty": _field_type(ann)} for name, ann in _annotations(cls).items()],
    }


_TS_TYPES = {
    "Bool": "boolean",
    "Int": "number",
    "Float": "number",
    "Str": "string",
    "List": "unknown[]",
    "Map": "Record<string, unknown>",
    "Submodel": "unknown",
    "Any": "unknown",
}


def schema_to_ts(schema: dict) -> str:
    """Render a core `Schema` as a TypeScript interface declaration."""
    lines = [f"export interface {schema['type_name']} {{"]
    for field in schema["fields"]:
        lines.append(f"  {field['name']}: {_TS_TYPES.get(field['ty'], 'unknown')};")
    lines.append("}")
    return "\n".join(lines)
