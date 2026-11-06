"""Bridge Python models <-> the core `Value` wire form, derive core schemas, and emit TS types.

Supports three model kinds with one contract — **pydantic** models, stdlib **dataclasses**, and
**msgspec.Struct**s:

- `to_value` / `from_value` convert an instance ⇄ the core `Value` (the externally-tagged enum the
  Rust core speaks: `{"Bool": true}`, `{"Int": 3}`, `{"Str": "x"}`, `"Null"`, `{"List": [...]}`,
  `{"Map": {...}}`). We obtain JSON-native python (pydantic `model_dump(mode="json")`, else via
  `msgspec`) and tag it, preserving native bytes when present. The inverse strips tags and lets
  the model library validate/coerce.
- `schema_of` derives a core `Schema` (type_name + fields) from the model class.
- `schema_to_ts` renders that schema as a TypeScript interface — one schema, both languages.

Nested models are inlined as `Map`s; `Submodel`-by-id references are a later refinement.
"""

import base64
import dataclasses
import inspect
import math
from copy import deepcopy
from functools import lru_cache
from types import UnionType
from typing import Annotated, Any, TypeVar, Union, cast, get_args, get_origin, get_type_hints

import msgspec
from pydantic import BaseModel
from pydantic_core import SchemaSerializer, core_schema, to_jsonable_python

M = TypeVar("M")

# pydantic 2.13+: dump polymorphically so a subclass instance in a base-annotated field keeps its
# fields on the wire — the mirror sees the hosted instance, not its annotation
_DUMP_KWARGS: dict = {"mode": "json"}
if "polymorphic_serialization" in inspect.signature(BaseModel.model_dump).parameters:
    _DUMP_KWARGS["polymorphic_serialization"] = True


def _contains_bytes(value: Any) -> bool:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return True
    if isinstance(value, BaseModel):
        return _contains_bytes(value.__dict__)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return any(_contains_bytes(getattr(value, field.name)) for field in dataclasses.fields(value))
    if isinstance(value, msgspec.Struct):
        return any(_contains_bytes(getattr(value, field.name)) for field in msgspec.structs.fields(type(value)))
    if isinstance(value, dict):
        return any(_contains_bytes(item) for item in value.values())
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(_contains_bytes(item) for item in value)
    return False


def _value_of(v: Any) -> Any:
    if v is None:
        return "Null"
    if isinstance(v, bool):
        return {"Bool": v}
    if isinstance(v, int):
        return {"Int": v}
    if isinstance(v, float):
        return {"Float": v} if math.isfinite(v) else "Null"
    if isinstance(v, str):
        return {"Str": v}
    if isinstance(v, (bytes, bytearray, memoryview)):
        return {"Bytes": base64.b64encode(bytes(v)).decode("ascii")}
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
    if tag == "Bytes":
        return base64.b64decode(inner, validate=True)
    if tag == "List":
        return [_py_of(x) for x in inner]
    if tag == "Map":
        return {k: _py_of(x) for k, x in inner.items()}
    raise ValueError(f"unrecognized tagged value: {value!r}")


def _restore_bytes(native: Any, serialized: Any) -> Any:
    if (
        isinstance(native, (bytes, bytearray, memoryview))
        and isinstance(serialized, str)
        and serialized == base64.urlsafe_b64encode(bytes(native)).decode("ascii")
    ):
        return bytes(native)
    if isinstance(native, dict) and isinstance(serialized, dict):
        keys = {
            key if isinstance(key, str) else next(iter(to_jsonable_python({key: None}, bytes_mode="base64"))): value for key, value in native.items()
        }
        return {key: _restore_bytes(keys.get(key), value) for key, value in serialized.items()}
    if isinstance(native, (list, tuple, set, frozenset)) and isinstance(serialized, list) and len(native) == len(serialized):
        return [_restore_bytes(raw, value) for raw, value in zip(native, serialized)]
    return serialized


def _serialize_any(value: Any, handler: Any) -> Any:
    if isinstance(value, BaseModel):
        return _json_bytes(value)
    if isinstance(value, dict):
        return {key: _serialize_any(item, handler) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_any(item, handler) for item in value]
    return handler(value)


@lru_cache(maxsize=128)
def _binary_serializer(cls: type[BaseModel]) -> SchemaSerializer:
    def transform(node: Any) -> Any:
        if isinstance(node, list):
            return [transform(item) for item in node]
        if not isinstance(node, dict):
            return node
        result = {key: transform(value) for key, value in node.items()}
        if result.get("type") == "model":
            declared = result["cls"]
            result.setdefault("config", {})["ser_json_bytes"] = "base64"
            inner = dict(result)

            def polymorphic(value: Any, handler: Any) -> Any:
                return _json_bytes(value) if type(value) is not declared else handler(value)

            result["serialization"] = core_schema.wrap_serializer_function_ser_schema(
                polymorphic, schema=cast(core_schema.CoreSchema, inner), when_used="json"
            )
        elif result.get("type") == "dataclass":
            result.setdefault("config", {})["ser_json_bytes"] = "base64"
        elif result.get("type") == "any" and "serialization" not in result:
            result["serialization"] = core_schema.wrap_serializer_function_ser_schema(_serialize_any, when_used="json")
        return result

    kwargs: dict[str, Any] = {"_use_prebuilt": False} if "_use_prebuilt" in inspect.signature(SchemaSerializer).parameters else {}
    return SchemaSerializer(transform(deepcopy(cls.__pydantic_core_schema__)), config={"ser_json_bytes": "base64"}, **kwargs)


def _json_bytes(model: BaseModel, include: set[str] | None = None) -> dict:
    kwargs: dict[str, Any] = {**_DUMP_KWARGS, "include": include}
    if "polymorphic_serialization" in kwargs:
        kwargs["polymorphic_serialization"] = False
    return _binary_serializer(type(model)).to_python(model, **kwargs)


def to_value(model: Any, *, include: set[str] | None = None) -> Any:
    """Convert model data to a core Value, preserving JSON serializers and native bytes."""
    if isinstance(model, BaseModel):
        if _contains_bytes({name: getattr(model, name) for name in include} if include is not None else model):
            native = model.model_dump(**{**_DUMP_KWARGS, "mode": "python", "include": include})
            plain = _restore_bytes(native, _json_bytes(model, include))
        else:
            plain = model.model_dump(**{**_DUMP_KWARGS, "include": include})
    else:
        if include is not None:
            model = {wire: getattr(model, name) for wire, name in _field_names(type(model)).items() if name in include}
        if not _contains_bytes(model):
            return _value_of(msgspec.json.decode(msgspec.json.encode(model)))
        plain = msgspec.to_builtins(model, builtin_types=(bytes, bytearray, memoryview))
    return _value_of(plain)


def from_value(value: Any, cls: type[M]) -> M:
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
        hints = get_type_hints(cls, include_extras=True)
        return {f.name: hints.get(f.name, f.type) for f in dataclasses.fields(cls)}
    raise TypeError(f"not a supported model class: {cls!r}")


def _field_names(cls: type) -> dict[str, str]:
    """Serialized field name to Python attribute name."""
    if issubclass(cls, msgspec.Struct):
        return {field.encode_name: field.name for field in msgspec.structs.fields(cls)}
    if issubclass(cls, BaseModel) and cls.model_config.get("serialize_by_alias"):
        return {field.serialization_alias or field.alias or name: name for name, field in cls.model_fields.items()}
    return {name: name for name in _annotations(cls)}


def _is_nested_model(ann: Any) -> bool:
    return isinstance(ann, type) and (issubclass(ann, BaseModel) or dataclasses.is_dataclass(ann) or issubclass(ann, msgspec.Struct))


def _field_type(ann: Any) -> str:
    origin = get_origin(ann)
    if origin is Annotated:
        return _field_type(get_args(ann)[0])
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
    if ann in (bytes, bytearray, memoryview):
        return "Bytes"
    if ann in (list, tuple, set, frozenset) or origin in (list, tuple, set, frozenset):
        return "List"
    if ann is dict or origin is dict:
        return "Map"
    if _is_nested_model(ann):
        return "Map"  # nested models inlined as Maps; Submodel-by-id is a later refinement
    return "Any"


def schema_of(cls: type, *, owner: str = "server") -> dict:
    """Derive a schema. Field metadata may override the model's default owner."""
    if owner not in ("server", "client"):
        raise ValueError("owner must be 'server' or 'client'")
    fields = []
    names = {name: wire for wire, name in _field_names(cls).items()}
    for name, ann in _annotations(cls).items():
        metadata = {}
        if issubclass(cls, BaseModel):
            extra = cls.model_fields[name].json_schema_extra
            metadata = extra if isinstance(extra, dict) else {}
        elif dataclasses.is_dataclass(cls):
            metadata = next(field.metadata for field in dataclasses.fields(cls) if field.name == name)
        elif get_origin(ann) is Annotated:
            for item in get_args(ann)[1:]:
                if isinstance(item, msgspec.Meta) and item.extra_json_schema:
                    metadata = item.extra_json_schema
        field_owner = metadata.get("owner", owner)
        if field_owner not in ("server", "client"):
            raise ValueError(f"invalid owner for field {name!r}: {field_owner!r}")
        field = {"name": names[name], "ty": _field_type(ann)}
        if field_owner != "server":
            field["owner"] = field_owner
        fields.append(field)
    return {"type_name": cls.__name__, "fields": fields}


_TS_TYPES = {
    "Bool": "boolean",
    "Int": "number",
    "Float": "number",
    "Str": "string",
    "Bytes": "Uint8Array",
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
