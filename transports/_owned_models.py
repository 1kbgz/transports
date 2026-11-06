"""Reconstruct owned field types without applying model or field validators."""

import dataclasses
import inspect
from copy import copy, deepcopy
from functools import lru_cache
from types import UnionType
from typing import Annotated, Any, Union, get_args, get_origin

import msgspec
from pydantic import BaseModel, TypeAdapter
from pydantic_core import SchemaValidator, core_schema

from ._bridge import _annotations, _field_names


def _construct(cls: type, values: dict, template: Any = None) -> Any:
    if issubclass(cls, BaseModel):
        obj = object.__new__(cls)
        object.__setattr__(obj, "__dict__", {name: value for name, value in values.items() if name in cls.model_fields})
        object.__setattr__(obj, "__pydantic_fields_set__", set(values))
        extra = {name: value for name, value in values.items() if name not in cls.model_fields} if cls.model_config.get("extra") == "allow" else None
        object.__setattr__(obj, "__pydantic_extra__", extra)
        object.__setattr__(obj, "__pydantic_private__", {name: field.get_default() for name, field in cls.__private_attributes__.items()} or None)
        return obj
    if issubclass(cls, msgspec.Struct):
        if template is None:
            if hasattr(cls, "__post_init__"):
                raise ValueError("owned Struct values with __post_init__ require an existing value to copy")
            return cls(**values)
        obj = copy(template)
        for name, value in values.items():
            msgspec.structs.force_setattr(obj, name, value)
        return obj
    obj = object.__new__(cls)
    for name, value in values.items():
        object.__setattr__(obj, name, value)
    return obj


@lru_cache(maxsize=128)
def _type_adapter(annotation: Any) -> TypeAdapter:
    return TypeAdapter(annotation)


def decode_owned(value: Any, annotation: Any, template: Any = None) -> Any:
    origin, args = get_origin(annotation), get_args(annotation)
    if origin is Annotated:
        return decode_owned(value, args[0], template)
    if annotation is Any:
        return value
    if origin in (Union, UnionType):
        for member in args:
            try:
                return decode_owned(value, member, template)
            except (ValueError, TypeError):
                pass
        raise ValueError(f"value does not match {annotation}")
    if origin in (list, tuple, set, frozenset) or annotation in (list, tuple, set, frozenset):
        if not isinstance(value, list):
            raise ValueError("expected a list")
        kind = origin or annotation
        if kind is tuple and args and args[-1] is not Ellipsis and len(args) != len(value):
            raise ValueError("tuple length does not match its annotation")
        items = [
            decode_owned(
                item,
                args[index] if kind is tuple and args and args[-1] is not Ellipsis else args[0] if args else Any,
                template[index] if isinstance(template, (list, tuple)) and index < len(template) else None,
            )
            for index, item in enumerate(value)
        ]
        return kind(items)
    if origin is dict or annotation is dict:
        if not isinstance(value, dict):
            raise ValueError("expected a map")
        return {
            decode_owned(key, args[0] if args else Any): decode_owned(
                item, args[1] if args else Any, template.get(key) if isinstance(template, dict) else None
            )
            for key, item in value.items()
        }
    if isinstance(annotation, type) and (issubclass(annotation, (BaseModel, msgspec.Struct)) or dataclasses.is_dataclass(annotation)):
        if not isinstance(value, dict):
            raise ValueError("expected a model map")
        if template is not None and isinstance(template, annotation) and _field_names(type(template)).keys() <= value.keys():
            annotation = type(template)
        names = _field_names(annotation)
        unknown = value.keys() - names.keys()
        if unknown and not (issubclass(annotation, BaseModel) and annotation.model_config.get("extra") == "allow"):
            raise ValueError(f"owned model contains unknown fields: {sorted(unknown)}")
        value = {names.get(name, name): item for name, item in value.items()}
        values = {name: value[name] for name in unknown}
        for name, ann in _annotations(annotation).items():
            if name not in value:
                raise ValueError(f"owned model value is missing field {name!r}")
            values[name] = decode_owned(value[name], ann, getattr(template, name, None))
        return _construct(annotation, values, template)
    # Primitive shape checks are strict; dates, enums, URLs and paths decode their JSON form.
    return _type_adapter(annotation).validate_python(value, strict=annotation in (str, int, bool, float, bytes))


@lru_cache(maxsize=128)
def server_validator(cls: type[BaseModel], owned: frozenset[str]) -> SchemaValidator:
    def transform(node: Any) -> Any:
        if isinstance(node, list):
            return [transform(item) for item in node]
        if not isinstance(node, dict):
            return node
        result = {key: transform(value) for key, value in node.items()}
        if result.get("type") == "model" and result.get("cls") is cls:
            fields = result["schema"]
            while fields.get("type") != "model-fields":
                fields = fields["schema"]
            for name in owned:
                fields["fields"][name]["schema"] = core_schema.any_schema()
        return result

    # Recent pydantic-core versions otherwise reuse the class's original validator.
    kwargs: dict[str, Any] = {"_use_prebuilt": False} if "_use_prebuilt" in inspect.signature(SchemaValidator).parameters else {}
    return SchemaValidator(transform(deepcopy(cls.__pydantic_core_schema__)), **kwargs)
