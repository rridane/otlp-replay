"""Promoted columns: span or resource attributes copied into their own column."""

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum

RESOURCE_PREFIX = "resource:"
NAME = re.compile(r"[a-z_][a-z0-9_]*")

GRPC_CODES = (
    "OK",
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
)


class ColumnType(StrEnum):
    STRING = "string"
    INT = "int"
    LONG = "long"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    GRPC_STATUS = "grpc_status"


@dataclass(frozen=True)
class Source:
    attribute: str
    resource: bool = False

    @classmethod
    def parse(cls, text: str) -> "Source":
        if text.startswith(RESOURCE_PREFIX):
            return cls(text.removeprefix(RESOURCE_PREFIX), resource=True)
        return cls(text)

    def __str__(self) -> str:
        return f"{RESOURCE_PREFIX}{self.attribute}" if self.resource else self.attribute


@dataclass(frozen=True)
class Column:
    """The first source present wins, then the text is converted to the type."""

    name: str
    sources: tuple[Source, ...]
    type: ColumnType = ColumnType.STRING

    def __post_init__(self) -> None:
        if not NAME.fullmatch(self.name):
            raise ValueError(f"invalid column name: {self.name!r}")
        if not self.sources:
            raise ValueError(f"column {self.name!r} has no source attribute")

    def value(
        self,
        span_attributes: Mapping[str, str | None],
        resource_attributes: Mapping[str, str | None],
    ) -> object:
        text = next(
            (
                found
                for source in self.sources
                if (
                    found := (
                        resource_attributes if source.resource else span_attributes
                    ).get(source.attribute)
                )
                is not None
            ),
            None,
        )
        return None if text is None else CONVERTERS[self.type](text)


def _int(text: str) -> int | None:
    try:
        return int(text)
    except ValueError:
        return None


def _double(text: str) -> float | None:
    try:
        return float(text)
    except ValueError:
        return None


def _boolean(text: str) -> bool | None:
    return {"true": True, "false": False}.get(text.lower())


def _grpc_status(text: str) -> str:
    code = _int(text)
    return (
        GRPC_CODES[code] if code is not None and 0 <= code < len(GRPC_CODES) else text
    )


CONVERTERS: dict[ColumnType, Callable[[str], object]] = {
    ColumnType.STRING: str,
    ColumnType.INT: _int,
    ColumnType.LONG: _int,
    ColumnType.DOUBLE: _double,
    ColumnType.BOOLEAN: _boolean,
    ColumnType.GRPC_STATUS: _grpc_status,
}
