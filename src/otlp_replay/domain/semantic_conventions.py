"""Normalized columns derived from span attributes."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

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


def as_int(text: str) -> int | None:
    try:
        return int(text)
    except ValueError:
        return None


def as_grpc_code(text: str) -> str:
    code = as_int(text)
    return (
        GRPC_CODES[code] if code is not None and 0 <= code < len(GRPC_CODES) else text
    )


@dataclass(frozen=True)
class Rule:
    column: str
    keys: tuple[str, ...]
    convert: Callable[[str], object] | None = None


DEFAULT_RULES = (
    Rule("http_method", ("http.request.method", "http.method")),
    Rule("http_route", ("http.route",)),
    Rule(
        "http_status_code",
        ("http.response.status_code", "http.status_code"),
        as_int,
    ),
    Rule("rpc_system", ("rpc.system.name", "rpc.system")),
    Rule("rpc_service", ("rpc.service",)),
    Rule("rpc_method", ("rpc.method",)),
    Rule(
        "rpc_status_code",
        ("rpc.response.status_code", "rpc.grpc.status_code", "grpc.status_code"),
        as_grpc_code,
    ),
    Rule("db_system", ("db.system.name", "db.system")),
    Rule("db_statement", ("db.query.text", "db.statement")),
    Rule("server_address", ("server.address",)),
    Rule("peer_service", ("peer.service",)),
    Rule("messaging_system", ("messaging.system",)),
    Rule("messaging_destination", ("messaging.destination.name",)),
)


class SemanticConventions:
    """The first key present wins: current convention first, then older ones."""

    def __init__(self, rules: Sequence[Rule] = DEFAULT_RULES) -> None:
        self._rules = tuple(rules)

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(rule.column for rule in self._rules)

    def normalize(self, attributes: Mapping[str, str | None]) -> dict[str, object]:
        return {rule.column: self._apply(rule, attributes) for rule in self._rules}

    @staticmethod
    def _apply(rule: Rule, attributes: Mapping[str, str | None]) -> object:
        value = next(
            (attributes[k] for k in rule.keys if attributes.get(k) is not None), None
        )
        if value is None or rule.convert is None:
            return value
        return rule.convert(value)
