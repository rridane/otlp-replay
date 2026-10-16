"""Promoted columns from YAML: the packaged defaults, then the user file.

defaults: true            # optional, false drops the packaged columns
columns:
  tenant: [tenant.id]
  retries: {from: [http.request.resend_count], type: int}
"""

from importlib import resources
from pathlib import Path

import yaml

from otlp_replay.domain.columns import Column, ColumnType, Source

DEFAULTS = "defaults.yaml"


class ConfigError(ValueError):
    pass


def load_columns(path: Path | None = None) -> tuple[Column, ...]:
    packaged = _columns(
        _read(resources.files("otlp_replay").joinpath(DEFAULTS).read_text())
    )
    if path is None:
        return tuple(packaged.values())
    document = _read(path.read_text())
    _only(document, {"defaults", "columns"}, str(path))
    defaults = document.get("defaults", True)
    if not isinstance(defaults, bool):
        raise ConfigError(f"{path}: 'defaults' must be true or false")
    user = _columns(document)
    return tuple(({**packaged, **user} if defaults else user).values())


def _read(text: str) -> dict:
    document = yaml.safe_load(text) or {}
    if not isinstance(document, dict):
        raise ConfigError("the configuration must be a mapping")
    return document


def _columns(document: dict) -> dict[str, Column]:
    columns = document.get("columns") or {}
    if not isinstance(columns, dict):
        raise ConfigError("'columns' must map column names to attributes")
    return {name: _column(name, spec) for name, spec in columns.items()}


def _column(name: str, spec: object) -> Column:
    if isinstance(spec, list):
        spec = {"from": spec}
    if not isinstance(spec, dict):
        raise ConfigError(f"column {name!r}: expected a list or a mapping")
    _only(spec, {"from", "type"}, f"column {name!r}")
    sources = spec.get("from")
    if not isinstance(sources, list) or not all(isinstance(s, str) for s in sources):
        raise ConfigError(f"column {name!r}: 'from' must be a list of attributes")
    try:
        kind = ColumnType(spec.get("type", ColumnType.STRING))
        return Column(name, tuple(Source.parse(s) for s in sources), kind)
    except ValueError as error:
        raise ConfigError(f"column {name!r}: {error}") from error


def _only(mapping: dict, allowed: set[str], where: str) -> None:
    unknown = sorted(set(mapping) - allowed)
    if unknown:
        raise ConfigError(f"{where}: unknown keys {unknown}")
