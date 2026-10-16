import pytest

from otlp_replay.config import ConfigError, load_columns
from otlp_replay.domain.columns import ColumnType, Source


def write(tmp_path, text):
    path = tmp_path / "columns.yaml"
    path.write_text(text)
    return path


def by_name(columns):
    return {c.name: c for c in columns}


def test_packaged_defaults_cover_http_rpc_db_messaging_and_k8s():
    columns = by_name(load_columns())
    assert columns["http_status_code"].type == ColumnType.INT
    assert columns["rpc_status_code"].type == ColumnType.GRPC_STATUS
    assert columns["k8s_pod"].sources == (Source("k8s.pod.name", resource=True),)


def test_user_columns_extend_and_override_the_defaults(tmp_path):
    path = write(
        tmp_path,
        """
columns:
  tenant: [tenant.id]
  http_method: {from: [custom.verb]}
""",
    )
    columns = by_name(load_columns(path))
    assert columns["tenant"].sources == (Source("tenant.id"),)
    assert columns["http_method"].sources == (Source("custom.verb"),)
    assert "k8s_pod" in columns


def test_defaults_can_be_dropped(tmp_path):
    path = write(tmp_path, "defaults: false\ncolumns:\n  tenant: [tenant.id]\n")
    assert [c.name for c in load_columns(path)] == ["tenant"]


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("colums: {}", "unknown keys"),
        ("columns:\n  a: {from: [x], kind: int}", "unknown keys"),
        ("columns:\n  a: {from: [x], type: integer}", "integer"),
        ("columns:\n  a: tenant.id", "list or a mapping"),
        ("columns:\n  Bad-Name: [x]", "invalid column name"),
        ("defaults: yes please", "true or false"),
    ],
)
def test_invalid_configuration_is_refused_with_a_clear_message(tmp_path, text, message):
    with pytest.raises(ConfigError, match=message):
        load_columns(write(tmp_path, text))
