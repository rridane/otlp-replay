from otlp_replay.domain.semantic_conventions import (
    DEFAULT_RULES,
    Rule,
    SemanticConventions,
)

conventions = SemanticConventions()


def test_every_rule_produces_its_column():
    assert set(conventions.normalize({})) == {r.column for r in DEFAULT_RULES}
    assert all(v is None for v in conventions.normalize({}).values())


def test_recent_convention_wins_over_legacy():
    normalized = conventions.normalize(
        {
            "http.request.method": "POST",
            "http.method": "GET",
            "db.query.text": "SELECT 1",
            "db.statement": "SELECT 2",
        }
    )
    assert (normalized["http_method"], normalized["db_statement"]) == (
        "POST",
        "SELECT 1",
    )


def test_legacy_convention_is_used_when_alone():
    assert conventions.normalize({"http.status_code": "404"})["http_status_code"] == 404


def test_unparsable_integer_becomes_null():
    assert (
        conventions.normalize({"http.status_code": "n/a"})["http_status_code"] is None
    )


def test_grpc_status_codes_become_names():
    numeric = conventions.normalize({"rpc.grpc.status_code": "14"})
    named = conventions.normalize({"rpc.response.status_code": "OK"})
    assert (numeric["rpc_status_code"], named["rpc_status_code"]) == (
        "UNAVAILABLE",
        "OK",
    )


def test_rules_are_injectable():
    custom = SemanticConventions([Rule("tenant", ("tenant.id",))])
    assert custom.columns == ("tenant",)
    assert custom.normalize({"tenant.id": "acme"}) == {"tenant": "acme"}
