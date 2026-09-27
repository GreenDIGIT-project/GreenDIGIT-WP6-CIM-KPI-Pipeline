import importlib.util
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "grafana_query", ROOT / "_sql_cnr/grafana_query.py"
)
grafana_query = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(grafana_query)


def prepare(sql, groups=None):
    return grafana_query.prepare_query(
        sql,
        groups or ["greendigit"],
        start_ms=1_700_000_000_000,
        end_ms=1_700_003_600_000,
        interval_seconds=60,
    )


def test_metrics_relation_is_always_scoped_and_macros_are_expanded():
    sql, params = prepare(
        "SELECT $__timeGroupAlias(m.bucket_15m, $__interval), SUM(m.energy_wh) "
        "FROM monitoring.mv_fact_site_event_15m m "
        "WHERE $__timeFilter(m.bucket_15m) AND '$__all' = '$__all' GROUP BY 1"
    )
    assert "WHERE group_name = ANY(%s)" in sql
    assert "$__time" not in sql
    assert 'AS "time"' in sql
    assert params == [["greendigit"]]


def test_existing_percent_literals_are_escaped_before_scope_parameter_is_added():
    sql, params = prepare(
        "SELECT m.vo FROM monitoring.mv_fact_site_event_15m m "
        "WHERE ('All' IN ('All', '%') OR m.activity = 'All')"
    )
    assert "'%%'" in sql
    assert "group_name = ANY(%s)" in sql
    assert params == [["greendigit"]]


def test_fact_details_must_be_joined_through_scoped_fact_relation():
    with pytest.raises(grafana_query.GrafanaQueryError):
        prepare("SELECT * FROM monitoring.detail_network")
    sql, params = prepare(
        "SELECT dn.event_id FROM monitoring.fact_site_event f "
        "JOIN monitoring.detail_network dn ON dn.event_id=f.event_id"
    )
    assert "monitoring.fact_site_event WHERE group_name = ANY(%s)" in sql
    assert params == [["greendigit"]]


def test_unknown_or_nested_relation_cannot_bypass_scope():
    with pytest.raises(grafana_query.GrafanaQueryError):
        prepare(
            "SELECT (SELECT usename FROM pg_catalog.pg_user LIMIT 1) "
            "FROM monitoring.mv_fact_site_event_15m LIMIT 1"
        )
    with pytest.raises(grafana_query.GrafanaQueryError):
        prepare("DELETE FROM monitoring.fact_site_event")


def test_reporting_views_are_scoped_and_operational_health_is_allowed():
    for relation in (
        "monitoring.v_reporting_record_listing",
        "monitoring.v_reporting_resource_listing",
    ):
        sql, params = prepare(f"SELECT * FROM {relation}")
        assert "group_name = ANY(%s)" in sql
        assert params == [["greendigit"]]
    sql, params = prepare("SELECT service_name FROM monitoring.service_health_probe")
    assert params == []
    assert "group_name" not in sql


class Cursor:
    description = [("time",), ("energy_wh",), ("site",)]

    def fetchall(self):
        return [
            (datetime(2026, 1, 1, tzinfo=timezone.utc), Decimal("1.25"), "A"),
        ]


def test_frame_serialises_postgres_values_for_grafana():
    frame = grafana_query.frame_for_cursor(Cursor(), "A", "SELECT")
    assert [field["type"] for field in frame["schema"]["fields"]] == [
        "time",
        "number",
        "string",
    ]
    assert frame["data"]["values"] == [[1767225600000], [1.25], ["A"]]
