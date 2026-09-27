import re
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any


MAX_QUERIES = 64

SCOPED_RELATIONS = {
    "monitoring.fact_site_event",
    "monitoring.mv_fact_site_event_15m",
    "monitoring.v_reporting_record_listing",
    "monitoring.v_reporting_resource_listing",
}

ALLOWED_RELATIONS = SCOPED_RELATIONS | {
    "monitoring.sites",
    "monitoring.detail_network",
    "monitoring.service_health_probe",
}

_RELATION_RE = re.compile(r"(?i)\b(from|join)\s+(monitoring\.[a-z_][a-z0-9_]*)\b")
_SOURCE_RE = re.compile(r"(?i)\b(?:from|join)\s+([^\s,]+)")
_TIME_FILTER_RE = re.compile(r"\$__timeFilter\(\s*([a-z_][a-z0-9_.]*)\s*\)", re.I)
_TIME_GROUP_RE = re.compile(
    r"\$__timeGroupAlias\(\s*([a-z_][a-z0-9_.]*)\s*,\s*([^\)]+)\)", re.I
)


class GrafanaQueryError(ValueError):
    pass


def _epoch_millis(value: Any, label: str) -> int:
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
        return int(value)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.timestamp() * 1000)
        except ValueError:
            pass
    raise GrafanaQueryError(f"Invalid Grafana {label} timestamp")


def _time_range(request_body: dict[str, Any]) -> tuple[int, int]:
    range_body = request_body.get("range") or {}
    start = request_body.get("from", range_body.get("from"))
    end = request_body.get("to", range_body.get("to"))
    start_ms = _epoch_millis(start, "from")
    end_ms = _epoch_millis(end, "to")
    if start_ms > end_ms:
        raise GrafanaQueryError("Grafana query range starts after it ends")
    return start_ms, end_ms


def _interval_seconds(query: dict[str, Any], start_ms: int, end_ms: int) -> int:
    raw = query.get("intervalMs")
    if raw is not None:
        try:
            return max(1, int(raw) // 1000)
        except (TypeError, ValueError):
            raise GrafanaQueryError("Invalid Grafana intervalMs")
    points = max(1, int(query.get("maxDataPoints") or 1000))
    return max(1, (end_ms - start_ms) // 1000 // points)


def _duration_seconds(raw: str) -> int:
    match = re.fullmatch(r"(\d+)(ms|s|m|h|d)", raw, re.I)
    if not match:
        raise GrafanaQueryError("Unsupported Grafana interval")
    amount = int(match.group(1))
    multipliers = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}
    return max(1, int(amount * multipliers[match.group(2).lower()]))


def prepare_query(
    raw_sql: str,
    groups: list[str],
    *,
    start_ms: int,
    end_ms: int,
    interval_seconds: int,
) -> tuple[str, list[Any]]:
    sql = str(raw_sql or "").strip()
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    if not sql or not re.match(r"(?is)^select\b", sql):
        raise GrafanaQueryError("Only SELECT dashboard queries are allowed")
    if ";" in sql or "--" in sql or "/*" in sql or "*/" in sql:
        raise GrafanaQueryError("Multiple statements and SQL comments are not allowed")
    if re.search(
        r"(?i)\b(insert|update|delete|alter|drop|create|grant|revoke|copy|call|do|execute|set|reset|show)\b",
        sql,
    ):
        raise GrafanaQueryError("The dashboard query contains a forbidden SQL operation")
    if re.search(r"(?i)\b(pg_catalog|information_schema|pg_read_|pg_ls_|dblink|lo_import|lo_export)\b", sql):
        raise GrafanaQueryError("The dashboard query contains a forbidden database object")
    if "${" in sql or re.search(r"\$(?!__timeFilter|__timeGroupAlias|__interval|__all)[A-Za-z_]", sql):
        raise GrafanaQueryError("Grafana sent an unresolved dashboard variable")

    relations = [match.group(2).lower() for match in _RELATION_RE.finditer(sql)]
    sources = [match.group(1).rstrip(";").lower() for match in _SOURCE_RE.finditer(sql)]
    # SQL's TRIM(... FROM CONCAT(...)) contains a FROM token but does not read
    # another relation.
    if any(source not in ALLOWED_RELATIONS and source != "concat(" for source in sources):
        raise GrafanaQueryError("Dashboard queries may only read approved monitoring relations")
    if not relations:
        raise GrafanaQueryError("Dashboard queries must use an approved monitoring relation")
    unknown = sorted(set(relations) - ALLOWED_RELATIONS)
    if unknown:
        raise GrafanaQueryError(f"Dashboard relation is not allowed: {unknown[0]}")
    scoped = [relation for relation in relations if relation in SCOPED_RELATIONS]
    if not scoped and set(relations) != {"monitoring.service_health_probe"}:
        raise GrafanaQueryError("Dashboard query is missing a group-scoped metrics relation")
    if {"monitoring.sites", "monitoring.detail_network"} & set(relations):
        if "monitoring.fact_site_event" not in scoped:
            raise GrafanaQueryError("Metric detail queries must join through the scoped fact relation")

    start_iso = datetime.fromtimestamp(start_ms / 1000, timezone.utc).isoformat()
    end_iso = datetime.fromtimestamp(end_ms / 1000, timezone.utc).isoformat()
    sql = _TIME_FILTER_RE.sub(
        lambda match: f"({match.group(1)} >= TIMESTAMPTZ '{start_iso}' AND {match.group(1)} <= TIMESTAMPTZ '{end_iso}')",
        sql,
    )

    def replace_time_group(match: re.Match[str]) -> str:
        expression = match.group(1)
        interval = match.group(2).strip()
        if interval != "$__interval" and not re.fullmatch(r"\d+(ms|s|m|h|d)", interval, re.I):
            raise GrafanaQueryError("Unsupported Grafana time-group interval")
        seconds = interval_seconds if interval == "$__interval" else _duration_seconds(interval)
        return (
            f"to_timestamp(floor(extract(epoch FROM {expression}) / {seconds}) * {seconds}) "
            'AS "time"'
        )

    sql = _TIME_GROUP_RE.sub(replace_time_group, sql)
    if re.search(r"\$__(?!all\b)", sql):
        raise GrafanaQueryError("Unsupported Grafana SQL macro")

    # psycopg2 uses percent-based placeholders whenever parameters are passed.
    # Escape percent characters already present in dashboard SQL before adding
    # our own group-scope %s placeholders below.
    sql = sql.replace("%", "%%")
    params: list[Any] = []

    def scope_relation(match: re.Match[str]) -> str:
        keyword, relation = match.group(1), match.group(2)
        if relation.lower() not in SCOPED_RELATIONS:
            return match.group(0)
        params.append(groups)
        return f"{keyword} (SELECT * FROM {relation} WHERE group_name = ANY(%s))"

    sql = _RELATION_RE.sub(scope_relation, sql)
    return sql, params


def _field_type(name: str, values: list[Any]) -> str:
    value = next((item for item in values if item is not None), None)
    if isinstance(value, (datetime, date)) or name.lower() in {"time", "time_bucket", "bucket_15m", "probe_ts"}:
        return "time"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float, Decimal)):
        return "number"
    return "string"


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return int(value.timestamp() * 1000)
    if isinstance(value, date):
        return int(datetime(value.year, value.month, value.day, tzinfo=timezone.utc).timestamp() * 1000)
    if isinstance(value, Decimal):
        return float(value)
    return value


def frame_for_cursor(cur: Any, ref_id: str, executed_sql: str) -> dict[str, Any]:
    rows = list(cur.fetchall())
    names = [description[0] for description in cur.description or []]
    columns = [[row[index] for row in rows] for index in range(len(names))]
    fields = [{"name": name, "type": _field_type(name, columns[index])} for index, name in enumerate(names)]
    values = [[_json_value(value) for value in column] for column in columns]
    return {
        "schema": {
            "refId": ref_id,
            "meta": {"executedQueryString": executed_sql},
            "fields": fields,
        },
        "data": {"values": values},
    }


def execute_grafana_request(cur: Any, groups: list[str], request_body: dict[str, Any]) -> dict[str, Any]:
    queries = request_body.get("queries")
    if not isinstance(queries, list) or not queries or len(queries) > MAX_QUERIES:
        raise GrafanaQueryError(f"Grafana request must contain 1-{MAX_QUERIES} queries")
    start_ms, end_ms = _time_range(request_body)
    results: dict[str, Any] = {}
    for index, query in enumerate(queries):
        if not isinstance(query, dict):
            raise GrafanaQueryError("Invalid Grafana query object")
        ref_id = str(query.get("refId") or chr(ord("A") + index))
        if query.get("hide") is True:
            continue
        raw_sql = query.get("rawSql") or query.get("query")
        sql, params = prepare_query(
            raw_sql,
            groups,
            start_ms=start_ms,
            end_ms=end_ms,
            interval_seconds=_interval_seconds(query, start_ms, end_ms),
        )
        cur.execute(sql, tuple(params))
        results[ref_id] = {"status": 200, "frames": [frame_for_cursor(cur, ref_id, sql)]}
    return {"results": results}
