from __future__ import annotations

from typing import Any, Optional

from .base_dialect import SqlDialect


class ClickHouseDialect(SqlDialect):
    """ClickHouse SQL dialect implementation."""

    def date_trunc(self, unit: str, column: str) -> str:
        return f"date_trunc('{unit}', {column})"

    def to_string(self, column: str) -> str:
        return f"toString({column})"

    def if_null(self, column: str, default: Any) -> str:
        return f"ifNull({column}, {default})"

    def null_if(self, column: str, value: Any) -> str:
        return f"nullIf({column}, {value})"

    def count_if(self, condition: str) -> str:
        return f"countIf({condition})"

    def sum_if(self, column: str, condition: str) -> str:
        return f"sumIf({column}, {condition})"

    def var_pop_if(self, column: str, condition: str) -> str:
        return f"varPopIf({column}, {condition})"

    def count_distinct(self, column: str) -> str:
        return f"countDistinct({column})"

    def arg_max(self, column: str, version_column: str) -> str:
        return f"argMax({column}, {version_column})"

    def quantile(self, probability: float, column: str) -> str:
        return f"quantile({probability})({column})"

    def array_join(
        self, column: str, alias: str, type_str: Optional[str] = None
    ) -> str:
        if type_str:
            return f"ARRAY JOIN CAST({column} AS {type_str}) AS {alias}"
        return f"ARRAY JOIN {column} AS {alias}"

    def json_extract(self, column: str, path: str, type_str: str) -> str:
        return f"JSONExtract({column}, '{path}', '{type_str}')"

    def split_by_char(self, char: str, column: str, index: int) -> str:
        return f"splitByChar('{char}', {column})[{index}]"

    def map_key_access(self, column: str, key: str) -> str:
        return f"{column}['{key}']"

    def tuple_element(self, column: str, index: int) -> str:
        return f"{column}.{index}"

    def array_element(self, column: str, index: int) -> str:
        return f"{column}[{index}]"

    def json_has_any_key(self, column: str, keys_param: str) -> str:
        return f"hasAny(mapKeys(CAST({column} AS Map(String, Float32))), {keys_param})"

    def json_has_any_theme(self, column: str, themes_param: str) -> str:
        return f"arrayExists(k -> splitByChar('.', k)[1] IN {themes_param}, mapKeys(CAST({column} AS Map(String, Float32))))"

    def day_of_week(self, column: str) -> str:
        return f"toDayOfWeek({column})"

    def hour(self, column: str) -> str:
        return f"toHour({column})"

    def date_diff(self, unit: str, start_column: str, end_column: str) -> str:
        return f"dateDiff('{unit}', {start_column}, {end_column})"

    def to_date(self, column: str) -> str:
        return f"toDate({column})"

    def to_datetime(self, value: str) -> str:
        return f"toDateTime({value})"

    def concat(self, *parts: str) -> str:
        return f"concat({', '.join(parts)})"

    def query_settings(self, timeout_seconds: int) -> str:
        return f"SETTINGS max_execution_time = {timeout_seconds}"


class PostgresDialect(SqlDialect):
    """PostgreSQL SQL dialect implementation."""

    def date_trunc(self, unit: str, column: str) -> str:
        return f"date_trunc('{unit}', {column})"

    def to_string(self, column: str) -> str:
        return f"CAST({column} AS VARCHAR)"

    def if_null(self, column: str, default: Any) -> str:
        return f"COALESCE({column}, {default})"

    def null_if(self, column: str, value: Any) -> str:
        return f"NULLIF({column}, {value})"

    def count_if(self, condition: str) -> str:
        return f"COUNT(*) FILTER (WHERE {condition})"

    def sum_if(self, column: str, condition: str) -> str:
        return f"SUM({column}) FILTER (WHERE {condition})"

    def var_pop_if(self, column: str, condition: str) -> str:
        return f"VAR_POP({column}) FILTER (WHERE {condition})"

    def count_distinct(self, column: str) -> str:
        return f"COUNT(DISTINCT {column})"

    def arg_max(self, column: str, version_column: str) -> str:
        return f"(ARRAY_AGG({column} ORDER BY {version_column} DESC))[1]"

    def quantile(self, probability: float, column: str) -> str:
        return f"percentile_cont({probability}) WITHIN GROUP (ORDER BY {column})"

    def array_join(
        self, column: str, alias: str, type_str: Optional[str] = None
    ) -> str:
        return f"CROSS JOIN LATERAL unnest({column}) AS {alias}"

    def json_extract(self, column: str, path: str, type_str: str) -> str:
        return f"{column}->>'{path}'"

    def split_by_char(self, char: str, column: str, index: int) -> str:
        return f"split_part({column}, '{char}', {index})"

    def map_key_access(self, column: str, key: str) -> str:
        return f"{column}->>'{key}'"

    def tuple_element(self, column: str, index: int) -> str:
        return f"{column}[{index}]"

    def array_element(self, column: str, index: int) -> str:
        return f"{column}[{index}]"

    def json_has_any_key(self, column: str, keys_param: str) -> str:
        return f"{column} ?| {keys_param}"

    def json_has_any_theme(self, column: str, themes_param: str) -> str:
        # Complex Postgres implementation using jsonb_object_keys and subquery
        return f"EXISTS (SELECT 1 FROM jsonb_object_keys({column}) k WHERE split_part(k, '.', 1) IN {themes_param})"

    def day_of_week(self, column: str) -> str:
        return f"EXTRACT(ISODOW FROM {column})"

    def hour(self, column: str) -> str:
        return f"EXTRACT(HOUR FROM {column})"

    def date_diff(self, unit: str, start_column: str, end_column: str) -> str:
        if unit == "minute":
            return f"EXTRACT(EPOCH FROM ({end_column} - {start_column})) / 60"
        return f"EXTRACT(EPOCH FROM ({end_column} - {start_column}))"

    def to_date(self, column: str) -> str:
        return f"CAST({column} AS DATE)"

    def to_datetime(self, value: str) -> str:
        return f"CAST({value} AS TIMESTAMP)"

    def concat(self, *parts: str) -> str:
        return f"CONCAT({', '.join(parts)})"

    def query_settings(self, timeout_seconds: int) -> str:
        return f"-- statement_timeout: {timeout_seconds}s"


class SQLiteDialect(SqlDialect):
    """SQLite SQL dialect implementation."""

    def date_trunc(self, unit: str, column: str) -> str:
        if unit == "day":
            return f"strftime('%Y-%m-%d', {column})"
        if unit == "week":
            return f"date({column}, 'weekday 0', '-6 days')"
        if unit == "month":
            return f"strftime('%Y-%m-01', {column})"
        return column

    def to_string(self, column: str) -> str:
        return f"CAST({column} AS TEXT)"

    def if_null(self, column: str, default: Any) -> str:
        return f"IFNULL({column}, {default})"

    def null_if(self, column: str, value: Any) -> str:
        return f"NULLIF({column}, {value})"

    def count_if(self, condition: str) -> str:
        return f"SUM(CASE WHEN {condition} THEN 1 ELSE 0 END)"

    def sum_if(self, column: str, condition: str) -> str:
        return f"SUM(CASE WHEN {condition} THEN {column} ELSE 0 END)"

    def var_pop_if(self, column: str, condition: str) -> str:
        # SQLite lacks native var_pop, return simplified aggregate
        return f"AVG(CASE WHEN {condition} THEN {column}*{column} ELSE NULL END) - AVG(CASE WHEN {condition} THEN {column} ELSE NULL END)*AVG(CASE WHEN {condition} THEN {column} ELSE NULL END)"

    def count_distinct(self, column: str) -> str:
        return f"COUNT(DISTINCT {column})"

    def arg_max(self, column: str, version_column: str) -> str:
        return f"MAX({column})"

    def quantile(self, probability: float, column: str) -> str:
        return f"AVG({column})"

    def array_join(
        self, column: str, alias: str, type_str: Optional[str] = None
    ) -> str:
        return f"JOIN json_each({column}) AS {alias}"

    def json_extract(self, column: str, path: str, type_str: str) -> str:
        sqlite_path = f"$.{path}" if not path.startswith("$") else path
        return f"json_extract({column}, '{sqlite_path}')"

    def split_by_char(self, char: str, column: str, index: int) -> str:
        return f"json_extract(json('[\"' || replace({column}, '{char}', '\",\"') || '\"]'), '$[{index - 1}]')"

    def map_key_access(self, column: str, key: str) -> str:
        return f"json_extract({column}, '$.{key}')"

    def tuple_element(self, column: str, index: int) -> str:
        return f"json_extract({column}, '$[{index - 1}]')"

    def array_element(self, column: str, index: int) -> str:
        return f"json_extract({column}, '$[{index - 1}]')"

    def json_has_any_key(self, column: str, keys_param: str) -> str:
        return f"EXISTS (SELECT 1 FROM json_each({column}) WHERE key IN {keys_param})"

    def json_has_any_theme(self, column: str, themes_param: str) -> str:
        return f"EXISTS (SELECT 1 FROM json_each({column}) WHERE json_extract(json('[\"' || replace(key, '.', '\",\"') || '\"]'), '$[0]') IN {themes_param})"

    def day_of_week(self, column: str) -> str:
        return f"((CAST(strftime('%w', {column}) AS INTEGER) + 6) % 7) + 1"

    def hour(self, column: str) -> str:
        return f"CAST(strftime('%H', {column}) AS INTEGER)"

    def date_diff(self, unit: str, start_column: str, end_column: str) -> str:
        if unit == "minute":
            return f"(julianday({end_column}) - julianday({start_column})) * 1440"
        return f"(julianday({end_column}) - julianday({start_column})) * 86400"

    def to_date(self, column: str) -> str:
        return f"date({column})"

    def to_datetime(self, value: str) -> str:
        return f"datetime({value})"

    def concat(self, *parts: str) -> str:
        return " || ".join(parts)

    def query_settings(self, timeout_seconds: int) -> str:
        return ""


def get_dialect(
    dsn: Optional[str] = None, backend_type: Optional[str] = None
) -> SqlDialect:
    """Get a SQL dialect implementation based on DSN or backend type."""
    from dev_health_ops.metrics.sinks.backend_types import detect_backend, SinkBackend

    if dsn:
        backend_type = detect_backend(dsn).value

    if backend_type == SinkBackend.CLICKHOUSE.value:
        return ClickHouseDialect()
    if backend_type == SinkBackend.POSTGRES.value:
        return PostgresDialect()
    if backend_type == SinkBackend.SQLITE.value:
        return SQLiteDialect()

    return ClickHouseDialect()
