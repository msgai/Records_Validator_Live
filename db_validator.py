import csv
import json
import os
import re
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

import clickhouse_connect
import pymysql
import yaml
from dotenv import load_dotenv
from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.table import Table


NULL_VALUES = {"", "null", "none", "nan", "\\n"}
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?)?$")
JSON_EMPTY_KEY_PATTERN = re.compile(r'"([a-zA-Z0-9_]+)"\s*:\s*"\s*(?=[,}])')
console = Console()
error_console = Console(stderr=True)


def is_null(value):
    return value is None or str(value).strip().lower() in NULL_VALUES


def equivalent(sql_value, ch_value):
    if is_null(sql_value) and is_null(ch_value):
        return True
    if is_null(sql_value) or is_null(ch_value):
        return False
    if type(sql_value) is type(ch_value) and sql_value == ch_value:
        return True
    if str(sql_value).strip() == str(ch_value).strip():
        return True
    try:
        if Decimal(str(sql_value).strip()) == Decimal(str(ch_value).strip()):
            return True
    except (InvalidOperation, TypeError, ValueError):
        pass

    if isinstance(sql_value, (date, datetime)) or isinstance(ch_value, (date, datetime)):
        try:
            return _as_datetime(sql_value) == _as_datetime(ch_value)
        except (TypeError, ValueError, OverflowError):
            pass

    sql_text = str(sql_value).strip()
    ch_text = str(ch_value).strip()
    if DATE_PATTERN.fullmatch(sql_text) and DATE_PATTERN.fullmatch(ch_text):
        try:
            return datetime.fromisoformat(sql_text) == datetime.fromisoformat(ch_text)
        except (TypeError, ValueError, OverflowError):
            pass

    if '":' in sql_text and '":' in ch_text:
        sql_normalized = JSON_EMPTY_KEY_PATTERN.sub(r'"\1":""', sql_text)
        ch_normalized = JSON_EMPTY_KEY_PATTERN.sub(r'"\1":""', ch_text)
        if sql_normalized == ch_normalized:
            return True
    return False


def _as_datetime(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    return datetime.fromisoformat(str(value).strip())


def _composite_key(row, primary_keys):
    return tuple(str(row.get(key)).strip() for key in primary_keys)


def _display_composite_key(primary_keys, key):
    return " || ".join(f"{name}={value}" for name, value in zip(primary_keys, key))


def _debug_query(query, primary_keys, key_values, dialect):
    base_query = query.rstrip().removesuffix(";")
    predicates = []
    for column, value in zip(primary_keys, key_values):
        identifier = f"`{column.replace('`', '``')}`"
        literal = str(value).strip().replace("\\", "\\\\").replace("'", "''")
        expression = f"CAST({identifier} AS CHAR)" if dialect == "sql" else f"toString({identifier})"
        predicates.append(f"{expression} = '{literal}'")
    return (
        "SELECT *\n"
        "FROM (\n"
        f"{base_query}\n"
        ") AS validator_debug_rows\n"
        f"WHERE {' AND '.join(predicates)}"
    )


def _compare_rows_by_key(result, table_cfg, sql_rows, ch_rows, sql_columns, ch_columns):
    primary_keys = table_cfg["primary_keys"]
    if (not isinstance(primary_keys, list) or not primary_keys
            or any(not isinstance(key, str) or not key for key in primary_keys)):
        raise ValueError("primary_keys must be a non-empty list of column names")
    for key in primary_keys:
        if key not in sql_columns:
            raise ValueError(f"Primary key '{key}' missing from SQL query results")
        if key not in ch_columns:
            raise ValueError(f"Primary key '{key}' missing from ClickHouse query results")

    excluded_rows = {"SQL": [], "ClickHouse": []}
    filtered_rows = []
    for source, rows in (("SQL", sql_rows), ("ClickHouse", ch_rows)):
        invalid = [row for row in rows if any(is_null(row.get(key)) for key in primary_keys)]
        if invalid and not table_cfg.get("exclude_null_primary_key_rows", False):
            raise ValueError(f"{source} query returned {len(invalid)} rows with null primary keys")
        if invalid:
            result["warnings"].append(f"Excluded {len(invalid)} {source} rows with null primary keys")
            excluded_rows[source] = invalid
        filtered_rows.append([
            row for row in rows
            if not any(is_null(row.get(key)) for key in primary_keys)
        ])
    sql_rows, ch_rows = filtered_rows
    for index in range(max(len(excluded_rows["ClickHouse"]), len(excluded_rows["SQL"]))):
        if index < len(excluded_rows["ClickHouse"]):
            result["manual_validation_rows"].append({
                "row_source": "ClickHouse",
                **excluded_rows["ClickHouse"][index],
            })
        if index < len(excluded_rows["SQL"]):
            result["manual_validation_rows"].append({
                "row_source": "SQL",
                **excluded_rows["SQL"][index],
            })

    sql_groups = {}
    ch_groups = {}
    for row in sql_rows:
        sql_groups.setdefault(_composite_key(row, primary_keys), []).append(row)
    for row in ch_rows:
        ch_groups.setdefault(_composite_key(row, primary_keys), []).append(row)

    duplicate_keys = {
        key for key, group in sql_groups.items() if len(group) > 1
    } | {
        key for key, group in ch_groups.items() if len(group) > 1
    }
    if duplicate_keys and not table_cfg.get("count_duplicate_records", False):
        raise ValueError("Duplicate primary keys found; enable count_duplicate_records to compare them")

    shared_columns = [column for column in sql_columns if column in ch_columns]
    value_columns = [column for column in shared_columns if column not in primary_keys]
    for key in sorted(set(sql_groups) | set(ch_groups)):
        left = sql_groups.get(key, [])
        right = ch_groups.get(key, [])
        label = _display_composite_key(primary_keys, key)
        if not left or not right:
            is_missing = bool(left)
            rows = left if is_missing else right
            for row in rows:
                result["missing_in_ch" if is_missing else "extra_in_ch"] += 1
                result["discrepancies"].append({
                    "table_name": result["table_name"],
                    "composite_key": label,
                    "_debug_key_values": [rows[0].get(key) for key in primary_keys],
                    "column_name": "ALL",
                    "sql_value": json.dumps(row, default=str) if is_missing else "MISSING",
                    "ch_value": "MISSING" if is_missing else json.dumps(row, default=str),
                    "diff_type": "MISSING_IN_TARGET" if is_missing else "EXTRA_IN_TARGET",
                })
            continue

        if len(left) != len(right):
            result["cell_mismatches"] += 1
            result["row_mismatches"] += 1
            result["discrepancies"].append({
                "table_name": result["table_name"],
                "composite_key": label,
                "_debug_key_values": [left[0].get(key) for key in primary_keys],
                "column_name": "DUPLICATE_GROUP",
                "sql_value": json.dumps(left, default=str),
                "ch_value": json.dumps(right, default=str),
                "diff_type": "DUPLICATE_GROUP_MISMATCH",
            })
            continue

        if len(left) > 1:
            remaining = list(right)
            groups_match = True
            for sql_row in left:
                match_index = next((
                    index for index, ch_row in enumerate(remaining)
                    if all(equivalent(sql_row.get(column), ch_row.get(column))
                           for column in value_columns)
                ), None)
                if match_index is None:
                    groups_match = False
                    break
                remaining.pop(match_index)
            if not groups_match:
                result["cell_mismatches"] += 1
                result["row_mismatches"] += 1
                result["discrepancies"].append({
                    "table_name": result["table_name"],
                    "composite_key": label,
                    "_debug_key_values": [left[0].get(key) for key in primary_keys],
                    "column_name": "DUPLICATE_GROUP",
                    "sql_value": json.dumps(left, default=str),
                    "ch_value": json.dumps(right, default=str),
                    "diff_type": "DUPLICATE_GROUP_MISMATCH",
                })
            continue

        row_mismatched = False
        for column in value_columns:
            sql_value = left[0].get(column)
            ch_value = right[0].get(column)
            if equivalent(sql_value, ch_value):
                continue
            row_mismatched = True
            result["cell_mismatches"] += 1
            result["discrepancies"].append({
                "table_name": result["table_name"],
                "composite_key": label,
                "_debug_key_values": [left[0].get(key) for key in primary_keys],
                "column_name": column,
                "sql_value": sql_value,
                "ch_value": ch_value,
                "diff_type": "VALUE_MISMATCH",
            })
        if row_mismatched:
            result["row_mismatches"] += 1


def compare_rows(
    table_cfg, sql_rows, ch_rows, sql_columns=None, ch_columns=None,
    sql_query=None, clickhouse_query=None,
):
    table_name = table_cfg["table_name"]
    result = {
        "table_name": table_name,
        "status": "PASSED",
        "sql_rows": len(sql_rows),
        "ch_rows": len(ch_rows),
        "missing_in_ch": 0,
        "extra_in_ch": 0,
        "cell_mismatches": 0,
        "row_mismatches": 0,
        "warnings": [],
        "discrepancies": [],
        "manual_validation_rows": [],
    }

    sql_columns = sql_columns or (list(sql_rows[0]) if sql_rows else list(table_cfg.get("columns", [])))
    ch_columns = ch_columns or (list(ch_rows[0]) if ch_rows else sql_columns)
    missing_columns = set(sql_columns) - set(ch_columns)
    extra_columns = set(ch_columns) - set(sql_columns)
    if missing_columns:
        result["warnings"].append(f"Columns missing in ClickHouse: {sorted(missing_columns)}")
        for column in sorted(missing_columns):
            result["discrepancies"].append({
                "table_name": table_name,
                "composite_key": "",
                "column_name": column,
                "sql_value": "COLUMN PRESENT",
                "ch_value": "COLUMN MISSING",
                "diff_type": "COLUMN_MISSING_IN_TARGET",
            })
    if extra_columns:
        result["warnings"].append(f"Ignoring extra ClickHouse columns: {sorted(extra_columns)}")

    if table_cfg.get("primary_keys") is not None:
        _compare_rows_by_key(result, table_cfg, sql_rows, ch_rows, sql_columns, ch_columns)
        for discrepancy in result["discrepancies"]:
            key_values = discrepancy.pop("_debug_key_values", None)
            if key_values is None or sql_query is None or clickhouse_query is None:
                continue
            discrepancy["sql_debug_query"] = _debug_query(
                sql_query, table_cfg["primary_keys"], key_values, "sql"
            )
            discrepancy["clickhouse_debug_query"] = _debug_query(
                clickhouse_query, table_cfg["primary_keys"], key_values, "clickhouse"
            )
    else:
        shared_columns = [column for column in sql_columns if column in ch_columns]
        shared_row_count = min(len(sql_rows), len(ch_rows))
        for index in range(shared_row_count):
            sql_row = sql_rows[index]
            ch_row = ch_rows[index]
            row_mismatched = False
            for column in shared_columns:
                sql_value = sql_row.get(column)
                ch_value = ch_row.get(column)
                if equivalent(sql_value, ch_value):
                    continue
                row_mismatched = True
                result["cell_mismatches"] += 1
                result["discrepancies"].append({
                    "table_name": table_name,
                    "composite_key": f"row_position={index + 1}",
                    "column_name": column,
                    "sql_value": sql_value,
                    "ch_value": ch_value,
                    "diff_type": "VALUE_MISMATCH",
                })
            if row_mismatched:
                result["row_mismatches"] += 1

        for index in range(shared_row_count, len(sql_rows)):
            result["missing_in_ch"] += 1
            result["discrepancies"].append({
                "table_name": table_name,
                "composite_key": f"row_position={index + 1}",
                "column_name": "ALL",
                "sql_value": json.dumps(sql_rows[index], default=str),
                "ch_value": "MISSING",
                "diff_type": "MISSING_IN_TARGET",
            })

        for index in range(shared_row_count, len(ch_rows)):
            result["extra_in_ch"] += 1
            result["discrepancies"].append({
                "table_name": table_name,
                "composite_key": f"row_position={index + 1}",
                "column_name": "ALL",
                "sql_value": "MISSING",
                "ch_value": json.dumps(ch_rows[index], default=str),
                "diff_type": "EXTRA_IN_TARGET",
            })

    if (result["missing_in_ch"] or result["extra_in_ch"] or result["cell_mismatches"]
            or missing_columns):
        result["status"] = "FAILED"
    return result


def parse_chunk_boundary(value, name):
    if not isinstance(value, str):
        raise ValueError(f"chunking.{name} must be an ISO date/time string")
    try:
        boundary = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"chunking.{name} must be an ISO date/time string") from exc
    if boundary.tzinfo is not None:
        raise ValueError(f"chunking.{name} must not include a timezone; use the database session timezone")
    return boundary


def build_date_chunks(chunking):
    start = parse_chunk_boundary(chunking.get("start"), "start")
    end_exclusive = parse_chunk_boundary(chunking.get("end_exclusive"), "end_exclusive")
    chunk_size_days = chunking.get("chunk_size_days")
    if type(chunk_size_days) is not int or chunk_size_days <= 0:
        raise ValueError("chunking.chunk_size_days must be a positive integer")
    if end_exclusive <= start:
        raise ValueError("chunking.end_exclusive must be after chunking.start")

    chunks = []
    chunk_start = start
    while chunk_start < end_exclusive:
        chunk_end = min(chunk_start + timedelta(days=chunk_size_days), end_exclusive)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end
    return chunks


def build_table_query_chunks(table_cfg):
    sql_query = table_cfg["sql_query"]
    clickhouse_query = table_cfg["clickhouse_query"]
    chunking = table_cfg.get("chunking")
    if chunking is None:
        return [(None, None, sql_query, clickhouse_query)]
    if not isinstance(chunking, dict):
        raise ValueError("table chunking config must be a mapping")

    query_chunks = []
    for start, end in build_date_chunks(chunking):
        chunk_start = start.isoformat(sep=" ", timespec="auto")
        chunk_end = end.isoformat(sep=" ", timespec="auto")
        rendered_queries = []
        for source_name, query in (("sql_query", sql_query), ("clickhouse_query", clickhouse_query)):
            for placeholder in ("{{chunk_start}}", "{{chunk_end}}"):
                if placeholder not in query:
                    raise ValueError(
                        f"Chunked table '{table_cfg['table_name']}' {source_name} "
                        f"must contain {placeholder}"
                    )
            rendered_queries.append(
                query.replace("{{chunk_start}}", chunk_start).replace("{{chunk_end}}", chunk_end)
            )
        query_chunks.append((start, end, *rendered_queries))
    return query_chunks


def validate_bot_filters(bot_filters_config):
    if bot_filters_config is None:
        return {"bot_ids": [], "bot_reference_ids": [], "env": []}
    if not isinstance(bot_filters_config, dict):
        raise ValueError("filters must be a mapping")

    def _extract_scalars(keys):
        for k in keys:
            if k in bot_filters_config:
                val = bot_filters_config[k]
                if val is None:
                    return []
                if isinstance(val, (str, int)):
                    val_str = str(val).strip()
                    return [val] if val_str else []
                if not isinstance(val, list):
                    raise ValueError(f"filter '{k}' must be a list or scalar")
                if any(v is None or isinstance(v, (dict, list)) for v in val):
                    raise ValueError(f"filter '{k}' values must be non-null scalar values")
                return list(dict.fromkeys(val))
        return []

    bot_ids = _extract_scalars(["bot_ids", "botId", "botIds", "bot_id"])
    bot_reference_ids = _extract_scalars([
        "bot_reference_ids", "botReferenceId", "botReferenceIds", "bot_reference_id"
    ])
    env = _extract_scalars(["env", "envs", "environment"])

    return {"bot_ids": bot_ids, "bot_reference_ids": bot_reference_ids, "env": env}


def _query_clause_positions(query):
    words = []
    index = 0
    depth = 0
    quote = None
    line_comment = False
    block_comment = False
    while index < len(query):
        char = query[index]
        next_char = query[index + 1] if index + 1 < len(query) else ""
        if line_comment:
            if char == "\n":
                line_comment = False
            index += 1
            continue
        if block_comment:
            if char == "*" and next_char == "/":
                block_comment = False
                index += 2
            else:
                index += 1
            continue
        if quote:
            if char == "\\" and quote != "`":
                index += 2
                continue
            if char == quote:
                if next_char == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in ("'", '"', "`"):
            quote = char
            index += 1
            continue
        if char == "-" and next_char == "-":
            line_comment = True
            index += 2
            continue
        if char == "/" and next_char == "*":
            block_comment = True
            index += 2
            continue
        if char == "(":
            depth += 1
            index += 1
            continue
        if char == ")":
            depth = max(0, depth - 1)
            index += 1
            continue
        if depth == 0 and (char.isalpha() or char == "_"):
            start = index
            index += 1
            while index < len(query) and (query[index].isalnum() or query[index] == "_"):
                index += 1
            words.append((query[start:index].upper(), start, index))
            continue
        index += 1

    clauses = []
    for word_index, (word, start, end) in enumerate(words):
        if word in {"WHERE", "PREWHERE", "HAVING", "LIMIT", "SETTINGS", "FORMAT", "UNION"}:
            clauses.append((word, start))
        elif word in {"ORDER", "GROUP"} and word_index + 1 < len(words):
            next_word, _, next_end = words[word_index + 1]
            if next_word == "BY":
                clauses.append((f"{word} BY", start))
    return clauses


def add_bot_filters_to_query(query, filter_columns, dialect):
    if not filter_columns:
        return query
    original = query.rstrip()
    has_semicolon = original.endswith(";")
    if has_semicolon:
        original = original[:-1].rstrip()

    predicates = []
    for column, values in filter_columns:
        if not values:
            continue
        identifier = f"`{column.replace('`', '``')}`"
        escaped_values = []
        for value in values:
            val_str = str(value)
            if dialect == "sql":
                escaped = val_str.replace("\\", "\\\\").replace("'", "''")
            else:
                escaped = val_str.replace("\\", "\\\\").replace("'", "\\'")
            escaped_values.append(f"'{escaped}'")
        if len(escaped_values) == 1:
            predicates.append(f"{identifier} = {escaped_values[0]}")
        else:
            predicates.append(f"{identifier} IN ({', '.join(escaped_values)})")

    if not predicates:
        return query

    condition = "(" + " AND ".join(predicates) + ")"

    clauses = _query_clause_positions(original)
    where_pos = next((pos for name, pos in clauses if name == "WHERE"), None)

    if where_pos is not None:
        insertion_candidates = [
            pos for name, pos in clauses
            if name != "WHERE" and pos > where_pos
        ]
        insertion = min(insertion_candidates, default=len(original))
        conjunction = " AND "
        filtered = (
            original[:insertion].rstrip()
            + conjunction
            + condition + " "
            + original[insertion:].lstrip()
        )
    else:
        prewhere_pos = next((pos for name, pos in clauses if name == "PREWHERE"), None)
        insertion_candidates = [
            pos for name, pos in clauses
            if name not in ("PREWHERE",) and (prewhere_pos is None or pos > prewhere_pos)
        ]
        insertion = min(insertion_candidates, default=len(original))
        conjunction = " WHERE "
        filtered = (
            original[:insertion].rstrip()
            + conjunction
            + condition + " "
            + original[insertion:].lstrip()
        )

    return filtered.rstrip() + (";" if has_semicolon else "")


def select_bot_filter_columns(bot_filters, sql_columns, ch_columns):
    configured = []
    if bot_filters.get("bot_ids"):
        configured.append(("botId", ("botid", "bot_id"), bot_filters["bot_ids"]))
    if bot_filters.get("bot_reference_ids"):
        configured.append((
            "botReferenceId",
            ("botreferenceid", "bot_reference_id"),
            bot_filters["bot_reference_ids"],
        ))
    if bot_filters.get("env"):
        configured.append(("env", ("env", "environment"), bot_filters["env"]))

    sql_lookup = {col.lower(): col for col in sql_columns}
    ch_lookup = {col.lower(): col for col in ch_columns}

    selected = []
    skipped_reasons = []

    for label, aliases, values in configured:
        sql_match = next((sql_lookup[alias] for alias in aliases if alias in sql_lookup), None)
        ch_match = next((ch_lookup[alias] for alias in aliases if alias in ch_lookup), None)

        if sql_match and ch_match:
            selected.append((sql_match, ch_match, values))
        elif sql_match and not ch_match:
            skipped_reasons.append(f"{label} present in Aurora ('{sql_match}') but missing in ClickHouse")
        elif ch_match and not sql_match:
            skipped_reasons.append(f"{label} present in ClickHouse ('{ch_match}') but missing in Aurora")

    warning = None
    if not selected:
        if skipped_reasons:
            warning = "Filters not applied: " + "; ".join(skipped_reasons)
        else:
            configured_names = ", ".join(label for label, _, _ in configured)
            warning = f"Filters not applied: none of the configured columns ({configured_names}) found in query results"

    return selected, warning


def combine_chunk_summaries(table_name, chunk_summaries):
    combined = {
        "table_name": table_name,
        "status": "PASSED",
        "sql_rows": 0,
        "ch_rows": 0,
        "missing_in_ch": 0,
        "extra_in_ch": 0,
        "cell_mismatches": 0,
        "row_mismatches": 0,
        "warnings": [],
        "discrepancies": [],
        "manual_validation_rows": [],
    }
    for start, end, summary in chunk_summaries:
        if summary.get("count_only"):
            combined["count_only"] = True
        if summary["status"] != "PASSED":
            combined["status"] = "FAILED"
        for key in ("sql_rows", "ch_rows", "missing_in_ch", "extra_in_ch",
                    "cell_mismatches", "row_mismatches"):
            combined[key] += summary[key]
        for warning in summary["warnings"]:
            if warning not in combined["warnings"]:
                combined["warnings"].append(warning)
        for discrepancy in summary["discrepancies"]:
            entry = {**discrepancy}
            if start is not None:
                entry["chunk_start"] = (
                    start.isoformat(sep=" ", timespec="auto")
                    if hasattr(start, "isoformat")
                    else str(start)
                )
            if end is not None:
                entry["chunk_end_exclusive"] = (
                    end.isoformat(sep=" ", timespec="auto")
                    if hasattr(end, "isoformat")
                    else str(end)
                )
            combined["discrepancies"].append(entry)
        combined["manual_validation_rows"].extend(summary["manual_validation_rows"])
    return combined


def finalize_table_summary(
    table_cfg,
    query_chunks,
    chunk_summaries,
    output_dir,
    bot_filter_warning=None,
    interruption_reason=None,
    count_only=False,
    applied_filters=None,
):
    raw_table_name = table_cfg["table_name"]
    is_chunked = bool(query_chunks and query_chunks[0][0] is not None)
    total_chunks = len(query_chunks) if query_chunks else 1

    if not chunk_summaries:
        summary = {
            "table_name": raw_table_name,
            "status": "INTERRUPTED" if interruption_reason else "FAILED",
            "sql_rows": 0,
            "ch_rows": 0,
            "missing_in_ch": 0,
            "extra_in_ch": 0,
            "cell_mismatches": 0,
            "row_mismatches": 0,
            "warnings": [],
            "discrepancies": [],
            "manual_validation_rows": [],
            "count_only": count_only,
        }
    elif not is_chunked:
        summary = dict(chunk_summaries[0][2])
    else:
        summary = combine_chunk_summaries(raw_table_name, chunk_summaries)

    if count_only:
        summary["count_only"] = True

    if applied_filters:
        summary["applied_filters"] = list(applied_filters)
        filter_str = "; ".join(f"{col}={vals}" for col, vals in applied_filters)
        for disc in summary["discrepancies"]:
            if "applied_filters" not in disc:
                disc["applied_filters"] = filter_str

    if is_chunked:
        summary["completed_chunks"] = len(chunk_summaries)
        summary["total_chunks"] = total_chunks

    if bot_filter_warning and bot_filter_warning not in summary["warnings"]:
        summary["warnings"].append(bot_filter_warning)

    if interruption_reason:
        summary["status"] = "INTERRUPTED"
        if is_chunked:
            interruption_msg = (
                f"Validation interrupted after {len(chunk_summaries)} of {total_chunks} chunks: "
                f"{interruption_reason}"
            )
        else:
            interruption_msg = f"Validation interrupted: {interruption_reason}"
        if interruption_msg not in summary["warnings"]:
            summary["warnings"].append(interruption_msg)

    summary["report_file"] = write_report(summary, output_dir)
    summary["manual_validation_report_file"] = write_manual_validation_report(
        summary, output_dir
    )

    applied_filter_str = (
        "; ".join(f"{col}={vals}" for col, vals in applied_filters)
        if applied_filters else ""
    )

    executed_queries = []
    if query_chunks:
        for chunk_index, (start, end, sql_q, ch_q) in enumerate(query_chunks, 1):
            chunk_label = (
                f"chunk {chunk_index}/{total_chunks} [{start:%Y-%m-%d %H:%M:%S}, {end:%Y-%m-%d %H:%M:%S})"
                if start is not None
                else (f"chunk {chunk_index}/{total_chunks}" if total_chunks > 1 else "Unchunked")
            )
            chunk_start_str = (
                start.isoformat(sep=" ", timespec="auto")
                if hasattr(start, "isoformat")
                else (str(start) if start is not None else "")
            )
            chunk_end_str = (
                end.isoformat(sep=" ", timespec="auto")
                if hasattr(end, "isoformat")
                else (str(end) if end is not None else "")
            )

            chunk_sum = (
                chunk_summaries[chunk_index - 1][2]
                if chunk_index - 1 < len(chunk_summaries)
                else None
            )

            if chunk_sum is not None:
                chunk_status = chunk_sum.get("status", "PASSED")
                sql_rows = chunk_sum.get("sql_rows", 0)
                ch_rows = chunk_sum.get("ch_rows", 0)
            elif interruption_reason and chunk_index - 1 == len(chunk_summaries):
                chunk_status = "INTERRUPTED"
                sql_rows = ""
                ch_rows = ""
            else:
                chunk_status = "NOT_EXECUTED"
                sql_rows = ""
                ch_rows = ""

            sql_actual = build_count_query(sql_q) if count_only else str(sql_q).strip()
            ch_actual = build_count_query(ch_q) if count_only else str(ch_q).strip()

            executed_queries.append({
                "table_name": raw_table_name,
                "chunk_index": chunk_index,
                "chunk_label": chunk_label,
                "chunk_start": chunk_start_str,
                "chunk_end_exclusive": chunk_end_str,
                "evaluation_mode": "COUNT_ONLY_SMOKE_TEST" if count_only else "FULL_VALIDATION",
                "status": chunk_status,
                "sql_rows": sql_rows,
                "ch_rows": ch_rows,
                "applied_filters": applied_filter_str,
                "sql_query": sql_actual,
                "clickhouse_query": ch_actual,
                "sql_base_query": str(sql_q).strip(),
                "clickhouse_base_query": str(ch_q).strip(),
            })

    summary["executed_queries"] = executed_queries
    summary["queries_report_file"] = write_queries_report(summary, output_dir)
    return summary


def load_table_configs(config, config_path):
    tables = config.get("tables", [])
    queries_file = config.get("queries_file")
    if queries_file is None:
        return tables

    query_path = Path(config_path).resolve().parent / queries_file
    with query_path.open(encoding="utf-8") as source:
        query_config = yaml.safe_load(source) or {}
    queries = query_config.get("queries")
    if not isinstance(queries, dict):
        raise ValueError(f"{query_path} must contain a 'queries' mapping")

    loaded_tables = []
    for table_cfg in tables:
        query_key = table_cfg.get("query_key", table_cfg["table_name"])
        query_definition = queries.get(query_key)
        if not isinstance(query_definition, dict):
            raise ValueError(f"No query definition '{query_key}' found in {query_path}")
        missing_queries = {"sql_query", "clickhouse_query"} - query_definition.keys()
        if missing_queries:
            raise ValueError(
                f"Query definition '{query_key}' is missing: {', '.join(sorted(missing_queries))}"
            )
        loaded_tables.append({**query_definition, **table_cfg})
    return loaded_tables


def connect_aurora(config):
    ssl_setting = config.get("ssl", True)
    if isinstance(ssl_setting, dict):
        ssl_options = {"verify_mode": "required", **ssl_setting}
    else:
        ssl_options = {"verify_mode": "required"} if ssl_setting else None
    return pymysql.connect(
        host=config["host"],
        port=int(config.get("port", 3306)),
        user=config["user"],
        password=os.environ[config["password_env"]],
        database=config["database"],
        cursorclass=pymysql.cursors.DictCursor,
        ssl=ssl_options,
        connect_timeout=int(config.get("connect_timeout", 10)),
        read_timeout=int(config.get("read_timeout", 300)),
    )


def connect_clickhouse(config):
    return clickhouse_connect.get_client(
        host=config["host"],
        port=int(config.get("port", 8123)),
        username=config["user"],
        password=os.environ[config["password_env"]],
        database=config.get("database"),
        secure=bool(config.get("secure", False)),
        connect_timeout=int(config.get("connect_timeout", 10)),
        send_receive_timeout=int(config.get("read_timeout", 300)),
    )


def fetch_aurora(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = list(cursor.fetchall())
        columns = [column[0] for column in cursor.description or []]
        return rows, columns


def fetch_clickhouse(client, query):
    response = client.query(query)
    return (
        [dict(zip(response.column_names, row)) for row in response.result_rows],
        list(response.column_names),
    )


def query_result_columns_aurora(connection, query):
    clean = query.strip()
    if clean.endswith(";"):
        clean = clean[:-1].rstrip()
    probe_query = f"SELECT * FROM ({clean}) AS validator_columns LIMIT 0"
    with connection.cursor() as cursor:
        cursor.execute(probe_query)
        return [column[0] for column in cursor.description or []]


def query_result_columns_clickhouse(client, query):
    clean = query.strip()
    if clean.endswith(";"):
        clean = clean[:-1].rstrip()
    probe_query = f"SELECT * FROM ({clean}) AS validator_columns LIMIT 0"
    response = client.query(probe_query)
    return list(response.column_names)


def build_count_query(query):
    clean = query.strip()
    while clean.endswith(";"):
        clean = clean[:-1].rstrip()
    return f"SELECT COUNT(*) AS total_count FROM (\n{clean}\n) AS validator_count"


def fetch_count_aurora(connection, query):
    count_query = build_count_query(query)
    with connection.cursor() as cursor:
        cursor.execute(count_query)
        row = cursor.fetchone()
        if not row:
            return 0
        val = list(row.values())[0]
        return int(val) if val is not None else 0


def fetch_count_clickhouse(client, query):
    count_query = build_count_query(query)
    response = client.query(count_query)
    if not response.result_rows or not response.result_rows[0]:
        return 0
    val = response.result_rows[0][0]
    return int(val) if val is not None else 0


def compare_counts(
    table_cfg, sql_count, ch_count, chunk_label="", sql_query=None, clickhouse_query=None
):
    table_name = table_cfg["table_name"]
    is_match = (sql_count == ch_count)
    diff = ch_count - sql_count
    missing_in_ch = max(0, sql_count - ch_count)
    extra_in_ch = max(0, ch_count - sql_count)

    result = {
        "table_name": table_name,
        "status": "PASSED" if is_match else "FAILED",
        "sql_rows": sql_count,
        "ch_rows": ch_count,
        "missing_in_ch": missing_in_ch,
        "extra_in_ch": extra_in_ch,
        "cell_mismatches": 0,
        "row_mismatches": abs(diff),
        "warnings": [],
        "discrepancies": [],
        "manual_validation_rows": [],
        "count_only": True,
    }
    if not is_match:
        discrepancy = {
            "table_name": table_name,
            "composite_key": chunk_label or "TOTAL_COUNT",
            "column_name": "RECORD_COUNT",
            "sql_value": str(sql_count),
            "ch_value": str(ch_count),
            "diff_type": "COUNT_MISMATCH",
            "evaluation_mode": "COUNT_ONLY_SMOKE_TEST",
        }
        if sql_query:
            discrepancy["sql_count_query"] = build_count_query(sql_query)
        if clickhouse_query:
            discrepancy["clickhouse_count_query"] = build_count_query(clickhouse_query)
        result["discrepancies"].append(discrepancy)
    return result


def is_count_only(table_cfg, config=None):
    for key in ("count_only", "smoke_test", "total_records_smoke_test"):
        if key in table_cfg:
            return bool(table_cfg[key])
    if config:
        for key in ("count_only", "smoke_test", "total_records_smoke_test"):
            if key in config:
                return bool(config[key])
    return False


def write_report(summary, output_dir):
    if not summary["discrepancies"]:
        return None
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, f"{summary['table_name']}_diff_report.csv")
    with open(report_path, "w", encoding="utf-8", newline="") as report:
        fieldnames = list(dict.fromkeys(
            key for discrepancy in summary["discrepancies"] for key in discrepancy
        ))
        writer = csv.DictWriter(report, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary["discrepancies"])
    return report_path


def write_manual_validation_report(summary, output_dir):
    rows = summary.get("manual_validation_rows", [])
    if not rows:
        return None
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(
        output_dir, f"{summary['table_name']}_null_primary_key_rows_report.csv"
    )
    fieldnames = ["row_source"] + list(dict.fromkeys(
        key for row in rows for key in row if key != "row_source"
    ))
    with open(report_path, "w", encoding="utf-8", newline="") as report:
        writer = csv.DictWriter(report, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return report_path


def write_queries_report(summary, output_dir):
    queries = summary.get("executed_queries", [])
    if not queries:
        return None
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, f"{summary['table_name']}_queries_report.csv")
    fieldnames = list(dict.fromkeys(
        key for item in queries for key in item
    ))
    with open(report_path, "w", encoding="utf-8", newline="") as report:
        writer = csv.DictWriter(report, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(queries)
    return report_path


def write_run_queries_report(summaries, output_dir):
    all_queries = []
    for summary in summaries:
        all_queries.extend(summary.get("executed_queries", []))
    if not all_queries:
        return None
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "executed_queries_report.csv")
    fieldnames = list(dict.fromkeys(
        key for item in all_queries for key in item
    ))
    with open(report_path, "w", encoding="utf-8", newline="") as report:
        writer = csv.DictWriter(report, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_queries)
    return report_path


def print_table_summary(summary, output):
    status = summary["status"]
    status_style = "green" if status == "PASSED" else "red"
    is_smoke = bool(summary.get("count_only"))
    applied_filters = summary.get("applied_filters", [])
    is_filtered = bool(applied_filters)

    metrics = Table.grid(padding=(0, 2))
    metrics.add_column(style="dim")
    metrics.add_column()
    metrics.add_row("Overall status", f"[bold {status_style}]{escape(status)}[/]")
    if is_smoke:
        metrics.add_row(
            "Evaluation mode",
            "[bold black on yellow] ⚡ Count Only (Smoke Test) ⚡ [/bold black on yellow]",
        )
        metrics.add_row(
            "Verification depth",
            "[bold yellow]Row counts only (cell contents & primary keys NOT verified)[/bold yellow]",
        )
    if is_filtered:
        metrics.add_row(
            "Filter status",
            "[bold black on magenta] 🎯 FILTERED SUBSET 🎯 [/bold black on magenta]",
        )
        filter_detail_strs = []
        for col, vals in applied_filters:
            val_preview = ", ".join(repr(v) for v in vals[:3])
            if len(vals) > 3:
                val_preview += f", ... (+{len(vals) - 3} more)"
            filter_detail_strs.append(f"{col}: [{val_preview}]")
        metrics.add_row("Applied filters", " | ".join(filter_detail_strs))
        metrics.add_row(
            "Data scope",
            "[bold magenta]Scoped subset only (unmatched table records excluded)[/bold magenta]",
        )
    if "completed_chunks" in summary and "total_chunks" in summary:
        metrics.add_row(
            "Chunks evaluated",
            f"{summary['completed_chunks']} of {summary['total_chunks']}",
        )
    metrics.add_row("Aurora rows", str(summary["sql_rows"]))
    metrics.add_row("ClickHouse rows", str(summary["ch_rows"]))
    metrics.add_row("Missing in ClickHouse", str(summary["missing_in_ch"]))
    metrics.add_row("Extra in ClickHouse", str(summary["extra_in_ch"]))
    if not is_smoke:
        metrics.add_row("Rows with mismatches", str(summary["row_mismatches"]))
        metrics.add_row("Value mismatches", str(summary["cell_mismatches"]))
    else:
        metrics.add_row("Count diff", str(summary["row_mismatches"]))
    report_file = summary.get("report_file")
    report_text = escape(str(report_file)) if report_file else "Not needed (no differences)"
    metrics.add_row("Diff report", report_text)
    manual_report = summary.get("manual_validation_report_file")
    if manual_report:
        metrics.add_row("Null primary-key rows", escape(str(manual_report)))
    queries_report = summary.get("queries_report_file")
    if queries_report:
        metrics.add_row("Queries report", escape(str(queries_report)))

    badges = []
    if is_smoke:
        badges.append("[bold black on yellow] ⚡ COUNT-ONLY SMOKE TEST ⚡ [/bold black on yellow]")
    if is_filtered:
        badges.append("[bold black on magenta] 🎯 FILTERED 🎯 [/bold black on magenta]")
    badge_str = (" " + " ".join(badges)) if badges else ""
    panel_title = f"[bold]Table: {escape(str(summary['table_name']))}[/bold]{badge_str}"

    subtitles = []
    if is_smoke:
        subtitles.append("⚡ Fast record count test only • Individual row data was NOT verified")
    if is_filtered:
        filter_labels = ", ".join(f"{col} ({len(vals)})" for col, vals in applied_filters)
        subtitles.append(f"🎯 Filtered by {filter_labels} • Whole table was NOT evaluated")

    panel_subtitle = (
        f"[bold {'yellow' if is_smoke else 'magenta'}]⚠ {' • '.join(subtitles)} ⚠[/bold {'yellow' if is_smoke else 'magenta'}]"
        if subtitles else None
    )

    output.print(Panel(
        metrics,
        title=panel_title,
        subtitle=panel_subtitle,
        border_style=status_style,
    ))
    if is_smoke:
        output.print(
            "[bold yellow]⚡ NOTE:[/] [yellow]This evaluation was a [bold]COUNT-ONLY SMOKE TEST[/bold]. "
            "Total record counts were compared, but row data, primary keys, and column values were NOT checked.[/yellow]"
        )
    if is_filtered:
        filter_summary = ", ".join(f"{col}={vals}" for col, vals in applied_filters)
        output.print(
            f"[bold magenta]🎯 NOTE:[/] [magenta]This evaluation was [bold]FILTERED[/bold] by ({filter_summary}). "
            "Only records matching these filters were evaluated. Unmatched rows were excluded. "
            "Run without filters to validate the entire table.[/magenta]"
        )
    for warning in summary["warnings"]:
        output.print(f"[yellow]Warning:[/yellow] {escape(str(warning))}")


def print_run_summary(summaries, output, run_queries_report=None):
    passed = sum(item["status"] == "PASSED" for item in summaries)
    failed = len(summaries) - passed
    verdict = "PASSED" if failed == 0 and len(summaries) > 0 else "FAILED"
    verdict_style = "green" if verdict == "PASSED" else "red"
    count_only_count = sum(1 for item in summaries if item.get("count_only"))
    full_eval_count = len(summaries) - count_only_count
    filtered_count = sum(1 for item in summaries if item.get("applied_filters"))
    unfiltered_count = len(summaries) - filtered_count

    metrics = Table.grid(padding=(0, 2))
    metrics.add_column(style="dim")
    metrics.add_column()
    metrics.add_row("Tables evaluated", str(len(summaries)))
    if count_only_count > 0:
        metrics.add_row("Full validation tables", str(full_eval_count))
        metrics.add_row("Count-only smoke tests", f"[bold yellow]{count_only_count}[/bold yellow]")
    if filtered_count > 0:
        metrics.add_row("Unfiltered tables", str(unfiltered_count))
        metrics.add_row("Filtered tables", f"[bold magenta]{filtered_count}[/bold magenta]")
    metrics.add_row("Tables passed", f"[green]{passed}[/green]")
    metrics.add_row("Tables failed", f"[{verdict_style}]{failed}[/{verdict_style}]")
    metrics.add_row("Verdict", f"[bold {verdict_style}]{verdict}[/]")
    if run_queries_report:
        metrics.add_row("Queries report", escape(str(run_queries_report)))

    title_tags = []
    if count_only_count == len(summaries) and len(summaries) > 0:
        title_tags.append("[bold yellow]⚡ (COUNT-ONLY SMOKE TEST)[/bold yellow]")
    elif count_only_count > 0:
        title_tags.append("[bold yellow]⚡ (MIXED RUN)[/bold yellow]")

    if filtered_count == len(summaries) and len(summaries) > 0:
        title_tags.append("[bold magenta]🎯 (FILTERED RUN)[/bold magenta]")
    elif filtered_count > 0:
        title_tags.append(f"[bold magenta]🎯 ({filtered_count} FILTERED)[/bold magenta]")

    title = "Final Run Summary" + ((" " + " ".join(title_tags)) if title_tags else "")

    subtitle_notes = []
    if count_only_count == len(summaries) and len(summaries) > 0:
        subtitle_notes.append("Fast record count test only • Individual row data was NOT verified")
    elif count_only_count > 0:
        subtitle_notes.append(f"{count_only_count} of {len(summaries)} table(s) evaluated in count-only smoke test mode")

    if filtered_count == len(summaries) and len(summaries) > 0:
        subtitle_notes.append("All tables evaluated with active bot/environment filters")
    elif filtered_count > 0:
        subtitle_notes.append(f"{filtered_count} of {len(summaries)} table(s) evaluated with active bot/environment filters")

    subtitle = f"[bold {'yellow' if count_only_count > 0 else 'magenta'}]⚠ {' • '.join(subtitle_notes)} ⚠[/bold {'yellow' if count_only_count > 0 else 'magenta'}]" if subtitle_notes else None

    output.print(Panel(metrics, title=title, subtitle=subtitle, border_style=verdict_style))
    if count_only_count > 0:
        if count_only_count == len(summaries):
            output.print(
                "[bold yellow]⚡ [COUNT-ONLY SMOKE TEST RUN][/bold yellow] [yellow]All evaluated tables were tested for total record counts only. "
                "Row values, column matches, and missing IDs were not validated.[/yellow]"
            )
        else:
            output.print(
                f"[bold yellow]⚡ [MIXED VALIDATION RUN][/bold yellow] [yellow]{count_only_count} table(s) were tested with count-only smoke tests. "
                "Row data was not validated for those tables.[/yellow]"
            )
    if filtered_count > 0:
        if filtered_count == len(summaries):
            output.print(
                "[bold magenta]🎯 [FILTERED RUN][/bold magenta] [magenta]All evaluated tables had active bot/environment filters applied. "
                "Unmatched records in those tables were excluded from validation.[/magenta]"
            )
        else:
            output.print(
                f"[bold magenta]🎯 [FILTERED RUN][/bold magenta] [magenta]{filtered_count} table(s) had active bot/environment filters applied. "
                "Unmatched records in those tables were excluded from validation.[/magenta]"
            )
    if run_queries_report:
        output.print(
            f"[bold cyan]📋 Executed queries report saved to:[/] [cyan]{escape(str(run_queries_report))}[/cyan]"
        )


def resolve_environment(config):
    environment = config.get("environment", "qa")
    environments = config.get("environments")
    if not isinstance(environment, str) or not isinstance(environments, dict):
        raise ValueError("Configure environment as 'qa' or 'prod' and provide environments")
    if environment not in ("qa", "prod"):
        raise ValueError("environment must be 'qa' or 'prod'")
    selected = environments.get(environment)
    if not isinstance(selected, dict):
        raise ValueError(f"Missing connection settings for environment '{environment}'")
    for database in ("aurora", "clickhouse"):
        if not isinstance(selected.get(database), dict):
            raise ValueError(
                f"Missing {database} connection settings for environment '{environment}'"
            )
    return environment, selected


def run(config_path):
    load_dotenv(Path(config_path).resolve().parent / ".env")
    with open(config_path, encoding="utf-8") as source:
        config = yaml.safe_load(source) or {}
    environment, connections = resolve_environment(config)

    output_dir = os.path.expanduser(config.get("output_dir", "./comparison_reports"))
    global_bot_filters = validate_bot_filters(
        config.get("filters") or config.get("bot_filters") or {
            "bot_ids": config.get("bot_ids", []),
            "bot_reference_ids": config.get("bot_reference_ids", []),
            "env": config.get("env", []),
        }
    )
    has_global_filters = any(global_bot_filters.values())
    tables = load_table_configs(config, config_path)
    if not tables:
        raise ValueError("No tables configured. Add table queries to config.yaml")
    table_plans = [
        (table_cfg, build_table_query_chunks(table_cfg))
        for table_cfg in tables
    ]

    all_count_only = all(is_count_only(table_cfg, config) for table_cfg, _ in table_plans)
    any_count_only = any(is_count_only(table_cfg, config) for table_cfg, _ in table_plans)
    any_table_filters = any(
        any(validate_bot_filters(t.get("filters") or t.get("bot_filters")).values())
        for t in tables
        if ("filters" in t or "bot_filters" in t)
    )

    banner_lines = [
        "[bold]Live DB Validator[/bold]",
        "[dim]Aurora MySQL -> ClickHouse[/dim]",
        f"[bold {'red' if environment == 'prod' else 'cyan'}]Environment: {environment.upper()}[/bold {'red' if environment == 'prod' else 'cyan'}]",
    ]
    if all_count_only:
        banner_lines.append("[bold black on yellow] ⚡ MODE: COUNT-ONLY SMOKE TEST (ALL TABLES) ⚡ [/bold black on yellow]")
        banner_lines.append("[yellow]Fast record count verification only • Individual row data will NOT be fetched[/yellow]")
    elif any_count_only:
        banner_lines.append("[bold yellow]⚡ MODE: MIXED (Some tables configured for Count-Only Smoke Test)[/bold yellow]")

    if has_global_filters:
        filter_parts = []
        if global_bot_filters.get("bot_ids"):
            filter_parts.append(f"bot_ids: {len(global_bot_filters['bot_ids'])}")
        if global_bot_filters.get("bot_reference_ids"):
            filter_parts.append(f"bot_reference_ids: {len(global_bot_filters['bot_reference_ids'])}")
        if global_bot_filters.get("env"):
            filter_parts.append(f"env: {', '.join(str(e) for e in global_bot_filters['env'])}")
        banner_lines.append(
            f"[bold black on magenta] 🎯 GLOBAL FILTERS ACTIVE: {', '.join(filter_parts)} 🎯 [/bold black on magenta]"
        )
        banner_lines.append(
            "[magenta]Scoped dataset evaluation • Records not matching filters will be excluded[/magenta]"
        )
    elif any_table_filters:
        banner_lines.append("[bold magenta]🎯 TABLE-SPECIFIC FILTERS CONFIGURED[/bold magenta]")

    console.print(Panel.fit(
        "\n".join(banner_lines),
        border_style="yellow" if all_count_only else ("magenta" if has_global_filters else ("red" if environment == "prod" else "cyan")),
    ))
    aurora = clickhouse = None
    summaries = []
    run_interrupted = False
    user_interrupted = False

    try:
        with console.status("[cyan]Connecting to Aurora MySQL...[/cyan]", spinner="dots"):
            aurora = connect_aurora(connections["aurora"])
        console.print("[green]Connected[/green] to Aurora MySQL")
        with console.status("[cyan]Connecting to ClickHouse...[/cyan]", spinner="dots"):
            clickhouse = connect_clickhouse(connections["clickhouse"])
        console.print("[green]Connected[/green] to ClickHouse")

        try:
            for table_cfg, query_chunks in table_plans:
                raw_table_name = table_cfg["table_name"]
                table_name = escape(str(raw_table_name))
                count_only = is_count_only(table_cfg, config)
                table_bot_filters = (
                    validate_bot_filters(table_cfg.get("filters") or table_cfg.get("bot_filters"))
                    if ("filters" in table_cfg or "bot_filters" in table_cfg)
                    else global_bot_filters
                )
                has_bot_filters = any(table_bot_filters.values())
                bot_filter_warning = None
                selected_filters = []

                if has_bot_filters:
                    try:
                        first_sql_query, first_clickhouse_query = query_chunks[0][2:4]
                        sql_columns = query_result_columns_aurora(aurora, first_sql_query)
                        ch_columns = query_result_columns_clickhouse(clickhouse, first_clickhouse_query)
                        selected_filters, bot_filter_warning = select_bot_filter_columns(
                            table_bot_filters, sql_columns, ch_columns
                        )
                        if selected_filters:
                            sql_filter_columns = [(s_col, vals) for s_col, _, vals in selected_filters]
                            ch_filter_columns = [(c_col, vals) for _, c_col, vals in selected_filters]
                            query_chunks = [
                                (
                                    start,
                                    end,
                                    add_bot_filters_to_query(sql_query, sql_filter_columns, "sql"),
                                    add_bot_filters_to_query(clickhouse_query, ch_filter_columns, "clickhouse"),
                                )
                                for start, end, sql_query, clickhouse_query in query_chunks
                            ]
                    except KeyboardInterrupt:
                        run_interrupted = True
                        user_interrupted = True
                        console.print(
                            f"\n[bold yellow]Validation manually interrupted during filter probing for {table_name}[/bold yellow]"
                        )
                        break
                    except Exception as exc:
                        bot_filter_warning = f"Could not probe/apply filters for {table_name}: {exc}"

                applied_filters_for_summary = [(s_col, vals) for s_col, _, vals in selected_filters]

                rule_badges = []
                if count_only:
                    rule_badges.append("[bold black on yellow] ⚡ COUNT-ONLY SMOKE TEST ⚡ [/bold black on yellow]")
                if selected_filters:
                    rule_badges.append("[bold black on magenta] 🎯 FILTERED 🎯 [/bold black on magenta]")

                rule_prefix = (" ".join(rule_badges) + " ") if rule_badges else ""
                rule_style = "yellow" if count_only else ("magenta" if selected_filters else "cyan")
                table_title_style = "yellow" if count_only else ("magenta" if selected_filters else "cyan")
                console.rule(
                    f"{rule_prefix}[bold {table_title_style}]Table: {table_name}[/bold {table_title_style}]",
                    style=rule_style,
                )

                if count_only:
                    console.print(
                        "[yellow]⚡ Mode: [bold]COUNT ONLY (SMOKE TEST)[/bold] "
                        "— Fast count validation via SELECT COUNT(*). Row values and keys are NOT checked.[/yellow]"
                    )
                if selected_filters:
                    filter_items = [f"{s_col}={vals}" for s_col, _, vals in selected_filters]
                    console.print(
                        f"[bold magenta]🎯 Active Filters:[/] [bold]{' | '.join(filter_items)}[/bold] "
                        f"[magenta](Evaluating scoped subset; unmatched table rows excluded)[/magenta]"
                    )
                elif bot_filter_warning:
                    console.print(f"[yellow]Warning:[/yellow] {escape(bot_filter_warning)}")

                chunk_summaries = []
                table_interrupted = False
                tags_list = []
                if count_only:
                    tags_list.append("[bold yellow][COUNT ONLY][/bold yellow]")
                if selected_filters:
                    tags_list.append("[bold magenta][FILTERED][/bold magenta]")
                prefix_tags = (" ".join(tags_list) + " ") if tags_list else ""

                for chunk_index, (start, end, sql_query, clickhouse_query) in enumerate(query_chunks, 1):
                    if start is not None:
                        chunk_label = (
                            f"chunk {chunk_index}/{len(query_chunks)} "
                            f"[{start:%Y-%m-%d %H:%M:%S}, {end:%Y-%m-%d %H:%M:%S})"
                        )
                        console.print(f"{prefix_tags}[bold cyan]{chunk_label}[/bold cyan]")
                    else:
                        chunk_label = ""
                    progress = f"{chunk_label} " if chunk_label else ""
                    try:
                        if count_only:
                            with console.status(
                                f"[cyan]{progress}Counting Aurora rows for {table_name}...[/cyan]",
                                spinner="dots",
                            ):
                                sql_count = fetch_count_aurora(aurora, sql_query)
                            with console.status(
                                f"[cyan]{progress}Counting ClickHouse rows for {table_name}...[/cyan]",
                                spinner="dots",
                            ):
                                ch_count = fetch_count_clickhouse(clickhouse, clickhouse_query)

                            diff = ch_count - sql_count
                            if diff == 0:
                                console.print(
                                    f"{prefix_tags}[green]Aurora count: {sql_count} | ClickHouse count: {ch_count} (MATCH)[/green]"
                                )
                            else:
                                diff_str = f"+{diff}" if diff > 0 else str(diff)
                                console.print(
                                    f"{prefix_tags}[bold red]Aurora count: {sql_count} | ClickHouse count: {ch_count} "
                                    f"(MISMATCH: {diff_str})[/bold red]"
                                )

                            chunk_summary = compare_counts(
                                table_cfg,
                                sql_count,
                                ch_count,
                                chunk_label=chunk_label,
                                sql_query=sql_query,
                                clickhouse_query=clickhouse_query,
                            )
                            chunk_summaries.append((start, end, chunk_summary))
                        else:
                            with console.status(
                                f"[cyan]{progress}Running Aurora query for {table_name}...[/cyan]",
                                spinner="dots",
                            ):
                                sql_rows, sql_columns = fetch_aurora(aurora, sql_query)
                            console.print(f"{prefix_tags}[green]Aurora query complete[/green] ({len(sql_rows)} rows)")
                            with console.status(
                                f"[cyan]{progress}Running ClickHouse query for {table_name}...[/cyan]",
                                spinner="dots",
                            ):
                                ch_rows, ch_columns = fetch_clickhouse(clickhouse, clickhouse_query)
                            console.print(f"{prefix_tags}[green]ClickHouse query complete[/green] ({len(ch_rows)} rows)")
                            with console.status(
                                f"[cyan]{progress}Comparing rows for {table_name}...[/cyan]",
                                spinner="dots",
                            ):
                                chunk_summary = compare_rows(
                                    table_cfg,
                                    sql_rows,
                                    ch_rows,
                                    sql_columns,
                                    ch_columns,
                                    sql_query,
                                    clickhouse_query,
                                )
                            chunk_summaries.append((start, end, chunk_summary))
                    except (Exception, KeyboardInterrupt) as exc:
                        table_interrupted = True
                        run_interrupted = True
                        is_kb = isinstance(exc, KeyboardInterrupt)
                        if is_kb:
                            user_interrupted = True
                            reason = "User interruption (Ctrl+C)"
                            console.print(
                                f"\n[bold yellow]Validation manually interrupted at {chunk_label or table_name}[/bold yellow]"
                            )
                        else:
                            reason = str(exc)
                            error_console.print(
                                f"\n[bold red]Query failed on {chunk_label or table_name}:[/bold red] {escape(reason)}"
                            )

                        if chunk_summaries:
                            console.print(
                                f"[cyan]Preserving and generating report for {len(chunk_summaries)} validated chunk(s)...[/cyan]"
                            )
                        summary = finalize_table_summary(
                            table_cfg,
                            query_chunks,
                            chunk_summaries,
                            output_dir,
                            bot_filter_warning=bot_filter_warning,
                            interruption_reason=reason,
                            count_only=count_only,
                            applied_filters=applied_filters_for_summary,
                        )
                        summaries.append(summary)
                        print_table_summary(summary, console)
                        break

                if not table_interrupted:
                    summary = finalize_table_summary(
                        table_cfg,
                        query_chunks,
                        chunk_summaries,
                        output_dir,
                        bot_filter_warning=bot_filter_warning,
                        count_only=count_only,
                        applied_filters=applied_filters_for_summary,
                    )
                    summaries.append(summary)
                    print_table_summary(summary, console)

                if run_interrupted:
                    remaining_tables = len(table_plans) - len(summaries)
                    if remaining_tables > 0:
                        console.print(
                            f"[yellow]Skipping remaining {remaining_tables} table(s) due to earlier interruption.[/yellow]"
                        )
                    break
        except KeyboardInterrupt:
            run_interrupted = True
            user_interrupted = True
            console.print("\n[bold yellow]Validation manually interrupted.[/bold yellow]")
    finally:
        if aurora is not None:
            try:
                aurora.close()
            except Exception:
                pass
        if clickhouse is not None:
            try:
                clickhouse.close()
            except Exception:
                pass

    if summaries:
        run_queries_report = write_run_queries_report(summaries, output_dir)
        print_run_summary(summaries, console, run_queries_report=run_queries_report)

    if user_interrupted:
        return 130
    return 0 if (summaries and all(item["status"] == "PASSED" for item in summaries)) else 1


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    try:
        return run(config_path)
    except KeyboardInterrupt:
        error_console.print("\n[bold yellow]Validation stopped by user.[/bold yellow]")
        return 130
    except Exception as exc:
        error_console.print(f"[bold red]Validation could not complete:[/bold red] {escape(str(exc))}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
