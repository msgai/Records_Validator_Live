import csv
from datetime import datetime

import db_validator
import pytest
from db_validator import (
    add_bot_filters_to_query,
    build_count_query,
    build_date_chunks,
    build_table_query_chunks,
    combine_chunk_summaries,
    compare_counts,
    compare_rows,
    connect_aurora,
    equivalent,
    finalize_table_summary,
    is_count_only,
    load_table_configs,
    print_run_summary,
    print_table_summary,
    resolve_environment,
    select_bot_filter_columns,
    validate_bot_filters,
    write_queries_report,
    write_run_queries_report,
)
from rich.console import Console


BASE_CONFIG = {"table_name": "sample"}


def test_equivalent_compares_numeric_values_exactly():
    assert equivalent("1.00", 1)
    assert not equivalent("1.0000001", 1)
    assert equivalent(None, "NULL")
    assert not equivalent(None, 0)


def test_equivalent_normalizes_timestamps_and_json_empty_values():
    assert equivalent(datetime(2026, 1, 1), "2026-01-01 00:00:00")
    assert equivalent('{"tone":", "other":"x"}', '{"tone":"", "other":"x"}')


def test_aurora_connection_requires_tls_by_default(monkeypatch):
    received = {}
    monkeypatch.setenv("TEST_AURORA_PASSWORD", "not-a-real-password")
    monkeypatch.setattr(
        db_validator.pymysql,
        "connect",
        lambda **kwargs: received.update(kwargs),
    )

    connect_aurora({
        "host": "db.example.test",
        "user": "reader",
        "database": "staging",
        "password_env": "TEST_AURORA_PASSWORD",
        "ssl": True,
    })

    assert received["ssl"]["verify_mode"] == "required"


def test_resolve_environment_selects_configured_profile():
    qa = {"aurora": {"host": "qa-db"}, "clickhouse": {"host": "qa-ch"}}
    prod = {"aurora": {"host": "prod-db"}, "clickhouse": {"host": "prod-ch"}}

    assert resolve_environment({
        "environment": "prod",
        "environments": {"qa": qa, "prod": prod},
    }) == ("prod", prod)


def test_resolve_environment_defaults_to_qa():
    qa = {"aurora": {"host": "qa-db"}, "clickhouse": {"host": "qa-ch"}}

    assert resolve_environment({"environments": {"qa": qa}}) == ("qa", qa)


def test_resolve_environment_rejects_invalid_selection():
    with pytest.raises(ValueError, match="environment must be 'qa' or 'prod'"):
        resolve_environment({"environment": "staging", "environments": {}})


def test_compare_rows_flags_any_numeric_difference():
    result = compare_rows(
        BASE_CONFIG,
        [{"id": 1, "amount": "12.000001"}],
        [{"id": 1, "amount": 12}],
    )

    assert result["status"] == "FAILED"
    assert result["cell_mismatches"] == 1


def test_compare_rows_reports_positional_value_differences():
    result = compare_rows(
        BASE_CONFIG,
        [{"id": 1, "value": "sql"}, {"id": 2, "value": 10}],
        [{"id": 1, "value": "clickhouse"}, {"id": 3, "value": 30}],
    )

    assert result["status"] == "FAILED"
    assert result["missing_in_ch"] == 0
    assert result["extra_in_ch"] == 0
    assert result["cell_mismatches"] == 3
    assert result["row_mismatches"] == 2
    assert {item["composite_key"] for item in result["discrepancies"]} == {
        "row_position=1", "row_position=2"
    }


def test_compare_rows_reports_trailing_rows():
    result = compare_rows(
        BASE_CONFIG,
        [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}],
        [{"id": 1, "value": "a"}],
    )

    assert result["status"] == "FAILED"
    assert result["missing_in_ch"] == 1
    assert result["extra_in_ch"] == 0
    assert result["discrepancies"][-1]["composite_key"] == "row_position=2"


def test_empty_query_results_still_compare_schema():
    result = compare_rows(
        BASE_CONFIG,
        [],
        [],
        sql_columns=["id", "value"],
        ch_columns=["id", "other"],
    )

    assert result["status"] == "FAILED"
    assert any(item["diff_type"] == "COLUMN_MISSING_IN_TARGET"
               for item in result["discrepancies"])


def test_compare_rows_always_checks_row_order_by_position():
    result = compare_rows(
        BASE_CONFIG,
        [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}],
        [{"id": 2, "value": "b"}, {"id": 1, "value": "a"}],
    )

    assert result["status"] == "FAILED"
    assert result["row_mismatches"] == 2
    assert result["cell_mismatches"] == 4


def test_composite_keys_match_rows_independent_of_order():
    result = compare_rows(
        {**BASE_CONFIG, "primary_keys": ["dateHour", "botId"]},
        [
            {"dateHour": "2026-08-01", "botId": 1, "count": 10},
            {"dateHour": "2026-08-02", "botId": 1, "count": 20},
        ],
        [
            {"dateHour": "2026-08-02", "botId": "1", "count": 21},
            {"dateHour": "2026-08-01", "botId": "1", "count": 10},
        ],
    )

    assert result["status"] == "FAILED"
    assert result["missing_in_ch"] == 0
    assert result["extra_in_ch"] == 0
    assert result["row_mismatches"] == 1
    assert result["cell_mismatches"] == 1
    assert result["discrepancies"][0]["composite_key"] == "dateHour=2026-08-02 || botId=1"


def test_composite_keys_report_missing_and_extra_rows():
    result = compare_rows(
        {**BASE_CONFIG, "primary_keys": ["region", "day"]},
        [{"region": "east", "day": 1, "value": "a"},
         {"region": "west", "day": 1, "value": "b"}],
        [{"region": "west", "day": 1, "value": "b"},
         {"region": "north", "day": 1, "value": "c"}],
    )

    assert result["status"] == "FAILED"
    assert result["missing_in_ch"] == 1
    assert result["extra_in_ch"] == 1
    assert {item["composite_key"] for item in result["discrepancies"]} == {
        "region=east || day=1", "region=north || day=1"
    }


def test_composite_key_discrepancies_include_rerunnable_debug_queries():
    result = compare_rows(
        {**BASE_CONFIG, "primary_keys": ["botId", "name"]},
        [{"botId": 7, "name": "O'Brien", "value": "source"}],
        [{"botId": 7, "name": "O'Brien", "value": "target"}],
        sql_query="SELECT * FROM staging.sample WHERE day >= '2026-01-01';",
        clickhouse_query="SELECT * FROM ch_mirror.sample WHERE day >= toDate('2026-01-01')",
    )

    discrepancy = result["discrepancies"][0]
    assert "CAST(`botId` AS CHAR) = '7'" in discrepancy["sql_debug_query"]
    assert "CAST(`name` AS CHAR) = 'O''Brien'" in discrepancy["sql_debug_query"]
    assert "toString(`botId`) = '7'" in discrepancy["clickhouse_debug_query"]
    assert "toString(`name`) = 'O''Brien'" in discrepancy["clickhouse_debug_query"]
    assert not any(key.startswith("_") for key in discrepancy)


def test_write_report_handles_mixed_discrepancy_fields(tmp_path):
    summary = {
        "table_name": "sample",
        "discrepancies": [
            {"table_name": "sample", "diff_type": "COLUMN_MISSING_IN_TARGET"},
            {
                "table_name": "sample",
                "diff_type": "VALUE_MISMATCH",
                "sql_debug_query": "SELECT 1",
                "clickhouse_debug_query": "SELECT 1",
            },
        ],
    }

    report_path = db_validator.write_report(summary, tmp_path)

    with open(report_path, encoding="utf-8", newline="") as report:
        rows = list(csv.DictReader(report))
    assert rows[0]["sql_debug_query"] == ""
    assert rows[1]["sql_debug_query"] == "SELECT 1"


def test_null_primary_key_rows_are_written_for_manual_validation_in_pairs(tmp_path):
    summary = compare_rows(
        {
            **BASE_CONFIG,
            "primary_keys": ["id"],
            "exclude_null_primary_key_rows": True,
        },
        [
            {"id": None, "value": "sql-1"},
            {"id": "NULL", "value": "sql-2"},
            {"id": 1, "value": "compared"},
        ],
        [
            {"id": None, "value": "ch-1"},
            {"id": "nan", "value": "ch-2"},
            {"id": 1, "value": "compared"},
        ],
    )

    report_path = db_validator.write_manual_validation_report(summary, tmp_path)

    with open(report_path, encoding="utf-8", newline="") as report:
        rows = list(csv.DictReader(report))
    assert [row["row_source"] for row in rows] == [
        "ClickHouse", "SQL", "ClickHouse", "SQL"
    ]
    assert [row["value"] for row in rows] == ["ch-1", "sql-1", "ch-2", "sql-2"]
    assert summary["warnings"] == [
        "Excluded 2 SQL rows with null primary keys",
        "Excluded 2 ClickHouse rows with null primary keys",
    ]


def test_null_primary_key_rows_can_be_written_when_only_one_side_has_rows(tmp_path):
    summary = compare_rows(
        {
            **BASE_CONFIG,
            "primary_keys": ["id"],
            "exclude_null_primary_key_rows": True,
        },
        [{"id": None, "value": "sql"}],
        [],
    )

    report_path = db_validator.write_manual_validation_report(summary, tmp_path)

    with open(report_path, encoding="utf-8", newline="") as report:
        rows = list(csv.DictReader(report))
    assert [(row["row_source"], row["value"]) for row in rows] == [("SQL", "sql")]


def test_chunk_combination_preserves_null_primary_key_rows_for_manual_report(tmp_path):
    chunk = compare_rows(
        {
            **BASE_CONFIG,
            "primary_keys": ["id"],
            "exclude_null_primary_key_rows": True,
        },
        [{"id": None, "value": "sql"}],
        [{"id": None, "value": "ch"}],
    )
    summary = db_validator.combine_chunk_summaries(
        "sample",
        [(datetime(2026, 1, 1), datetime(2026, 1, 2), chunk)],
    )

    report_path = db_validator.write_manual_validation_report(summary, tmp_path)

    with open(report_path, encoding="utf-8", newline="") as report:
        rows = list(csv.DictReader(report))
    assert [(row["row_source"], row["value"]) for row in rows] == [
        ("ClickHouse", "ch"), ("SQL", "sql")
    ]


def test_duplicate_composite_keys_require_opt_in():
    config = {**BASE_CONFIG, "primary_keys": ["id"]}
    with pytest.raises(ValueError, match="Duplicate primary keys found"):
        compare_rows(
            config,
            [{"id": 1, "value": "a"}, {"id": 1, "value": "b"}],
            [{"id": 1, "value": "a"}, {"id": 1, "value": "b"}],
        )


def test_duplicate_composite_rows_can_be_compared_as_a_group():
    config = {**BASE_CONFIG, "primary_keys": ["id"], "count_duplicate_records": True}
    result = compare_rows(
        config,
        [{"id": 1, "value": "a"}, {"id": 1, "value": "b"}],
        [{"id": 1, "value": "b"}, {"id": 1, "value": "a"}],
    )

    assert result["status"] == "PASSED"
    assert result["cell_mismatches"] == 0


def test_table_summary_displays_colored_validation_metrics():
    output = Console(record=True, width=100)
    print_table_summary({
        "table_name": "sample",
        "status": "PASSED",
        "sql_rows": 2,
        "ch_rows": 2,
        "missing_in_ch": 0,
        "extra_in_ch": 0,
        "row_mismatches": 0,
        "cell_mismatches": 0,
        "warnings": [],
        "report_file": None,
    }, output)

    rendered = output.export_text()
    assert "Table: sample" in rendered
    assert "PASSED" in rendered
    assert "Aurora rows" in rendered
    assert "Value mismatches" in rendered
    assert "Not needed (no differences)" in rendered


def test_build_date_chunks_uses_half_open_equal_intervals():
    chunks = build_date_chunks({
        "start": "2026-08-01 00:00:00",
        "end_exclusive": "2026-08-03 06:00:00",
        "chunk_size_days": 1,
    })

    assert chunks == [
        (datetime(2026, 8, 1), datetime(2026, 8, 2)),
        (datetime(2026, 8, 2), datetime(2026, 8, 3)),
        (datetime(2026, 8, 3), datetime(2026, 8, 3, 6)),
    ]


def test_chunk_queries_render_source_specific_date_literals():
    query_chunks = build_table_query_chunks({
        "table_name": "sample",
        "sql_query": "dateHour >= '{{chunk_start}}' AND dateHour < '{{chunk_end}}'",
        "clickhouse_query": (
            "dateHour >= toDateTime('{{chunk_start}}') "
            "AND dateHour < toDateTime('{{chunk_end}}')"
        ),
        "chunking": {
            "start": "2026-08-01 00:00:00",
            "end_exclusive": "2026-08-02 00:00:00",
            "chunk_size_days": 1,
        },
    })

    assert len(query_chunks) == 1
    start, end, sql_query, clickhouse_query = query_chunks[0]
    assert start == datetime(2026, 8, 1)
    assert end == datetime(2026, 8, 2)
    assert "dateHour >= '2026-08-01 00:00:00'" in sql_query
    assert "dateHour < '2026-08-02 00:00:00'" in sql_query
    assert "toDateTime('2026-08-01 00:00:00')" in clickhouse_query
    assert "toDateTime('2026-08-02 00:00:00')" in clickhouse_query


def test_chunk_summaries_combine_counts_and_keep_chunk_ranges():
    first_chunk = compare_rows(
        BASE_CONFIG,
        [{"id": 1, "value": "aurora"}],
        [{"id": 1, "value": "clickhouse"}],
    )
    second_chunk = compare_rows(
        BASE_CONFIG,
        [{"id": 2, "value": "same"}],
        [{"id": 2, "value": "same"}],
    )

    combined = combine_chunk_summaries("sample", [
        (datetime(2026, 8, 1), datetime(2026, 8, 2), first_chunk),
        (datetime(2026, 8, 2), datetime(2026, 8, 3), second_chunk),
    ])

    assert combined["status"] == "FAILED"
    assert combined["sql_rows"] == 2
    assert combined["ch_rows"] == 2
    assert combined["cell_mismatches"] == 1
    assert combined["row_mismatches"] == 1
    assert combined["discrepancies"][0]["chunk_start"] == "2026-08-01 00:00:00"
    assert combined["discrepancies"][0]["chunk_end_exclusive"] == "2026-08-02 00:00:00"


def test_load_table_configs_resolves_queries_file_relative_to_config(tmp_path):
    query_file = tmp_path / "queries.yaml"
    query_file.write_text(
        "queries:\n"
        "  sample:\n"
        "    sql_query: SELECT 1\n"
        "    clickhouse_query: SELECT 1\n",
        encoding="utf-8",
    )
    config = {
        "queries_file": "queries.yaml",
        "tables": [{
            "table_name": "sample",
            "chunking": {"chunk_size_days": 1},
        }],
    }

    tables = load_table_configs(config, tmp_path / "config.yaml")

    assert tables == [{
        "table_name": "sample",
        "chunking": {"chunk_size_days": 1},
        "sql_query": "SELECT 1",
        "clickhouse_query": "SELECT 1",
    }]


def test_validate_bot_filters_handles_formats_and_deduplicates():
    assert validate_bot_filters(None) == {"bot_ids": [], "bot_reference_ids": [], "env": []}
    assert validate_bot_filters({}) == {"bot_ids": [], "bot_reference_ids": [], "env": []}
    assert validate_bot_filters({
        "bot_ids": ["b1", "b1", "b2"],
        "bot_reference_ids": ["r1"],
        "env": "SANDBOX",
    }) == {
        "bot_ids": ["b1", "b2"],
        "bot_reference_ids": ["r1"],
        "env": ["SANDBOX"],
    }
    assert validate_bot_filters({
        "botId": ["b1"],
        "botReferenceId": ["r1"],
        "env": ["SANDBOX", "LIVE"],
    }) == {
        "bot_ids": ["b1"],
        "bot_reference_ids": ["r1"],
        "env": ["SANDBOX", "LIVE"],
    }


def test_validate_bot_filters_rejects_invalid_types():
    with pytest.raises(ValueError, match="filters must be a mapping"):
        validate_bot_filters("invalid")
    with pytest.raises(ValueError, match="must be a list or scalar"):
        validate_bot_filters({"bot_ids": {"not": "list"}})
    with pytest.raises(ValueError, match="non-null scalar values"):
        validate_bot_filters({"bot_ids": [None]})


def test_select_bot_filter_columns_matches_available_columns():
    filters = {"bot_ids": ["b1"], "bot_reference_ids": ["r1"], "env": ["SANDBOX"]}

    # All columns present (case-insensitive)
    selected, warning = select_bot_filter_columns(
        filters, ["botid", "botreferenceid", "env"], ["botId", "botReferenceId", "env"]
    )
    assert selected == [
        ("botid", "botId", ["b1"]),
        ("botreferenceid", "botReferenceId", ["r1"]),
        ("env", "env", ["SANDBOX"]),
    ]
    assert warning is None

    # botId and env present
    selected, warning = select_bot_filter_columns(
        filters, ["botid", "dateHour", "env"], ["botId", "dateHour", "env"]
    )
    assert selected == [("botid", "botId", ["b1"]), ("env", "env", ["SANDBOX"])]
    assert warning is None

    # Only botReferenceId present
    selected, warning = select_bot_filter_columns(
        {"bot_ids": [], "bot_reference_ids": ["r1"], "env": []},
        ["botReferenceId"],
        ["botReferenceId"],
    )
    assert selected == [("botReferenceId", "botReferenceId", ["r1"])]
    assert warning is None

    # Neither present
    selected, warning = select_bot_filter_columns(filters, ["id", "name"], ["id", "name"])
    assert selected == []
    assert "none of the configured columns" in warning


def test_add_bot_filters_to_query_injects_into_where_and_order_by():
    q1 = "SELECT * FROM t WHERE dateHour >= '2024-01-01' ORDER BY id ASC;"
    f1 = add_bot_filters_to_query(q1, [("botId", ["b1", "b2"])], "sql")
    assert f1 == "SELECT * FROM t WHERE dateHour >= '2024-01-01' AND (`botId` IN ('b1', 'b2')) ORDER BY id ASC;"

    # Multiple filters combined with AND
    q2 = "SELECT * FROM t ORDER BY id ASC;"
    f2 = add_bot_filters_to_query(
        q2, [("botId", ["b1"]), ("botReferenceId", ["r1"]), ("env", ["SANDBOX"])], "sql"
    )
    assert f2 == "SELECT * FROM t WHERE (`botId` = 'b1' AND `botReferenceId` = 'r1' AND `env` = 'SANDBOX') ORDER BY id ASC;"

    q3 = "SELECT * FROM t WHERE dateHour >= '2024-01-01';"
    f3 = add_bot_filters_to_query(q3, [("botId", ["b1"])], "clickhouse")
    assert f3 == "SELECT * FROM t WHERE dateHour >= '2024-01-01' AND (`botId` = 'b1');"

    # Single value uses = while multiple values fallback to IN
    f4 = add_bot_filters_to_query(q3, [("botId", ["b1", "b2"])], "clickhouse")
    assert f4 == "SELECT * FROM t WHERE dateHour >= '2024-01-01' AND (`botId` IN ('b1', 'b2'));"

    # Empty filter columns returns original query
    f5 = add_bot_filters_to_query(q3, [("botId", [])], "clickhouse")
    assert f5 == q3


def test_finalize_table_summary_partial_interruption_preserves_diff_report(tmp_path):
    table_cfg = {**BASE_CONFIG, "primary_keys": ["id"]}
    chunk_1 = compare_rows(
        table_cfg,
        [{"id": 1, "value": "aurora"}],
        [{"id": 1, "value": "clickhouse"}],
    )
    chunk_summaries = [
        (datetime(2026, 1, 1), datetime(2026, 1, 2), chunk_1),
    ]
    query_chunks = [
        (datetime(2026, 1, 1), datetime(2026, 1, 2), "q1", "q1"),
        (datetime(2026, 1, 2), datetime(2026, 1, 3), "q2", "q2"),
    ]

    summary = finalize_table_summary(
        table_cfg,
        query_chunks,
        chunk_summaries,
        tmp_path,
        interruption_reason="Lost connection to MySQL server",
    )

    assert summary["status"] == "INTERRUPTED"
    assert summary["completed_chunks"] == 1
    assert summary["total_chunks"] == 2
    assert summary["cell_mismatches"] == 1
    assert summary["report_file"] is not None
    assert any("Lost connection to MySQL server" in w for w in summary["warnings"])

    with open(summary["report_file"], encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
    assert len(reader) == 1
    assert reader[0]["table_name"] == "sample"
    assert reader[0]["diff_type"] == "VALUE_MISMATCH"


def test_finalize_table_summary_zero_chunks_interrupted(tmp_path):
    table_cfg = {**BASE_CONFIG, "primary_keys": ["id"]}
    query_chunks = [
        (datetime(2026, 1, 1), datetime(2026, 1, 2), "q1", "q1"),
    ]

    summary = finalize_table_summary(
        table_cfg,
        query_chunks,
        [],
        tmp_path,
        interruption_reason="User interruption (Ctrl+C)",
    )

    assert summary["status"] == "INTERRUPTED"
    assert summary["completed_chunks"] == 0
    assert summary["total_chunks"] == 1
    assert summary["report_file"] is None
    assert any("User interruption" in w for w in summary["warnings"])


def test_print_table_summary_shows_interrupted_status_and_chunks():
    output = Console(record=True, width=100)
    print_table_summary({
        "table_name": "sample",
        "status": "INTERRUPTED",
        "completed_chunks": 64,
        "total_chunks": 73,
        "sql_rows": 1000,
        "ch_rows": 1000,
        "missing_in_ch": 0,
        "extra_in_ch": 0,
        "row_mismatches": 2,
        "cell_mismatches": 2,
        "warnings": ["Validation interrupted after 64 of 73 chunks: timeout"],
        "report_file": "/path/to/report.csv",
    }, output)

    rendered = output.export_text()
    assert "Table: sample" in rendered
    assert "INTERRUPTED" in rendered
    assert "64 of 73" in rendered
    assert "timeout" in rendered


def test_print_run_summary_with_interrupted_tables():
    output = Console(record=True, width=100)
    summaries = [
        {"table_name": "t1", "status": "PASSED"},
        {"table_name": "t2", "status": "INTERRUPTED"},
    ]
    print_run_summary(summaries, output)
    rendered = output.export_text()
    assert "Tables evaluated  2" in rendered
    assert "Tables passed     1" in rendered
    assert "Tables failed     1" in rendered
    assert "Verdict           FAILED" in rendered


def test_run_preserves_earlier_chunks_on_timeout(tmp_path, monkeypatch):
    import pymysql
    config_file = tmp_path / "config.yaml"
    reports_dir = tmp_path / "reports"
    config_file.write_text(f"""
environment: qa
output_dir: "{reports_dir}"
environments:
  qa:
    aurora:
      host: "localhost"
      user: "root"
      password_env: "TEST_PW"
      database: "test"
    clickhouse:
      host: "localhost"
      user: "default"
      password_env: "TEST_PW"
tables:
  - table_name: "test_table"
    primary_keys: ["id"]
    sql_query: "SELECT * FROM t WHERE d >= '{{{{chunk_start}}}}' AND d < '{{{{chunk_end}}}}';"
    clickhouse_query: "SELECT * FROM t WHERE d >= '{{{{chunk_start}}}}' AND d < '{{{{chunk_end}}}}';"
    chunking:
      start: "2026-01-01 00:00:00"
      end_exclusive: "2026-01-03 00:00:00"
      chunk_size_days: 1
""", encoding="utf-8")

    monkeypatch.setenv("TEST_PW", "pw")
    monkeypatch.setattr(db_validator, "connect_aurora", lambda cfg: object())
    monkeypatch.setattr(db_validator, "connect_clickhouse", lambda cfg: object())

    call_count = {"aurora": 0}

    def fake_fetch_aurora(conn, query):
        call_count["aurora"] += 1
        if call_count["aurora"] == 1:
            return [{"id": 1, "val": "aurora_diff"}], ["id", "val"]
        raise pymysql.err.OperationalError(2013, "Lost connection to MySQL server during query")

    def fake_fetch_ch(conn, query):
        return [{"id": 1, "val": "ch_val"}], ["id", "val"]

    monkeypatch.setattr(db_validator, "fetch_aurora", fake_fetch_aurora)
    monkeypatch.setattr(db_validator, "fetch_clickhouse", fake_fetch_ch)

    exit_code = db_validator.run(str(config_file))
    assert exit_code == 1

    report_file = reports_dir / "test_table_diff_report.csv"
    assert report_file.exists()
    with open(report_file, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["table_name"] == "test_table"
    assert rows[0]["diff_type"] == "VALUE_MISMATCH"


def test_run_preserves_earlier_chunks_on_keyboard_interrupt(tmp_path, monkeypatch):
    config_file = tmp_path / "config.yaml"
    reports_dir = tmp_path / "reports"
    config_file.write_text(f"""
environment: qa
output_dir: "{reports_dir}"
environments:
  qa:
    aurora:
      host: "localhost"
      user: "root"
      password_env: "TEST_PW"
      database: "test"
    clickhouse:
      host: "localhost"
      user: "default"
      password_env: "TEST_PW"
tables:
  - table_name: "test_table"
    primary_keys: ["id"]
    sql_query: "SELECT * FROM t WHERE d >= '{{{{chunk_start}}}}' AND d < '{{{{chunk_end}}}}';"
    clickhouse_query: "SELECT * FROM t WHERE d >= '{{{{chunk_start}}}}' AND d < '{{{{chunk_end}}}}';"
    chunking:
      start: "2026-01-01 00:00:00"
      end_exclusive: "2026-01-03 00:00:00"
      chunk_size_days: 1
""", encoding="utf-8")

    monkeypatch.setenv("TEST_PW", "pw")
    monkeypatch.setattr(db_validator, "connect_aurora", lambda cfg: object())
    monkeypatch.setattr(db_validator, "connect_clickhouse", lambda cfg: object())

    call_count = {"aurora": 0}

    def fake_fetch_aurora(conn, query):
        call_count["aurora"] += 1
        if call_count["aurora"] == 1:
            return [{"id": 1, "val": "aurora_diff"}], ["id", "val"]
        raise KeyboardInterrupt()

    def fake_fetch_ch(conn, query):
        return [{"id": 1, "val": "ch_val"}], ["id", "val"]

    monkeypatch.setattr(db_validator, "fetch_aurora", fake_fetch_aurora)
    monkeypatch.setattr(db_validator, "fetch_clickhouse", fake_fetch_ch)

    exit_code = db_validator.run(str(config_file))
    assert exit_code == 130

    report_file = reports_dir / "test_table_diff_report.csv"
    assert report_file.exists()


def test_build_count_query_wraps_statement_and_strips_semicolons():
    raw_query = "SELECT * FROM staging.conversation_metadata WHERE id > 10;\n;  "
    count_query = build_count_query(raw_query)
    assert count_query == (
        "SELECT COUNT(*) AS total_count FROM (\n"
        "SELECT * FROM staging.conversation_metadata WHERE id > 10\n"
        ") AS validator_count"
    )


def test_is_count_only_resolves_table_and_root_flags():
    assert is_count_only({"count_only": True}, {}) is True
    assert is_count_only({"smoke_test": True}, {}) is True
    assert is_count_only({}, {"count_only": True}) is True
    assert is_count_only({"count_only": False}, {"count_only": True}) is False
    assert is_count_only({}, {}) is False


def test_compare_counts_reports_match_and_mismatch():
    table_cfg = {"table_name": "test_table"}

    # Match
    match_result = compare_counts(table_cfg, 100, 100, chunk_label="chunk 1")
    assert match_result["status"] == "PASSED"
    assert match_result["sql_rows"] == 100
    assert match_result["ch_rows"] == 100
    assert match_result["row_mismatches"] == 0
    assert match_result["discrepancies"] == []

    # Mismatch
    mismatch_result = compare_counts(
        table_cfg,
        100,
        95,
        chunk_label="chunk 1",
        sql_query="SELECT * FROM sql_t;",
        clickhouse_query="SELECT * FROM ch_t",
    )
    assert mismatch_result["status"] == "FAILED"
    assert mismatch_result["sql_rows"] == 100
    assert mismatch_result["ch_rows"] == 95
    assert mismatch_result["missing_in_ch"] == 5
    assert mismatch_result["extra_in_ch"] == 0
    assert mismatch_result["row_mismatches"] == 5
    assert len(mismatch_result["discrepancies"]) == 1
    disc = mismatch_result["discrepancies"][0]
    assert disc["diff_type"] == "COUNT_MISMATCH"
    assert disc["sql_value"] == "100"
    assert disc["ch_value"] == "95"
    assert "SELECT COUNT(*) AS total_count FROM (\nSELECT * FROM sql_t\n) AS validator_count" == disc["sql_count_query"]


def test_print_table_summary_with_count_only():
    output = Console(record=True, width=100)
    print_table_summary({
        "table_name": "sample",
        "status": "PASSED",
        "count_only": True,
        "completed_chunks": 10,
        "total_chunks": 10,
        "sql_rows": 5000,
        "ch_rows": 5000,
        "missing_in_ch": 0,
        "extra_in_ch": 0,
        "row_mismatches": 0,
        "warnings": [],
        "report_file": None,
    }, output)

    rendered = output.export_text()
    assert "Table: sample" in rendered
    assert "COUNT-ONLY SMOKE TEST" in rendered
    assert "Count Only (Smoke Test)" in rendered
    assert "Row counts only (cell contents & primary keys NOT verified)" in rendered
    assert "Fast record count test only • Individual row data was NOT verified" in rendered
    assert "NOTE: This evaluation was a COUNT-ONLY SMOKE TEST" in rendered
    assert "Count diff" in rendered
    assert "5000" in rendered


def test_print_run_summary_with_count_only_tables():
    output = Console(record=True, width=100)
    summaries = [
        {"table_name": "t1", "status": "PASSED", "count_only": True},
        {"table_name": "t2", "status": "PASSED", "count_only": True},
    ]
    print_run_summary(summaries, output)
    rendered = output.export_text()
    assert "Final Run Summary ⚡ (COUNT-ONLY SMOKE TEST)" in rendered
    assert "Count-only smoke tests  2" in rendered
    assert "Full validation tables  0" in rendered
    assert "Verdict                 PASSED" in rendered
    assert "Fast record count test only • Individual row data was NOT verified" in rendered
    assert "[COUNT-ONLY SMOKE TEST RUN]" in rendered


def test_print_run_summary_with_mixed_tables():
    output = Console(record=True, width=100)
    summaries = [
        {"table_name": "t1", "status": "PASSED"},
        {"table_name": "t2", "status": "PASSED", "count_only": True},
    ]
    print_run_summary(summaries, output)
    rendered = output.export_text()
    assert "Final Run Summary ⚡ (MIXED RUN)" in rendered
    assert "Full validation tables  1" in rendered
    assert "Count-only smoke tests  1" in rendered
    assert "1 of 2 table(s) evaluated in count-only smoke test mode" in rendered
    assert "[MIXED VALIDATION RUN]" in rendered


def test_run_executes_count_only_smoke_test(tmp_path, monkeypatch):
    config_file = tmp_path / "config.yaml"
    reports_dir = tmp_path / "reports"
    config_file.write_text(f"""
environment: qa
output_dir: "{reports_dir}"
environments:
  qa:
    aurora:
      host: "localhost"
      user: "root"
      password_env: "TEST_PW"
      database: "test"
    clickhouse:
      host: "localhost"
      user: "default"
      password_env: "TEST_PW"
tables:
  - table_name: "test_smoke"
    count_only: true
    sql_query: "SELECT * FROM t WHERE d >= '{{{{chunk_start}}}}' AND d < '{{{{chunk_end}}}}';"
    clickhouse_query: "SELECT * FROM t WHERE d >= '{{{{chunk_start}}}}' AND d < '{{{{chunk_end}}}}';"
    chunking:
      start: "2026-01-01 00:00:00"
      end_exclusive: "2026-01-03 00:00:00"
      chunk_size_days: 1
""", encoding="utf-8")

    monkeypatch.setenv("TEST_PW", "pw")
    monkeypatch.setattr(db_validator, "connect_aurora", lambda cfg: object())
    monkeypatch.setattr(db_validator, "connect_clickhouse", lambda cfg: object())

    counts = {
        "sql": [50, 60],
        "ch": [50, 58],  # second chunk has mismatch: 60 vs 58
    }
    call_idx = {"sql": 0, "ch": 0}

    def fake_count_aurora(conn, query):
        val = counts["sql"][call_idx["sql"]]
        call_idx["sql"] += 1
        return val

    def fake_count_ch(conn, query):
        val = counts["ch"][call_idx["ch"]]
        call_idx["ch"] += 1
        return val

    def fail_if_called(*args, **kwargs):
        raise AssertionError("Full row fetch should not be called in count_only mode")

    monkeypatch.setattr(db_validator, "fetch_count_aurora", fake_count_aurora)
    monkeypatch.setattr(db_validator, "fetch_count_clickhouse", fake_count_ch)
    monkeypatch.setattr(db_validator, "fetch_aurora", fail_if_called)
    monkeypatch.setattr(db_validator, "fetch_clickhouse", fail_if_called)

    exit_code = db_validator.run(str(config_file))
    assert exit_code == 1

    report_file = reports_dir / "test_smoke_diff_report.csv"
    assert report_file.exists()
    with open(report_file, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["table_name"] == "test_smoke"
    assert rows[0]["diff_type"] == "COUNT_MISMATCH"
    assert rows[0]["sql_value"] == "60"
    assert rows[0]["ch_value"] == "58"
    assert rows[0]["chunk_start"] == "2026-01-02 00:00:00"


def test_print_table_summary_with_applied_filters():
    output = Console(record=True, width=100)
    print_table_summary({
        "table_name": "agent_details",
        "status": "PASSED",
        "applied_filters": [("botId", ["bot_1", "bot_2"]), ("env", ["LIVE"])],
        "completed_chunks": 5,
        "total_chunks": 5,
        "sql_rows": 250,
        "ch_rows": 250,
        "missing_in_ch": 0,
        "extra_in_ch": 0,
        "row_mismatches": 0,
        "cell_mismatches": 0,
        "warnings": [],
        "report_file": None,
    }, output)

    rendered = output.export_text()
    assert "Table: agent_details" in rendered
    assert "🎯 FILTERED 🎯" in rendered
    assert "Filtered by botId (2), env (1)" in rendered
    assert "FILTERED SUBSET" in rendered
    assert "botId: ['bot_1', 'bot_2']" in rendered
    assert "env: ['LIVE']" in rendered
    assert "Scoped subset only (unmatched table records excluded)" in rendered
    assert "NOTE: This evaluation was FILTERED by (botId=['bot_1', 'bot_2'], env=['LIVE'])" in rendered


def test_print_table_summary_with_both_smoke_and_filters():
    output = Console(record=True, width=100)
    print_table_summary({
        "table_name": "sample",
        "status": "PASSED",
        "count_only": True,
        "applied_filters": [("botId", ["b1"])],
        "completed_chunks": 2,
        "total_chunks": 2,
        "sql_rows": 100,
        "ch_rows": 100,
        "missing_in_ch": 0,
        "extra_in_ch": 0,
        "row_mismatches": 0,
        "warnings": [],
        "report_file": None,
    }, output)

    rendered = output.export_text()
    assert "⚡ COUNT-ONLY SMOKE TEST ⚡" in rendered
    assert "🎯 FILTERED 🎯" in rendered
    assert "Count Only (Smoke Test)" in rendered
    assert "FILTERED SUBSET" in rendered
    assert "NOTE: This evaluation was a COUNT-ONLY SMOKE TEST" in rendered
    assert "NOTE: This evaluation was FILTERED by (botId=['b1'])" in rendered


def test_print_run_summary_with_filtered_tables():
    output = Console(record=True, width=100)
    summaries = [
        {"table_name": "t1", "status": "PASSED", "applied_filters": [("botId", ["b1"])]},
        {"table_name": "t2", "status": "PASSED"},
    ]
    print_run_summary(summaries, output)
    rendered = output.export_text()
    assert "Final Run Summary 🎯 (1 FILTERED)" in rendered
    assert "Unfiltered tables  1" in rendered
    assert "Filtered tables    1" in rendered
    assert "1 of 2 table(s) evaluated with active bot/environment filters" in rendered
    assert "[FILTERED RUN] 1 table(s) had active bot/environment filters applied" in rendered


def test_run_with_bot_filters_applies_and_reports_filtered_scope(tmp_path, monkeypatch):
    config_file = tmp_path / "config.yaml"
    reports_dir = tmp_path / "reports"
    config_file.write_text(f"""
environment: qa
output_dir: "{reports_dir}"
filters:
  bot_ids: ["b1"]
  env: ["LIVE"]
environments:
  qa:
    aurora:
      host: "localhost"
      user: "root"
      password_env: "TEST_PW"
      database: "test"
    clickhouse:
      host: "localhost"
      user: "default"
      password_env: "TEST_PW"
tables:
  - table_name: "test_filtered"
    primary_keys: ["id"]
    sql_query: "SELECT id, botId, env, val FROM t WHERE d >= '{{{{chunk_start}}}}' AND d < '{{{{chunk_end}}}}';"
    clickhouse_query: "SELECT id, botId, env, val FROM t WHERE d >= '{{{{chunk_start}}}}' AND d < '{{{{chunk_end}}}}';"
    chunking:
      start: "2026-01-01 00:00:00"
      end_exclusive: "2026-01-02 00:00:00"
      chunk_size_days: 1
""", encoding="utf-8")

    monkeypatch.setenv("TEST_PW", "pw")
    monkeypatch.setattr(db_validator, "connect_aurora", lambda cfg: object())
    monkeypatch.setattr(db_validator, "connect_clickhouse", lambda cfg: object())

    captured_queries = {"aurora": [], "clickhouse": []}

    def fake_query_cols_aurora(conn, query):
        return ["id", "botId", "env", "val"]

    def fake_query_cols_ch(conn, query):
        return ["id", "botId", "env", "val"]

    def fake_fetch_aurora(conn, query):
        captured_queries["aurora"].append(query)
        return [{"id": 1, "botId": "b1", "env": "LIVE", "val": "aurora"}], ["id", "botId", "env", "val"]

    def fake_fetch_ch(conn, query):
        captured_queries["clickhouse"].append(query)
        return [{"id": 1, "botId": "b1", "env": "LIVE", "val": "ch"}], ["id", "botId", "env", "val"]

    monkeypatch.setattr(db_validator, "query_result_columns_aurora", fake_query_cols_aurora)
    monkeypatch.setattr(db_validator, "query_result_columns_clickhouse", fake_query_cols_ch)
    monkeypatch.setattr(db_validator, "fetch_aurora", fake_fetch_aurora)
    monkeypatch.setattr(db_validator, "fetch_clickhouse", fake_fetch_ch)

    exit_code = db_validator.run(str(config_file))
    assert exit_code == 1

    # Verified filter injection into queries
    assert any("`botId` = 'b1'" in q for q in captured_queries["aurora"])
    assert any("`env` = 'LIVE'" in q for q in captured_queries["aurora"])

    # Verified diff report contains applied_filters
    report_file = reports_dir / "test_filtered_diff_report.csv"
    assert report_file.exists()
    with open(report_file, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert "botId=" in rows[0]["applied_filters"]
    assert "env=" in rows[0]["applied_filters"]

    # Verified executed queries report and table queries report exist
    table_queries_file = reports_dir / "test_filtered_queries_report.csv"
    assert table_queries_file.exists()
    with open(table_queries_file, encoding="utf-8") as f:
        q_rows = list(csv.DictReader(f))
    assert len(q_rows) == 1
    assert q_rows[0]["table_name"] == "test_filtered"
    assert "`botId` = 'b1'" in q_rows[0]["sql_query"]
    assert "`botId` = 'b1'" in q_rows[0]["clickhouse_query"]

    run_queries_file = reports_dir / "executed_queries_report.csv"
    assert run_queries_file.exists()
    with open(run_queries_file, encoding="utf-8") as f:
        run_q_rows = list(csv.DictReader(f))
    assert len(run_q_rows) == 1
    assert run_q_rows[0]["table_name"] == "test_filtered"


def test_write_queries_report_and_write_run_queries_report(tmp_path):
    summary_1 = {
        "table_name": "t1",
        "executed_queries": [
            {
                "table_name": "t1",
                "chunk_index": 1,
                "chunk_label": "chunk 1/2",
                "chunk_start": "2026-01-01 00:00:00",
                "chunk_end_exclusive": "2026-01-02 00:00:00",
                "evaluation_mode": "FULL_VALIDATION",
                "status": "PASSED",
                "sql_rows": 10,
                "ch_rows": 10,
                "applied_filters": "botId=['b1']",
                "sql_query": "SELECT * FROM t1 WHERE d = 1;",
                "clickhouse_query": "SELECT * FROM ch1 WHERE d = 1;",
                "sql_base_query": "SELECT * FROM t1 WHERE d = 1;",
                "clickhouse_base_query": "SELECT * FROM ch1 WHERE d = 1;",
            }
        ],
    }
    summary_2 = {
        "table_name": "t2",
        "executed_queries": [
            {
                "table_name": "t2",
                "chunk_index": 1,
                "chunk_label": "Unchunked",
                "chunk_start": "",
                "chunk_end_exclusive": "",
                "evaluation_mode": "COUNT_ONLY_SMOKE_TEST",
                "status": "PASSED",
                "sql_rows": 50,
                "ch_rows": 50,
                "applied_filters": "",
                "sql_query": "SELECT COUNT(*) AS total_count FROM (SELECT * FROM t2) AS validator_count",
                "clickhouse_query": "SELECT COUNT(*) AS total_count FROM (SELECT * FROM ch2) AS validator_count",
                "sql_base_query": "SELECT * FROM t2",
                "clickhouse_base_query": "SELECT * FROM ch2",
            }
        ],
    }

    # Test single table write
    report_file_1 = write_queries_report(summary_1, tmp_path)
    assert report_file_1 == str(tmp_path / "t1_queries_report.csv")
    with open(report_file_1, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["table_name"] == "t1"
    assert rows[0]["sql_query"] == "SELECT * FROM t1 WHERE d = 1;"

    # Test run-level aggregation
    run_report_file = write_run_queries_report([summary_1, summary_2], tmp_path)
    assert run_report_file == str(tmp_path / "executed_queries_report.csv")
    with open(run_report_file, encoding="utf-8") as f:
        run_rows = list(csv.DictReader(f))
    assert len(run_rows) == 2
    assert run_rows[0]["table_name"] == "t1"
    assert run_rows[1]["table_name"] == "t2"
    assert run_rows[1]["evaluation_mode"] == "COUNT_ONLY_SMOKE_TEST"


def test_finalize_table_summary_generates_queries_report_with_count_only(tmp_path):
    table_cfg = {"table_name": "sample_table"}
    query_chunks = [
        (datetime(2026, 1, 1), datetime(2026, 1, 2), "SELECT 1;", "SELECT 1;"),
    ]
    chunk_summaries = [
        (datetime(2026, 1, 1), datetime(2026, 1, 2), compare_counts(table_cfg, 1, 1)),
    ]

    summary = finalize_table_summary(
        table_cfg,
        query_chunks,
        chunk_summaries,
        tmp_path,
        count_only=True,
    )

    assert summary["queries_report_file"] is not None
    with open(summary["queries_report_file"], encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["table_name"] == "sample_table"
    assert rows[0]["evaluation_mode"] == "COUNT_ONLY_SMOKE_TEST"
    assert "SELECT COUNT(*)" in rows[0]["sql_query"]
    assert rows[0]["sql_base_query"] == "SELECT 1;"


def test_print_run_summary_displays_queries_report_path():
    output = Console(record=True, width=120)
    summaries = [{"table_name": "t1", "status": "PASSED"}]
    print_run_summary(summaries, output, run_queries_report="/path/to/executed_queries_report.csv")
    rendered = output.export_text()
    assert "Queries report" in rendered
    assert "/path/to/executed_queries_report.csv" in rendered





