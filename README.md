# Live DB Validator

Runs the configured SELECT statements against Aurora MySQL and ClickHouse at execution time, then compares returned rows by position and values. It writes a CSV diff report for each table with discrepancies. The original dump-file validator remains unchanged in `DB_Validator`.

## Setup

Use Python 3.10 or newer. Create the virtual environment and install dependencies:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp .env.example .env
```

Set `AURORA_PASSWORD` and `CLICKHOUSE_PASSWORD` in `.env` for QA, and `AURORA_PROD_PASSWORD` and `CLICKHOUSE_PROD_PASSWORD` for production. The `.env` file is ignored by Git. Do not put passwords in `config.yaml` or commit them.

## Configure and run

Set `environment: qa` or `environment: prod` in `config.yaml` to choose the connection profile; QA is the default. The selected environment is displayed at startup, with production highlighted. Edit the matching connection profile for endpoint settings, and set its password environment variables in `.env`. Put each table's `sql_query` and `clickhouse_query` in `queries.yaml`, keyed by the matching `table_name`. Queries should return the same column names and use explicit column lists. Both queries must have deterministic `ORDER BY` clauses because rows are compared by position; include tie-breaker columns when the main sort column can repeat. Set `queries_file` if you want to use a different query file; its path is relative to `config.yaml`.

For tables where full-dump queries take too much time, configure `bot_filters` with `bot_ids`, `bot_reference_ids`, and/or `env` in `config.yaml`. For each table, the validator inspects the query columns and automatically applies filtering for whichever column(s) (`botId`, `botReferenceId`, `env`) are present in both databases. When multiple filters are active, they are combined with `AND`. Tables with none of the configured columns continue to run unfiltered with a warning.

```yaml
bot_filters:
  bot_ids: [29cded70-ee26-470a-a9ed-3aad5805ba91]
  bot_reference_ids: [18a69f39-9112-4c4a-acde-d5210ea50c44]
  env: 'SANDBOX'  # 'SANDBOX' or 'LIVE' (or a list like ['SANDBOX', 'LIVE'])
```

You do not need to change your SQL queries in `queries.yaml`—the validator automatically injects the filter into the existing `WHERE` / `ORDER BY` clauses.

For example, given standard queries in `queries.yaml`:
```yaml
queries:
  message_aggregate_raw:
    sql_query: |
      SELECT *
      FROM staging.message_aggregate
      WHERE dateHour >= '{{chunk_start}}'
        AND dateHour < '{{chunk_end}}'
      ORDER BY dateHour ASC, botId ASC;

    clickhouse_query: |
      SELECT *
      FROM ch_agg.message_aggregate
      WHERE dateHour >= toDateTime('{{chunk_start}}')
        AND dateHour < toDateTime('{{chunk_end}}')
      ORDER BY dateHour ASC, botId ASC
```

The validator dynamically executes:
```sql
-- Aurora MySQL
SELECT *
FROM staging.message_aggregate
WHERE dateHour >= '2024-10-08 00:00:00'
  AND dateHour < '2024-11-07 00:00:00'
  AND (`botId` IN ('29cded70-ee26-470a-a9ed-3aad5805ba91'))
ORDER BY dateHour ASC, botId ASC;

-- ClickHouse
SELECT *
FROM ch_agg.message_aggregate
WHERE dateHour >= toDateTime('2024-10-08 00:00:00')
  AND dateHour < toDateTime('2024-11-07 00:00:00')
  AND (`botId` IN ('29cded70-ee26-470a-a9ed-3aad5805ba91'))
ORDER BY dateHour ASC, botId ASC
```

If a table has multiple matching filter columns (e.g., `botId` and `env`), they are combined with `AND`:
```sql
WHERE ... AND (`botId` IN ('29cded70-...') AND `env` IN ('SANDBOX'))
```

Filtered tests are prominently highlighted across the console with high-visibility magenta badges (`🎯 FILTERED 🎯`), active filter value displays in table headers, chunk-level `[FILTERED]` tags, explicit data-scope notices in summary panels, and separate accounting in the Final Run Summary so scoped subset tests are clearly distinguished from full table validations. Discrepancy reports also record `applied_filters` for auditability.

You can also hardcode explicit filters in `queries.yaml` if you prefer static queries:
```yaml
queries:
  sample_bot_table:
    sql_query: |
      SELECT *
      FROM staging.message_aggregate
      WHERE botId IN ('29cded70-ee26-470a-a9ed-3aad5805ba91')
      ORDER BY dateHour ASC;

    clickhouse_query: |
      SELECT *
      FROM ch_agg.message_aggregate
      WHERE botId IN ('29cded70-ee26-470a-a9ed-3aad5805ba91')
      ORDER BY dateHour ASC
```

To match records independent of query row order, add a non-empty `primary_keys` list to that table's entry in `config.yaml`. Use one or more columns that uniquely identify a row (for example, `primary_keys: [dateHour, botId]`). Key columns must be returned by both queries. Duplicate keys are rejected by default; set `count_duplicate_records: true` to compare duplicate-key rows as an unordered group. Null keys are rejected unless `exclude_null_primary_key_rows: true` is set. When enabled, rows with null primary keys are excluded from comparison and written to a separate `<table>_null_primary_key_rows_report.csv` for manual review, with each ClickHouse row immediately followed by the corresponding SQL row. Tables without `primary_keys` continue to compare by row position.

Large date-ranged tables can opt into per-table chunking with `chunking.start`, `chunking.end_exclusive`, and either `chunking.chunk_size_days` or `chunking.chunk_size_hours`. Use `{{chunk_start}}` and `{{chunk_end}}` in both query filters, with inclusive start and exclusive end bounds (`>= start AND < end`). For ClickHouse, wrap the placeholders with `toDateTime(...)` when needed. Tables without `chunking` continue to run once. Each chunk is fetched fully and compared before the next chunk; this is chunked execution, not cursor streaming. Chunk ranges must use the same timezone and deterministic ordering in both databases.

```yaml
chunking:
	start: '2026-08-01 00:00:00'
	end_exclusive: '2026-09-14 00:00:00'
	chunk_size_days: 1 # Or use chunk_size_hours: 1 (or any positive integer) for hourly validation
```

### Total Records Smoke Test (`count_only`)

To perform a fast total record count comparison across the specified date range without downloading or comparing individual rows, enable `count_only: true` either per table or globally:

```yaml
tables:
  - table_name: conversation_metadata
    count_only: true  # or smoke_test: true
    chunking:
      start: '2024-10-08 00:00:00'
      end_exclusive: '2026-10-08 00:00:00'
      chunk_size_days: 30
```

When enabled, the validator wraps queries in `COUNT(*)` for each chunk directly in Aurora MySQL and ClickHouse. If any chunk has a count mismatch, it logs the discrepancy and writes a diff report with the mismatched chunk date ranges and query definitions. Primary keys are not required in this mode.

Count-only smoke tests are prominently highlighted across the console with bright yellow badges (`⚡ COUNT-ONLY SMOKE TEST ⚡`), explicit scope warnings in table summary panels, chunk-level `[COUNT ONLY]` tags, and separate breakdown metrics in the Final Run Summary so smoke tests can never be mistaken for full row validations.

```sh
.venv/bin/python db_validator.py config.yaml
.venv/bin/python -m pytest -q
```

Exit code `0` means every configured comparison passed, `1` means one or more comparisons found differences, and `2` means validation could not complete (for example, a connection or query error).

Numeric values are compared exactly after numeric parsing, so formatting differences such as `1`, `1.0`, and `"1.00"` are treated as equal, but any numeric value difference is reported. Equivalent date/time values and null representations are normalized. Value differences are reported with their row position and column; additional or missing trailing rows are reported separately.

To ignore minor floating-point representation differences (such as ClickHouse `Float32` vs MySQL `DECIMAL`), enable `numeric_tolerance: true` either per table or globally in `config.yaml`. This treats differences $\le \pm 0.001$ as matching (or specify a custom threshold such as `numeric_tolerance: 0.0001`). Non-numeric fields and keys continue to be compared strictly.

## Connection security

Aurora TLS is enabled for both profiles. The configured ClickHouse endpoints use HTTP on port 8123, so credentials and results are not protected by TLS in transit. Prefer HTTPS (`secure: true`) or a trusted private network/tunnel before running with real credentials. Rotate credentials that have been shared in chat before using this project.
