Migrations after the ClickHouse head baseline, applied by `dho migrate
clickhouse upgrade` in file-name order. Each file is recorded in
`schema_migrations` only after all its statements succeed, so every
statement must be re-runnable (`IF NOT EXISTS`).

Until the Python chain is deleted, a new migration is added in both places:
here and in `src/dev_health_ops/migrations/clickhouse/`, byte-identical and
`.sql` only. `TestChainAfterHeadMatchesThePythonChain` enforces it.
