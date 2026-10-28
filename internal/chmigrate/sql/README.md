Migrations after the ClickHouse head baseline, applied by `dho migrate
clickhouse upgrade` in file-name order. Each file is recorded in
`schema_migrations` only after all its statements succeed, so every
statement must be re-runnable (`IF NOT EXISTS`).

The baseline moved once to the chain head, 144 (CHAOS-11751): the files
099..144 are in `../archive/sql/`, and `../baseline/head.json` records their
versions. A database below 144 is refused; it reaches 144 with the last
pre-squash build, or is re-created. The next migration is 145.

A new migration is added ONLY here, `.sql` only. The pre-099 text is frozen in
`../history/`, the 099..144 text in `../archive/`, and
`src/dev_health_ops/migrations` may not come back (`internal/cigates` guards
all three).

A migration that an operator may need to undo has a hand-run file
`../rollback/<NNN_name>.down.sql`. It is not part of the chain. It undoes the
change and ends with `ALTER TABLE schema_migrations DELETE WHERE version =
'<NNN_name>.sql' SETTINGS mutations_sync = 2;`, because with the version row
left, the next `dho migrate clickhouse upgrade` reads the migration as applied
and never applies it again. `TestEveryRollbackFileDeletesItsOwnVersionRowLast`
enforces the last statement. A rollback file of a version at or below the
baseline (124..144) removes a baseline version row, so this build then refuses
the database as below the head: it is valid only together with an image
rollback to a pre-squash build, which re-applies the migration.
