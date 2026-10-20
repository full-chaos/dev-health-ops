Migrations after the ClickHouse head baseline, applied by `dho migrate
clickhouse upgrade` in file-name order. Each file is recorded in
`schema_migrations` only after all its statements succeed, so every
statement must be re-runnable (`IF NOT EXISTS`).

A new migration is added ONLY here, `.sql` only. The pre-099 text is frozen in
`../history/` and `src/dev_health_ops/migrations` may not come back
(`internal/cigates` guards both).

A migration that an operator may need to undo has a hand-run file
`../rollback/<NNN_name>.down.sql`. It is not part of the chain. It undoes the
change and ends with `ALTER TABLE schema_migrations DELETE WHERE version =
'<NNN_name>.sql' SETTINGS mutations_sync = 2;`, because with the version row
left, the next `dho migrate clickhouse upgrade` reads the migration as applied
and never applies it again. `TestEveryRollbackFileDeletesItsOwnVersionRowLast`
enforces the last statement, and
`TestRollbackFilesLeaveNoVersionRowSoTheNextUpgradeAppliesTheMigrationAgain`
rehearses every rollback file through the migrator (a new file needs a row in
`rollbackRehearsals`).
