package domaingrants

import "testing"

func has(t *testing.T, res StatementResult, table string, privs ...Privilege) {
	t.Helper()
	set, ok := res.Tables[table]
	if !ok {
		t.Fatalf("table %q not found in %+v", table, res.Tables)
	}
	for _, p := range privs {
		if !set.Has(p) {
			t.Errorf("table %q missing expected privilege %s (have %+v)", table, p, set)
		}
	}
}

func notPresent(t *testing.T, res StatementResult, table string) {
	t.Helper()
	if _, ok := res.Tables[table]; ok {
		t.Errorf("table %q unexpectedly present: %+v", table, res.Tables[table])
	}
}

func TestParseStatement_InsertOnConflictDoNothing(t *testing.T) {
	sql := `
INSERT INTO public.daily_metrics_runs
    (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3::date, $4, 'pending', 'pending', $5, $5)
ON CONFLICT DO NOTHING`
	res := ParseStatement(sql)
	has(t, res, "daily_metrics_runs", PrivInsert)
	if res.Tables["daily_metrics_runs"].Has(PrivUpdate) {
		t.Errorf("DO NOTHING must not imply UPDATE")
	}
}

func TestParseStatement_SimpleSelect(t *testing.T) {
	res := ParseStatement(`SELECT status FROM public.daily_metrics_runs WHERE id = $1::uuid`)
	has(t, res, "daily_metrics_runs", PrivSelect)
}

func TestParseStatement_CTEWithSelectAndUpdate_ExcludesCTEAliases(t *testing.T) {
	sql := `
WITH authenticated AS (
    SELECT id, scopes FROM public.internal_service_credentials WHERE token_hash = $1
), touched AS (
    UPDATE public.internal_service_credentials AS credential SET last_used_at = statement_timestamp()
    WHERE credential.id = (SELECT id FROM authenticated)
    RETURNING id, scopes
)
SELECT id, scopes FROM touched`
	res := ParseStatement(sql)
	has(t, res, "internal_service_credentials", PrivSelect, PrivUpdate)
	notPresent(t, res, "authenticated")
	notPresent(t, res, "touched")
}

func TestParseStatement_UpsertOnConflictDoUpdate(t *testing.T) {
	sql := `INSERT INTO public.sync_watermarks (a) VALUES ($1)
ON CONFLICT (a) DO UPDATE SET a = EXCLUDED.a`
	res := ParseStatement(sql)
	has(t, res, "sync_watermarks", PrivInsert, PrivUpdate)
}

func TestParseStatement_DeleteFrom(t *testing.T) {
	res := ParseStatement(`DELETE FROM public.worker_job_outbox WHERE id = $1`)
	has(t, res, "worker_job_outbox", PrivDelete)
}

func TestParseStatement_ForUpdateEscalatesToUpdate(t *testing.T) {
	res := ParseStatement(`SELECT status FROM public.sync_configurations WHERE id = $1::uuid FOR UPDATE`)
	has(t, res, "sync_configurations", PrivSelect, PrivUpdate)
}

func TestParseStatement_JoinAcrossTables(t *testing.T) {
	sql := `
SELECT run.id
FROM public.sync_runs AS run
JOIN public.sync_run_units AS unit ON unit.sync_run_id = run.id`
	res := ParseStatement(sql)
	has(t, res, "sync_runs", PrivSelect)
	has(t, res, "sync_run_units", PrivSelect)
}

func TestParseStatement_FunctionCallAfterFromIsNotATable(t *testing.T) {
	res := ParseStatement(`SELECT * FROM generate_series(1, 10) AS s`)
	notPresent(t, res, "generate_series")
}

func TestParseStatement_LateralIsNotATable(t *testing.T) {
	sql := `SELECT * FROM public.sync_runs AS run, LATERAL (SELECT 1) AS x`
	res := ParseStatement(sql)
	notPresent(t, res, "lateral")
	has(t, res, "sync_runs", PrivSelect)
}

func TestParseStatement_NonPublicSchemaExcluded(t *testing.T) {
	sql := `SELECT id FROM river.river_job WHERE id = $1`
	res := ParseStatement(sql)
	notPresent(t, res, "river_job")
	if !res.NonPublicTables["river_job"] {
		t.Errorf("expected river_job recorded as non-public, got %+v", res.NonPublicTables)
	}
}

func TestParseStatement_LockTableExclusiveModeRequiresAnyWriteNotSpecificallyUpdate(t *testing.T) {
	// Postgres requires SELECT plus at least one of INSERT/UPDATE/DELETE
	// for a mode stricter than ROW EXCLUSIVE -- NOT specifically UPDATE.
	// internal/jobroute/control.go's `LOCK TABLE worker_job_outbox IN SHARE
	// ROW EXCLUSIVE MODE` is already satisfied by an existing INSERT grant;
	// asserting PrivUpdate here would be a false positive (see
	// RequiresAnyWriteLock's doc comment).
	res := ParseStatement(`LOCK TABLE public.sync_dispatch_outbox IN SHARE ROW EXCLUSIVE MODE`)
	has(t, res, "sync_dispatch_outbox", PrivSelect)
	if res.Tables["sync_dispatch_outbox"].Has(PrivUpdate) {
		t.Errorf("LOCK TABLE must not directly assert PrivUpdate: %+v", res.Tables["sync_dispatch_outbox"])
	}
	if !res.RequiresAnyWriteLock["sync_dispatch_outbox"] {
		t.Errorf("expected sync_dispatch_outbox in RequiresAnyWriteLock, got %+v", res.RequiresAnyWriteLock)
	}
}

func TestParseStatement_ForUpdateOfScopesEscalationToNamedAlias(t *testing.T) {
	// Mirrors internal/providersync/repository_postgres.go's claimUnitSQL:
	// locks only `unit` (sync_run_units) while joining other tables
	// read-only for enrichment. Those other tables must NOT be escalated to
	// UPDATE just because SOME table in the statement is locked.
	sql := `
SELECT unit.id, integ.name
FROM public.sync_run_units AS unit
JOIN public.integrations AS integ ON integ.id = unit.integration_id
WHERE unit.status = 'pending'
FOR UPDATE OF unit`
	res := ParseStatement(sql)
	has(t, res, "sync_run_units", PrivSelect, PrivUpdate)
	has(t, res, "integrations", PrivSelect)
	if res.Tables["integrations"].Has(PrivUpdate) {
		t.Errorf("integrations must not be escalated to UPDATE: FOR UPDATE OF unit only locks sync_run_units, got %+v", res.Tables["integrations"])
	}
}

func TestParseStatement_ForUpdateWithoutOfEscalatesAllTables(t *testing.T) {
	sql := `
SELECT a.id
FROM public.sync_configurations AS a
JOIN public.scheduled_jobs AS b ON b.config_id = a.id
FOR UPDATE`
	res := ParseStatement(sql)
	has(t, res, "sync_configurations", PrivSelect, PrivUpdate)
	has(t, res, "scheduled_jobs", PrivSelect, PrivUpdate)
}

func TestParseStatement_CTEWithExplicitColumnListIsNotATable(t *testing.T) {
	// Mirrors domain_authorization.go's own
	// `required_table_privileges(table_name, allow_insert, allow_update) AS (...)`
	// shape, including a later self-reference to the CTE name.
	sql := `
WITH required_table_privileges(table_name, allow_insert, allow_update) AS (
	VALUES ('integrations', false, false)
), required_tables AS (
	SELECT class.oid
	FROM required_table_privileges AS required
	JOIN pg_catalog.pg_class AS class ON class.relname = required.table_name
)
SELECT count(*) FROM required_tables`
	res := ParseStatement(sql)
	notPresent(t, res, "required_table_privileges")
	notPresent(t, res, "required_tables")
}

func TestParseStatement_CommentsDoNotSmuggleTables(t *testing.T) {
	sql := `-- FROM public.fake_table
SELECT 1 /* JOIN public.also_fake */ FROM public.real_table`
	res := ParseStatement(sql)
	notPresent(t, res, "fake_table")
	notPresent(t, res, "also_fake")
	has(t, res, "real_table", PrivSelect)
}

// TestLockModeRequiresWritePrivilege pins the PostgreSQL LOCK privilege rule
// this detector previously got wrong. It matched a substring "EXCLUSIVE", which
// silently missed SHARE and ROW SHARE — both of which require a write privilege
// — so a LOCK in either mode passed the grant-surface gate unflagged.
func TestLockModeRequiresWritePrivilege(t *testing.T) {
	t.Parallel()

	// Only ACCESS SHARE is satisfied by SELECT alone.
	for _, mode := range []string{"ACCESS SHARE", "access share", "  ACCESS   SHARE  "} {
		if lockModeRequiresWritePrivilege(mode) {
			t.Errorf("lockModeRequiresWritePrivilege(%q) = true, want false", mode)
		}
	}
	// Every other mode needs at least one write privilege. SHARE and ROW SHARE
	// are the two the old substring test failed open on.
	for _, mode := range []string{
		"ROW SHARE",
		"SHARE",
		"ROW EXCLUSIVE",
		"SHARE UPDATE EXCLUSIVE",
		"SHARE ROW EXCLUSIVE",
		"EXCLUSIVE",
		"ACCESS EXCLUSIVE",
	} {
		if !lockModeRequiresWritePrivilege(mode) {
			t.Errorf("lockModeRequiresWritePrivilege(%q) = false, want true", mode)
		}
	}
}

// TestAnalyzeFlagsBareShareLockAsRequiringWrite exercises the same rule through
// the real statement analyzer rather than the helper alone.
func TestAnalyzeFlagsBareShareLockAsRequiringWrite(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name string
		sql  string
		want bool
	}{
		{name: "bare share", sql: "LOCK TABLE public.worker_job_outbox IN SHARE MODE", want: true},
		{name: "row share", sql: "LOCK TABLE public.worker_job_outbox IN ROW SHARE MODE", want: true},
		{name: "access share", sql: "LOCK TABLE public.worker_job_outbox IN ACCESS SHARE MODE", want: false},
	} {
		result := ParseStatement(testCase.sql)
		if got := result.RequiresAnyWriteLock["worker_job_outbox"]; got != testCase.want {
			t.Errorf("%s: RequiresAnyWriteLock = %v, want %v", testCase.name, got, testCase.want)
		}
	}
}
