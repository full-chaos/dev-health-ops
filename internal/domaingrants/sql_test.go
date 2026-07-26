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

func TestParseStatement_LockTableRecordsADisjunctionNotAPrivilege(t *testing.T) {
	// A LOCK's demand is a DISJUNCTION over a mode-specific set, so it must not
	// be recorded as a PrivilegeSet entry: asserting PrivUpdate directly would be
	// a false positive for ROW EXCLUSIVE, which INSERT alone satisfies.
	res := ParseStatement(`LOCK TABLE public.sync_dispatch_outbox IN SHARE ROW EXCLUSIVE MODE`)
	has(t, res, "sync_dispatch_outbox", PrivSelect)
	if res.Tables["sync_dispatch_outbox"].Has(PrivUpdate) {
		t.Errorf("LOCK TABLE must not directly assert PrivUpdate: %+v", res.Tables["sync_dispatch_outbox"])
	}
	requirement, ok := res.LockRequirements["sync_dispatch_outbox"]
	if !ok {
		t.Fatalf("expected a LockRequirement for sync_dispatch_outbox, got %+v", res.LockRequirements)
	}
	if requirement.Mode != "SHARE ROW EXCLUSIVE" {
		t.Errorf("Mode = %q, want SHARE ROW EXCLUSIVE", requirement.Mode)
	}
	// Measured on PostgreSQL 18.4: INSERT does NOT satisfy this mode.
	if requirement.Satisfying.Has(PrivInsert) {
		t.Errorf("SHARE ROW EXCLUSIVE must not accept INSERT: %+v", requirement.Satisfying)
	}
	if !requirement.Satisfying.Has(PrivUpdate) || !requirement.Satisfying.Has(PrivDelete) {
		t.Errorf("SHARE ROW EXCLUSIVE must accept UPDATE or DELETE: %+v", requirement.Satisfying)
	}
}

// TestParseStatement_LockTableStrictestModeWins covers a table locked twice: the
// weaker mode must never mask the stronger one's requirement.
func TestParseStatement_LockTableStrictestModeWins(t *testing.T) {
	res := ParseStatement(`LOCK TABLE public.t IN ROW EXCLUSIVE MODE;
		LOCK TABLE public.t IN ACCESS EXCLUSIVE MODE`)
	requirement := res.LockRequirements["t"]
	if requirement.Mode != "ACCESS EXCLUSIVE" {
		t.Errorf("Mode = %q, want the strictest (ACCESS EXCLUSIVE)", requirement.Mode)
	}
	if requirement.Satisfying.Has(PrivInsert) {
		t.Error("the weaker ROW EXCLUSIVE lock masked the stricter one's requirement")
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

// TestLockRequirementForMode pins the mode-to-privilege mapping MEASURED against
// PostgreSQL 18.4 (the table in LockRequirement's doc comment). Two predecessors
// of this rule both failed OPEN: a substring test for "EXCLUSIVE" (missing bare
// SHARE and ROW SHARE), then a "not ACCESS SHARE" boolean satisfied by any of
// INSERT/UPDATE/DELETE (missing that INSERT satisfies nothing above ROW
// EXCLUSIVE).
func TestLockRequirementForMode(t *testing.T) {
	t.Parallel()

	// ACCESS SHARE: SELECT alone suffices, so there is no extra demand at all.
	for _, mode := range []string{"ACCESS SHARE", "access share", "  ACCESS   SHARE  "} {
		if _, needsMore := lockRequirementForMode(mode); needsMore {
			t.Errorf("lockRequirementForMode(%q) reported an extra demand, want none", mode)
		}
	}

	// ROW SHARE and ROW EXCLUSIVE: any of INSERT/UPDATE/DELETE. Note ROW SHARE --
	// the PostgreSQL docs say it needs only SELECT; on 18.4 it does not.
	for _, mode := range []string{"ROW SHARE", "ROW EXCLUSIVE"} {
		requirement, needsMore := lockRequirementForMode(mode)
		if !needsMore {
			t.Fatalf("lockRequirementForMode(%q) reported no demand, want any-write", mode)
		}
		for _, p := range []Privilege{PrivInsert, PrivUpdate, PrivDelete} {
			if !requirement.Satisfying.Has(p) {
				t.Errorf("%s must accept %s", mode, p)
			}
		}
	}

	// Everything stricter: UPDATE or DELETE only -- INSERT must NOT satisfy.
	for _, mode := range []string{
		"SHARE UPDATE EXCLUSIVE", "SHARE", "SHARE ROW EXCLUSIVE", "EXCLUSIVE", "ACCESS EXCLUSIVE",
	} {
		requirement, needsMore := lockRequirementForMode(mode)
		if !needsMore {
			t.Fatalf("lockRequirementForMode(%q) reported no demand", mode)
		}
		if requirement.Satisfying.Has(PrivInsert) {
			t.Errorf("%s must NOT accept INSERT -- measured DENIED on 18.4", mode)
		}
		if !requirement.Satisfying.Has(PrivUpdate) || !requirement.Satisfying.Has(PrivDelete) {
			t.Errorf("%s must accept UPDATE and DELETE", mode)
		}
	}

	// An unknown mode must fail CLOSED: strictest tier, and flagged so a human
	// verifies it rather than the tool trusting a guess.
	requirement, needsMore := lockRequirementForMode("SOME FUTURE MODE")
	if !needsMore || !requirement.Unknown {
		t.Errorf("an unrecognized mode must fail closed and be flagged Unknown, got %+v", requirement)
	}
	if requirement.Satisfying.Has(PrivInsert) {
		t.Error("an unrecognized mode must be treated as the strictest tier")
	}
}

// TestAnalyzeRecordsLockRequirementPerMode exercises the same rule through the
// real statement analyzer rather than the helper alone, including the two modes
// the original substring test failed open on.
func TestAnalyzeRecordsLockRequirementPerMode(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name           string
		sql            string
		wantDemand     bool
		insertSuffices bool
	}{
		{name: "bare share", sql: "LOCK TABLE public.worker_job_outbox IN SHARE MODE",
			wantDemand: true, insertSuffices: false},
		{name: "row share", sql: "LOCK TABLE public.worker_job_outbox IN ROW SHARE MODE",
			wantDemand: true, insertSuffices: true},
		{name: "row exclusive", sql: "LOCK TABLE public.worker_job_outbox IN ROW EXCLUSIVE MODE",
			wantDemand: true, insertSuffices: true},
		{name: "share row exclusive", sql: "LOCK TABLE public.worker_job_outbox IN SHARE ROW EXCLUSIVE MODE",
			wantDemand: true, insertSuffices: false},
		{name: "access share", sql: "LOCK TABLE public.worker_job_outbox IN ACCESS SHARE MODE",
			wantDemand: false},
	} {
		result := ParseStatement(testCase.sql)
		requirement, got := result.LockRequirements["worker_job_outbox"]
		if got != testCase.wantDemand {
			t.Errorf("%s: recorded a LockRequirement = %v, want %v", testCase.name, got, testCase.wantDemand)
			continue
		}
		if !got {
			continue
		}
		if requirement.Satisfying.Has(PrivInsert) != testCase.insertSuffices {
			t.Errorf("%s: INSERT satisfies = %v, want %v", testCase.name,
				requirement.Satisfying.Has(PrivInsert), testCase.insertSuffices)
		}
	}
}
