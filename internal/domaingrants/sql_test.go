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
	// A lock-only statement asserts NO per-privilege requirement at all -- not
	// UPDATE (the demand is a disjunction, not a specific privilege) and not
	// SELECT (the backend's check is one OR-mask; requiring SELECT alongside it
	// over-grants every lock-only path).
	if !res.Tables["sync_dispatch_outbox"].Empty() {
		t.Errorf("a lock-only statement must assert no standalone privilege: %+v",
			res.Tables["sync_dispatch_outbox"])
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

func TestParseStatement_MaterializedCTEIsNotATable(t *testing.T) {
	sql := `
WITH candidates AS MATERIALIZED (
	SELECT conversation.id
	FROM public.dev_conversations AS conversation
	FOR UPDATE OF conversation
)
DELETE FROM public.dev_conversations AS conversation
USING candidates
WHERE conversation.id = candidates.id`
	res := ParseStatement(sql)
	has(t, res, "dev_conversations", PrivSelect, PrivUpdate, PrivDelete)
	notPresent(t, res, "candidates")
}

func TestParseStatement_CommentsDoNotSmuggleTables(t *testing.T) {
	sql := `-- FROM public.fake_table
SELECT 1 /* JOIN public.also_fake */ FROM public.real_table`
	res := ParseStatement(sql)
	notPresent(t, res, "fake_table")
	notPresent(t, res, "also_fake")
	has(t, res, "real_table", PrivSelect)
}

// TestLockRequirementForMode pins the mode-to-privilege DISJUNCTION derived from
// LockTableAclCheck's OR-mask and measured on PostgreSQL 18.4 -- see the table in
// sql.go. Every privilege is tested ALONE, because the previous version of this
// test granted SELECT in every fixture, which made a conjunction and a
// disjunction indistinguishable and let a wrong OPERATOR ship with correct tiers.
func TestLockRequirementForMode(t *testing.T) {
	t.Parallel()

	// mode -> the privileges that must EACH, on their own, satisfy it.
	satisfiesAlone := map[string][]Privilege{
		"ACCESS SHARE":           {PrivSelect, PrivInsert, PrivUpdate, PrivDelete},
		"ROW SHARE":              {PrivInsert, PrivUpdate, PrivDelete},
		"ROW EXCLUSIVE":          {PrivInsert, PrivUpdate, PrivDelete},
		"SHARE UPDATE EXCLUSIVE": {PrivUpdate, PrivDelete},
		"SHARE":                  {PrivUpdate, PrivDelete},
		"SHARE ROW EXCLUSIVE":    {PrivUpdate, PrivDelete},
		"EXCLUSIVE":              {PrivUpdate, PrivDelete},
		"ACCESS EXCLUSIVE":       {PrivUpdate, PrivDelete},
	}
	for mode, expected := range satisfiesAlone {
		requirement, known := lockRequirementForMode(mode)
		if !known {
			t.Fatalf("%s must be a recognized mode", mode)
		}
		want := map[Privilege]bool{}
		for _, p := range expected {
			want[p] = true
			// Each one ALONE must satisfy: this is what distinguishes the
			// disjunction from a conjunction with SELECT.
			var alone PrivilegeSet
			alone.add(p)
			if !lockSatisfiedBy(requirement, alone) {
				t.Errorf("%s: %s ALONE must satisfy the lock (measured ok on 18.4)", mode, p)
			}
		}
		for p := Privilege(0); p < numPrivileges; p++ {
			if !want[p] && lockSatisfiedBy(requirement, singleton(p)) {
				t.Errorf("%s: %s ALONE must NOT satisfy the lock (measured DENIED on 18.4)", mode, p)
			}
		}
	}

	// The whole point of the correction: a lock-only path is NOT required to hold
	// SELECT. If SELECT were a separate requirement, UPDATE alone would fail here.
	strict, _ := lockRequirementForMode("SHARE ROW EXCLUSIVE")
	if !lockSatisfiedBy(strict, singleton(PrivUpdate)) {
		t.Error("UPDATE alone must satisfy SHARE ROW EXCLUSIVE: modelling the demand as " +
			"'SELECT AND a write privilege' over-grants every lock-only path, which is the failure " +
			"the two-role split exists to prevent")
	}

	// An unknown mode is NOT a guess. It is refused, so the caller records it as
	// an unparsed statement and the gate fails. The previous version returned a
	// guessed strict set and called that fail-closed; it was not, because a role
	// already holding UPDATE satisfied the guess and nothing was ever reported.
	if _, known := lockRequirementForMode("SOME FUTURE MODE"); known {
		t.Error("an unrecognized mode must be REFUSED, not given a guessed privilege set: a guess " +
			"that happens to be satisfied is indistinguishable from knowledge")
	}
}

func singleton(p Privilege) PrivilegeSet {
	var set PrivilegeSet
	set.add(p)
	return set
}

// TestParseLockStatementGrammar covers every shape PostgreSQL accepts, all nine
// of which were verified against a live 18.4 server. The previous regex
// recognised only the first, and silently derived NOTHING for the rest -- no
// table, no privilege, no diagnostic.
func TestParseLockStatementGrammar(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		sql     string
		targets []string
		mode    string
	}{
		{"LOCK TABLE public.t IN SHARE ROW EXCLUSIVE MODE", []string{"public.t"}, "SHARE ROW EXCLUSIVE"},
		{"LOCK public.t IN SHARE ROW EXCLUSIVE MODE", []string{"public.t"}, "SHARE ROW EXCLUSIVE"},
		{"LOCK TABLE public.t", []string{"public.t"}, "ACCESS EXCLUSIVE"},
		{"LOCK public.t", []string{"public.t"}, "ACCESS EXCLUSIVE"},
		{"LOCK TABLE public.a, public.b IN SHARE MODE", []string{"public.a", "public.b"}, "SHARE"},
		{"LOCK TABLE ONLY public.t IN EXCLUSIVE MODE", []string{"public.t"}, "EXCLUSIVE"},
		{"LOCK TABLE public.t * IN SHARE MODE", []string{"public.t"}, "SHARE"},
		{"LOCK TABLE public.t IN SHARE MODE NOWAIT", []string{"public.t"}, "SHARE"},
		{"LOCK   TABLE   public.t   IN   SHARE   ROW   EXCLUSIVE   MODE", []string{"public.t"}, "SHARE ROW EXCLUSIVE"},
	} {
		statements, unparsed := parseLockStatements(tc.sql)
		if len(unparsed) != 0 {
			t.Errorf("%q reported unparsed: %v", tc.sql, unparsed)
			continue
		}
		if len(statements) != 1 {
			t.Errorf("%q produced %d statements, want 1", tc.sql, len(statements))
			continue
		}
		if statements[0].Mode != tc.mode {
			t.Errorf("%q mode = %q, want %q", tc.sql, statements[0].Mode, tc.mode)
		}
		if len(statements[0].Targets) != len(tc.targets) {
			t.Errorf("%q targets = %v, want %v", tc.sql, statements[0].Targets, tc.targets)
			continue
		}
		for i, want := range tc.targets {
			if statements[0].Targets[i] != want {
				t.Errorf("%q target %d = %q, want %q", tc.sql, i, statements[0].Targets[i], want)
			}
		}
	}
}

// TestParseLockStatementReportsWhatItCannotParse is the fail-closed half. A LOCK
// the parser does not understand must be REPORTED, never dropped.
func TestParseLockStatementReportsWhatItCannotParse(t *testing.T) {
	t.Parallel()

	for _, sql := range []string{
		"LOCK TABLE public.t IN NONSENSE MODE",
		"LOCK TABLE public.t IN SHARE",
		"LOCK TABLE",
	} {
		statements, unparsed := parseLockStatements(sql)
		if len(unparsed) == 0 {
			t.Errorf("%q was silently ignored (%d statements parsed); an unparseable LOCK must be "+
				"reported so the gate fails rather than treating it as an absence", sql, len(statements))
		}
	}

	// And the word LOCK inside another statement must NOT be read as one, since a
	// false positive here is now a false GATE FAILURE.
	for _, sql := range []string{
		"SELECT id FROM public.t FOR UPDATE SKIP LOCKED",
		"SELECT lock_key FROM public.t",
	} {
		if _, unparsed := parseLockStatements(sql); len(unparsed) != 0 {
			t.Errorf("%q must not be read as a LOCK statement: %v", sql, unparsed)
		}
	}
}

// TestAnalyzeRecordsLockRequirementPerMode exercises the rule through the real
// statement analyzer, and pins that a LOCK does NOT add a SELECT requirement.
func TestAnalyzeRecordsLockRequirementPerMode(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name           string
		sql            string
		insertSuffices bool
		selectSuffices bool
	}{
		{name: "access share", sql: "LOCK TABLE public.worker_job_outbox IN ACCESS SHARE MODE",
			insertSuffices: true, selectSuffices: true},
		{name: "row share", sql: "LOCK TABLE public.worker_job_outbox IN ROW SHARE MODE",
			insertSuffices: true, selectSuffices: false},
		{name: "row exclusive", sql: "LOCK TABLE public.worker_job_outbox IN ROW EXCLUSIVE MODE",
			insertSuffices: true, selectSuffices: false},
		{name: "bare share", sql: "LOCK TABLE public.worker_job_outbox IN SHARE MODE",
			insertSuffices: false, selectSuffices: false},
		{name: "share row exclusive", sql: "LOCK TABLE public.worker_job_outbox IN SHARE ROW EXCLUSIVE MODE",
			insertSuffices: false, selectSuffices: false},
		{name: "mode omitted defaults to access exclusive", sql: "LOCK TABLE public.worker_job_outbox",
			insertSuffices: false, selectSuffices: false},
	} {
		result := ParseStatement(testCase.sql)
		requirement, got := result.LockRequirements["worker_job_outbox"]
		if !got {
			t.Errorf("%s: no LockRequirement recorded", testCase.name)
			continue
		}
		if requirement.Satisfying.Has(PrivInsert) != testCase.insertSuffices {
			t.Errorf("%s: INSERT satisfies = %v, want %v", testCase.name,
				requirement.Satisfying.Has(PrivInsert), testCase.insertSuffices)
		}
		if requirement.Satisfying.Has(PrivSelect) != testCase.selectSuffices {
			t.Errorf("%s: SELECT satisfies = %v, want %v", testCase.name,
				requirement.Satisfying.Has(PrivSelect), testCase.selectSuffices)
		}
		// A lock-only statement must NOT assert a standalone SELECT requirement.
		// The backend's check is one OR-mask; requiring SELECT alongside it
		// over-grants every lock-only path.
		if result.Tables["worker_job_outbox"].Has(PrivSelect) {
			t.Errorf("%s: a LOCK must not add a separate SELECT requirement -- the demand is a "+
				"disjunction, so a path authorized by UPDATE alone needs no SELECT", testCase.name)
		}
	}
}
