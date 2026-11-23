//go:build integration

package syncroute

// DO NOT MERGE: a throwaway exec cell for the one-shot read FORM-SCHED-ALIVE-1. It runs the FORM bytes as embedded
// (sha256 pinned below) against the migrated schema with synthetic rows, then a few mutated copies.

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed testdata/FORM-SCHED-ALIVE-1.sql
var formSchedAlive1 string

const formSchedAlive1SHA256 = "db3b84fa41f9a9c206e11e5b5b4f2e217e05cd87f6cb446a8516d8bc8d5992df"

const (
	formTargetOrg      = "c6a38355-0000-4000-8000-000000000001"
	formOtherOrg       = "d7b49466-0000-4000-8000-000000000001"
	formTargetInteg    = "00000000-0000-4000-8000-00000000f001"
	formTargetSource   = "00000000-0000-4000-8000-00000000f002"
	formOtherInteg     = "00000000-0000-4000-8000-00000000f003"
	formOtherSource    = "00000000-0000-4000-8000-00000000f004"
	formGateName       = "STOP-COST-GATE-SCHED-ALIVE-1"
	formSchedPredicate = "r.triggered_by = 'schedule'"
	formRunOrgFilter   = "starts_with(r.org_id::text, 'c6a38355')"
	formSyncRunsBound  = "('sync_runs',     1000000::bigint, 2147483648::bigint)"
)

type formResult struct {
	cols []string
	rows [][]string
}

// formStatements strips the "--" comments and splits on ";". The FORM holds no "--" or ";" inside a literal.
func formStatements(script string) []string {
	var kept []string
	for _, line := range strings.Split(script, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		kept = append(kept, line)
	}
	var out []string
	for _, stmt := range strings.Split(strings.Join(kept, "\n"), ";") {
		if s := strings.TrimSpace(stmt); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func formValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case time.Time:
		return x.UTC().Format("2006-01-02 15:04Z")
	default:
		return fmt.Sprint(x)
	}
}

// runForm runs the script statement by statement on one connection. It returns the SELECT results in order; on the
// first error it sends ROLLBACK and returns the results so far with the error.
func runForm(ctx context.Context, t *testing.T, pool *pgxpool.Pool, script string) ([]formResult, error) {
	t.Helper()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()
	var results []formResult
	for _, stmt := range formStatements(script) {
		upper := strings.ToUpper(stmt)
		if strings.HasPrefix(upper, "BEGIN") || strings.HasPrefix(upper, "ROLLBACK") {
			if _, err := conn.Exec(ctx, stmt); err != nil {
				_, _ = conn.Exec(ctx, "ROLLBACK")
				return results, err
			}
			continue
		}
		rows, err := conn.Query(ctx, stmt)
		if err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
			return results, err
		}
		var res formResult
		for _, fd := range rows.FieldDescriptions() {
			res.cols = append(res.cols, fd.Name)
		}
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				_, _ = conn.Exec(ctx, "ROLLBACK")
				return results, err
			}
			var row []string
			for _, v := range values {
				row = append(row, formValue(v))
			}
			res.rows = append(res.rows, row)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
			return results, err
		}
		results = append(results, res)
	}
	return results, nil
}

func logForm(t *testing.T, label string, results []formResult) {
	t.Helper()
	names := []string{"stmt0", "S0", "S1", "C1", "C2"}
	for i, res := range results {
		name := fmt.Sprintf("stmt%d", i)
		if i < len(names) {
			name = names[i]
		}
		t.Logf("FORM-SCHED-ALIVE-1 %s %s cols=%s", label, name, strings.Join(res.cols, ","))
		for _, row := range res.rows {
			t.Logf("FORM-SCHED-ALIVE-1 %s %s   %s", label, name, strings.Join(row, " | "))
		}
	}
}

type formWant struct {
	s1 []string
	c1 []string
	c2 []string
}

// formProblems lists every way the results differ from the want; empty = the FORM answered as wanted.
func formProblems(results []formResult, want formWant) []string {
	var problems []string
	if len(results) != 5 {
		return []string{fmt.Sprintf("want 5 SELECT results, got %d", len(results))}
	}
	gate := results[0]
	if len(gate.rows) != 2 {
		problems = append(problems, fmt.Sprintf("stmt0: want 2 rows, got %d", len(gate.rows)))
	}
	for _, row := range gate.rows {
		if row[1] != "0" {
			problems = append(problems, fmt.Sprintf("stmt0 %s: cost_gate_ok_0 %s", row[0], row[1]))
		}
	}
	one := func(name string, res formResult, want []string) {
		if len(res.rows) != 1 {
			problems = append(problems, fmt.Sprintf("%s: want 1 row, got %d", name, len(res.rows)))
			return
		}
		if got := strings.Join(res.rows[0], "|"); got != strings.Join(want, "|") {
			problems = append(problems, fmt.Sprintf("%s: got %s want %s", name, got, strings.Join(want, "|")))
		}
	}
	one("S0", results[1], []string{"1"})
	one("S1", results[2], want.s1)
	one("C1", results[3], want.c1)
	var c2 []string
	for _, row := range results[4].rows {
		c2 = append(c2, strings.Join(row, "|"))
	}
	if strings.Join(c2, " / ") != strings.Join(want.c2, " / ") {
		problems = append(problems, fmt.Sprintf("C2: got %s want %s", strings.Join(c2, " / "), strings.Join(want.c2, " / ")))
	}
	return problems
}

func mutateForm(t *testing.T, script, from, to string, wantCount int) string {
	t.Helper()
	if got := strings.Count(script, from); got != wantCount {
		t.Fatalf("mutant %q -> %q: want %d sites, got %d", from, to, wantCount, got)
	}
	return strings.ReplaceAll(script, from, to)
}

func insertFormRun(ctx context.Context, t *testing.T, pool *pgxpool.Pool, id, org, integ, triggeredBy, mode, status, createdAt string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_runs (id, org_id, integration_id, triggered_by, mode, status,
	total_units, completed_units, failed_units, created_at)
VALUES ($1::uuid, $2, $3::uuid, $4, $5, $6, 1, 0, 0, $7::timestamptz)`,
		id, org, integ, triggeredBy, mode, status, createdAt); err != nil {
		t.Fatalf("insert sync_runs %s: %v", id, err)
	}
}

func TestFormSchedAlive1Exec(t *testing.T) {
	sum := sha256.Sum256([]byte(formSchedAlive1))
	if got := hex.EncodeToString(sum[:]); got != formSchedAlive1SHA256 {
		t.Fatalf("embedded FORM sha256 %s, want %s", got, formSchedAlive1SHA256)
	}
	t.Logf("FORM-SCHED-ALIVE-1 embedded sha256 %s (%d bytes, %d statements)", formSchedAlive1SHA256,
		len(formSchedAlive1), len(formStatements(formSchedAlive1)))

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pgschema.Apply(ctx, t, pool)

	// GATE-UNANALYZED: a never-analyzed table has reltuples -1; the gate must stop with its named error.
	var syncRunsTuples, orgTuples int64
	if err := pool.QueryRow(ctx, `
SELECT max(c.reltuples::bigint) FILTER (WHERE c.relname = 'sync_runs'),
       max(c.reltuples::bigint) FILTER (WHERE c.relname = 'organizations')
FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relname IN ('sync_runs', 'organizations')`).Scan(&syncRunsTuples, &orgTuples); err != nil {
		t.Fatalf("read reltuples: %v", err)
	}
	t.Logf("FORM-SCHED-ALIVE-1 GATE-UNANALYZED reltuples sync_runs=%d organizations=%d", syncRunsTuples, orgTuples)
	if syncRunsTuples < 0 || orgTuples < 0 {
		res, err := runForm(ctx, t, pool, formSchedAlive1)
		if err == nil || !strings.Contains(err.Error(), formGateName+" table=") || !strings.Contains(err.Error(), "rows=-1") {
			t.Errorf("GATE-UNANALYZED: want the named gate error with rows=-1, got err=%v results=%d", err, len(res))
		} else if len(res) != 0 {
			t.Errorf("GATE-UNANALYZED: a statement ran after the gate: %d results", len(res))
		} else {
			t.Logf("FORM-SCHED-ALIVE-1 GATE-UNANALYZED stopped: %v", err)
		}
	} else {
		t.Logf("FORM-SCHED-ALIVE-1 GATE-UNANALYZED not controllable here: both tables already have reltuples >= 0")
	}

	// Seed: the target org and another org of the same shape (the org filter must drop its rows).
	pgseed.Org(ctx, t, pool, formTargetOrg, "team")
	pgseed.Org(ctx, t, pool, formOtherOrg, "team")
	pgseed.EnsureSyncIntegration(ctx, t, pool, formTargetOrg, formTargetInteg, formTargetSource)
	pgseed.EnsureSyncIntegration(ctx, t, pool, formOtherOrg, formOtherInteg, formOtherSource)

	// CASE A: no schedule row of the target org since the instant; other triggers are there.
	insertFormRun(ctx, t, pool, "00000000-0000-4000-8000-00000000a101", formTargetOrg, formTargetInteg, "manual", "incremental", "success", "2026-11-23 02:00:20+00")
	insertFormRun(ctx, t, pool, "00000000-0000-4000-8000-00000000a102", formTargetOrg, formTargetInteg, "backfill", "backfill", "running", "2026-11-23 02:10:05+00")
	insertFormRun(ctx, t, pool, "00000000-0000-4000-8000-00000000a103", formTargetOrg, formTargetInteg, "schedule", "incremental", "success", "2026-11-22 17:00:00+00")
	insertFormRun(ctx, t, pool, "00000000-0000-4000-8000-00000000a104", formTargetOrg, formTargetInteg, "schedule", "incremental", "success", "2026-11-23 00:59:59+00")
	insertFormRun(ctx, t, pool, "00000000-0000-4000-8000-00000000b101", formOtherOrg, formOtherInteg, "schedule", "incremental", "success", "2026-11-23 02:30:00+00")
	insertFormRun(ctx, t, pool, "00000000-0000-4000-8000-00000000b102", formOtherOrg, formOtherInteg, "schedule", "incremental", "planned", "2026-11-23 04:00:00+00")
	insertFormRun(ctx, t, pool, "00000000-0000-4000-8000-00000000b103", formOtherOrg, formOtherInteg, "manual", "incremental", "success", "2026-11-23 02:05:00+00")
	if _, err := pool.Exec(ctx, `ANALYZE public.sync_runs, public.organizations`); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	resA, err := runForm(ctx, t, pool, formSchedAlive1)
	if err != nil {
		t.Fatalf("CASE A: %v", err)
	}
	logForm(t, "CASE-A", resA)
	wantA := formWant{
		s1: []string{"0", "NULL", "0", "0"},
		c1: []string{"2", "2026-11-23 02:10Z", "1", "1"},
		c2: []string{"backfill|1|2026-11-23 02:10Z|0|1", "manual|1|2026-11-23 02:00Z|1|0"},
	}
	if p := formProblems(resA, wantA); len(p) != 0 {
		t.Errorf("CASE A: %s", strings.Join(p, "; "))
	}

	// GATE-FORCED: bound substitutions on copies (never on the embedded bytes).
	for _, g := range []struct{ name, to, want string }{
		{"rows", "('sync_runs',     1::bigint, 2147483648::bigint)", formGateName + " table=sync_runs rows="},
		{"bytes", "('sync_runs',     1000000::bigint, 1::bigint)", formGateName + " table=sync_runs rows="},
		{"missing", "('sync_runs_x',   1000000::bigint, 2147483648::bigint)", formGateName + " table=sync_runs_x rows=missing"},
	} {
		res, err := runForm(ctx, t, pool, mutateForm(t, formSchedAlive1, formSyncRunsBound, g.to, 1))
		switch {
		case err == nil || !strings.Contains(err.Error(), g.want):
			t.Errorf("GATE-FORCED %s: want %q, got err=%v", g.name, g.want, err)
		case len(res) != 0:
			t.Errorf("GATE-FORCED %s: a statement ran after the gate: %d results", g.name, len(res))
		default:
			t.Logf("FORM-SCHED-ALIVE-1 GATE-FORCED %s stopped: %v", g.name, err)
		}
	}

	// CASE B: schedule rows of the target org since the instant (01:00:00 exactly is in).
	insertFormRun(ctx, t, pool, "00000000-0000-4000-8000-00000000a201", formTargetOrg, formTargetInteg, "schedule", "incremental", "success", "2026-11-23 01:00:00+00")
	insertFormRun(ctx, t, pool, "00000000-0000-4000-8000-00000000a202", formTargetOrg, formTargetInteg, "schedule", "incremental", "success", "2026-11-23 03:00:00+00")
	insertFormRun(ctx, t, pool, "00000000-0000-4000-8000-00000000a203", formTargetOrg, formTargetInteg, "schedule", "incremental", "partial_failed", "2026-11-23 03:15:40+00")
	insertFormRun(ctx, t, pool, "00000000-0000-4000-8000-00000000a204", formTargetOrg, formTargetInteg, "schedule", "incremental", "failed", "2026-11-23 03:20:10+00")
	insertFormRun(ctx, t, pool, "00000000-0000-4000-8000-00000000a205", formTargetOrg, formTargetInteg, "schedule", "incremental", "planned", "2026-11-23 03:45:59+00")
	insertFormRun(ctx, t, pool, "00000000-0000-4000-8000-00000000a206", formTargetOrg, formTargetInteg, "schedule", "full_resync", "dispatching", "2026-11-23 03:46:30+00")

	wantB := formWant{
		s1: []string{"6", "2026-11-23 03:46Z", "4", "2"},
		c1: []string{"8", "2026-11-23 03:46Z", "5", "3"},
		c2: []string{
			"backfill|1|2026-11-23 02:10Z|0|1",
			"manual|1|2026-11-23 02:00Z|1|0",
			"schedule|6|2026-11-23 03:46Z|4|2",
		},
	}
	resB, err := runForm(ctx, t, pool, formSchedAlive1)
	if err != nil {
		t.Fatalf("CASE B: %v", err)
	}
	logForm(t, "CASE-B", resB)
	if p := formProblems(resB, wantB); len(p) != 0 {
		t.Errorf("CASE B: %s", strings.Join(p, "; "))
	}

	// MUTANTS on copies, run on the CASE B rows: each must make the cell's own check fail.
	for _, m := range []struct {
		name, from, to string
		sites          int
	}{
		{"m1-wrong-literal", formSchedPredicate, "r.triggered_by = 'scheduled'", 1},
		{"m2-no-org-filter", formRunOrgFilter, "true", 3},
	} {
		res, err := runForm(ctx, t, pool, mutateForm(t, formSchedAlive1, m.from, m.to, m.sites))
		if err != nil {
			t.Errorf("%s: %v", m.name, err)
			continue
		}
		logForm(t, "MUTANT-"+m.name, res)
		p := formProblems(res, wantB)
		if len(p) == 0 {
			t.Errorf("%s: SURVIVED (the cell did not see the mutant)", m.name)
			continue
		}
		t.Logf("FORM-SCHED-ALIVE-1 MUTANT %s KILLED: %s", m.name, strings.Join(p, "; "))
	}
}
