//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-13826: the completeness rows go through the production sink into the migrated table (153), with merges stopped so every
// version stays in its own part and only FINAL decides. Within one run attempt a complete verdict is never replaced by a later
// partial one; a partial verdict is replaced by a later complete one; a re-run attempt is its own key, and the reader rule (the
// row of max(run_attempt), read FINAL) names the run by its newest attempt. The readback settles a write its stored row outranks.
func TestRunReportCompletenessRowsThroughTheSinkAndFinal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	chschema.Apply(ctx, t, instance)
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES testops_run_report_completeness`); err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("github", "tests")
	sink := TestOpsClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	const repo = "c7198fbc-1945-3717-05d8-eb78866b4e79"
	base := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	row := func(run string, attempt int64, complete bool, at time.Time) testOpsRunReportCompletenessRow {
		reasons := []string{githubTestsArtifactExpiredCause}
		if complete {
			reasons = []string{}
		}
		return testOpsRunReportCompletenessRow{OrgID: claim.OrgID, RepoID: repo, RunID: run, RunAttempt: attempt, Provider: "github_actions",
			Complete: complete, Reasons: reasons, ReportsExpected: 2, ReportsAdmitted: 1, LastSynced: at,
			VerdictVersion: testOpsRunReportVerdictVersion(complete, at)}
	}
	effectOf := func(rows ...testOpsRunReportCompletenessRow) EffectBatch {
		t.Helper()
		effects, err := testOpsEffects(nil, nil, nil, nil, nil, nil, rows...)
		if err != nil {
			t.Fatal(err)
		}
		for _, effect := range effects {
			if effect.Destination == testOpsRunReportCompletenessDestination {
				return effect
			}
		}
		t.Fatal("no completeness effect")
		return EffectBatch{}
	}
	write := func(rows ...testOpsRunReportCompletenessRow) EffectBatch {
		t.Helper()
		effect := effectOf(rows...)
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatalf("write: %v", err)
		}
		return effect
	}
	verdict := func(run string, attempt int64) (uint8, []string) {
		t.Helper()
		var complete uint8
		var reasons []string
		if err := conn.QueryRow(ctx, `SELECT complete, CAST(reasons, 'Array(String)') FROM testops_run_report_completeness FINAL
WHERE org_id = ? AND repo_id = ? AND run_id = ? AND run_attempt = ?`, claim.OrgID, repo, run, attempt).Scan(&complete, &reasons); err != nil {
			t.Fatal(err)
		}
		return complete, reasons
	}
	inspect := func(effect EffectBatch) EffectInspection {
		t.Helper()
		got, err := sink.InspectEffect(ctx, claim, effect)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	// Run A, attempt 1: complete, then a later walk loses a report. The complete row stays.
	first := write(row("A", 1, true, base))
	if got := inspect(first); got != EffectExact {
		t.Fatalf("readback of the first write = %v, want exact", got)
	}
	later := write(row("A", 1, false, base.Add(time.Hour)))
	if complete, reasons := verdict("A", 1); complete != 1 || len(reasons) != 0 {
		t.Fatalf("A attempt 1 after a later partial walk: complete=%d reasons=%v, want 1 [] (no downgrade)", complete, reasons)
	}
	if got := inspect(later); got != EffectExact {
		t.Fatalf("readback of the outranked partial write = %v, want exact (settled)", got)
	}

	// Run B, attempt 1: partial, then a later walk reads every report. It becomes complete.
	write(row("B", 1, false, base))
	write(row("B", 1, true, base.Add(time.Hour)))
	if complete, _ := verdict("B", 1); complete != 1 {
		t.Fatalf("B attempt 1 after a later complete walk: complete=%d, want 1", complete)
	}
	if got := inspect(effectOf(row("B", 1, true, base.Add(2*time.Hour)))); got != EffectAbsent {
		t.Fatalf("readback of a newer complete row that was never written = %v, want absent", got)
	}

	// Run A, attempt 2: a re-run loses a report. It is its own key; the reader rule names run A partial.
	write(row("A", 2, false, base.Add(2*time.Hour)))
	if complete, reasons := verdict("A", 2); complete != 0 || len(reasons) != 1 || reasons[0] != githubTestsArtifactExpiredCause {
		t.Fatalf("A attempt 2: complete=%d reasons=%v, want 0 [artifact_expired]", complete, reasons)
	}
	rows, err := conn.Query(ctx, `SELECT run_id, complete FROM testops_run_report_completeness FINAL
WHERE org_id = ? AND (repo_id, run_id, run_attempt) IN (
  SELECT repo_id, run_id, max(run_attempt) FROM testops_run_report_completeness FINAL WHERE org_id = ? GROUP BY repo_id, run_id)
ORDER BY run_id`, claim.OrgID, claim.OrgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]uint8{}
	for rows.Next() {
		var run string
		var complete uint8
		if err := rows.Scan(&run, &complete); err != nil {
			t.Fatal(err)
		}
		got[run] = complete
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["A"] != 0 || got["B"] != 1 {
		t.Fatalf("newest-attempt verdicts %v, want A=0 (attempt 2 partial) B=1", got)
	}

	var parts uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'testops_run_report_completeness' AND active`).Scan(&parts); err != nil {
		t.Fatal(err)
	}
	if parts < 5 {
		t.Fatalf("active parts = %d, want one per write (merges stopped); the cell did not measure FINAL", parts)
	}

	// The sink refuses a row whose version is not its verdict's.
	bad := row("C", 1, false, base)
	bad.VerdictVersion = testOpsRunReportVerdictVersion(true, base)
	if err := sink.WriteEffect(ctx, claim, effectOf(bad)); err == nil {
		t.Fatal("a partial row carrying a complete version was written")
	}
}
