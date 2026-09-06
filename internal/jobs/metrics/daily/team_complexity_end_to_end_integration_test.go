//go:build integration

package daily

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestTeamComplexityEndToEndFromNativeComplexityExecutor is CHAOS-4291's
// byte-identity close-out for the complexity family's native cutover: it
// proves the two independently-ported native executors compose correctly
// when chained through a REAL ClickHouse -- remaining.ComplexityExecutor
// (per-repo scan, wired in this same PR) writing repo_complexity_daily, and
// TeamComplexityExecutor (CHAOS-5051, already on main) reading it back and
// aggregating by team.
//
// Each side's own arithmetic is already proven byte-identical to Python
// SEPARATELY: complexity's own Testcontainers parity test
// (TestComplexityExecutorMatchesPythonAgainstClickHouse, package remaining)
// against its frozen golden, and team_complexity's own frozen-golden test
// (TestTeamComplexityMatchesTheFrozenPythonGolden, this package) for
// buildTeamComplexityRows. Neither proves the SEAM between them: that a real
// ComplexityExecutor write is actually the shape loadRepoComplexityInputsForDay
// expects to read. That seam is this test's only job.
//
// Two repos, IDENTICAL content to remaining's own parity fixture (a
// multi-function Python file and a multi-function Go file), same team: the
// per-repo numbers are already pinned by that test's frozen golden (LOC 24,
// cyclomatic 8 per repo -- see testdata/complexity_python_golden.json in the
// remaining package), so the team-level SUM this test expects (LOC 48,
// cyclomatic 16, two contributing repos) is an exact arithmetic consequence
// of a value already proven correct, not a fresh number asserted on trust.
func TestTeamComplexityEndToEndFromNativeComplexityExecutor(t *testing.T) {
	ctx := context.Background()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)

	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer conn.Close()

	const orgID = "88888888-8888-4888-8888-888888888888"
	repoOne := uuid.MustParse("99999999-9999-4999-8999-999999999991")
	repoTwo := uuid.MustParse("99999999-9999-4999-8999-999999999992")
	const day = "2026-05-01"
	lastSynced := mustParseTeamComplexityE2ETime(t, "2026-05-01T00:00:00Z")

	seedTeamComplexityE2ERepo(t, ctx, conn, repoOne, orgID, "acme/repo-one", lastSynced)
	seedTeamComplexityE2ERepo(t, ctx, conn, repoTwo, orgID, "acme/repo-two", lastSynced)

	// One team, pattern owning BOTH repos -- the pure prefix-pattern path
	// (no authoritative-ownership row seeded), same resolution shape
	// resolveDailyFinalizeRepoToTeam falls back to for a repo its ownership
	// read leaves unresolved.
	if err := conn.Exec(ctx,
		"INSERT INTO teams (id, name, members, repo_patterns, org_id) VALUES (?, ?, ?, ?, ?)",
		"team-metrics", "Metrics Team", []string{}, []string{"acme/*"}, orgID,
	); err != nil {
		t.Fatalf("insert team: %v", err)
	}

	complexityExecutor, err := remaining.NewComplexityExecutor(ctx, conn, teamComplexityE2EConfigPath(t), nil)
	if err != nil {
		t.Fatalf("new complexity executor: %v", err)
	}
	for _, repoID := range []uuid.UUID{repoOne, repoTwo} {
		scope, err := json.Marshal(map[string]any{
			"version":       1,
			"day":           day,
			"backfill_days": 1,
			"repo_id":       repoID.String(),
		})
		if err != nil {
			t.Fatalf("marshal scope: %v", err)
		}
		outcome, err := complexityExecutor.ComputePartition(
			ctx, remaining.Run{OrganizationID: orgID}, remaining.Partition{ID: "p-" + repoID.String(), Scope: scope},
		)
		if err != nil {
			t.Fatalf("complexity executor for %s: %v", repoID, err)
		}
		if outcome.RowsWritten == nil || *outcome.RowsWritten == 0 {
			t.Fatalf("complexity executor for %s wrote no rows -- fixture is vacuous", repoID)
		}
	}

	teamExecutor, err := NewTeamComplexityExecutor(conn)
	if err != nil {
		t.Fatalf("new team complexity executor: %v", err)
	}
	targetDay, err := time.Parse("2006-01-02", day)
	if err != nil {
		t.Fatalf("parse target day: %v", err)
	}
	rowsWritten, err := teamExecutor.ComputeFinalizeFamily(ctx, Run{OrganizationID: orgID, TargetDay: targetDay})
	if err != nil {
		t.Fatalf("team complexity finalize: %v", err)
	}
	if rowsWritten != 1 {
		t.Fatalf("rows written = %d, want 1 (one team)", rowsWritten)
	}

	rows, err := conn.Query(ctx, `
		SELECT team_id, loc_total, cyclomatic_total, cyclomatic_per_kloc,
		       high_complexity_functions, very_high_complexity_functions, contributing_repo_count
		FROM team_complexity_daily
		WHERE org_id = ? AND day = ?
	`, orgID, day)
	if err != nil {
		t.Fatalf("read team_complexity_daily: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("no team_complexity_daily row for org %s day %s", orgID, day)
	}
	var (
		teamID                                                                            string
		locTotal, cyclomaticTotal, highComplexity, veryHighComplexity, contributingRepoCnt uint64
		cyclomaticPerKLOC                                                                  float64
	)
	if err := rows.Scan(
		&teamID, &locTotal, &cyclomaticTotal, &cyclomaticPerKLOC,
		&highComplexity, &veryHighComplexity, &contributingRepoCnt,
	); err != nil {
		t.Fatalf("scan team_complexity_daily row: %v", err)
	}

	// The exact SUM of two identical repos already proven byte-identical to
	// Python individually (see this test's own doc): 2x each per-repo total
	// from testdata/complexity_python_golden.json's repo_daily block.
	if teamID != "team-metrics" {
		t.Errorf("team_id = %q, want team-metrics", teamID)
	}
	if locTotal != 48 {
		t.Errorf("loc_total = %d, want 48 (2x the proven per-repo 24)", locTotal)
	}
	if cyclomaticTotal != 16 {
		t.Errorf("cyclomatic_total = %d, want 16 (2x the proven per-repo 8)", cyclomaticTotal)
	}
	if contributingRepoCnt != 2 {
		t.Errorf("contributing_repo_count = %d, want 2", contributingRepoCnt)
	}
	// cyclomatic_per_kloc is recomputed from the SUMMED totals (loc-weighted,
	// see buildTeamComplexityRows' own doc), not averaged per-repo -- with
	// two IDENTICAL repos the two happen to coincide (333.33...), but the sum
	// path is what this test actually exercises.
	wantPerKLOC := float64(16) / (float64(48) / 1000.0)
	if diff := cyclomaticPerKLOC - wantPerKLOC; diff > 1e-6 || diff < -1e-6 {
		t.Errorf("cyclomatic_per_kloc = %v, want %v", cyclomaticPerKLOC, wantPerKLOC)
	}
}

// teamComplexityE2EConfigPath resolves the real complexity.yaml from the
// repo root -- same file remaining.complexityTestConfigPath resolves,
// duplicated here because that helper is unexported in that package.
func teamComplexityE2EConfigPath(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	return filepath.Join(root, "src", "dev_health_ops", "config", "complexity.yaml")
}

func mustParseTeamComplexityE2ETime(t *testing.T, text string) time.Time {
	t.Helper()
	value, err := time.Parse(time.RFC3339, text)
	if err != nil {
		t.Fatalf("parse time %q: %v", text, err)
	}
	return value
}

// seedTeamComplexityE2ERepo writes IDENTICAL content to
// remaining.seedComplexityFixture's own git_files fixture (duplicated here
// because that helper is unexported in the remaining package): a real
// multi-function Python file and a real multi-function Go file, whose
// combined LOC/cyclomatic totals are the exact numbers frozen in
// remaining's own testdata/complexity_python_golden.json.
func seedTeamComplexityE2ERepo(
	t *testing.T, ctx context.Context, conn driver.Conn, repoID uuid.UUID, orgID, name string, lastSynced time.Time,
) {
	t.Helper()
	if err := conn.Exec(ctx,
		"INSERT INTO repos (id, repo, org_id, last_synced) VALUES (?, ?, ?, ?)",
		repoID, name, orgID, lastSynced,
	); err != nil {
		t.Fatalf("insert repo %s: %v", name, err)
	}

	pythonSource := "" +
		"def add(a, b):\n" +
		"    return a + b\n" +
		"\n" +
		"def classify(n):\n" +
		"    if n < 0:\n" +
		"        return \"negative\"\n" +
		"    elif n == 0:\n" +
		"        return \"zero\"\n" +
		"    else:\n" +
		"        return \"positive\"\n"
	if err := conn.Exec(ctx,
		"INSERT INTO git_files (repo_id, path, executable, contents, last_synced, org_id) VALUES (?, ?, ?, ?, ?, ?)",
		repoID, "src/main.py", uint8(0), pythonSource, lastSynced, orgID,
	); err != nil {
		t.Fatalf("insert git_files src/main.py: %v", err)
	}

	goSource := "" +
		"package main\n" +
		"\n" +
		"func add(a, b int) int {\n" +
		"	return a + b\n" +
		"}\n" +
		"\n" +
		"func classify(n int) string {\n" +
		"	if n < 0 {\n" +
		"		return \"negative\"\n" +
		"	} else if n == 0 {\n" +
		"		return \"zero\"\n" +
		"	}\n" +
		"	return \"positive\"\n" +
		"}\n"
	if err := conn.Exec(ctx,
		"INSERT INTO git_files (repo_id, path, executable, contents, last_synced, org_id) VALUES (?, ?, ?, ?, ?, ?)",
		repoID, "src/main.go", uint8(0), goSource, lastSynced, orgID,
	); err != nil {
		t.Fatalf("insert git_files src/main.go: %v", err)
	}
}
