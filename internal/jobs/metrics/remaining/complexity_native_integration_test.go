//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// End-to-end parity against a real ClickHouse.
//
// The pure-compute tests (compute_test.go and its own package's parity
// suites) prove the per-file ARITHMETIC against real radon/lizard output.
// This test proves the SQL and control flow this file adds on top of that:
// the git_files/git_blame budget-and-fallback read logic, the ref/last_synced
// derivation, and the two table writes -- by running the Go executor against
// a real ClickHouse and comparing what it wrote to a FROZEN golden captured
// once from the real, shipped run_complexity_db_job against this exact
// fixture (testdata/complexity_python_golden.json, captured on bigboy
// 2026-09-06). This repo carries no committed tooling that invokes the
// Python job at test time: PR (b) deletes run_complexity_db_job outright
// once every language it needs is ported, and a live-Python oracle here
// would be dead weight this port would have to delete again alongside it.
// The golden only needs recapturing if seedComplexityFixture's content
// changes -- run the real Python job against the same fixture once more and
// overwrite the JSON file.
func TestComplexityExecutorMatchesPythonAgainstClickHouse(t *testing.T) {
	ctx := context.Background()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)

	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	conn := openLoaderClickHouse(t, ctx, dsn)

	const orgID = "33333333-3333-4333-8333-333333333333"
	repoGo := uuid.MustParse("44444444-4444-4444-8444-444444444444")
	day := "2026-05-01"
	lastSynced := mustParseComplexityTime(t, "2026-05-01T00:00:00Z")

	seedComplexityFixture(t, ctx, conn, repoGo, orgID, lastSynced)

	executor, err := NewComplexityExecutor(ctx, conn, complexityTestConfigPath(t), nil)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}
	scope, err := json.Marshal(map[string]any{
		"version":       1,
		"day":           day,
		"backfill_days": 1,
		"repo_id":       repoGo.String(),
	})
	if err != nil {
		t.Fatalf("marshal scope: %v", err)
	}
	outcome, err := executor.ComputePartition(ctx, Run{OrganizationID: orgID}, Partition{ID: "p1", Scope: scope})
	if err != nil {
		t.Fatalf("go executor: %v", err)
	}
	if outcome.RowsWritten == nil || *outcome.RowsWritten == 0 {
		t.Fatalf("go executor wrote no rows -- fixture is vacuous")
	}

	golden := loadComplexityPythonGolden(t)
	if len(golden.Snapshots) == 0 {
		t.Fatalf("golden has no snapshots -- fixture is vacuous")
	}

	goSnapshots := readComplexitySnapshots(t, ctx, conn, repoGo, day, orgID)
	if len(goSnapshots) != len(golden.Snapshots) {
		t.Fatalf("snapshot count: go=%d golden=%d\ngo=%+v\ngolden=%+v",
			len(goSnapshots), len(golden.Snapshots), goSnapshots, golden.Snapshots)
	}

	goldenByPath := make(map[string]complexitySnapshotJSON, len(golden.Snapshots))
	for _, s := range golden.Snapshots {
		goldenByPath[s.FilePath] = s
	}
	for _, got := range goSnapshots {
		want, ok := goldenByPath[got.FilePath]
		if !ok {
			t.Errorf("go produced a snapshot for %q that the golden does not", got.FilePath)
			continue
		}
		if got != want {
			t.Errorf("snapshot mismatch for %q:\n  go=%+v\n  golden=%+v", got.FilePath, got, want)
		}
	}

	goDaily := readComplexityDaily(t, ctx, conn, repoGo, day, orgID)
	if golden.RepoDaily == nil {
		t.Fatalf("golden has no repo_complexity_daily row -- fixture is vacuous")
	}
	if goDaily != *golden.RepoDaily {
		t.Errorf("repo_complexity_daily mismatch:\n  go=%+v\n  golden=%+v", goDaily, *golden.RepoDaily)
	}
}

// TestComplexityExecutorNoDataProducedFailsLikePython proves the asymmetric
// failure mode this port has to reproduce exactly (see complexity_native.go's
// own doc, point 2): a partition whose only repo has ZERO scannable content
// (git_files present but every row empty, no git_blame at all) is a hard
// failure on the Go side, matching run_complexity_db_job's exit code 1 ->
// worker_metrics.py's raised RuntimeError today. Verified once against the
// real Python job on this exact fixture during the 2026-09-06 golden capture
// (exit code 1, "wrote no data") -- not re-verified live here for the same
// reason TestComplexityExecutorMatchesPythonAgainstClickHouse no longer
// shells out to Python; see that test's own doc.
func TestComplexityExecutorNoDataProducedFailsLikePython(t *testing.T) {
	ctx := context.Background()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)

	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	conn := openLoaderClickHouse(t, ctx, dsn)

	const orgID = "66666666-6666-4666-8666-666666666666"
	repoID := uuid.MustParse("77777777-7777-4777-8777-777777777777")
	lastSynced := mustParseComplexityTime(t, "2026-05-01T00:00:00Z")

	// A repo that exists (so _load_repos/loadComplexityRepos finds it) but
	// whose only file has EMPTY contents and no git_blame at all -- nothing
	// for either source to recover.
	insertComplexityRepo(t, ctx, conn, repoID, "acme/empty-repo", orgID, lastSynced)
	insertGitFileRow(t, ctx, conn, repoID, orgID, "empty.py", "", lastSynced)

	executor, err := NewComplexityExecutor(ctx, conn, complexityTestConfigPath(t), nil)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}
	scope, err := json.Marshal(map[string]any{
		"version": 1, "day": "2026-05-01", "backfill_days": 1, "repo_id": repoID.String(),
	})
	if err != nil {
		t.Fatalf("marshal scope: %v", err)
	}
	_, err = executor.ComputePartition(ctx, Run{OrganizationID: orgID}, Partition{ID: "p2", Scope: scope})
	if err == nil {
		t.Fatal("expected an error (no repo produced scannable data), got nil")
	}
	if !strings.Contains(err.Error(), "complexity: none of the org's repos") {
		t.Errorf("error should identify itself as ErrComplexityNoDataProduced, got: %v", err)
	}
}

func complexityTestConfigPath(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	return filepath.Join(root, "src", "dev_health_ops", "config", "complexity.yaml")
}

func mustParseComplexityTime(t *testing.T, text string) time.Time {
	t.Helper()
	value, err := time.Parse(time.RFC3339, text)
	if err != nil {
		t.Fatalf("parse time %q: %v", text, err)
	}
	return value
}

// seedComplexityFixture writes IDENTICAL content under a given repo id:
//   - "src/main.py": a real multi-function Python file (radon/pycc territory).
//   - "src/main.go": a real multi-function Go file (lizardcc territory).
//   - "src/legacy.rb": present ONLY in git_blame, not git_files -- exercises
//     the blame-fallback path both implementations must take identically.
//     NOTE: .rb has no native Go analyzer yet (CHAOS-4291's remaining
//     language-coverage gap) -- deliberately excluded from language_globs via
//     the scope's default include_globs (complexity.yaml does not list .rb),
//     so it should be READ via the fallback but never ANALYSED, on both sides.
func seedComplexityFixture(
	t *testing.T, ctx context.Context, conn driver.Conn, repoID uuid.UUID, orgID string, lastSynced time.Time,
) {
	t.Helper()
	insertComplexityRepo(t, ctx, conn, repoID, "acme/"+repoID.String()[:8], orgID, lastSynced)

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
	insertGitFileRow(t, ctx, conn, repoID, orgID, "src/main.py", pythonSource, lastSynced)

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
	insertGitFileRow(t, ctx, conn, repoID, orgID, "src/main.go", goSource, lastSynced)

	// Blame-only: no git_files row for this path at all, only git_blame,
	// reconstructed line-by-line -- exercises loadComplexityBlameContents /
	// _load_blame_contents identically on both sides. Not an include_globs
	// extension, so it must be READ but produce no analysed row on either
	// side (ErrLanguageNotPorted is never reached: fnmatch on *.rb fails
	// should_process first, exactly like Python's LANGUAGE_BY_EXTENSION
	// gate for an extension absent from include_globs).
	insertBlameRows(t, ctx, conn, repoID, orgID, "src/legacy.rb", []string{
		"def legacy",
		"  puts 'hi'",
		"end",
	}, lastSynced)
}

func insertComplexityRepo(
	t *testing.T, ctx context.Context, conn driver.Conn, repoID uuid.UUID, name, orgID string, lastSynced time.Time,
) {
	t.Helper()
	if err := conn.Exec(ctx,
		"INSERT INTO repos (id, repo, org_id, last_synced) VALUES (?, ?, ?, ?)",
		repoID, name, orgID, lastSynced,
	); err != nil {
		t.Fatalf("insert repo %s: %v", name, err)
	}
}

func insertGitFileRow(
	t *testing.T, ctx context.Context, conn driver.Conn, repoID uuid.UUID, orgID, path, contents string, lastSynced time.Time,
) {
	t.Helper()
	if err := conn.Exec(ctx,
		"INSERT INTO git_files (repo_id, path, executable, contents, last_synced, org_id) VALUES (?, ?, ?, ?, ?, ?)",
		repoID, path, uint8(0), contents, lastSynced, orgID,
	); err != nil {
		t.Fatalf("insert git_files %s: %v", path, err)
	}
}

func insertBlameRows(
	t *testing.T, ctx context.Context, conn driver.Conn, repoID uuid.UUID, orgID, path string, lines []string, lastSynced time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx,
		"INSERT INTO git_blame (repo_id, path, line_no, line, last_synced, org_id)")
	if err != nil {
		t.Fatalf("prepare git_blame batch: %v", err)
	}
	for i, line := range lines {
		if err := batch.Append(repoID, path, uint32(i+1), line, lastSynced, orgID); err != nil {
			t.Fatalf("append git_blame row %d: %v", i, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send git_blame batch: %v", err)
	}
}

type complexitySnapshotJSON struct {
	FilePath                    string  `json:"file_path"`
	Language                    string  `json:"language"`
	LOC                         uint32  `json:"loc"`
	FunctionsCount              uint32  `json:"functions_count"`
	CyclomaticTotal             uint32  `json:"cyclomatic_total"`
	CyclomaticAvg               float64 `json:"cyclomatic_avg"`
	HighComplexityFunctions     uint32  `json:"high_complexity_functions"`
	VeryHighComplexityFunctions uint32  `json:"very_high_complexity_functions"`
}

type complexityDailyJSON struct {
	LOCTotal                    uint64  `json:"loc_total"`
	CyclomaticTotal             uint64  `json:"cyclomatic_total"`
	CyclomaticPerKLOC           float64 `json:"cyclomatic_per_kloc"`
	HighComplexityFunctions     uint64  `json:"high_complexity_functions"`
	VeryHighComplexityFunctions uint64  `json:"very_high_complexity_functions"`
}

// complexityPythonGolden is the frozen shape run_complexity_db_job's real
// output was captured into (see testdata/complexity_python_golden.json's own
// generation note).
type complexityPythonGolden struct {
	ExitCode  int                      `json:"exit_code"`
	Snapshots []complexitySnapshotJSON `json:"snapshots"`
	RepoDaily *complexityDailyJSON     `json:"repo_daily"`
}

func loadComplexityPythonGolden(t *testing.T) complexityPythonGolden {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "complexity_python_golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var golden complexityPythonGolden
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return golden
}

func readComplexitySnapshots(
	t *testing.T, ctx context.Context, conn driver.Conn, repoID uuid.UUID, day, orgID string,
) []complexitySnapshotJSON {
	t.Helper()
	rows, err := conn.Query(ctx, `
		SELECT file_path, language, loc, functions_count, cyclomatic_total,
		       cyclomatic_avg, high_complexity_functions, very_high_complexity_functions
		FROM file_complexity_snapshots
		WHERE repo_id = {repo_id:UUID} AND as_of_day = {day:Date} AND org_id = {org_id:String}
		ORDER BY file_path
	`, namedArguments(map[string]any{"repo_id": repoID.String(), "day": day, "org_id": orgID})...)
	if err != nil {
		t.Fatalf("read snapshots: %v", err)
	}
	defer rows.Close()
	var results []complexitySnapshotJSON
	for rows.Next() {
		var s complexitySnapshotJSON
		if err := rows.Scan(
			&s.FilePath, &s.Language, &s.LOC, &s.FunctionsCount, &s.CyclomaticTotal,
			&s.CyclomaticAvg, &s.HighComplexityFunctions, &s.VeryHighComplexityFunctions,
		); err != nil {
			t.Fatalf("scan snapshot: %v", err)
		}
		results = append(results, s)
	}
	return results
}

func readComplexityDaily(
	t *testing.T, ctx context.Context, conn driver.Conn, repoID uuid.UUID, day, orgID string,
) complexityDailyJSON {
	t.Helper()
	rows, err := conn.Query(ctx, `
		SELECT loc_total, cyclomatic_total, cyclomatic_per_kloc,
		       high_complexity_functions, very_high_complexity_functions
		FROM repo_complexity_daily
		WHERE repo_id = {repo_id:UUID} AND day = {day:Date} AND org_id = {org_id:String}
		ORDER BY computed_at DESC
		LIMIT 1
	`, namedArguments(map[string]any{"repo_id": repoID.String(), "day": day, "org_id": orgID})...)
	if err != nil {
		t.Fatalf("read repo daily: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("no repo_complexity_daily row for repo %s day %s", repoID, day)
	}
	var d complexityDailyJSON
	if err := rows.Scan(
		&d.LOCTotal, &d.CyclomaticTotal, &d.CyclomaticPerKLOC,
		&d.HighComplexityFunctions, &d.VeryHighComplexityFunctions,
	); err != nil {
		t.Fatalf("scan repo daily: %v", err)
	}
	return d
}
