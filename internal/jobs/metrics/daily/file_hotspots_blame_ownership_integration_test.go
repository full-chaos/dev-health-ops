//go:build integration

package daily

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/filehotspots"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// fullScanBlameMapQuery is the blame reader as it was before
// git_blame_file_ownership existed, kept verbatim as the parity oracle. It
// aggregates every git_blame row of the repository on each call.
const fullScanBlameMapQuery = `
SELECT
    path,
    max(author_lines) / sum(author_lines) AS concentration
FROM
(
    SELECT
        path,
        author,
        count() AS author_lines
    FROM
    (
        SELECT
            path,
            line_no,
            argMax(
                coalesce(author_email, author_name, ''),
                last_synced
            ) AS author
        FROM git_blame
        WHERE repo_id = ? AND org_id = ?
        GROUP BY path, line_no
    )
    WHERE author != ''
    GROUP BY path, author
)
GROUP BY path`

const (
	ownershipOrgA = "00000000-0000-4000-8000-00000000f0a0"
	ownershipOrgB = "00000000-0000-4000-8000-00000000f0b0"
)

var (
	ownershipRepoShared = uuid.MustParse("00000000-0000-4000-8000-00000000f0c1")
	ownershipRepoOther  = uuid.MustParse("00000000-0000-4000-8000-00000000f0c2")
	ownershipBaseSync   = time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)
)

type blameSeedLine struct {
	orgID  string
	repoID uuid.UUID
	path   string
	lineNo uint32
	email  *string
	name   *string
	synced time.Time
}

func seedText(value string) *string { return &value }

// blameLines builds lines [from, to] of one file with one author.
func blameLines(
	orgID string, repoID uuid.UUID, path string, from, to uint32, email, name *string, synced time.Time,
) []blameSeedLine {
	lines := make([]blameSeedLine, 0, to-from+1)
	for lineNo := from; lineNo <= to; lineNo++ {
		lines = append(lines, blameSeedLine{
			orgID: orgID, repoID: repoID, path: path, lineNo: lineNo, email: email, name: name, synced: synced,
		})
	}
	return lines
}

// insertBlameBlock writes the lines as ONE insert, so the materialized view
// fires once for the whole slice.
func insertBlameBlock(ctx context.Context, t *testing.T, conn driver.Conn, lines []blameSeedLine) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx,
		`INSERT INTO git_blame (org_id, repo_id, path, line_no, author_email, author_name, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range lines {
		if err := batch.Append(line.orgID, line.repoID, line.path, line.lineNo, line.email, line.name, line.synced); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func fullScanBlameMap(
	ctx context.Context, t *testing.T, conn driver.Conn, organizationID string, repoID uuid.UUID,
) map[string]float64 {
	t.Helper()
	rows, err := conn.Query(ctx, fullScanBlameMapQuery, repoID, organizationID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := make(map[string]float64)
	for rows.Next() {
		var path string
		var concentration float64
		if err := rows.Scan(&path, &concentration); err != nil {
			t.Fatal(err)
		}
		result[path] = concentration
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func assertBlameMapsBitEqual(t *testing.T, label string, want, got map[string]float64) {
	t.Helper()
	if len(want) != len(got) {
		t.Errorf("%s: settled map has %d files, full scan has %d (settled=%v full=%v)", label, len(got), len(want), got, want)
	}
	for path, wantValue := range want {
		gotValue, ok := got[path]
		if !ok {
			t.Errorf("%s: %q missing from the settled map", label, path)
			continue
		}
		if math.Float64bits(gotValue) != math.Float64bits(wantValue) {
			t.Errorf("%s: %q concentration = %v (bits %x), full scan = %v (bits %x)",
				label, path, gotValue, math.Float64bits(gotValue), wantValue, math.Float64bits(wantValue))
		}
	}
}

// assertEveryRepositoryMatches compares the settled reader with the full scan
// for every (org, repository) pair the seed touches, including the pair with
// no rows at all.
func assertEveryRepositoryMatches(ctx context.Context, t *testing.T, conn driver.Conn) {
	t.Helper()
	for _, pair := range []struct {
		label  string
		orgID  string
		repoID uuid.UUID
	}{
		{"org A shared repo", ownershipOrgA, ownershipRepoShared},
		{"org B shared repo", ownershipOrgB, ownershipRepoShared},
		{"org A other repo", ownershipOrgA, ownershipRepoOther},
		{"org B other repo", ownershipOrgB, ownershipRepoOther},
	} {
		want := fullScanBlameMap(ctx, t, conn, pair.orgID, pair.repoID)
		got, err := loadBlameMap(ctx, conn, pair.orgID, pair.repoID)
		if err != nil {
			t.Fatalf("%s: %v", pair.label, err)
		}
		assertBlameMapsBitEqual(t, pair.label, want, got)
	}
}

func execStatements(ctx context.Context, t *testing.T, conn driver.Conn, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func countRows(ctx context.Context, t *testing.T, conn driver.Conn, query string, args ...any) uint64 {
	t.Helper()
	var count uint64
	if err := conn.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return count
}

// ownershipMigrationStatements returns the statements of the migration that
// creates the ownership tables, split the way the Python migration runner
// splits a .sql file: line comments removed, then split on ';'.
func ownershipMigrationStatements(t *testing.T) []string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test file")
	}
	path := filepath.Join(filepath.Dir(file), "..", "..", "..", "..",
		"src", "dev_health_ops", "migrations", "clickhouse", "095_git_blame_file_ownership.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		kept = append(kept, line)
	}
	var statements []string
	for _, fragment := range strings.Split(strings.Join(kept, "\n"), ";") {
		if statement := strings.TrimSpace(fragment); statement != "" {
			statements = append(statements, statement)
		}
	}
	if len(statements) != 4 {
		t.Fatalf("expected 4 statements in %s, got %d", path, len(statements))
	}
	return statements
}

type taggedStatement struct {
	kind       string
	readsBlame bool
	readRows   uint64
}

func taggedStatements(ctx context.Context, t *testing.T, conn driver.Conn, tag string) []taggedStatement {
	t.Helper()
	execStatements(ctx, t, conn, "SYSTEM FLUSH LOGS")
	rows, err := conn.Query(ctx, `
SELECT toString(query_kind), has(tables, concat(currentDatabase(), '.git_blame')), read_rows
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment = ?
ORDER BY event_time_microseconds`, tag)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []taggedStatement
	for rows.Next() {
		var statement taggedStatement
		var readsBlame uint8
		if err := rows.Scan(&statement.kind, &readsBlame, &statement.readRows); err != nil {
			t.Fatal(err)
		}
		statement.readsBlame = readsBlame == 1
		result = append(result, statement)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func withLogComment(ctx context.Context, tag string) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": tag}))
}

// TestBlameOwnershipMatchesFullScanBlameMap proves, on the real migration
// chain, that the settled per-file ownership reader returns the exact map the
// full git_blame scan returns, and that once files are settled the hotspot
// read no longer scans git_blame.
func TestBlameOwnershipMatchesFullScanBlameMap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	}()
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	alice, bob, dave := seedText("alice@example.com"), seedText("bob@example.com"), seedText("dave@example.com")
	later := ownershipBaseSync.Add(time.Hour)

	var sharedA []blameSeedLine
	sharedA = append(sharedA, blameLines(ownershipOrgA, ownershipRepoShared, "owned.go", 1, 4, alice, nil, ownershipBaseSync)...)
	sharedA = append(sharedA, blameLines(ownershipOrgA, ownershipRepoShared, "owned.go", 5, 6, bob, nil, ownershipBaseSync)...)
	// A NULL email falls back to the author name.
	sharedA = append(sharedA, blameLines(ownershipOrgA, ownershipRepoShared, "fallback.go", 1, 2, nil, seedText("Carol"), ownershipBaseSync)...)
	sharedA = append(sharedA, blameLines(ownershipOrgA, ownershipRepoShared, "fallback.go", 3, 3, dave, nil, ownershipBaseSync)...)
	// Every line blank: the full scan drops the file entirely.
	sharedA = append(sharedA, blameLines(ownershipOrgA, ownershipRepoShared, "blank.go", 1, 2, nil, nil, ownershipBaseSync)...)
	// One blank line among attributed lines: excluded from both counts.
	sharedA = append(sharedA, blameLines(ownershipOrgA, ownershipRepoShared, "mixed_blank.go", 1, 1, nil, nil, ownershipBaseSync)...)
	sharedA = append(sharedA, blameLines(ownershipOrgA, ownershipRepoShared, "mixed_blank.go", 2, 3, alice, nil, ownershipBaseSync)...)
	sharedA = append(sharedA, blameLines(ownershipOrgA, ownershipRepoShared, "superseded.go", 1, 4, alice, nil, ownershipBaseSync)...)
	// Enough lines to span several granules, with a non-terminating ratio.
	for lineNo := uint32(1); lineNo <= 60000; lineNo++ {
		var email, name *string
		switch lineNo % 7 {
		case 0, 1, 2, 3:
			email = seedText("erin@example.com")
		case 4, 5:
			email = seedText("frank@example.com")
		default:
			name = seedText("Gus")
		}
		sharedA = append(sharedA, blameSeedLine{
			orgID: ownershipOrgA, repoID: ownershipRepoShared, path: "large.go", lineNo: lineNo,
			email: email, name: name, synced: ownershipBaseSync,
		})
	}
	insertBlameBlock(ctx, t, conn, sharedA)
	// The same block again: a re-sync must not count any line twice.
	insertBlameBlock(ctx, t, conn, blameLines(ownershipOrgA, ownershipRepoShared, "owned.go", 1, 4, alice, nil, ownershipBaseSync))
	// A newer sync re-attributes three lines.
	insertBlameBlock(ctx, t, conn, blameLines(ownershipOrgA, ownershipRepoShared, "superseded.go", 1, 3, bob, nil, later))
	// The same repo_id under another tenant, with different authors.
	sharedB := blameLines(ownershipOrgB, ownershipRepoShared, "owned.go", 1, 2, seedText("zed@example.com"), nil, ownershipBaseSync)
	sharedB = append(sharedB, blameLines(ownershipOrgB, ownershipRepoShared, "owned.go", 3, 6, seedText("yan@example.com"), nil, ownershipBaseSync)...)
	insertBlameBlock(ctx, t, conn, sharedB)
	other := blameLines(ownershipOrgA, ownershipRepoOther, "other.go", 1, 3, alice, nil, ownershipBaseSync)
	other = append(other, blameLines(ownershipOrgA, ownershipRepoOther, "other.go", 4, 4, bob, nil, ownershipBaseSync)...)
	insertBlameBlock(ctx, t, conn, other)

	if !t.Run("first settle equals the full scan", func(t *testing.T) {
		full := fullScanBlameMap(ctx, t, conn, ownershipOrgA, ownershipRepoShared)
		for _, path := range []string{"owned.go", "fallback.go", "mixed_blank.go", "superseded.go", "large.go"} {
			if _, ok := full[path]; !ok {
				t.Fatalf("seed does not exercise %q: full scan = %v", path, full)
			}
		}
		if _, ok := full["blank.go"]; ok {
			t.Fatalf("seed does not exercise the all-blank file: full scan = %v", full)
		}
		if full["superseded.go"] != 0.75 {
			t.Fatalf("seed does not exercise supersession: superseded.go = %v, want 0.75", full["superseded.go"])
		}
		assertEveryRepositoryMatches(ctx, t, conn)
	}) {
		t.FailNow()
	}

	if !t.Run("later writes settle to the full scan", func(t *testing.T) {
		newest := later.Add(time.Hour)
		insertBlameBlock(ctx, t, conn, blameLines(ownershipOrgA, ownershipRepoShared, "owned.go", 7, 12, seedText("carol@example.com"), nil, newest))
		insertBlameBlock(ctx, t, conn, blameLines(ownershipOrgA, ownershipRepoShared, "fallback.go", 1, 1, dave, nil, newest))
		insertBlameBlock(ctx, t, conn, blameLines(ownershipOrgA, ownershipRepoShared, "new.go", 1, 2, alice, nil, newest))
		// An older sync: it loses line 1 to the newer author but adds line 9.
		older := ownershipBaseSync.Add(-time.Hour)
		stale := blameLines(ownershipOrgA, ownershipRepoShared, "superseded.go", 1, 1, seedText("zed@example.com"), nil, older)
		stale = append(stale, blameLines(ownershipOrgA, ownershipRepoShared, "superseded.go", 9, 9, seedText("zed@example.com"), nil, older)...)
		insertBlameBlock(ctx, t, conn, stale)
		assertEveryRepositoryMatches(ctx, t, conn)
	}) {
		t.FailNow()
	}

	if !t.Run("file_hotspot_daily equals the full-scan result", func(t *testing.T) {
		targetDay := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
		execStatements(ctx, t, conn,
			`INSERT INTO git_commits (org_id, repo_id, hash, author_email, author_name, committer_when) VALUES
('`+ownershipOrgA+`', toUUID('`+ownershipRepoShared.String()+`'), 'c1', 'alice@example.com', 'Alice', toDateTime64('2026-03-01 12:00:00', 3, 'UTC')),
('`+ownershipOrgA+`', toUUID('`+ownershipRepoShared.String()+`'), 'c2', 'bob@example.com', 'Bob', toDateTime64('2026-02-20 12:00:00', 3, 'UTC'))`,
			`INSERT INTO git_commit_stats (org_id, repo_id, commit_hash, file_path, additions, deletions) VALUES
('`+ownershipOrgA+`', toUUID('`+ownershipRepoShared.String()+`'), 'c1', 'owned.go', 12, 3),
('`+ownershipOrgA+`', toUUID('`+ownershipRepoShared.String()+`'), 'c1', 'blank.go', 4, 0),
('`+ownershipOrgA+`', toUUID('`+ownershipRepoShared.String()+`'), 'c2', 'superseded.go', 7, 2),
('`+ownershipOrgA+`', toUUID('`+ownershipRepoShared.String()+`'), 'c2', 'untracked.go', 30, 10)`,
			`INSERT INTO file_complexity_snapshots (org_id, repo_id, as_of_day, file_path, cyclomatic_total, cyclomatic_avg, computed_at) VALUES
('`+ownershipOrgA+`', toUUID('`+ownershipRepoShared.String()+`'), '2026-03-01', 'fallback.go', 40, 2.5, now()),
('`+ownershipOrgA+`', toUUID('`+ownershipRepoShared.String()+`'), '2026-03-01', 'large.go', 900, 6.25, now())`,
		)

		executor, err := NewFileRiskHotspotsExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		dayStart, dayEnd, windowStart := filehotspots.DayBoundaries(targetDay, filehotspots.WindowDays)
		windowStats, err := executor.loader.LoadWindowCommitStats(ctx, ownershipOrgA, []uuid.UUID{ownershipRepoShared}, windowStart, dayEnd)
		if err != nil {
			t.Fatal(err)
		}
		complexity, err := loadComplexityMap(ctx, conn, ownershipOrgA, ownershipRepoShared, dayStart)
		if err != nil {
			t.Fatal(err)
		}
		expected := filehotspots.ComputeFileRiskHotspots(ownershipRepoShared, windowStats,
			complexity, fullScanBlameMap(ctx, t, conn, ownershipOrgA, ownershipRepoShared))

		written, err := executor.ComputeFamily(ctx,
			Run{OrganizationID: ownershipOrgA, TargetDay: targetDay},
			Partition{
				ID: "00000000-0000-4000-8000-00000000f0d1", RunID: "00000000-0000-4000-8000-00000000f0d0",
				RepoIDs: []RepositoryID{RepositoryID(ownershipRepoShared.String())},
			})
		if err != nil {
			t.Fatal(err)
		}
		if written != len(expected) {
			t.Fatalf("wrote %d file_hotspot_daily rows, the full-scan result has %d", written, len(expected))
		}

		rows, err := conn.Query(ctx, `
SELECT file_path, churn_loc_30d, churn_commits_30d, cyclomatic_total, cyclomatic_avg, blame_concentration, risk_score
FROM file_hotspot_daily
WHERE org_id = ? AND repo_id = ? AND day = ?`, ownershipOrgA, ownershipRepoShared, dayStart)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		byPath := make(map[string]filehotspots.RiskMetric)
		for rows.Next() {
			var (
				path            string
				churnLOC        uint64
				churnCommits    uint32
				cyclomaticTotal uint32
				metric          filehotspots.RiskMetric
			)
			if err := rows.Scan(&path, &churnLOC, &churnCommits, &cyclomaticTotal,
				&metric.CyclomaticAvg, &metric.BlameConcentration, &metric.RiskScore); err != nil {
				t.Fatal(err)
			}
			metric.FilePath = path
			metric.ChurnLOC30d = int(churnLOC)
			metric.ChurnCommits30d = int(churnCommits)
			metric.CyclomaticTotal = int(cyclomaticTotal)
			byPath[path] = metric
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}

		attributed, unattributed := 0, 0
		for _, want := range expected {
			got, ok := byPath[want.FilePath]
			if !ok {
				t.Errorf("%q missing from file_hotspot_daily", want.FilePath)
				continue
			}
			if got.ChurnLOC30d != want.ChurnLOC30d || got.ChurnCommits30d != want.ChurnCommits30d ||
				got.CyclomaticTotal != want.CyclomaticTotal ||
				math.Float64bits(got.CyclomaticAvg) != math.Float64bits(want.CyclomaticAvg) ||
				math.Float64bits(got.RiskScore) != math.Float64bits(want.RiskScore) {
				t.Errorf("%q: written %+v, full-scan result %+v", want.FilePath, got, want)
			}
			switch {
			case want.BlameConcentration == nil && got.BlameConcentration == nil:
				unattributed++
			case want.BlameConcentration == nil || got.BlameConcentration == nil:
				t.Errorf("%q: blame_concentration written %v, full-scan result %v", want.FilePath, got.BlameConcentration, want.BlameConcentration)
			case math.Float64bits(*want.BlameConcentration) != math.Float64bits(*got.BlameConcentration):
				t.Errorf("%q: blame_concentration written %v, full-scan result %v", want.FilePath, *got.BlameConcentration, *want.BlameConcentration)
			default:
				attributed++
			}
		}
		if attributed == 0 || unattributed == 0 {
			t.Fatalf("seed must produce files with and without blame concentration, got %d with and %d without", attributed, unattributed)
		}
	}) {
		t.FailNow()
	}

	if !t.Run("migration backfill settles rows written before the view", func(t *testing.T) {
		execStatements(ctx, t, conn, "TRUNCATE TABLE git_blame_dirty_paths", "TRUNCATE TABLE git_blame_file_ownership")
		// Without markers the reader has nothing to settle: this is the state of
		// every file written before the view existed.
		got, err := loadBlameMap(ctx, conn, ownershipOrgA, ownershipRepoShared)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("expected an empty map with no markers, got %v", got)
		}
		execStatements(ctx, t, conn, ownershipMigrationStatements(t)...)
		assertEveryRepositoryMatches(ctx, t, conn)
	}) {
		t.FailNow()
	}

	t.Run("settled read does not scan git_blame", func(t *testing.T) {
		// Every file last written an hour ago, nothing settled yet.
		execStatements(ctx, t, conn,
			"TRUNCATE TABLE git_blame_dirty_paths",
			"TRUNCATE TABLE git_blame_file_ownership",
			`INSERT INTO git_blame_dirty_paths (org_id, repo_id, path, marked_at)
SELECT org_id, repo_id, path, now64(3, 'UTC') - INTERVAL 1 HOUR
FROM git_blame
GROUP BY org_id, repo_id, path`,
		)
		blameRows := countRows(ctx, t, conn,
			"SELECT count() FROM git_blame WHERE org_id = ? AND repo_id = ?", ownershipOrgA, ownershipRepoShared)

		fullTag := "blame-ownership-full-scan"
		fullScanBlameMap(withLogComment(ctx, fullTag), t, conn, ownershipOrgA, ownershipRepoShared)
		full := taggedStatements(ctx, t, conn, fullTag)
		if len(full) != 1 || !full[0].readsBlame || full[0].readRows < blameRows {
			t.Fatalf("full scan should read all %d git_blame rows of the repo, query_log = %+v", blameRows, full)
		}

		firstTag := "blame-ownership-first-settle"
		first, err := loadBlameMap(withLogComment(ctx, firstTag), conn, ownershipOrgA, ownershipRepoShared)
		if err != nil {
			t.Fatal(err)
		}
		assertBlameMapsBitEqual(t, "first settle", fullScanBlameMap(ctx, t, conn, ownershipOrgA, ownershipRepoShared), first)
		firstStatements := taggedStatements(ctx, t, conn, firstTag)
		if len(firstStatements) != 2 || firstStatements[0].kind != "Insert" || firstStatements[0].readRows < blameRows {
			t.Fatalf("first settle should read the repo's %d git_blame rows once, query_log = %+v", blameRows, firstStatements)
		}

		settledRows := countRows(ctx, t, conn, "SELECT count() FROM git_blame_file_ownership") +
			countRows(ctx, t, conn, "SELECT count() FROM git_blame_dirty_paths")
		if settledRows >= blameRows {
			t.Fatalf("seed too small to tell the reads apart: %d ownership+marker rows vs %d blame rows", settledRows, blameRows)
		}

		cleanTag := "blame-ownership-clean-read"
		clean, err := loadBlameMap(withLogComment(ctx, cleanTag), conn, ownershipOrgA, ownershipRepoShared)
		if err != nil {
			t.Fatal(err)
		}
		assertBlameMapsBitEqual(t, "clean read", fullScanBlameMap(ctx, t, conn, ownershipOrgA, ownershipRepoShared), clean)
		cleanStatements := taggedStatements(ctx, t, conn, cleanTag)
		if len(cleanStatements) != 2 {
			t.Fatalf("expected a settle and a read, query_log = %+v", cleanStatements)
		}
		settle, read := cleanStatements[0], cleanStatements[1]
		if settle.kind != "Insert" || read.kind != "Select" {
			t.Fatalf("expected Insert then Select, query_log = %+v", cleanStatements)
		}
		if read.readsBlame {
			t.Errorf("the hotspot read references git_blame: %+v", read)
		}
		if settle.readRows > settledRows {
			t.Errorf("a settle with nothing dirty read %d rows, more than the %d ownership and marker rows: it scanned git_blame",
				settle.readRows, settledRows)
		}

		// One more line in one small file: only that file is settled again.
		insertBlameBlock(ctx, t, conn, blameLines(ownershipOrgA, ownershipRepoShared, "owned.go", 13, 13, bob, nil, later.Add(3*time.Hour)))
		incrementalTag := "blame-ownership-incremental-settle"
		incremental, err := loadBlameMap(withLogComment(ctx, incrementalTag), conn, ownershipOrgA, ownershipRepoShared)
		if err != nil {
			t.Fatal(err)
		}
		assertBlameMapsBitEqual(t, "incremental settle", fullScanBlameMap(ctx, t, conn, ownershipOrgA, ownershipRepoShared), incremental)
		incrementalStatements := taggedStatements(ctx, t, conn, incrementalTag)
		if len(incrementalStatements) != 2 || incrementalStatements[0].kind != "Insert" {
			t.Fatalf("expected a settle and a read, query_log = %+v", incrementalStatements)
		}
		if incrementalStatements[0].readRows >= blameRows/2 {
			t.Errorf("settling one dirty file read %d rows, not far below the repo's %d git_blame rows",
				incrementalStatements[0].readRows, blameRows)
		}
	})
}
