//go:build integration

package repouser

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A stat row synced twice (git_commit_stats is ReplacingMergeTree(last_synced), merges stopped) is one file delta of
// the commit: its lines count once; a second org on the same repository id, hash and file keeps its own row. Q2 of
// CHAOS-14285. The test also prints EXPLAIN indexes = 1 of the read with FINAL on both tables (the cost line of the
// PLAN); the EXPLAIN text is a record, not an assertion.
func TestCommitStatsLoaderReadsAResyncedStatRowOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	chschema.CreateTables(ctx, t, conn, "git_commits", "git_commit_stats")
	for _, tbl := range []string{"git_commits", "git_commit_stats"} {
		if err := conn.Exec(ctx, `SYSTEM STOP MERGES `+tbl); err != nil {
			t.Fatal(err)
		}
	}
	const (
		orgA = "00000000-0000-4000-8000-0000001428a0"
		orgB = "00000000-0000-4000-8000-0000001428b0"
	)
	repo := uuid.MustParse("00000000-0000-4000-8000-000000142855")
	other := uuid.MustParse("00000000-0000-4000-8000-000000142856")
	exec := func(q string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	commit := func(org string, r uuid.UUID, hash, when string) {
		exec(`INSERT INTO git_commits (repo_id, hash, author_email, author_name, committer_when, org_id, last_synced) VALUES
(toUUID('` + r.String() + `'), '` + hash + `', 'alice@example.com', 'alice', toDateTime64('` + when + `', 3, 'UTC'), '` + org + `', toDateTime64('2026-08-24 13:00:00', 3, 'UTC'))`)
	}
	stat := func(org string, r uuid.UUID, hash, adds, dels, synced string) {
		exec(`INSERT INTO git_commit_stats (repo_id, commit_hash, file_path, additions, deletions, org_id, last_synced) VALUES
(toUUID('` + r.String() + `'), '` + hash + `', 'src/a.go', ` + adds + `, ` + dels + `, '` + org + `', toDateTime64('` + synced + `', 3, 'UTC'))`)
	}
	// org A: commit c1 once, its stat row synced twice (+10/-5 both times).
	commit(orgA, repo, "c1", "2026-08-24 12:00:00")
	stat(orgA, repo, "c1", "10", "5", "2026-08-24 13:00:00")
	stat(orgA, repo, "c1", "10", "5", "2026-08-25 13:00:00")
	// org B: same repository id, hash and file, one version (+100/-50).
	commit(orgB, repo, "c1", "2026-08-24 12:00:00")
	stat(orgB, repo, "c1", "100", "50", "2026-08-24 13:00:00")
	// other rows, so the EXPLAIN shows what the key prunes: org A on another repository, and other days.
	commit(orgA, other, "c9", "2026-08-24 12:00:00")
	stat(orgA, other, "c9", "1", "1", "2026-08-24 13:00:00")
	commit(orgA, repo, "c0", "2026-07-01 12:00:00")
	stat(orgA, repo, "c0", "1", "1", "2026-07-01 13:00:00")

	loader, err := NewClickHouseLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		org string
		loc int
	}{{orgA, 15}, {orgB, 150}} {
		rows, err := loader.loadCommitStats(ctx, tc.org, []uuid.UUID{repo}, day, day.Add(24*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		result := Compute(day, rows, nil, nil, day, DefaultNormalizeIdentity, 1000, nil, nil, nil, nil, nil)
		loc := 0
		for _, c := range result.CommitMetrics {
			loc += c.TotalLOC
		}
		t.Logf("org %s: %d commit stat rows, commit total_loc=%d", tc.org[len(tc.org)-4:], len(rows), loc)
		if len(rows) != 1 || loc != tc.loc {
			t.Errorf("org %s: %d rows, total_loc %d, want 1 row and %d", tc.org[len(tc.org)-4:], len(rows), loc, tc.loc)
		}
	}

	final := strings.Replace(strings.Replace(commitStatsQuery,
		"FROM git_commits AS c\n", "FROM git_commits AS c FINAL\n", 1),
		"LEFT JOIN git_commit_stats AS s\n", "LEFT JOIN git_commit_stats AS s FINAL\n", 1)
	if strings.Count(final, " FINAL\n") != 2 {
		t.Fatalf("the EXPLAIN statement does not carry FINAL on both tables:\n%s", final)
	}
	for _, mode := range []string{"EXPLAIN indexes = 1 ", "EXPLAIN PIPELINE "} {
		bound := strings.NewReplacer(
			"{start:DateTime}", "toDateTime('"+dateTimeArgument(day)+"')",
			"{end:DateTime}", "toDateTime('"+dateTimeArgument(day.Add(24*time.Hour))+"')",
			"{repo_ids:Array(UUID)}", "[toUUID('"+repo.String()+"')]",
			"{org_id:String}", "'"+orgA+"'",
		).Replace(final)
		explain, err := conn.Query(ctx, mode+bound)
		if err != nil {
			t.Fatal(err)
		}
		var lines []string
		for explain.Next() {
			var line string
			if err := explain.Scan(&line); err != nil {
				t.Fatal(err)
			}
			lines = append(lines, line)
		}
		explain.Close()
		t.Logf("%s(parts: git_commits 4, git_commit_stats 5; org A, repo %s, 2026-08-24):\n%s", mode, repo, strings.Join(lines, "\n"))
	}
}
