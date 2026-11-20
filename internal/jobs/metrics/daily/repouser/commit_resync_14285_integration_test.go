//go:build integration

package repouser

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A commit synced twice keeps both git_commits versions until ClickHouse merges. With merges stopped, the commit
// read must give one row per (commit, file), so the commit's lines are counted once; a second org on the same
// repository id and hash keeps its own row. CHAOS-14285.
func TestCommitStatsLoaderReadsAResyncedCommitOnce(t *testing.T) {
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
	repo := uuid.MustParse("00000000-0000-4000-8000-000000142851")
	exec := func(q string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	commit := func(org, synced string) {
		exec(`INSERT INTO git_commits (repo_id, hash, author_email, author_name, committer_when, org_id, last_synced) VALUES
(toUUID('` + repo.String() + `'), 'c1', 'alice@example.com', 'alice', toDateTime64('2026-08-24 12:00:00', 3, 'UTC'), '` + org + `', toDateTime64('` + synced + `', 3, 'UTC'))`)
	}
	stat := func(org string, additions, deletions int) {
		exec(`INSERT INTO git_commit_stats (repo_id, commit_hash, file_path, additions, deletions, org_id, last_synced) VALUES
(toUUID('` + repo.String() + `'), 'c1', 'src/a.go', ` + strconv.Itoa(additions) + `, ` + strconv.Itoa(deletions) + `, '` + org + `', toDateTime64('2026-08-24 13:00:00', 3, 'UTC'))`)
	}
	// org A: commit c1 synced twice, one stat row (+10/-5).
	commit(orgA, "2026-08-24 13:00:00")
	commit(orgA, "2026-08-25 13:00:00")
	stat(orgA, 10, 5)
	// org B: the same repository id and hash, one version, its own stat row (+100/-50).
	commit(orgB, "2026-08-24 13:00:00")
	stat(orgB, 100, 50)

	loader, err := NewClickHouseLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		org                  string
		additions, deletions int
		loc                  int
	}{{orgA, 10, 5, 15}, {orgB, 100, 50, 150}} {
		rows, err := loader.loadCommitStats(ctx, tc.org, []uuid.UUID{repo}, day, day.Add(24*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("org %s: %d commit stat rows %+v", tc.org[len(tc.org)-4:], len(rows), rows)
		if len(rows) != 1 || rows[0].Additions != tc.additions || rows[0].Deletions != tc.deletions {
			t.Errorf("org %s: %d rows %+v, want one row +%d/-%d", tc.org[len(tc.org)-4:], len(rows), rows, tc.additions, tc.deletions)
		}
		result := Compute(day, rows, nil, nil, day, DefaultNormalizeIdentity, 1000, nil, nil, nil, nil, nil)
		var loc int
		for _, c := range result.CommitMetrics {
			loc += c.TotalLOC
		}
		var userLOC int
		for _, u := range result.UserMetrics {
			userLOC += u.LOCAdded + u.LOCDeleted
		}
		t.Logf("org %s: commit total_loc=%d user loc_added+loc_deleted=%d", tc.org[len(tc.org)-4:], loc, userLOC)
		if loc != tc.loc || userLOC != tc.loc {
			t.Errorf("org %s: commit total_loc=%d user loc=%d, want %d", tc.org[len(tc.org)-4:], loc, userLOC, tc.loc)
		}
	}
}
