//go:build integration

package workerservice

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// A commit of day D is read by the daily run under its committer day, and so are
// the line counts of the git_commit_stats rows that belong to it. When the stats
// are written by a later sync than the commit, the post-sync touched-day record
// must mark the day of the commit, or no run of D ever reads the new stats.
func TestDailyDayDStatsSyncedAfterTheCommitReachItsRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	markers, err := daily.NewClickHouseRunMarkerStore(rig.conn)
	if err != nil {
		t.Fatal(err)
	}
	rig.store.SetRunMarkerWriter(markers)
	rig.store.SetRunMarkerReader(markers)
	day := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	next := day.AddDate(0, 0, 1)

	orgID := uuid.NewString()
	repo := uuid.New()
	insertTouchedRepo(t, ctx, rig.conn, orgID, repo, "acme/api", "github")
	// The commit was synced an hour ago, long before the sync below starts.
	insertDayCommitSyncedAt(t, ctx, rig.conn, orgID, repo, "c1", day.Add(10*time.Hour), time.Now().UTC().Add(-time.Hour))
	rig.scheduledFanoutRun(t, ctx, orgID, day.Add(time.Hour))
	rig.endStep(t, ctx, orgID, day, "stats-a")
	if got := dayUserSums(t, ctx, rig.conn, orgID, day, "commits_count"); got["dev-a@example.com"] != 1 {
		t.Fatalf("commits of %s after the fan-out of the day = %v, want 1 for dev-a (control)", day.Format("2006-01-02"), got)
	}
	if got := dayUserSums(t, ctx, rig.conn, orgID, day, "loc_added"); len(got) != 0 {
		t.Fatalf("lines added on %s before any stats = %v, want none (control)", day.Format("2006-01-02"), got)
	}

	insertCommitStat(t, ctx, rig.conn, orgID, repo, "c1", "main.go", 10, 2)
	overnight := rig.seedOpenEndedSync(t, ctx, orgID, "commit-stats", next, next.Add(30*time.Minute))
	runs := rig.fanoutAndRun(t, ctx, rig.service(t, rig.touched, nil), orgID, overnight)
	t.Logf("stats: post-sync runs by day = %v", touchedRunDays(runs))
	rig.endStep(t, ctx, orgID, day, "stats-b")
	if got, want := dayUserSums(t, ctx, rig.conn, orgID, day, "loc_added"), map[string]uint64{"dev-a@example.com": 10}; len(got) != 1 || got["dev-a@example.com"] != 10 {
		t.Fatalf("lines added on %s after the overnight sync = %v, want %v", day.Format("2006-01-02"), got, want)
	}
}

func insertDayCommitSyncedAt(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repo uuid.UUID, hash string, at, synced time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO git_commits
		(repo_id, hash, author_name, author_email, author_when, committer_when, parents, last_synced, org_id)
		VALUES (?, ?, 'Dev A', 'dev-a@example.com', ?, ?, 1, ?, ?)`,
		repo, hash, at, at, synced, orgID); err != nil {
		t.Fatal(err)
	}
}

func insertCommitStat(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repo uuid.UUID, hash, file string, additions, deletions int32) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO git_commit_stats
		(repo_id, commit_hash, file_path, additions, deletions, last_synced, org_id)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		repo, hash, file, additions, deletions, time.Now().UTC(), orgID); err != nil {
		t.Fatal(err)
	}
}
