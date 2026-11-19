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

// A re-synced pull request (merge moved to another day) keeps its old part
// until ClickHouse merges. With merges stopped, the loader must return the
// current version only, scoped to the org.
func TestPullRequestLoaderReadsCurrentVersionOfResyncedPullRequest(t *testing.T) {
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

	chschema.CreateTables(ctx, t, conn, "git_pull_requests")
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES git_pull_requests`); err != nil {
		t.Fatal(err)
	}

	const (
		orgA = "00000000-0000-4000-8000-0000000000a0"
		orgB = "00000000-0000-4000-8000-0000000000b0"
	)
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	insert := func(org string, number int, createdAt, mergedAt, synced string) {
		t.Helper()
		merged := "NULL"
		if mergedAt != "" {
			merged = "toDateTime64('" + mergedAt + "', 3, 'UTC')"
		}
		if err := conn.Exec(ctx, `INSERT INTO git_pull_requests (repo_id, number, created_at, merged_at, org_id, last_synced) VALUES
(toUUID('`+repo.String()+`'), `+strconv.Itoa(number)+`, toDateTime64('`+createdAt+`', 3, 'UTC'), `+merged+`, '`+org+`', toDateTime64('`+synced+`', 3, 'UTC'))`); err != nil {
			t.Fatal(err)
		}
	}
	// PR 1: created long before, merged on day X first, then merge moved to day Y.
	insert(orgA, 1, "2026-08-01 10:00:00", "2026-08-24 12:00:00", "2026-08-24 13:00:00")
	insert(orgA, 1, "2026-08-01 10:00:00", "2026-08-25 12:00:00", "2026-08-25 13:00:00")
	// PR 2: created on day X, one version.
	insert(orgA, 2, "2026-08-24 09:00:00", "", "2026-08-24 10:00:00")
	// other org, same repo and number, old version merged on day X.
	insert(orgB, 1, "2026-08-01 10:00:00", "2026-08-24 12:00:00", "2026-08-24 13:00:00")

	loader, err := NewClickHouseLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	dayX := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	dayY := dayX.Add(24 * time.Hour)
	numbers := func(org string, day time.Time) []int {
		t.Helper()
		rows, err := loader.loadPullRequests(ctx, org, []uuid.UUID{repo}, day, day.Add(24*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		out := make([]int, 0, len(rows))
		for _, row := range rows {
			out = append(out, row.Number)
		}
		return out
	}
	if got := numbers(orgA, dayX); len(got) != 1 || got[0] != 2 {
		t.Fatalf("org A day X = %v, want [2] (PR 1 merge moved to day Y)", got)
	}
	if got := numbers(orgA, dayY); len(got) != 1 || got[0] != 1 {
		t.Fatalf("org A day Y = %v, want [1]", got)
	}
	if got := numbers(orgB, dayX); len(got) != 1 || got[0] != 1 {
		t.Fatalf("org B day X = %v, want [1] (its own row)", got)
	}
}
