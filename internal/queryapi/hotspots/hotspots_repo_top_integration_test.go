//go:build integration

// CHAOS-8488: the per-repository top file is read from the real engine with
// the rows query's argMax (day, computed_at) selection, one file per
// repository, and is not cut by the row limit.
package hotspots

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	repoTopOrgID = "org-8488-repo-top"
	repoTopRepoA = "8488aaaa-0000-4000-8000-00000000000a"
	repoTopRepoB = "8488bbbb-0000-4000-8000-00000000000b"
)

func TestResolve_ReposHoldEachRepositorysTopFileOnTheRealEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)
	conn := openRawClickHouse(t, inst.URI)

	seed := func(repo, day, file string, risk float64, computedAt string) {
		t.Helper()
		insert := fmt.Sprintf(
			"INSERT INTO file_hotspot_daily (repo_id, day, file_path, churn_loc_30d, churn_commits_30d, "+
				"cyclomatic_total, cyclomatic_avg, blame_concentration, risk_score, computed_at, org_id) VALUES "+
				"(toUUID('%s'), toDate('%s'), '%s', 1, 1, 1, 1, 0.5, %g, toDateTime('%s'), '%s')",
			repo, day, file, risk, computedAt, repoTopOrgID)
		if err := conn.Exec(ctx, insert); err != nil {
			t.Fatalf("seed %s %s: %v", repo, file, err)
		}
	}
	// Repo A: a2 outranks a1 on the latest day. a2's OLDER day carries a
	// higher score and a LATER computed_at (a backfill), which must lose.
	seed(repoTopRepoA, "2026-09-07", "a/a1.go", 5, "2026-09-07 04:00:00")
	seed(repoTopRepoA, "2026-09-07", "a/a2.go", 9, "2026-09-07 04:00:00")
	seed(repoTopRepoA, "2026-08-20", "a/a2.go", 99, "2026-09-07 06:00:00")
	// Repo B: one file, below every repo A file, so a row limit of 1 drops it.
	seed(repoTopRepoB, "2026-09-07", "b/my file.go", 1, "2026-09-07 04:00:00")

	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	limit := 1
	since := time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	result, err := Resolve(ctx, client, repoTopOrgID, since, until, nil, &limit)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("len(Rows) = %d, want 1 (row limit)", len(result.Rows))
	}
	if len(result.Repos) != 2 {
		t.Fatalf("len(Repos) = %d, want 2 (one per repository, past the row limit): %+v", len(result.Repos), result.Repos)
	}
	byRepo := map[string]int{}
	for i, r := range result.Repos {
		byRepo[r.RepoID] = i
	}
	a, b := result.Repos[byRepo[repoTopRepoA]], result.Repos[byRepo[repoTopRepoB]]
	if a.TopFilePath != "a/a2.go" || a.TopRiskScore != 9 {
		t.Errorf("repo A top = %s (%v), want a/a2.go (9): the latest day wins, not the backfilled 99", a.TopFilePath, a.TopRiskScore)
	}
	if b.TopFilePath != "b/my file.go" || b.TopRiskScore != 1 {
		t.Errorf("repo B top = %s (%v), want b/my file.go (1)", b.TopFilePath, b.TopRiskScore)
	}
	if b.EvidenceURL == nil || *b.EvidenceURL != "/code?file=b/my%20file.go" {
		t.Errorf("repo B EvidenceURL = %v", b.EvidenceURL)
	}
	if b.RepoName != repoTopRepoB {
		t.Errorf("repo B RepoName = %q, want the id fallback when the catalog has no row", b.RepoName)
	}
}
