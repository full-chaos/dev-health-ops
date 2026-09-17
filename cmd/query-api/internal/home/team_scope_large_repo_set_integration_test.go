//go:build integration

// Team scope resolved from team_repo_ownership, against a real ClickHouse
// engine rather than a fake: a fake client enforces no max_result_rows at
// all, so the row ceiling these tests turn on can only be shown against the
// genuine driver. Mirrors
// internal/explain/team_scope_large_repo_set_integration_test.go and
// internal/investmentexplain's own copies for the sibling packages that
// share this resolution.
package home

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// resultRowsExceededCode is ClickHouse's own numeric code for "Limit for
// result exceeded" (max_result_rows), the ceiling dev-health-go's
// clickhouse/options.go applies to every read-only client at 1,000 rows.
const resultRowsExceededCode = 396

// A team above the read-only client's ceiling. teamOwnedRepoCount is the
// number of repositories the seeded team owns, chosen above 1,000 so that
// the team's own repository SET -- the list a caller materialises before it
// can filter by it -- cannot be returned as a query result at all.
// teamOwnershipRunCount is how many ownership generations each of those
// repositories carries; valid_from is part of team_repo_ownership's sorting
// key and every writer stamps it with its own run instant, so one
// team/repository pair holds one row per sync run and FINAL collapses none
// of them.
const (
	teamOwnedRepoCount    = 1200
	teamOwnershipRunCount = 1
)

// newHomeTestClickHouse starts a real testcontainers ClickHouse, migrates
// it to the real chain's head, and returns both an admin connection (for
// seeding) and a QueryClient built through chquery.NewProductionClient --
// the SAME defaults production's readers run through (no MaxResultRows
// override).
func newHomeTestClickHouse(ctx context.Context, t *testing.T) (admin stdclickhouse.Conn, client QueryClient) {
	t.Helper()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err = stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })

	queryClient, err := chquery.NewProductionClient(ch.URI)
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	t.Cleanup(func() { _ = queryClient.Close() })

	return admin, queryClient
}

// seedHomeTeamOwnership writes repoCount repos, one repo_metrics_daily row
// per repo inside the window the tests read, and runCount ownership
// generations per repo -- each generation a distinct valid_from, exactly as
// a re-running sync writes them.
func seedHomeTeamOwnership(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, orgID, teamID string, repoCount, runCount int) []string {
	t.Helper()
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	syncedAt := time.Date(2026, 9, 1, 5, 0, 0, 0, time.UTC)

	repoBatch, err := admin.PrepareBatch(ctx, `
        INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
    `)
	if err != nil {
		t.Fatalf("prepare repos batch: %v", err)
	}
	metricsBatch, err := admin.PrepareBatch(ctx, `
        INSERT INTO repo_metrics_daily (org_id, repo_id, day, total_loc_touched, computed_at)
    `)
	if err != nil {
		t.Fatalf("prepare repo_metrics_daily batch: %v", err)
	}
	ownershipBatch, err := admin.PrepareBatch(ctx, `
        INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
    `)
	if err != nil {
		t.Fatalf("prepare team_repo_ownership batch: %v", err)
	}

	ids := make([]string, 0, repoCount)
	for i := 0; i < repoCount; i++ {
		id := uuid.New()
		name := fmt.Sprintf("acme/repo-%d", i)
		ids = append(ids, id.String())
		if err := repoBatch.Append(id, name, "github", orgID, syncedAt, syncedAt); err != nil {
			t.Fatalf("append repos row %d: %v", i, err)
		}
		if err := metricsBatch.Append(orgID, id, day, float64(100), computedAt); err != nil {
			t.Fatalf("append repo_metrics_daily row %d: %v", i, err)
		}
		for run := 0; run < runCount; run++ {
			validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(run) * time.Hour)
			if err := ownershipBatch.Append(
				orgID, "github", teamID, id, name, "exact", "inferred",
				uint8(0), uint16(0), int32(0), validFrom, nil, validFrom,
			); err != nil {
				t.Fatalf("append team_repo_ownership row %d/%d: %v", i, run, err)
			}
		}
	}
	if err := repoBatch.Send(); err != nil {
		t.Fatalf("send repos batch: %v", err)
	}
	if err := metricsBatch.Send(); err != nil {
		t.Fatalf("send repo_metrics_daily batch: %v", err)
	}
	if err := ownershipBatch.Send(); err != nil {
		t.Fatalf("send team_repo_ownership batch: %v", err)
	}
	return ids
}

// This is what the shared condition avoids having to do. The team's own
// repository set, read as a query result of its own -- the list a caller must
// hold before it can filter by it -- exceeds the read-only client's 1,000-row
// ceiling and fails with ClickHouse code 396. The condition resolves the same
// set inside the caller's own statement, where only that statement's own rows
// cross back.
func TestHomeTeamOwnership_MaterializingTheOwnedRepositorySetHitsTheResultRowCap(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)

	const orgID = "home-org-ownership-cap"
	const teamID = "home-team-ownership-cap"
	seedHomeTeamOwnership(ctx, t, admin, orgID, teamID, teamOwnedRepoCount, teamOwnershipRunCount)

	rows, err := client.Query(ctx, `
SELECT DISTINCT coalesce(toString(o.repo_id), toString(r.id)) AS owned_repo_id
FROM team_repo_ownership AS o FINAL
LEFT JOIN (
    SELECT org_id, provider, id, repo, 1 AS matched
    FROM repos FINAL
    WHERE org_id = {org_id:String}
) AS r
    ON r.org_id = o.org_id
       AND r.provider = o.provider
       AND lower(r.repo) = lower(o.repo_full_name)
WHERE o.org_id = {org_id:String}
  AND o.team_id IN {team_ids:Array(String)}
  AND (o.repo_id IS NOT NULL OR r.matched = 1)
SETTINGS max_execution_time = 30
`, []dhclickhouse.Binding{
		{Name: "team_ids", Value: []string{teamID}},
		{Name: "org_id", Value: orgID},
	})
	if err == nil {
		defer rows.Close()
		for rows.Next() {
		}
		err = rows.Err()
	}
	if err == nil {
		t.Fatalf("reading team %q's own repository set as a query result succeeded over %d repositories -- expected ClickHouse code %d (result row cap); without that failure this test states nothing about what the pushed-down condition avoids", teamID, teamOwnedRepoCount, resultRowsExceededCode)
	}
	var exc *chproto.Exception
	if !errors.As(err, &exc) || exc.Code != resultRowsExceededCode {
		t.Fatalf("materializing the owned repository set failed with %v, want ClickHouse code %d (result row cap)", err, resultRowsExceededCode)
	}
}

// TestHomeTeamOwnership_ScopeFilterForMetricNarrowsToTheOwnedRepos drives
// the same team through scopeFilterForMetric exactly as BuildResponse calls
// it and then through a real downstream reader. The read succeeds over a repository
// count above the ceiling, and it narrows: a repository the team does not own
// contributes nothing.
func TestHomeTeamOwnership_ScopeFilterForMetricNarrowsToTheOwnedRepos(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)

	const orgID = "home-org-ownership-narrow"
	const teamID = "home-team-ownership-narrow"
	seedHomeTeamOwnership(ctx, t, admin, orgID, teamID, teamOwnedRepoCount, teamOwnershipRunCount)
	seedHomeUnownedRepo(ctx, t, admin, orgID)

	f := Filters{Scope: ScopeFilter{Level: "team", IDs: []string{teamID}}}
	scopeFilter, scopeBindings, err := scopeFilterForMetric(ctx, client, "repo", f, orgID, "team_id", "repo_id", teamScopeReadAsOf)
	if err != nil {
		t.Fatalf("scopeFilterForMetric over %d owned repositories: %v", teamOwnedRepoCount, err)
	}

	startDay := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	endDay := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	got, err := fetchMetricValue(ctx, client, "repo_metrics_daily", "total_loc_touched", startDay, endDay, scopeFilter, scopeBindings, "sum", orgID)
	if err != nil {
		t.Fatalf("fetchMetricValue with a team scope: %v", err)
	}
	want := float64(teamOwnedRepoCount * 100)
	if got != want {
		t.Fatalf("fetchMetricValue over team %q = %v, want %v (the owned repositories only -- the unowned repository's 999 must not contribute)", teamID, got, want)
	}
}

// TestHomeTeamOwnership_TeamWithNoOwnershipRowsReadsEmpty pins the contract
// teamscope.RepoCondition's doc comment states: a team that owns no
// repository sees no repo-keyed rows, never an unscoped org-wide read.
func TestHomeTeamOwnership_TeamWithNoOwnershipRowsReadsEmpty(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)

	const orgID = "home-org-ownership-none"
	seedHomeTeamOwnership(ctx, t, admin, orgID, "home-team-owns-something", 1, 1)
	seedHomeUnownedRepo(ctx, t, admin, orgID)

	f := Filters{Scope: ScopeFilter{Level: "team", IDs: []string{"home-team-owns-nothing"}}}
	scopeFilter, scopeBindings, err := scopeFilterForMetric(ctx, client, "repo", f, orgID, "team_id", "repo_id", teamScopeReadAsOf)
	if err != nil {
		t.Fatalf("scopeFilterForMetric: %v", err)
	}
	if scopeFilter == "" {
		t.Fatal("scopeFilterForMetric returned no filter for a team scope -- an unscoped read answers org-wide, which is the outcome this resolution exists to prevent")
	}

	startDay := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	endDay := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	got, err := fetchMetricValue(ctx, client, "repo_metrics_daily", "total_loc_touched", startDay, endDay, scopeFilter, scopeBindings, "sum", orgID)
	if err != nil {
		t.Fatalf("fetchMetricValue: %v", err)
	}
	if got != 0 {
		t.Fatalf("fetchMetricValue for a team owning no repository = %v, want 0", got)
	}
}

// seedHomeUnownedRepo adds one repository with metric rows and no ownership
// row at all, so a narrowing assertion can fail when narrowing stops.
func seedHomeUnownedRepo(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, orgID string) {
	t.Helper()
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	syncedAt := time.Date(2026, 9, 1, 5, 0, 0, 0, time.UTC)
	id := uuid.New()
	if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`,
		id, "acme/unowned", "github", orgID, syncedAt, syncedAt); err != nil {
		t.Fatalf("seed unowned repo: %v", err)
	}
	if err := admin.Exec(ctx, `INSERT INTO repo_metrics_daily (org_id, repo_id, day, total_loc_touched, computed_at) VALUES (?, ?, ?, ?, ?)`,
		orgID, id, day, float64(999), computedAt); err != nil {
		t.Fatalf("seed unowned repo metrics: %v", err)
	}
}

// teamScopeReadAsOf is the instant these tests resolve ownership at: after
// every seeded valid_from, so every seeded generation is current.
var teamScopeReadAsOf = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
