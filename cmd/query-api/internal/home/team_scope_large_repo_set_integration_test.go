//go:build integration

// This file reproduces the team-scope repo-id row-cap defect class
// against a real ClickHouse engine, not a fake: a fake client cannot
// enforce max_result_rows at all, so the row-cap failure this pins can
// only be shown against the genuine driver. Mirrors
// internal/explain/team_scope_large_repo_set_integration_test.go's own
// copy of this exact reproduction for a sibling package hit by the same
// production defect class.
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
// result exceeded" (max_result_rows) -- the exact code the production
// telemetry this class was diagnosed from carries (code 396).
const resultRowsExceededCode = 396

// teamScopeLargeRepoCount exceeds the read-only client's default
// max_result_rows ceiling (1,000 -- dev-health-go's clickhouse/options.go)
// so the seeded team's own DISTINCT repo_id count genuinely cannot be
// returned to a caller as a standalone query result, matching production
// (1.88k distinct rows observed there for the failing request).
const teamScopeLargeRepoCount = 1500

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

// seedHomeTeamScopedRepos writes n repos rows plus one user_metrics_daily
// row per repo, all under the same org/team, giving the team exactly n
// DISTINCT matching repo ids -- the shape the old resolveRepoIDsForTeams
// query (and teamRepoScopeCondition's own nested one) both select over.
func seedHomeTeamScopedRepos(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, orgID, teamID string, n int) []string {
	t.Helper()
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	syncedAt := time.Date(2026, 9, 1, 5, 0, 0, 0, time.UTC)

	repoBatch, err := admin.PrepareBatch(ctx, `
        INSERT INTO repos (id, repo, org_id, created_at, last_synced)
    `)
	if err != nil {
		t.Fatalf("prepare repos batch: %v", err)
	}
	metricsBatch, err := admin.PrepareBatch(ctx, `
        INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, team_id, computed_at)
    `)
	if err != nil {
		t.Fatalf("prepare user_metrics_daily batch: %v", err)
	}

	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := uuid.New()
		idStr := id.String()
		ids = append(ids, idStr)
		if err := repoBatch.Append(id, fmt.Sprintf("acme/repo-%d", i), orgID, syncedAt, syncedAt); err != nil {
			t.Fatalf("append repos row %d: %v", i, err)
		}
		if err := metricsBatch.Append(orgID, id, day, fmt.Sprintf("user-%d@example.com", i), teamID, computedAt); err != nil {
			t.Fatalf("append user_metrics_daily row %d: %v", i, err)
		}
	}
	if err := repoBatch.Send(); err != nil {
		t.Fatalf("send repos batch: %v", err)
	}
	if err := metricsBatch.Send(); err != nil {
		t.Fatalf("send user_metrics_daily batch: %v", err)
	}
	return ids
}

// TestHomeLargeTeamRepoScope_OldMaterializedQueryHitsResultRowCap
// reproduces the underlying defect directly: the EXACT statement text the
// prior resolveRepoIDsForTeams sent (a standalone SELECT DISTINCT of
// every matching repo_id) against a team whose true matching count
// exceeds the read-only client's default row ceiling. This must fail
// with ClickHouse's own code-396 "Limit for result exceeded" -- the same
// failure production's own telemetry recorded for this request shape --
// proving the defect is real against the genuine engine, not a
// characteristic of a fake client that can't enforce max_result_rows at
// all.
func TestHomeLargeTeamRepoScope_OldMaterializedQueryHitsResultRowCap(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)

	const orgID = "home-org-scale-old"
	const teamID = "home-team-scale-old"
	seedHomeTeamScopedRepos(ctx, t, admin, orgID, teamID, teamScopeLargeRepoCount)

	oldQuery := `
SELECT DISTINCT toString(repo_id) AS id
FROM user_metrics_daily FINAL
WHERE org_id = {org_id:String}
  AND team_id IN {team_ids:Array(String)}
SETTINGS max_execution_time = 30
`
	rows, err := client.Query(ctx, oldQuery, []dhclickhouse.Binding{
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
		t.Fatalf("old materialized query succeeded against %d distinct repos -- expected ClickHouse code %d (result row cap); the reproduction did not trigger, so this test cannot certify the fix against it", teamScopeLargeRepoCount, resultRowsExceededCode)
	}
	var exc *chproto.Exception
	if !errors.As(err, &exc) || exc.Code != resultRowsExceededCode {
		t.Fatalf("old materialized query failed with %v, want ClickHouse code %d (result row cap)", err, resultRowsExceededCode)
	}
}

// TestHomeLargeTeamRepoScope_ScopeFilterForMetricSucceeds is this
// ticket's own proof: the SAME oversized team scope, driven through
// scopeFilterForMetric exactly as BuildResponse calls it, then through a
// real downstream reader (fetchMetricValue), against the SAME
// default-options client the reproduction above used, must succeed -- the
// pushed-down team condition never asks the client to materialize the
// team's own repo-id set as a standalone result.
func TestHomeLargeTeamRepoScope_ScopeFilterForMetricSucceeds(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)

	const orgID = "home-org-scale-new"
	const teamID = "home-team-scale-new"
	seedHomeTeamScopedRepos(ctx, t, admin, orgID, teamID, teamScopeLargeRepoCount)

	f := Filters{Scope: ScopeFilter{Level: "team", IDs: []string{teamID}}}
	scopeFilter, scopeBindings, err := scopeFilterForMetric(ctx, client, "repo", f, orgID, "team_id", "repo_id")
	if err != nil {
		t.Fatalf("scopeFilterForMetric with a %d-repo team scope: %v (this is the exact request shape production's telemetry recorded degrading to 503)", teamScopeLargeRepoCount, err)
	}

	startDay := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	endDay := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	if _, err := fetchMetricValue(ctx, client, "repo_metrics_daily", "total_loc_touched", startDay, endDay, scopeFilter, scopeBindings, "sum", orgID); err != nil {
		t.Fatalf("fetchMetricValue with a %d-repo team scope: %v", teamScopeLargeRepoCount, err)
	}
}
