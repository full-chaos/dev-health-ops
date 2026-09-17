//go:build integration

// Team scope resolved from team_repo_ownership, against a real ClickHouse
// engine rather than a fake: a fake client enforces no max_result_rows at
// all, so the row ceiling these tests turn on can only be shown against the
// genuine driver. Mirrors
// internal/home/team_scope_large_repo_set_integration_test.go and
// internal/explain/team_scope_large_repo_set_integration_test.go for the
// sibling packages that share this resolution.
package investmentexplain

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

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// repoScopeColumn matches investment_explain_route.go's own constant of
// the same name: both FetchInvestmentBreakdown and FetchWorkUnitInvestments
// read FROM %s AS work_unit_investments (reader.go/workunitreader.go), so
// one qualified column name is correct for either reader.
const repoScopeColumn = "work_unit_investments.repo_id"

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

// workUnitsReadLimit is the limit the read below carries, under the
// read-only client's own 1,000-row result ceiling: a team owning more
// repositories than that is bounded by the request's limit, exactly as the
// route bounds it (boundedWorkUnitsLimit, cmd/query-api/workunits_route.go).
const workUnitsReadLimit = 200

// teamScopeReadAsOf is the instant these tests resolve ownership at: after
// every seeded valid_from, so every seeded generation is current.
var teamScopeReadAsOf = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

// newInvestmentExplainTestClickHouse starts a real testcontainers
// ClickHouse, migrates it to the real chain's head, and returns both an
// admin connection (for seeding) and a QueryClient built through
// chquery.NewProductionClient -- the SAME defaults production's readers
// run through (no MaxResultRows override).
func newInvestmentExplainTestClickHouse(ctx context.Context, t *testing.T) (admin stdclickhouse.Conn, client *dhclickhouse.Client) {
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

// seedTeamOwnedWorkUnits writes repoCount repos, one in-window work unit
// per repo, and runCount ownership generations per repo -- each generation
// a distinct valid_from, exactly as a re-running sync writes them. It also
// writes one repository the team does NOT own, carrying its own work unit,
// so a narrowing assertion can fail when narrowing stops.
func seedTeamOwnedWorkUnits(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, orgID, teamID string, repoCount, runCount int) (ownedUnitIDs []string, unownedUnitID string) {
	t.Helper()
	syncedAt := time.Date(2026, 9, 1, 5, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	fromTS := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	toTS := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)

	repoBatch, err := admin.PrepareBatch(ctx, `
        INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
    `)
	if err != nil {
		t.Fatalf("prepare repos batch: %v", err)
	}
	unitBatch, err := admin.PrepareBatch(ctx, `
        INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
    `)
	if err != nil {
		t.Fatalf("prepare work_unit_investments batch: %v", err)
	}
	ownershipBatch, err := admin.PrepareBatch(ctx, `
        INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
    `)
	if err != nil {
		t.Fatalf("prepare team_repo_ownership batch: %v", err)
	}

	appendUnit := func(unitID string, repoID uuid.UUID, effort float64) {
		provider := "github"
		if err := unitBatch.Append(
			unitID, fromTS, toTS, &repoID, &provider, "churn_loc", effort,
			map[string]float64{"maintenance": 1}, map[string]float64{"maintenance.debt": 1},
			"{}", float64(0.7), "moderate", "ok", "{}", "v1", "hash", "run-1", computedAt, orgID,
		); err != nil {
			t.Fatalf("append work_unit_investments row %s: %v", unitID, err)
		}
	}

	for i := 0; i < repoCount; i++ {
		id := uuid.New()
		name := fmt.Sprintf("acme/repo-%d", i)
		unitID := fmt.Sprintf("wu-owned-%d", i)
		ownedUnitIDs = append(ownedUnitIDs, unitID)
		if err := repoBatch.Append(id, name, "github", orgID, syncedAt, syncedAt); err != nil {
			t.Fatalf("append repos row %d: %v", i, err)
		}
		appendUnit(unitID, id, float64(10+i))
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

	unownedRepoID := uuid.New()
	unownedUnitID = "wu-unowned"
	if err := repoBatch.Append(unownedRepoID, "acme/unowned", "github", orgID, syncedAt, syncedAt); err != nil {
		t.Fatalf("append unowned repos row: %v", err)
	}
	appendUnit(unownedUnitID, unownedRepoID, float64(9999))

	if err := repoBatch.Send(); err != nil {
		t.Fatalf("send repos batch: %v", err)
	}
	if err := unitBatch.Send(); err != nil {
		t.Fatalf("send work_unit_investments batch: %v", err)
	}
	if err := ownershipBatch.Send(); err != nil {
		t.Fatalf("send team_repo_ownership batch: %v", err)
	}
	return ownedUnitIDs, unownedUnitID
}

// This is what the shared condition avoids having to do. The team's own
// repository set, read as a query result of its own -- the list a caller must
// hold before it can filter by it -- exceeds the read-only client's 1,000-row
// ceiling and fails with ClickHouse code 396. The condition resolves the same
// set inside the caller's own statement, where only that statement's own rows
// cross back.
func TestInvestmentExplainTeamOwnership_MaterializingTheOwnedRepositorySetHitsTheResultRowCap(t *testing.T) {
	ctx := context.Background()
	admin, client := newInvestmentExplainTestClickHouse(ctx, t)

	const orgID = "investmentexplain-org-ownership-cap"
	const teamID = "investmentexplain-team-ownership-cap"
	seedTeamOwnedWorkUnits(ctx, t, admin, orgID, teamID, teamOwnedRepoCount, teamOwnershipRunCount)

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

// TestInvestmentExplainTeamOwnership_WorkUnitsNarrowToTheOwnedRepos drives
// the same team through BuildWorkUnitInvestments exactly as both the GET and
// POST /api/v1/work-units handlers call it. Every work unit on an owned
// repository comes back, the work unit on the unowned repository does not, and
// a repository count above the ceiling does not stop the read.
func TestInvestmentExplainTeamOwnership_WorkUnitsNarrowToTheOwnedRepos(t *testing.T) {
	ctx := context.Background()
	admin, client := newInvestmentExplainTestClickHouse(ctx, t)

	const orgID = "investmentexplain-org-ownership-wu"
	const teamID = "investmentexplain-team-ownership-wu"
	ownedUnitIDs, unownedUnitID := seedTeamOwnedWorkUnits(ctx, t, admin, orgID, teamID, teamOwnedRepoCount, teamOwnershipRunCount)

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	repoIDs, err := reader.ResolveRepoFilterIDs(ctx, "team", []string{teamID}, nil, orgID)
	if err != nil {
		t.Fatalf("ResolveRepoFilterIDs: %v", err)
	}
	if len(repoIDs) != 0 {
		t.Fatalf("ResolveRepoFilterIDs(team) = %v, want none -- a team's repositories are carried by the pushed-down condition, never materialized here", repoIDs)
	}
	teamCondition, teamBindings := teamscope.RepoCondition(orgID, repoScopeColumn, []string{teamID}, teamScopeReadAsOf)

	investments, err := reader.BuildWorkUnitInvestments(ctx, BuildWorkUnitInvestmentsOptions{
		OrgID:              orgID,
		StartTS:            time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC),
		EndTS:              time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC),
		RepoIDs:            repoIDs,
		TeamScopeCondition: teamCondition,
		TeamScopeBindings:  teamBindings,
		Limit:              workUnitsReadLimit,
	})
	if err != nil {
		t.Fatalf("BuildWorkUnitInvestments over %d owned repositories: %v", teamOwnedRepoCount, err)
	}

	// The route's own limit is what bounds this read, not the team's size:
	// every returned unit sits on a repository the team owns, the unowned
	// repository's unit is absent, and the read fills the limit rather than
	// coming back short.
	owned := map[string]bool{}
	for _, unitID := range ownedUnitIDs {
		owned[unitID] = true
	}
	got := map[string]bool{}
	for _, investment := range investments {
		got[investment.WorkUnitID] = true
	}
	if len(got) != workUnitsReadLimit {
		t.Fatalf("BuildWorkUnitInvestments returned %d work units, want %d (the request's own limit, filled from %d owned repositories)", len(got), workUnitsReadLimit, teamOwnedRepoCount)
	}
	for unitID := range got {
		if !owned[unitID] {
			t.Errorf("work unit %q is not on any repository team %q owns and came back anyway", unitID, teamID)
		}
	}
	if got[unownedUnitID] {
		t.Errorf("work unit %q is on a repository team %q does not own and came back anyway", unownedUnitID, teamID)
	}
}

// TestInvestmentExplainTeamOwnership_TeamWithNoOwnershipRowsReadsEmpty pins
// the contract teamscope.RepoCondition's doc comment states: a team that
// owns no repository sees no repo-keyed rows, never an unscoped org-wide
// read.
func TestInvestmentExplainTeamOwnership_TeamWithNoOwnershipRowsReadsEmpty(t *testing.T) {
	ctx := context.Background()
	admin, client := newInvestmentExplainTestClickHouse(ctx, t)

	const orgID = "investmentexplain-org-ownership-none"
	seedTeamOwnedWorkUnits(ctx, t, admin, orgID, "investmentexplain-team-owns-something", 1, 1)

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	teamCondition, teamBindings := teamscope.RepoCondition(orgID, repoScopeColumn, []string{"investmentexplain-team-owns-nothing"}, teamScopeReadAsOf)
	if teamCondition == "" {
		t.Fatal("RepoCondition returned no condition for a non-empty team scope -- an unscoped read answers org-wide, which is the outcome this resolution exists to prevent")
	}

	investments, err := reader.BuildWorkUnitInvestments(ctx, BuildWorkUnitInvestmentsOptions{
		OrgID:              orgID,
		StartTS:            time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC),
		EndTS:              time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC),
		TeamScopeCondition: teamCondition,
		TeamScopeBindings:  teamBindings,
		Limit:              200,
	})
	if err != nil {
		t.Fatalf("BuildWorkUnitInvestments: %v", err)
	}
	if len(investments) != 0 {
		t.Fatalf("BuildWorkUnitInvestments for a team owning no repository returned %d work units, want 0", len(investments))
	}
}
