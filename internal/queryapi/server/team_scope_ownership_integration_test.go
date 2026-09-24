//go:build integration

// The team-scope membership rule, exercised against a real ClickHouse
// migrated by the real chain. Every case here is a shape production carries
// and a fake client cannot represent: a ReplacingMergeTree whose version
// column decides a revocation, an unmatched LEFT JOIN column that fills with
// a zero UUID rather than NULL, and an ownership row count far larger than
// the repository count it resolves to.
package server

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/investment"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// workUnitsTeamScopeAsOf is the instant every team-scope assertion in this
// package resolves ownership at. It sits after every seeded valid_from, so a
// seeded row is current unless a seeded revocation closed it.
var workUnitsTeamScopeAsOf = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

// teamScopeUnitWindow is the window every seeded work unit falls inside.
var (
	teamScopeUnitFrom = time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	teamScopeUnitTo   = time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	teamScopeReadFrom = time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	teamScopeReadTo   = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
)

// startTeamScopeClickHouse starts a real ClickHouse, migrates it to the
// chain's head, and hands back a raw connection for seeding plus the
// read-only query client production's readers run through.
func startTeamScopeClickHouse(t *testing.T) (chdriver.Conn, *dhclickhouse.Client) {
	t.Helper()
	ctx := context.Background()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })

	chschema.Apply(ctx, t, instance)

	options, err := stdclickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return conn, client
}

// seedTeamScopeRepo writes one repos row.
func seedTeamScopeRepo(t *testing.T, conn chdriver.Conn, orgID, name string, id uuid.UUID) {
	t.Helper()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := conn.Exec(context.Background(),
		`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`,
		id, name, "github", orgID, at, at); err != nil {
		t.Fatalf("seed repo %q: %v", name, err)
	}
}

// seedTeamScopeOwnership writes one team_repo_ownership row. repoID nil is
// the shape the GitHub team-autoimport writer produces for every row it
// writes: only repo_full_name, resolution deferred to read time.
func seedTeamScopeOwnership(t *testing.T, conn chdriver.Conn, orgID, teamID, fullName, matchType, source string, repoID *uuid.UUID, validFrom time.Time, validTo *time.Time, updatedAt time.Time) {
	t.Helper()
	// A nil *uuid.UUID cannot go to the driver as a typed nil pointer: its
	// Value method dereferences. An untyped nil is what a NULL repo_id is.
	var repoIDValue any
	if repoID != nil {
		repoIDValue = *repoID
	}
	var validToValue any
	if validTo != nil {
		validToValue = *validTo
	}
	if err := conn.Exec(context.Background(),
		`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		orgID, "github", teamID, repoIDValue, fullName, matchType, source,
		uint8(0), uint16(0), int32(0), validFrom, validToValue, updatedAt); err != nil {
		t.Fatalf("seed ownership %q: %v", fullName, err)
	}
}

// seedTeamScopeWorkUnit writes one in-window work unit on repoID.
func seedTeamScopeWorkUnit(t *testing.T, conn chdriver.Conn, orgID, unitID string, repoID uuid.UUID, effort float64) {
	t.Helper()
	const provider = "github"
	if err := conn.Exec(context.Background(),
		`INSERT INTO work_unit_investments
			(work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value,
			 theme_distribution_json, subcategory_distribution_json, structural_evidence_json,
			 evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json,
			 categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		unitID, teamScopeUnitFrom, teamScopeUnitTo, repoID, provider, "churn_loc", effort,
		map[string]float64{"maintenance": 1}, map[string]float64{"maintenance.debt": 1}, "{}",
		float64(0.7), "moderate", "ok", "{}", "v1", "hash", "run-1",
		time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC), orgID); err != nil {
		t.Fatalf("seed work unit %q: %v", unitID, err)
	}
}

// readTeamScopedUnitIDs runs the shared condition through the production
// work-unit read, exactly as both /api/v1/work-units handlers do.
func readTeamScopedUnitIDs(t *testing.T, client *dhclickhouse.Client, orgID, teamID string) map[string]bool {
	t.Helper()
	reader, err := investmentexplain.NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	condition, bindings := teamscope.RepoCondition(orgID, repoScopeColumn, []string{teamID}, workUnitsTeamScopeAsOf)
	investments, err := reader.BuildWorkUnitInvestments(context.Background(), investmentexplain.BuildWorkUnitInvestmentsOptions{
		OrgID:              orgID,
		StartTS:            teamScopeReadFrom,
		EndTS:              teamScopeReadTo,
		TeamScopeCondition: condition,
		TeamScopeBindings:  bindings,
		Limit:              200,
	})
	if err != nil {
		t.Fatalf("BuildWorkUnitInvestments: %v", err)
	}
	got := map[string]bool{}
	for _, investment := range investments {
		got[investment.WorkUnitID] = true
	}
	return got
}

// TestTeamScopeOwnership_ResolutionMatrix covers, in one seeded org, every
// shape an ownership row arrives in and states which repositories each one
// does and does not put inside a team's scope.
func TestTeamScopeOwnership_ResolutionMatrix(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	const orgID = "teamscope-org-matrix"
	const teamID = "ABC-123"
	validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Carries its own repo_id, and that repository is in the catalog.
	directID := uuid.New()
	seedTeamScopeRepo(t, conn, orgID, "acme/direct", directID)
	seedTeamScopeOwnership(t, conn, orgID, teamID, "acme/direct", "exact", "inferred", &directID, validFrom, nil, validFrom)
	seedTeamScopeWorkUnit(t, conn, orgID, "wu-direct", directID, 10)

	// No repo_id at all: the autoimport shape, resolved at read time by
	// name. The catalog spells the name with different case, which the join
	// folds.
	namedID := uuid.New()
	seedTeamScopeRepo(t, conn, orgID, "Acme/Named", namedID)
	seedTeamScopeOwnership(t, conn, orgID, teamID, "acme/named", "exact", "provider_access", nil, validFrom, nil, validFrom)
	seedTeamScopeWorkUnit(t, conn, orgID, "wu-named", namedID, 20)

	// No repo_id and a name the catalog does not hold. This must resolve to
	// nothing -- an unmatched LEFT JOIN column fills with the ZERO UUID, not
	// NULL, so without the matched sentinel it would resolve to that zero id
	// and pull in any row carrying it.
	seedTeamScopeOwnership(t, conn, orgID, teamID, "acme/ghost", "exact", "provider_access", nil, validFrom, nil, validFrom)
	zeroID := uuid.UUID{}
	seedTeamScopeWorkUnit(t, conn, orgID, "wu-zero-repo-id", zeroID, 30)

	// A pattern claim that never resolved to a name the catalog holds has
	// nothing to filter a repo-keyed read by.
	seedTeamScopeOwnership(t, conn, orgID, teamID, "acme/pattern-unresolved", "pattern", "manual", nil, validFrom, nil, validFrom)

	// A pattern claim whose name the catalog DOES hold resolves like any
	// other: match_type never decides membership on its own.
	patternID := uuid.New()
	seedTeamScopeRepo(t, conn, orgID, "acme/pattern-resolved", patternID)
	seedTeamScopeOwnership(t, conn, orgID, teamID, "acme/pattern-resolved", "pattern", "manual", nil, validFrom, nil, validFrom)
	seedTeamScopeWorkUnit(t, conn, orgID, "wu-pattern", patternID, 40)

	// Revoked: a replacement row under the SAME sorting key with valid_to
	// set and a newer updated_at. Both physical versions are on disk and
	// unmerged, so only a version-resolving read sees the revocation.
	revokedID := uuid.New()
	seedTeamScopeRepo(t, conn, orgID, "acme/revoked", revokedID)
	seedTeamScopeOwnership(t, conn, orgID, teamID, "acme/revoked", "exact", "inferred", &revokedID, validFrom, nil, validFrom)
	revokedAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	seedTeamScopeOwnership(t, conn, orgID, teamID, "acme/revoked", "exact", "inferred", &revokedID, validFrom, &revokedAt, revokedAt)
	seedTeamScopeWorkUnit(t, conn, orgID, "wu-revoked", revokedID, 50)

	// Not yet in force: valid_from after the instant this request resolves
	// ownership at.
	futureID := uuid.New()
	seedTeamScopeRepo(t, conn, orgID, "acme/future", futureID)
	future := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	seedTeamScopeOwnership(t, conn, orgID, teamID, "acme/future", "exact", "inferred", &futureID, future, nil, future)
	seedTeamScopeWorkUnit(t, conn, orgID, "wu-future", futureID, 60)

	// An ownership row whose repository the catalog does not hold. Nothing
	// revoked the claim and the id is well formed, but no repository in this
	// org carries it.
	orphanID := uuid.New()
	seedTeamScopeOwnership(t, conn, orgID, teamID, "acme/orphan", "exact", "inferred", &orphanID, validFrom, nil, validFrom)
	seedTeamScopeWorkUnit(t, conn, orgID, "wu-orphan", orphanID, 70)

	// Another team's repository, to prove the team predicate is load-bearing.
	otherID := uuid.New()
	seedTeamScopeRepo(t, conn, orgID, "acme/other-team", otherID)
	seedTeamScopeOwnership(t, conn, orgID, "XYZ-789", "acme/other-team", "exact", "inferred", &otherID, validFrom, nil, validFrom)
	seedTeamScopeWorkUnit(t, conn, orgID, "wu-other-team", otherID, 80)

	// user_metrics_daily carries no row for this team. That is the production
	// shape for a real team: its team_id is per-author membership
	// attribution, and a team whose repositories are recorded as ownership
	// resolves to nothing there. Stated here so the assertions below are read
	// against the data they actually run on.
	membershipRows, err := client.Query(context.Background(), `
SELECT DISTINCT toString(repo_id) AS id
FROM user_metrics_daily FINAL
WHERE org_id = {org_id:String}
  AND team_id IN {team_ids:Array(String)}
SETTINGS max_execution_time = 30
`, []dhclickhouse.Binding{
		{Name: "team_ids", Value: []string{teamID}},
		{Name: "org_id", Value: orgID},
	})
	if err != nil {
		t.Fatalf("read user_metrics_daily for team %q: %v", teamID, err)
	}
	membershipCount := 0
	for membershipRows.Next() {
		membershipCount++
	}
	_ = membershipRows.Close()
	if membershipCount != 0 {
		t.Fatalf("user_metrics_daily holds %d rows for team %q, want 0 -- this fixture is meant to carry that team's repositories as ownership rows only", membershipCount, teamID)
	}

	got := readTeamScopedUnitIDs(t, client, orgID, teamID)

	for _, want := range []struct {
		unitID string
		why    string
	}{
		{"wu-direct", "the ownership row carries the repository id and the repository is in the catalog"},
		{"wu-named", "the ownership row carries only a name, which the catalog holds under different case"},
		{"wu-pattern", "a pattern claim resolves like any other once its name is in the catalog"},
	} {
		if !got[want.unitID] {
			t.Errorf("work unit %q did not come back: %s", want.unitID, want.why)
		}
	}
	for _, notWanted := range []struct {
		unitID string
		why    string
	}{
		{"wu-zero-repo-id", "an unmatched ownership name must not resolve to the zero UUID"},
		{"wu-revoked", "the newest version of that ownership row carries valid_to"},
		{"wu-future", "that ownership row's valid_from has not arrived at this request's instant"},
		{"wu-orphan", "the org's catalog holds no repository with that id"},
		{"wu-other-team", "that repository belongs to a different team"},
	} {
		if got[notWanted.unitID] {
			t.Errorf("work unit %q came back and must not have: %s", notWanted.unitID, notWanted.why)
		}
	}
	if len(got) != 3 {
		t.Fatalf("team %q resolved to %d work units (%v), want exactly 3", teamID, len(got), got)
	}
}

// TestTeamScopeOwnership_GenerationsResolveToOneRepository pins the shape
// production actually carries: valid_from is part of the sorting key, so a
// re-running sync writes a new row per run and none of them collapse. The
// resolved repository set is still one repository, and the read still
// answers.
func TestTeamScopeOwnership_GenerationsResolveToOneRepository(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	const orgID = "teamscope-org-generations"
	const teamID = "ABC-123"
	const runCount = 1200

	repoID := uuid.New()
	seedTeamScopeRepo(t, conn, orgID, "acme/repo", repoID)
	seedTeamScopeWorkUnit(t, conn, orgID, "wu-generations", repoID, 10)

	batch, err := conn.PrepareBatch(context.Background(), `
        INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
    `)
	if err != nil {
		t.Fatalf("prepare team_repo_ownership batch: %v", err)
	}
	for run := 0; run < runCount; run++ {
		validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(run) * time.Minute)
		if err := batch.Append(orgID, "github", teamID, repoID, "acme/repo", "exact", "inferred",
			uint8(0), uint16(0), int32(0), validFrom, nil, validFrom); err != nil {
			t.Fatalf("append ownership generation %d: %v", run, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send team_repo_ownership batch: %v", err)
	}

	var rows uint64
	if err := conn.QueryRow(context.Background(),
		`SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ? AND team_id = ?`, orgID, teamID).Scan(&rows); err != nil {
		t.Fatalf("count ownership rows: %v", err)
	}
	if rows != runCount {
		t.Fatalf("team_repo_ownership FINAL holds %d rows for one repository, want %d -- this test is only meaningful while distinct valid_from generations do not collapse", rows, runCount)
	}

	got := readTeamScopedUnitIDs(t, client, orgID, teamID)
	if len(got) != 1 || !got["wu-generations"] {
		t.Fatalf("team %q resolved to %v, want exactly [wu-generations] from %d ownership generations", teamID, got, runCount)
	}
}

// TestTeamScopeOwnership_InvestmentRoutesNarrowToTheOwnedRepos is the proof
// for GET/POST /api/v1/investment and GET /api/v1/investment/sunburst: a
// team-scoped request answers over that team's repositories, not over the
// whole organization.
func TestTeamScopeOwnership_InvestmentRoutesNarrowToTheOwnedRepos(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	const orgID = "teamscope-org-investment"
	const teamID = "ABC-123"
	validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	ownedID := uuid.New()
	seedTeamScopeRepo(t, conn, orgID, "acme/owned", ownedID)
	seedTeamScopeOwnership(t, conn, orgID, teamID, "acme/owned", "exact", "inferred", &ownedID, validFrom, nil, validFrom)
	seedTeamScopeWorkUnit(t, conn, orgID, "wu-owned", ownedID, 10)

	unownedID := uuid.New()
	seedTeamScopeRepo(t, conn, orgID, "acme/unowned", unownedID)
	seedTeamScopeWorkUnit(t, conn, orgID, "wu-unowned", unownedID, 990)

	reader, err := investment.NewReader(client)
	if err != nil {
		t.Fatalf("investment.NewReader: %v", err)
	}
	ctx := context.Background()
	params := investment.Params{
		OrgID:   orgID,
		StartTS: teamScopeReadFrom,
		EndTS:   teamScopeReadTo,
	}

	orgWide, err := investment.BuildResponse(ctx, reader, orgID, params)
	if err != nil {
		t.Fatalf("investment.BuildResponse (org scope): %v", err)
	}
	teamParams := params
	teamParams.ScopeLevel = "team"
	teamParams.ScopeIDs = []string{teamID}
	teamScoped, err := investment.BuildResponse(ctx, reader, orgID, teamParams)
	if err != nil {
		t.Fatalf("investment.BuildResponse (team scope): %v", err)
	}

	orgEffort := totalThemeEffort(orgWide.ThemeDistribution)
	teamEffort := totalThemeEffort(teamScoped.ThemeDistribution)
	if orgEffort != 1000 {
		t.Fatalf("org-wide investment total = %v, want 1000 (both seeded work units)", orgEffort)
	}
	if teamEffort != 10 {
		t.Fatalf("team-scoped investment total = %v, want 10 (the owned repository's work unit only); equal to the org-wide total means the team scope applied no filter at all", teamEffort)
	}

	orgSlices, err := investment.BuildSunburstResponse(ctx, reader, orgID, investment.SunburstParams{
		OrgID: orgID, StartTS: teamScopeReadFrom, EndTS: teamScopeReadTo, Limit: 100,
	})
	if err != nil {
		t.Fatalf("investment.BuildSunburstResponse (org scope): %v", err)
	}
	teamSlices, err := investment.BuildSunburstResponse(ctx, reader, orgID, investment.SunburstParams{
		OrgID: orgID, StartTS: teamScopeReadFrom, EndTS: teamScopeReadTo, Limit: 100,
		ScopeLevel: "team", ScopeIDs: []string{teamID},
	})
	if err != nil {
		t.Fatalf("investment.BuildSunburstResponse (team scope): %v", err)
	}
	orgSunburst := totalSliceValue(orgSlices)
	teamSunburst := totalSliceValue(teamSlices)
	if orgSunburst != 1000 {
		t.Fatalf("org-wide sunburst total = %v, want 1000 (both seeded work units)", orgSunburst)
	}
	if teamSunburst != 10 {
		t.Fatalf("team-scoped sunburst total = %v, want 10 (the owned repository's work unit only); equal to the org-wide total means the team scope applied no filter at all", teamSunburst)
	}
}

func totalThemeEffort(distribution map[string]float64) float64 {
	total := 0.0
	for _, value := range distribution {
		total += value
	}
	return total
}

func totalSliceValue(slices []investment.SunburstSlice) float64 {
	total := 0.0
	for _, slice := range slices {
		total += slice.Value
	}
	return total
}
