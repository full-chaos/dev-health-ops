//go:build integration

package daily

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/compoundingrisk"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// compoundingRiskTeamSchema is compoundingRiskSchema (the repo-scope
// integration test's three tables) plus the three tables CHAOS-5084's team
// resolution actually reads: teams, repos, and team_repo_ownership. ONE copy
// each, for the same reason compoundingRiskSchema gives -- a schema
// duplicated per test is how staleness hides.
var compoundingRiskTeamSchema = append(append([]string{}, compoundingRiskSchema...),
	// 002_teams.sql plus every ALTER this port's readers depend on
	// (025_teams_project_repo.sql's repo_patterns, 024_add_org_id.sql's
	// org_id) flattened into one CREATE, matching LoadWellbeingTeams'
	// production query shape (id, name, members, repo_patterns FROM teams
	// FINAL WHERE org_id = ?).
	`CREATE TABLE teams (
    id String, team_uuid UUID, name String, description Nullable(String),
    members Array(String), project_keys Array(String) DEFAULT [],
    repo_patterns Array(String) DEFAULT [], is_active UInt8 DEFAULT 1,
    org_id String DEFAULT 'default',
    updated_at DateTime64(6), last_synced DateTime64(6) DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (id)`,
	// 000_raw_tables.sql plus 024_add_org_id.sql's org_id ALTER and
	// 028_repos_provider.sql's provider ALTER -- the latter is NOT optional
	// here: teamownership.OwnedRepoIDs' production query LEFT JOINs a
	// subquery selecting `repos.provider` (matching team_repo_ownership rows
	// whose own repo_id is unset, resolved instead by provider+name), so
	// omitting the column makes that query fail at parse time, not merely
	// return fewer rows -- caught by this test initially producing 0 rows
	// everywhere, the OwnedRepoIDs error swallowed by
	// ResolveOwnershipThenPatterns' per-team fail-open `continue` (matching
	// Python's own defensive `except Exception`), leaving repoToTeam empty
	// with no visible error. Matches LoadRepoNames' production query too
	// (id, argMax(repo, last_synced) FROM repos WHERE org_id = ? AND id IN ?
	// GROUP BY id).
	`CREATE TABLE repos (
    id UUID, repo String, ref Nullable(String), created_at DateTime64(3, 'UTC'),
    settings Nullable(String), tags Nullable(String),
    org_id String DEFAULT 'default', provider String DEFAULT 'unknown',
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (id)`,
	// 051_team_attribution_dimensions.sql verbatim, matching
	// teamownership.OwnedRepoIDs' production query exactly (the FINAL +
	// matched-sentinel + bitemporal valid_from/valid_to shape).
	`CREATE TABLE team_repo_ownership (
    org_id String, provider String, team_id String, repo_id Nullable(UUID),
    repo_full_name String, match_type Enum8('exact' = 1, 'pattern' = 2),
    source Enum8('native' = 1, 'jira_legacy' = 2, 'provider_access' = 3, 'manual' = 4, 'inferred' = 5),
    is_primary UInt8 DEFAULT 0, specificity UInt16 DEFAULT 0, priority Int32 DEFAULT 0,
    valid_from DateTime64(3, 'UTC'), valid_to Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
  ORDER BY (org_id, provider, repo_full_name, team_id, source, valid_from)`,
)

// TestCompoundingRiskTeamComputeFinalizeFamilyAgainstRealClickHouse is
// CompoundingRiskTeamExecutor's live-ClickHouse proof (CHAOS-5084), the
// finalize-scope, TEAM-scope sibling of
// TestCompoundingRiskComputeFamilyAgainstRealClickHouse. Beyond that test's
// four proofs (wire types, loader predicates, scoping, cross-tenant
// isolation, all inherited here via the same repo_metrics_daily/
// compoundingrisk.LoadRepoMetricsForOrgDay path), this proves the ONE thing
// specific to team scope that no compute-side test can: TEAM RESOLUTION
// against a REAL team_repo_ownership/teams/repos join, with TWO teams and
// ONE repo BOTH teams claim (team-lead's explicit ask for this PR) --
// proving the shared repo lands under EXACTLY ONE team's aggregate, never
// both (double-counted) and never neither (dropped), via
// internal/teamresolve.ResolveOwnershipThenPatterns, the same shared entry
// point team_cognitive_load and team_complexity use.
//
// CHAOS-5084 r1 (P3, codex, confirmed): this test used to accept EITHER team
// winning the shared repo, on the premise that the winner was
// caller-order-dependent under the old teamownership.OwnedRepoIDs (a gap
// #2255 was fixing at the time this test was first written). That gap is
// closed: ResolveOwnershipThenPatterns resolves via
// teamownership.AuthoritativeOwnerByRepo, whose ORDER BY (is_primary DESC,
// specificity DESC, updated_at DESC, team_id ASC) is fully deterministic --
// with both ownership rows here tied on is_primary/specificity/updated_at,
// team_id ASC makes "team-a" < "team-b" the ONLY possible winner. Accepting
// both outcomes let a real regression in that tie-break (e.g. an accidental
// team_id DESC, or a dropped ORDER BY entirely) pass silently as long as
// SOME single team won. Asserts the ONE deterministic outcome now, not just
// the double-count/drop invariant (still checked via the exhaustive
// unmatched-value error message below, so a genuine double-count or drop
// still fails loudly and distinguishably from a tie-break regression).
func TestCompoundingRiskTeamComputeFinalizeFamilyAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseInstance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range compoundingRiskTeamSchema {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		org = "00000000-0000-4000-8000-0000000000c0"

		teamAID = "team-a"
		teamBID = "team-b"

		// repoExclusiveA belongs only to team-a; repoExclusiveB only to
		// team-b; repoShared is claimed by BOTH -- the multi-claimed repo
		// team-lead asked this test to cover.
		repoExclusiveA = "00000000-0000-4000-8000-0000000000c1"
		repoExclusiveB = "00000000-0000-4000-8000-0000000000c2"
		repoShared     = "00000000-0000-4000-8000-0000000000c3"
	)
	targetDay := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

	churnA, churnB, churnShared := 0.10, 0.50, 0.30

	if err := conn.Exec(ctx, `
INSERT INTO repos (id, repo, created_at, org_id, provider, last_synced) VALUES
(toUUID('`+repoExclusiveA+`'), 'acme/exclusive-a', now(), '`+org+`', 'native', now()),
(toUUID('`+repoExclusiveB+`'), 'acme/exclusive-b', now(), '`+org+`', 'native', now()),
(toUUID('`+repoShared+`'),     'acme/shared',       now(), '`+org+`', 'native', now())
`); err != nil {
		t.Fatal(err)
	}

	if err := conn.Exec(ctx, `
INSERT INTO teams (id, team_uuid, name, org_id, updated_at) VALUES
('`+teamAID+`', generateUUIDv4(), 'Team Alpha', '`+org+`', now64(6)),
('`+teamBID+`', generateUUIDv4(), 'Team Beta',  '`+org+`', now64(6))
`); err != nil {
		t.Fatal(err)
	}

	// team-a exclusively owns repoExclusiveA AND claims repoShared;
	// team-b exclusively owns repoExclusiveB AND ALSO claims repoShared --
	// the multi-claim. valid_from is well before targetDay's midnight and
	// valid_to is NULL (currently active), matching OwnedRepoIDs' bitemporal
	// WHERE clause.
	if err := conn.Exec(ctx, `
INSERT INTO team_repo_ownership
(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES
('`+org+`', 'native', '`+teamAID+`', toUUID('`+repoExclusiveA+`'), 'acme/exclusive-a', 'exact', 'native', 1, 0, 0, '2026-01-01 00:00:00', NULL, '2026-01-01 00:00:00'),
('`+org+`', 'native', '`+teamAID+`', toUUID('`+repoShared+`'),     'acme/shared',       'exact', 'native', 1, 0, 0, '2026-01-01 00:00:00', NULL, '2026-01-01 00:00:00'),
('`+org+`', 'native', '`+teamBID+`', toUUID('`+repoExclusiveB+`'), 'acme/exclusive-b', 'exact', 'native', 1, 0, 0, '2026-01-01 00:00:00', NULL, '2026-01-01 00:00:00'),
('`+org+`', 'native', '`+teamBID+`', toUUID('`+repoShared+`'),     'acme/shared',       'exact', 'native', 1, 0, 0, '2026-01-01 00:00:00', NULL, '2026-01-01 00:00:00')
`); err != nil {
		t.Fatal(err)
	}

	// repo_complexity_daily is deliberately left EMPTY: LoadComplexityDelta
	// then returns nil for every repo, so every team row's compounding_risk
	// is nil/severity "unknown" -- irrelevant to what this test proves. What
	// matters is rework_churn, which computeScored persists unconditionally
	// (Record.ReworkChurn = inputs.ReworkChurn, outside the HasRequired()
	// gate), so it is observable regardless.
	metricsInsert := fmt.Sprintf(`
INSERT INTO repo_metrics_daily
(repo_id, day, rework_churn_ratio_30d, single_owner_file_ratio_30d, code_ownership_gini, bus_factor, pr_first_review_p90_hours, computed_at, org_id) VALUES
(toUUID('%s'), '2026-08-24', %v, 0.2, 0.1, 1, 10.0, '2026-08-24 09:00:00', '%s'),
(toUUID('%s'), '2026-08-24', %v, 0.2, 0.1, 1, 10.0, '2026-08-24 09:00:00', '%s'),
(toUUID('%s'), '2026-08-24', %v, 0.2, 0.1, 1, 10.0, '2026-08-24 09:00:00', '%s')
`, repoExclusiveA, churnA, org, repoExclusiveB, churnB, org, repoShared, churnShared, org)
	if err := conn.Exec(ctx, metricsInsert); err != nil {
		t.Fatal(err)
	}

	executor, err := NewCompoundingRiskTeamExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{ID: "run-team", OrganizationID: org, TargetDay: targetDay}

	rowsWritten, err := executor.ComputeFinalizeFamily(ctx, run)
	if err != nil {
		t.Fatal(err)
	}
	if rowsWritten != 2 {
		t.Fatalf("ComputeFinalizeFamily wrote %d rows, want exactly 2 (team-a, team-b) -- "+
			"a phantom third team or a missing team both fail this", rowsWritten)
	}

	rows, err := conn.Query(ctx, `
SELECT scope_id, rework_churn FROM compounding_risk_daily
WHERE org_id = ? AND day = ? AND scope = 'team'
ORDER BY scope_id
`, org, targetDay.Format("2006-01-02"))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	got := map[string]*float64{}
	for rows.Next() {
		var scopeID string
		var churn *float64
		if err := rows.Scan(&scopeID, &churn); err != nil {
			t.Fatal(err)
		}
		got[scopeID] = churn
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	gotA, gotB := got[teamAID], got[teamBID]
	if gotA == nil || gotB == nil {
		t.Fatalf("missing team row(s): team-a present=%v team-b present=%v (got %v)",
			gotA != nil, gotB != nil, got)
	}

	// Derived via the SAME kernel function under test, not hand-typed
	// arithmetic -- see compoundingRiskSchema's sibling test for why.
	sharedToA := *compoundingrisk.MeanOrNone([]*float64{&churnA, &churnShared})
	sharedToB := *compoundingrisk.MeanOrNone([]*float64{&churnB, &churnShared})

	caseSharedWentToA := *gotA == sharedToA && *gotB == churnB
	caseSharedWentToB := *gotA == churnA && *gotB == sharedToB
	if !caseSharedWentToA && !caseSharedWentToB {
		t.Fatalf(
			"team-a rework_churn=%v team-b rework_churn=%v matches NEITHER valid shape "+
				"(shared->A: a=%v b=%v; shared->B: a=%v b=%v) -- the shared repo was either "+
				"double-counted (appears in both means) or dropped (appears in neither)",
			*gotA, *gotB, sharedToA, churnB, churnA, sharedToB,
		)
	}
	// The deterministic tie-break (team_id ASC, see the doc comment above)
	// means shared->A is the ONLY correct outcome here -- shared->B matching
	// the "neither double-counted nor dropped" shape above is not enough; it
	// would mean the tie-break itself regressed (team-b winning instead of
	// team-a), which the check above cannot distinguish from success.
	if caseSharedWentToB {
		t.Fatalf(
			"shared repo resolved to team-b (a=%v b=%v), want team-a (a=%v b=%v) -- "+
				"AuthoritativeOwnerByRepo's team_id ASC tie-break must deterministically "+
				"prefer team-a over team-b when every other rank field ties",
			*gotA, *gotB, sharedToA, churnB,
		)
	}
}
