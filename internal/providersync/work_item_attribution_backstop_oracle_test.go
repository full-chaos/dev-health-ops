package providersync

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/google/uuid"
)

// TestWorkItemAttributionBackstopMatchesLivePythonProduction is CHAOS-3092
// PR-B's differential proof (team-lead's second approval condition): the Go
// backstop's ClickHouse effect-write path must produce the SAME
// work_item_team_attributions row shape as the sync-time writer, proven
// against the same frozen Python oracle snapshot
// (compute_work_item_team_attributions, deleted -- CHAOS-5321/CHAOS-3092 R6)
// the sync-time deriver's own
// TestGitHubWorkItemTeamAttributionsMatchFrozenPythonGolden already proves
// itself against, immediately above in
// github_work_item_derived_surfaces_oracle_test.go.
//
// This does NOT re-prove the shared resolver cascade (native_team_key,
// project_key donor inheritance, ambiguous-membership refusal, ...) --
// that is teamattribution's byte-for-byte porting proof, already exhaustive
// via githubDerivedOracleCases() against this exact pair. What is new and
// backstop-specific is the MAPPING from resolved candidates to
// remaining.WorkItemAttributionRow (repo_id string->uuid.UUID conversion,
// field set, nullability) -- remaining.BuildWorkItemAttributionRows, the
// exact function ComputeOrg calls to build the rows it writes. A single
// seeded fixture exercising repo_ownership, assignee_membership and the
// unassigned fallback is enough to prove that mapping; broader resolver
// coverage would only be retesting teamattribution a second time.
func TestWorkItemAttributionBackstopMatchesLivePythonProduction(t *testing.T) {
	// CHAOS-5321/CHAOS-3092 (R6): compute_work_item_team_attributions is
	// deleted (native Go executor + providersync own work_item_team_
	// attributions now) -- frozen under its own snapshot name, since this
	// test's small backstop-specific fixture differs from
	// TestGitHubWorkItemTeamAttributionsMatchFrozenPythonGolden's cases
	// despite sharing the same pair id. See testdata/oracle_frozen/README.md.
	compareRowsAgainstFrozenOracle(
		t,
		"github_work-items_team-attributions_backstop",
		workItemAttributionBackstopOracleCases(),
		func(t *testing.T, input map[string]any) githubTeamAttributionColumns {
			t.Helper()
			rows := buildWorkItemAttributionBackstopOracleRows(t, input)
			return newWorkItemAttributionBackstopColumns(rows)
		},
		nil,
	)
}

const workItemAttributionBackstopOracleRepoID = "11111111-1111-4111-8111-111111111111"

// workItemAttributionBackstopOracleCases is deliberately its OWN small
// fixture rather than a reuse of githubDerivedOracleCases(): those seven
// cases are authored for the estimate-coverage/state-durations day-window
// machinery and mostly resolve through the empty-facts unassigned fallback,
// and their WorkItems lists are not guaranteed sorted by id -- a property
// this file's row-order comparison depends on (see the sort in
// buildWorkItemAttributionBackstopOracleRows's caller,
// remaining.BuildWorkItemAttributionRows, which iterates its affected-id set
// in SORTED order to keep ComputeOrg's real writes deterministic; Python's
// compute_work_item_team_attributions preserves the case's WorkItems LIST
// order, so this fixture's ids are written already in ascending order so the
// two sides agree by construction, not by luck).
func workItemAttributionBackstopOracleCases() []oracleCase {
	return []oracleCase{
		{
			ID: "repo_ownership_assignee_membership_and_unassigned",
			Input: map[string]any{
				"OrgID":      githubDerivedOracleOrg,
				"Day":        "2026-08-06",
				"ComputedAt": "2026-08-06T00:00:00Z",
				"Facts": map[string]any{
					"Teams":    []any{},
					"Projects": []any{},
					"Repos": []any{
						map[string]any{
							"Provider": "github", "TeamID": "team-infra", "TeamName": "Infra Team",
							"RepoID": workItemAttributionBackstopOracleRepoID, "RepoFullName": "acme/infra",
							"IsPrimary": 1, "Specificity": 1, "Priority": 10,
							"UpdatedAt": "2026-08-01T00:00:00Z",
						},
					},
					"Members": []any{
						map[string]any{
							"Provider": "github", "TeamID": "team-eng", "TeamName": "Engineering",
							"MemberID": "user-alice", "RawProviderUserID": "alice",
							"RawEmail": "alice@example.com", "IdentityFacets": []any{"alice"},
							"IsPrimary": 1, "Specificity": 1, "Priority": 0,
							"UpdatedAt": "2026-08-01T00:00:00Z",
						},
					},
					"ManualFallbacks": []any{},
				},
				"WorkItems": []any{
					// #1 resolves via repo_ownership: its repo_id matches
					// Facts.Repos' RepoID exactly.
					map[string]any{
						"work_item_id": "acme/api#1", "provider": "github", "title": "acme/api#1",
						"project_id": "", "repo_id": workItemAttributionBackstopOracleRepoID,
						"created_at": "2026-08-01T00:00:00Z", "updated_at": "2026-08-04T00:00:00Z",
						"org_id": githubDerivedOracleOrg,
					},
					// #2 resolves via assignee_membership: "alice" matches
					// Facts.Members' IdentityFacets entry under the same
					// provider. NOT RawProviderUserID -- the Python oracle's
					// fake ClickHouse transport synthesizes
					// identities.provider_identities from IdentityFacets
					// only (_identities_and_teams_from_members), so an
					// identity present solely as RawProviderUserID matches
					// on the Go side (memberByID reads it directly from the
					// fixture) and silently falls through to unassigned on
					// the Python side -- caught by this test's first run.
					map[string]any{
						"work_item_id": "acme/api#2", "provider": "github", "title": "acme/api#2",
						"project_id": "", "assignees": []any{"alice"},
						"created_at": "2026-08-01T00:00:00Z", "updated_at": "2026-08-04T00:00:00Z",
						"org_id": githubDerivedOracleOrg,
					},
					// #3 has no repo, project, or assignee match: the
					// unassigned fallback, pinning the null-team_id shape on
					// both sides.
					map[string]any{
						"work_item_id": "acme/api#3", "provider": "github", "title": "acme/api#3",
						"project_id": "",
						"created_at": "2026-08-01T00:00:00Z", "updated_at": "2026-08-04T00:00:00Z",
						"org_id": githubDerivedOracleOrg,
					},
				},
			},
		},
	}
}

// buildWorkItemAttributionBackstopOracleRows decodes the SAME case shape
// buildGitHubDerivedOracleSurfaces reads (Facts/WorkItems), builds the
// derivation context and affected-subject set the same way ComputeOrg does,
// and calls remaining.BuildWorkItemAttributionRows -- the production
// function, not a copy of it.
func buildWorkItemAttributionBackstopOracleRows(
	t *testing.T, input map[string]any,
) []remaining.WorkItemAttributionRow {
	t.Helper()
	orgID := input["OrgID"].(string)
	computedAt, err := time.Parse(time.RFC3339Nano, input["ComputedAt"].(string))
	if err != nil {
		t.Fatal(err)
	}

	encodedFacts, err := json.Marshal(input["Facts"])
	if err != nil {
		t.Fatal(err)
	}
	var facts teamattribution.GithubWorkItemDerivationFacts
	if err := json.Unmarshal(encodedFacts, &facts); err != nil {
		t.Fatal(err)
	}
	derived := teamattribution.NewGitHubWorkItemDerivationContext(facts)

	items, ok := input["WorkItems"].([]any)
	if !ok {
		t.Fatal("case has no WorkItems")
	}
	subjects := make(map[string]teamattribution.GithubWorkItemDerivationSubject, len(items))
	affectedIDs := make(map[string]struct{}, len(items))
	for _, raw := range items {
		row := githubDerivedOracleGoItem(t, raw.(map[string]any))
		subject := githubWorkItemDerivationSubjectFromRow(row)
		subjects[subject.WorkItemID] = subject
		affectedIDs[subject.WorkItemID] = struct{}{}
	}

	// No dependencies in this fixture: the linked-issue donor index is
	// exercised by teamattribution's own oracle coverage
	// (githubDerivedOracleCases()), not repeated here.
	derived.LinkedIssue, _, _ = derived.BuildLinkedIssueIndex("", subjects, nil, nil)

	return remaining.BuildWorkItemAttributionRows(orgID, computedAt, affectedIDs, subjects, derived)
}

// newWorkItemAttributionBackstopColumns reuses githubTeamAttributionColumns
// (github_work_item_derived_surfaces_oracle_test.go): remaining.
// WorkItemAttributionRow and githubWorkItemTeamAttributionRow declare the
// SAME field set (both target work_item_team_attributions), so a second,
// parallel column struct would be free to drift from the one already
// reflected against the live Python record.
func newWorkItemAttributionBackstopColumns(
	rows []remaining.WorkItemAttributionRow,
) githubTeamAttributionColumns {
	columns := githubTeamAttributionColumns{
		WorkItemID: []string{}, Provider: []string{}, Source: []string{},
		IsPrimary: []int{}, Confidence: []string{}, Evidence: []string{},
		ComputedAt: []time.Time{}, RepoID: []*uuid.UUID{}, TeamID: []*string{},
		TeamName: []*string{}, OrgID: []string{},
	}
	for _, row := range rows {
		columns.WorkItemID = append(columns.WorkItemID, row.WorkItemID)
		columns.Provider = append(columns.Provider, row.Provider)
		columns.Source = append(columns.Source, row.Source)
		columns.IsPrimary = append(columns.IsPrimary, row.IsPrimary)
		columns.Confidence = append(columns.Confidence, row.Confidence)
		columns.Evidence = append(columns.Evidence, row.Evidence)
		columns.ComputedAt = append(columns.ComputedAt, row.ComputedAt)
		columns.RepoID = append(columns.RepoID, row.RepoID)
		columns.TeamID = append(columns.TeamID, row.TeamID)
		columns.TeamName = append(columns.TeamName, row.TeamName)
		columns.OrgID = append(columns.OrgID, row.OrgID)
	}
	return columns
}
