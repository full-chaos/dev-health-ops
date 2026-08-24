package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
)

type gitHubProjectV2DecisionOracleRow struct {
	Emitted         bool `json:"emitted"`
	TransitionCount int  `json:"transition_count"`
}

type gitHubProjectV2TargetsOracleRow struct {
	Targets []string `json:"targets"`
}

type gitHubProjectV2CompositionOracleRow struct {
	ItemIDs     []string `json:"item_ids"`
	Titles      []string `json:"titles"`
	Transitions []string `json:"transitions"`
}

type gitHubProjectV2PaginationOracleRow struct {
	ItemIDs      []string `json:"item_ids"`
	ChangeCounts []int    `json:"change_counts"`
	OuterAfter   []string `json:"outer_after"`
	ChangeAfter  []string `json:"change_after"`
}

func TestGitHubProjectV2WorkItemMatchesLivePythonProductionRow(t *testing.T) {
	input := gitHubProjectV2OracleInput()
	compareRowsAgainstPythonOracle(
		t, "github/work-items-project-v2/row",
		[]oracleCase{{ID: "issue_with_project_dimensions", Input: input}},
		func(t *testing.T, input map[string]any) githubWorkItemRow {
			row, _, emitted := buildGitHubProjectV2OracleRows(t, input)
			if !emitted {
				t.Fatal("Go normalizer skipped issue")
			}
			return row
		}, githubWorkItemWriteStampGoOnly,
	)
}

func TestGitHubProjectV2DraftIssueMatchesLivePythonProductionRow(t *testing.T) {
	input := gitHubProjectV2DraftOracleInput()
	row, _, emitted := buildGitHubProjectV2OracleRows(t, input)
	if !emitted || row.WorkItemID != "ghproj:PVTI_DRAFT_1" {
		t.Fatalf("Go draft issue emitted=%t work_item_id=%q", emitted, row.WorkItemID)
	}
	compareRowsAgainstPythonOracle(
		t, "github/work-items-project-v2/row",
		[]oracleCase{{ID: "draft_issue_with_project_dimensions", Input: input}},
		func(t *testing.T, input map[string]any) githubWorkItemRow {
			row, _, emitted := buildGitHubProjectV2OracleRows(t, input)
			if !emitted {
				t.Fatal("Go normalizer skipped draft issue")
			}
			return row
		}, githubWorkItemWriteStampGoOnly,
	)
}

func TestGitHubProjectV2TransitionMatchesLivePythonProductionRow(t *testing.T) {
	input := gitHubProjectV2OracleInput()
	input["transition_index"] = 1
	compareRowsAgainstPythonOracle(
		t, "github/work-items-project-v2/transition",
		[]oracleCase{{ID: "complete_status_history", Input: input}},
		func(t *testing.T, input map[string]any) githubWorkItemTransitionRow {
			_, transitions, emitted := buildGitHubProjectV2OracleRows(t, input)
			if !emitted || len(transitions) < 2 {
				t.Fatalf("emitted=%t transitions=%+v", emitted, transitions)
			}
			return transitions[input["transition_index"].(int)]
		}, githubWorkItemWriteStampGoOnly,
	)
}

// TestGitHubProjectV2PullRequestEmissionDivergesFromLivePythonDecision is a
// DOCUMENTED DIVERGENCE, and it used to be a parity pin.
//
// Python drops a PullRequest board item outright (normalize.py:514) and so did
// Go (:452-454) -- the pin recorded that agreement. chris ruled on 2026-08-23
// that "if there's a project mapping it should be in the graph ... especially
// PRs", and Context Fabric ruled the shape on 2026-08-24, so Go now EMITS a
// board-membership row where Python emits nothing. That reverses the pin's
// direction, and this is the one case where the pair's own excluded_fields
// convention applies rather than goOnlyFields: `emitted` is a field BOTH sides
// produce and now genuinely disagree on, not a Go-only column Python lacks.
//
// The pin is not deleted, and that matters. `transition_count` stays compared,
// so the other half of the decision -- that a PR board item still contributes
// no status transitions, its `changes` history being CHAOS-4221's problem --
// remains pinned to live Python. Deleting the pair would have thrown that away
// to record one divergence.
//
// The case input gained a repository and a createdAt so the divergence is real
// rather than incidental. Without them Go would decline to emit for lack of a
// subject identity, the two sides would agree by accident, and the exclusion
// below would be documenting a disagreement that never happened.
func TestGitHubProjectV2PullRequestEmissionDivergesFromLivePythonDecision(t *testing.T) {
	input := map[string]any{
		"project_scope_id": "ghprojv2:acme#3",
		"item_node": map[string]any{
			"id": "PVTI_PR", "createdAt": "2026-08-01T08:00:00Z",
			"content": map[string]any{
				"__typename": "PullRequest", "number": 9, "title": "PR",
				"repository": map[string]any{"nameWithOwner": "acme/api"},
			},
			"fieldValues": map[string]any{"nodes": []any{}}, "changes": map[string]any{"nodes": []any{}},
		},
	}
	membership, transitions, emitted := buildGitHubProjectV2OracleDecision(t, withOracleOrg(input))
	if !emitted || len(transitions) != 0 {
		t.Fatalf("Go Projects v2 PR decision emitted=%t transitions=%d", emitted, len(transitions))
	}
	// The divergence is only worth recording if what Go emits is joinable. A
	// row that emitted but keyed to nothing would be a worse outcome than the
	// drop it replaced.
	if membership.SubjectKind != "pull_request" || membership.SubjectID != "9" {
		t.Fatalf("emitted membership does not identify the PR: %+v", membership)
	}
	compareRowsAgainstPythonOracle(
		t, "github/work-items-project-v2/pr-skip", []oracleCase{{ID: "pull_request", Input: input}},
		func(t *testing.T, input map[string]any) gitHubProjectV2DecisionOracleRow {
			_, transitions, emitted := buildGitHubProjectV2OracleDecision(t, withOracleOrg(input))
			return gitHubProjectV2DecisionOracleRow{Emitted: emitted, TransitionCount: len(transitions)}
		}, nil,
	)
}

// buildGitHubProjectV2OracleDecision answers the inclusion question the pair
// actually asks -- "does this board item produce a durable row?" -- across BOTH
// row families the loop now produces, because that is the question Python's
// single normalizer answers with one boolean. Asking only the work-item
// normalizer would report `emitted=false` for a PR whose membership row was
// written moments later, which is the shape that hid this defect.
func buildGitHubProjectV2OracleDecision(
	t *testing.T, input map[string]any,
) (projectmembership.Row, []githubWorkItemTransitionRow, bool) {
	t.Helper()
	raw, err := json.Marshal(input["item_node"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var item gitHubProjectV2ItemPayload
	if err := decoder.Decode(&item); err != nil {
		t.Fatal(err)
	}
	claim := githubWorkItemOracleClaim()
	claim.OrgID = input["org_id"].(string)
	normalizedAt := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	scopeID := input["project_scope_id"].(string)
	membership, hasMembership := githubProjectV2MembershipRow(claim, item, scopeID, normalizedAt)
	_, transitions, emittedWorkItem, err := normalizeGitHubProjectV2Item(claim, item, scopeID, nil, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	return membership, transitions, hasMembership || emittedWorkItem
}

func withOracleOrg(input map[string]any) map[string]any {
	copied := make(map[string]any, len(input)+1)
	for key, value := range input {
		copied[key] = value
	}
	copied["org_id"] = "org-acme"
	return copied
}

func TestGitHubProjectV2TargetParserMatchesLivePythonValidTargetSemantics(t *testing.T) {
	input := map[string]any{"raw": " acme:3, labs:12, acme:3 "}
	compareRowsAgainstPythonOracle(
		t, "github/work-items-project-v2/target-parser", []oracleCase{{ID: "ordered_duplicates", Input: input}},
		func(t *testing.T, _ map[string]any) gitHubProjectV2TargetsOracleRow {
			claim := githubWorkItemOracleClaim()
			claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
				map[string]any{"org_login": "acme", "project_number": 3},
				map[string]any{"org_login": "labs", "project_number": 12},
				map[string]any{"org_login": "acme", "project_number": 3},
			}}
			targets, err := githubProjectV2Targets(claim)
			if err != nil {
				t.Fatal(err)
			}
			row := gitHubProjectV2TargetsOracleRow{Targets: make([]string, 0, len(targets))}
			for _, target := range targets {
				row.Targets = append(row.Targets, target.OrgLogin+":"+strconv.Itoa(target.ProjectNumber))
			}
			return row
		}, nil,
	)
}

func TestMergeGitHubProjectV2RowsMatchesLivePythonComposition(t *testing.T) {
	input := map[string]any{
		"repository_items":       []any{map[string]any{"work_item_id": "same", "title": "repository"}, map[string]any{"work_item_id": "repo-only", "title": "repo"}},
		"project_items":          []any{map[string]any{"work_item_id": "same", "title": "project"}, map[string]any{"work_item_id": "project-only", "title": "project"}},
		"repository_transitions": []any{"repo-transition"}, "project_transitions": []any{"project-transition"},
	}
	compareRowsAgainstPythonOracle(
		t, "github/work-items-project-v2/composition", []oracleCase{{ID: "last_wins_append", Input: input}},
		func(t *testing.T, _ map[string]any) gitHubProjectV2CompositionOracleRow {
			got := mergeGitHubProjectV2Rows(
				githubWorkItemRows{WorkItems: []githubWorkItemRow{{WorkItemID: "same", Title: "repository"}, {WorkItemID: "repo-only", Title: "repo"}}, StatusTransitions: []githubWorkItemTransitionRow{{WorkItemID: "repo-transition"}}},
				githubWorkItemRows{WorkItems: []githubWorkItemRow{{WorkItemID: "same", Title: "project"}, {WorkItemID: "project-only", Title: "project"}}, StatusTransitions: []githubWorkItemTransitionRow{{WorkItemID: "project-transition"}}},
			)
			row := gitHubProjectV2CompositionOracleRow{}
			for _, item := range got.WorkItems {
				row.ItemIDs = append(row.ItemIDs, item.WorkItemID)
				row.Titles = append(row.Titles, item.Title)
			}
			for _, transition := range got.StatusTransitions {
				row.Transitions = append(row.Transitions, transition.WorkItemID)
			}
			return row
		}, nil,
	)
}

func TestGitHubProjectV2PaginationMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "github/work-items-project-v2/pagination", []oracleCase{{ID: "outer_and_nested", Input: map[string]any{}}},
		func(t *testing.T, _ map[string]any) gitHubProjectV2PaginationOracleRow {
			doer := &gitHubProjectV2Doer{t: t, replies: []string{
				`{"data":{"organization":{"projectV2":{"items":{"nodes":[{"id":"PVTI_1","content":{"__typename":"DraftIssue","title":"one"},"fieldValues":{"nodes":[]},"changes":{"nodes":[{"createdAt":"2026-08-01T08:00:00Z"}],"pageInfo":{"hasNextPage":true,"endCursor":"change-1"}}}],"pageInfo":{"hasNextPage":true,"endCursor":"item-1"}}}}}}`,
				`{"data":{"node":{"changes":{"nodes":[{"createdAt":"2026-08-02T08:00:00Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}`,
				`{"data":{"organization":{"projectV2":{"items":{"nodes":[{"id":"PVTI_2","content":{"__typename":"DraftIssue","title":"two"},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
			}}
			evidence := FetchEvidence{}
			items, err := fetchGitHubProjectV2Target(context.Background(), githubProjectV2TestClient(t, doer), GitHubProjectV2Target{OrgLogin: "acme", ProjectNumber: 3}, &evidence)
			if err != nil {
				t.Fatal(err)
			}
			row := gitHubProjectV2PaginationOracleRow{}
			for _, item := range items {
				row.ItemIDs = append(row.ItemIDs, item.ID)
				row.ChangeCounts = append(row.ChangeCounts, len(item.Changes.Nodes))
			}
			for _, body := range doer.bodies {
				variables := body["variables"].(map[string]any)
				if itemID, nested := variables["itemId"]; nested {
					row.ChangeAfter = append(row.ChangeAfter, itemID.(string)+":"+variables["after"].(string))
					continue
				}
				after := "<none>"
				if variables["after"] != nil {
					after = variables["after"].(string)
				}
				row.OuterAfter = append(row.OuterAfter, after)
			}
			return row
		}, nil,
	)
}

func gitHubProjectV2OracleInput() map[string]any {
	return map[string]any{
		"org_id": "org-acme", "project_scope_id": "ghprojv2:acme#3",
		"item_node": map[string]any{
			"id": "PVTI_1",
			"content": map[string]any{
				"__typename": "Issue", "number": 7, "title": "Repair delivery path",
				"url": "https://github.com/acme/api/issues/7", "state": "OPEN",
				"createdAt":  "2026-08-01T08:00:00.123456Z",
				"updatedAt":  "2026-08-03T09:30:00.654321Z",
				"repository": map[string]any{"nameWithOwner": "acme/api"},
				"labels":     map[string]any{"nodes": []any{map[string]any{"name": "bug"}}},
				"assignees":  map[string]any{"nodes": []any{map[string]any{"login": "reviewer"}}},
				"author":     map[string]any{"login": "author"},
			},
			"fieldValues": map[string]any{"nodes": []any{
				map[string]any{"__typename": "ProjectV2ItemFieldSingleSelectValue", "name": "Doing", "field": map[string]any{"name": "Status"}},
				map[string]any{"__typename": "ProjectV2ItemFieldIterationValue", "title": "Sprint 8", "id": "ITER_8", "field": map[string]any{"name": "Iteration"}},
				map[string]any{"__typename": "ProjectV2ItemFieldNumberValue", "number": 5.0, "field": map[string]any{"name": "Story Points"}},
			}},
			"changes": map[string]any{"nodes": []any{
				map[string]any{"field": map[string]any{"name": "Status"}, "previousValue": map[string]any{"name": "Todo"}, "newValue": map[string]any{"name": "Doing"}, "createdAt": "2026-08-02T08:00:00Z", "actor": map[string]any{"login": "maintainer"}},
				map[string]any{"field": map[string]any{"name": "Phase"}, "previousValue": map[string]any{"name": "Doing"}, "newValue": map[string]any{"name": "Done"}, "createdAt": "2026-08-03T08:00:00Z", "actor": map[string]any{"login": "maintainer"}},
			}},
		},
	}
}

func gitHubProjectV2DraftOracleInput() map[string]any {
	return map[string]any{
		"org_id": "org-acme", "project_scope_id": "ghprojv2:acme#3",
		"item_node": map[string]any{
			"id": "PVTI_DRAFT_1",
			"content": map[string]any{
				"__typename": "DraftIssue", "title": "Shape the migration",
				"createdAt": "2026-08-01T08:00:00.123456Z",
				"updatedAt": "2026-08-03T09:30:00.654321Z",
			},
			"fieldValues": map[string]any{"nodes": []any{
				map[string]any{"__typename": "ProjectV2ItemFieldSingleSelectValue", "name": "Ready", "field": map[string]any{"name": "Status"}},
				map[string]any{"__typename": "ProjectV2ItemFieldIterationValue", "title": "Sprint 9", "id": "ITER_9", "field": map[string]any{"name": "Sprint"}},
				map[string]any{"__typename": "ProjectV2ItemFieldNumberValue", "number": 8.5, "field": map[string]any{"name": "Estimate"}},
			}},
			"changes": map[string]any{"nodes": []any{}},
		},
	}
}

func buildGitHubProjectV2OracleRows(t *testing.T, input map[string]any) (githubWorkItemRow, []githubWorkItemTransitionRow, bool) {
	t.Helper()
	raw, err := json.Marshal(input["item_node"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var item gitHubProjectV2ItemPayload
	if err := decoder.Decode(&item); err != nil {
		t.Fatal(err)
	}
	claim := githubWorkItemOracleClaim()
	claim.OrgID = input["org_id"].(string)
	row, transitions, emitted, err := normalizeGitHubProjectV2Item(
		claim, item, input["project_scope_id"].(string), nil,
		time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return row, transitions, emitted
}

func buildGitHubProjectV2OracleRowsWithDefaults(t *testing.T, input map[string]any) (githubWorkItemRow, []githubWorkItemTransitionRow, bool) {
	t.Helper()
	copy := make(map[string]any, len(input)+1)
	for key, value := range input {
		copy[key] = value
	}
	copy["org_id"] = "org-acme"
	return buildGitHubProjectV2OracleRows(t, copy)
}
