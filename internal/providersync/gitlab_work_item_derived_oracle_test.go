package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

// The GitLab cases reuse the same boundary-rich inputs as the already-landed
// provider-neutral Python work-item producers, changing only provider leaves.
// The Python side still executes the real production functions; this helper
// only prevents nine hand-maintained fixture sets from drifting apart.
func gitlabOracleCases(cases []oracleCase) []oracleCase {
	result := make([]oracleCase, 0, len(cases))
	for _, testCase := range cases {
		result = append(result, oracleCase{ID: "gitlab_" + testCase.ID, Input: gitlabOracleValue(testCase.Input).(map[string]any)})
	}
	return result
}

func gitlabOracleValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, nested := range typed {
			result[key] = gitlabOracleValue(nested)
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for index, nested := range typed {
			result[index] = gitlabOracleValue(nested)
		}
		return result
	case string:
		if typed == "github" {
			return "gitlab"
		}
		return typed
	default:
		return value
	}
}

func gitlabWorkItemOracleClaim(input map[string]any) Claim {
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = input["OrgID"].(string)
	return claim
}

func gitlabWorkItemOracleDay(t *testing.T, input map[string]any) time.Time {
	t.Helper()
	dayValue, ok := input["Day"].(string)
	if !ok {
		dayValue = input["SinceAt"].(string)[:len(time.DateOnly)]
	}
	day, err := time.Parse(time.DateOnly, dayValue)
	if err != nil {
		t.Fatal(err)
	}
	return day
}

func TestGitLabAIAttributionMatchesFrozenPythonProductionRows(t *testing.T) {
	base := map[string]any{
		"iid": 9, "title": "Repair delivery path", "description": "Routine repair",
		"state": "opened", "created_at": "2026-08-01T08:00:00Z",
		"updated_at": "2026-08-03T09:30:00Z", "assignees": []any{},
		"source_branch": "feature/repair",
	}
	labelMR := cloneSemanticOracleMap(base)
	labelMR["created_at"] = nil
	labelMR["labels"] = []any{"codex"}
	labelMR["author"] = map[string]any{"username": "author", "bot": false}
	botMR := cloneSemanticOracleMap(base)
	botMR["labels"] = []any{}
	botMR["author"] = map[string]any{"username": "chatgpt-codex[bot]", "bot": true}
	unknownBotMR := cloneSemanticOracleMap(base)
	unknownBotMR["labels"] = []any{}
	unknownBotMR["author"] = map[string]any{"username": "ai-helper[bot]", "bot": true}
	trailerMR := cloneSemanticOracleMap(base)
	trailerMR["labels"] = []any{}
	trailerMR["description"] = "Repair delivery path\n\nAI-Assisted-By: Claude Code"
	trailerMR["author"] = map[string]any{"username": "author", "bot": false}
	branchMR := cloneSemanticOracleMap(base)
	branchMR["labels"] = []any{}
	branchMR["source_branch"] = "copilot/repair"
	branchMR["author"] = map[string]any{"username": "author", "bot": false}
	bodyMR := cloneSemanticOracleMap(base)
	bodyMR["labels"] = []any{}
	bodyMR["description"] = "Initial scaffold created with Codex."
	bodyMR["author"] = map[string]any{"username": "author", "bot": false}
	cases := []oracleCase{
		{ID: "codex_label", Input: gitlabAIAttributionOracleInput(labelMR, "pr_label")},
		{ID: "known_bot_author", Input: gitlabAIAttributionOracleInput(botMR, "bot_author")},
		{ID: "unknown_bot_author", Input: gitlabAIAttributionOracleInput(unknownBotMR, "bot_author")},
		{ID: "commit_trailer", Input: gitlabAIAttributionOracleInput(trailerMR, "commit_trailer")},
		{ID: "source_branch", Input: gitlabAIAttributionOracleInput(branchMR, "branch_name")},
		{ID: "description_body", Input: gitlabAIAttributionOracleInput(bodyMR, "pr_body")},
	}
	compareRowsAgainstPythonOracle(
		t, "gitlab/work-items/ai-attribution", cases,
		buildGitLabAIAttributionOracleRow, nil,
	)
}

func TestGitLabAIAttributionIsRetryStableAndDoesNotFabricateSignals(t *testing.T) {
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	updated := "2026-08-03T09:30:00Z"
	username := "author"
	bot := false
	payload := gitlabMergeRequestWorkItemPayload{
		IID: 9, Labels: []string{"codex"}, UpdatedAt: &updated,
		Author:       &gitlabWorkItemUserPayload{Username: &username, Bot: bot},
		SourceBranch: "feature/repair",
	}
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	first, err := normalizeGitLabMRAIAttributions(claim, repoID, payload, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	second, err := normalizeGitLabMRAIAttributions(claim, repoID, payload, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	wantObserved := time.Date(2026, 8, 3, 9, 30, 0, 0, time.UTC)
	if len(first) != 1 || len(second) != 1 || first[0].RecordID != second[0].RecordID ||
		!first[0].IngestedAt.Equal(second[0].IngestedAt) ||
		!first[0].ObservedAt.Equal(wantObserved) {
		t.Fatalf("retry/fallback rows=%+v/%+v", first, second)
	}

	description := "Routine repair without an AI signal."
	nonAI := payload
	nonAI.Labels = []string{"maintenance"}
	nonAI.Description = &description
	nonAI.SourceBranch = "feature/repair"
	if rows, err := normalizeGitLabMRAIAttributions(claim, repoID, nonAI, normalizedAt); err != nil || len(rows) != 0 {
		t.Fatalf("non-AI MR rows=%+v error=%v", rows, err)
	}
	ciUsername := "dependabot[bot]"
	nonAI.Author = &gitlabWorkItemUserPayload{Username: &ciUsername, Bot: true}
	if rows, err := normalizeGitLabMRAIAttributions(claim, repoID, nonAI, normalizedAt); err != nil || len(rows) != 0 {
		t.Fatalf("CI bot rows=%+v error=%v", rows, err)
	}
}

func gitlabAIAttributionOracleInput(rawMR map[string]any, source string) map[string]any {
	return map[string]any{
		"repo_full_name": "acme/api",
		"repo_id":        "c7198fbc-1945-3717-05d8-eb78866b4e79",
		"org_id":         "77777777-7777-4777-8777-777777777777",
		"raw_mr":         rawMR,
		"signal_source":  source,
	}
}

// buildGitLabAIAttributionOracleRow runs the PR/MR attribution through the
// ONE route that writes it, the prs unit, over a single merge request list
// item, and returns the row of the requested source. The frozen Python answer
// is the same function (gitlab_mr_ai_attributions) the work-items route used
// to be compared against.
func buildGitLabAIAttributionOracleRow(
	t *testing.T,
	input map[string]any,
) githubAIAttributionRow {
	t.Helper()
	rawMR := map[string]any{"user_notes_count": 0, "target_branch": "main"}
	for key, value := range input["raw_mr"].(map[string]any) {
		rawMR[key] = value
	}
	list, err := json.Marshal([]any{rawMR})
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("gitlab", "prs")
	claim.OrgID = input["org_id"].(string)
	// The oracle merge requests were updated in early August.
	since, before := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	claim.SinceAt, claim.BeforeAt = &since, &before
	batch, err := (GitLabPullRequestRouteHandler{PerPage: 100}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		glAttributionClient(t, &glAttributionDoer{
			list: string(list), project: `{"id":123,"path":"api","path_with_namespace":"acme/api","name":"api"}`,
		}),
		time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, effect := range batch.Effects {
		if effect.Destination != "ai_attribution" {
			continue
		}
		for _, raw := range effect.Rows {
			var row githubAIAttributionRow
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			if row.Source == input["signal_source"] {
				return row
			}
		}
	}
	t.Errorf("Go GitLab prs route omitted authoritative ai_attribution source %q", input["signal_source"])
	return githubAIAttributionRow{
		Evidence: map[string]any{"oracle_failure": fmt.Sprint(input["signal_source"])},
	}
}

func gitlabRowsFromGitHub(rows githubWorkItemRows) gitlabWorkItemRows {
	return gitlabWorkItemRows{
		WorkItems: rows.WorkItems, StatusTransitions: rows.StatusTransitions,
		Dependencies: rows.Dependencies, ReopenEvents: rows.ReopenEvents,
		Interactions: rows.Interactions, Sprints: rows.Sprints,
	}
}

// The unit writes the six raw tables and ai_attribution, nothing computed
// from stored rows, and its watermark is the end of the window it stored.
func TestGitLabWorkItemsRouteComposesRawEffectsAndAdvancesWatermark(t *testing.T) {
	responses := gitLabWorkItemResponses()
	root := "/api/v4/projects/123"
	responses[root+"/merge_requests?page=1"] = []string{
		`[{"iid":9,"title":"Ship the API","description":"","state":"opened","created_at":"2026-07-04T09:00:00Z","updated_at":"2026-07-04T10:00:00Z","labels":["priority::low","codex"],"assignees":[],"author":{"username":"alice","bot":false},"source_branch":"feature/ship-api"}]`,
		`[]`,
	}
	doer := &gitLabWorkItemsDoer{responses: responses}
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	batch, err := (GitLabWorkItemsRouteHandler{
		StatusMapping: loadRealStatusMapping(t),
		PerPage:       2, MaxPages: 10, NestedMaxPages: 10,
	}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 7 {
		t.Fatalf("effects=%d want the six raw tables and ai_attribution", len(batch.Effects))
	}
	if err := batch.validate(CompleteRouteDescriptor{Destinations: workItemRouteDestinations()}); err != nil {
		t.Fatalf("governed destination contract: %v", err)
	}
	for _, effect := range batch.Effects {
		if slices.Contains(githubWorkItemDerivedDestinations, effect.Destination) {
			t.Fatalf("the unit built an effect for the derived table %q", effect.Destination)
		}
	}
	var aiEffect *EffectBatch
	for _, effect := range batch.Effects {
		if effect.Destination == "ai_attribution" {
			copy := effect
			aiEffect = &copy
		}
	}
	// The destination stays in the manifest, but the work-items route writes no
	// merge-request attribution: the prs unit is its only writer.
	if aiEffect == nil || len(aiEffect.Rows) != 0 {
		t.Fatalf("AI effect=%+v want the destination present and empty", aiEffect)
	}
	summary, ok := batch.Result["gitlab_work_items"].(GitLabWorkItemsResult)
	if !ok {
		t.Fatalf("summary type=%T", batch.Result["gitlab_work_items"])
	}
	if summary.WorkItemsSynced == 0 || batch.Watermark == nil ||
		claim.BeforeAt == nil || !batch.Watermark.Equal(claim.BeforeAt.UTC()) {
		t.Fatalf("summary/watermark=%+v/%v", summary, batch.Watermark)
	}
	for _, key := range []string{
		"derived_destinations_implemented", "derived_destinations_unimplemented",
		"watermark_held_for_derived_gap", "team_inheritance",
	} {
		if _, present := batch.Result[key]; present {
			t.Fatalf("result still carries %q: %+v", key, batch.Result)
		}
	}
}
