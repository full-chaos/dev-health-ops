package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/storedversion"
)

type descriptionCase struct {
	name       string
	payload    string
	wantStated bool
	wantText   string
}

func descriptionCases(key string) []descriptionCase {
	return []descriptionCase{
		{"text", `"` + key + `": "body",`, true, "body"},
		{"null", `"` + key + `": null,`, true, ""},
		{"empty", `"` + key + `": "",`, true, ""},
		{"absent", ``, false, ""},
	}
}

func checkStatedDescription(t *testing.T, got *string, c descriptionCase) {
	t.Helper()
	if !c.wantStated {
		if got != nil {
			t.Fatalf("%s: description=%q, want nil (unstated)", c.name, *got)
		}
		return
	}
	if got == nil || *got != c.wantText {
		t.Fatalf("%s: description=%v, want %q", c.name, valueOrNil(got), c.wantText)
	}
}

func TestGitHubIssueWorkItemStatesAClearedDescription(t *testing.T) {
	for _, c := range descriptionCases("body") {
		t.Run(c.name, func(t *testing.T) {
			raw := json.RawMessage(`{` + c.payload + `"number": 7, "title": "T", "state": "open",
				"created_at": "2026-08-01T08:00:00Z", "updated_at": "2026-08-03T09:30:00Z", "labels": [], "assignees": []}`)
			row, err := normalizeGitHubIssueWorkItem(
				githubWorkItemOracleClaim(), sameGitHubRepoNames("acme/api"),
				uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79"), raw, nil,
				time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
			)
			if err != nil {
				t.Fatal(err)
			}
			checkStatedDescription(t, row.Description, c)
		})
	}
}

func TestGitLabIssueWorkItemStatesAClearedDescription(t *testing.T) {
	for _, c := range descriptionCases("description") {
		t.Run(c.name, func(t *testing.T) {
			var payload gitlabIssueWorkItemPayload
			raw := `{` + c.payload + `"iid": 9, "title": "T", "state": "opened",
				"created_at": "2026-10-01T00:00:00Z", "updated_at": "2026-10-02T00:00:00Z", "labels": []}`
			if err := json.Unmarshal([]byte(raw), &payload); err != nil {
				t.Fatal(err)
			}
			row, _, err := normalizeGitLabIssueWorkItem(
				nativeTestClaim("gitlab", "work-items"), "acme/api", "acme/api", "org:gitlab:1",
				uuid.MustParse("0b7e3d4e-1111-4222-8333-944455556666"), payload, nil,
				loadRealStatusMapping(t), nil, time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC),
			)
			if err != nil {
				t.Fatal(err)
			}
			checkStatedDescription(t, row.Description, c)
		})
	}
}

func TestLinearWorkItemStatesAClearedDescription(t *testing.T) {
	for _, c := range descriptionCases("description") {
		t.Run(c.name, func(t *testing.T) {
			var payload linearWorkItemPayload
			raw := `{` + c.payload + `"id": "l1", "identifier": "ENG-1", "title": "T",
				"createdAt": "2026-10-01T00:00:00Z", "updatedAt": "2026-10-02T00:00:00Z"}`
			if err := json.Unmarshal([]byte(raw), &payload); err != nil {
				t.Fatal(err)
			}
			row, _, err := normalizeLinearWorkItem(
				nativeTestClaim("linear", "work-items"), payload, time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC),
			)
			if err != nil {
				t.Fatal(err)
			}
			checkStatedDescription(t, row.Description, c)
		})
	}
}

func TestJiraWorkItemStatesAClearedDescription(t *testing.T) {
	input := jiraWorkItemOraclePrepCases()[0].Input
	raw := input["raw_issue"].(map[string]any)
	build := func(t *testing.T, mutate func(map[string]any)) *string {
		fields := map[string]any{}
		for key, value := range raw["fields"].(map[string]any) {
			fields[key] = value
		}
		mutate(fields)
		claim := nativeTestClaim("jira", "work-items")
		claim.OrgID = input["org_id"].(string)
		row, _, err := normalizeJiraWorkItem(
			claim, jiraWorkItemFixtureInput{Raw: map[string]any{"key": raw["key"], "self": raw["self"], "fields": fields}},
			loadRealStatusMapping(t), jiraOracleIdentity, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
		)
		if err != nil {
			t.Fatal(err)
		}
		return row.Description
	}
	for name, want := range map[string]*string{"null": ptr(""), "empty": ptr(""), "empty document": ptr(""), "empty paragraph": ptr(""), "text": ptr("body")} {
		got := build(t, func(f map[string]any) {
			switch name {
			case "null":
				f["description"] = nil
			case "empty":
				f["description"] = ""
			case "empty paragraph":
				f["description"] = map[string]any{"type": "doc", "version": 1, "content": []any{map[string]any{"type": "paragraph"}}}
			case "empty document":
				f["description"] = map[string]any{"type": "doc", "version": 1, "content": []any{}}
			default:
				f["description"] = "body"
			}
		})
		if got == nil || *got != *want {
			t.Fatalf("%s: description=%v want %q", name, valueOrNil(got), *want)
		}
	}
	if got := build(t, func(f map[string]any) { delete(f, "description") }); got != nil {
		t.Fatalf("absent: description=%q want nil", *got)
	}
}

// frozenOracleDescription maps a description stated empty to nil: the frozen
// Python producers fold empty to None, and the Go row's stated empty is the
// addition that lets the sink clear a stored description. Whether a payload
// states or omits the key is proven by the *StatesAClearedDescription tests.
func frozenOracleDescription(description *string) *string {
	if description != nil && *description == "" {
		return nil
	}
	return description
}

func ptr(text string) *string { return &text }

func TestWorkItemSyncClearsAHeldDescriptionOnlyWhenTheRowStatesEmpty(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   *string
		want string
	}{
		{"stated empty clears", ptr(""), ""},
		{"unstated keeps", nil, "pushed body"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := projectWorkItem(githubWorkItemRow{WorkItemID: "gh:acme/api#1", Provider: "github", Description: tc.in})
			rows := workItemStoredVersionRows([]workItemStoredRow{row})
			if _, err := workItemsContract.Fold(withHeldWorkItemColumns(gitHubWorkItemsInsert), rows, heldWorkItemVersion("pushed body")); err != nil {
				t.Fatal(err)
			}
			positions, err := storedversion.Positions(withHeldWorkItemColumns(gitHubWorkItemsInsert))
			if err != nil {
				t.Fatal(err)
			}
			got := rows[0].Values[positions["description"]]
			if p, ok := got.(*string); ok && p != nil {
				got = *p
			}
			if got != tc.want {
				t.Fatalf("description = %v, want %q", got, tc.want)
			}
		})
	}
}

func TestWorkItemPayloadCarryClearsOnlyOnAStatedEmptyString(t *testing.T) {
	carry := workItemsPayloadCarry(workItemsContract)
	for _, tc := range []struct {
		name    string
		payload map[string]any
		carried bool
	}{
		{"empty string clears", map[string]any{"description": ""}, false},
		{"text clears", map[string]any{"description": "x"}, false},
		{"null carries", map[string]any{"description": nil}, true},
		{"absent carries", map[string]any{}, true},
	} {
		if got := carry(tc.payload)["description"]; got != tc.carried {
			t.Errorf("%s: carried=%v want %v", tc.name, got, tc.carried)
		}
	}
}

// A unit whose v1 ledger was written before this change hashed a cleared
// description as null. The retry after the upgrade collects the same provider
// payload, which now states "" and so has another digest. The recovery must
// commit, not stop on the manifest check.
func runLegacyLedgerRetry(t *testing.T, writing bool) (*replanMemoryLedger, CompleteRouteExecutionResult, error, int) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedWorkItemsSession(t, now, "gitlab", "work-items")
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	descriptor, ok := Descriptor("gitlab", "work-items")
	if !ok || !descriptor.PreparedManifestRecovery {
		t.Fatalf("descriptor=%+v ok=%v", descriptor, ok)
	}
	build := func(description *string) CompleteRouteBatch {
		row := gitlabWorkItemRow{
			WorkItemID: "gitlab:acme/api#9", Provider: "gitlab", Title: "T", Type: "issue", Status: "todo",
			Description: description, RepoID: &repoID, CreatedAt: now, UpdatedAt: now, LastSynced: now, OrgID: claim.OrgID,
		}
		var effects []EffectBatch
		for _, destination := range descriptor.Destinations {
			values := []map[string]string{}
			if destination == "work_items" {
				effect, err := effectBatchFromValues(destination, EffectReadbackRequired, []gitlabWorkItemRow{row})
				if err != nil {
					t.Fatal(err)
				}
				effects = append(effects, effect)
				continue
			}
			effect, err := effectBatchFromValues(destination, EffectReadbackRequired, values)
			if err != nil {
				t.Fatal(err)
			}
			effects = append(effects, effect)
		}
		return CompleteRouteBatch{
			Effects: effects, Result: map[string]any{"work_items_synced": 1}, Watermark: claim.BeforeAt,
			Evidence: FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Records: 1},
		}
	}
	legacy, err := NewEffectLedgerState(claim, build(nil).Effects, now)
	if err != nil {
		t.Fatal(err)
	}
	if writing {
		legacy.Effects[0].Status = GenerationBlockWriting
		started := now
		legacy.Effects[0].StartedAt = &started
	}
	ledger := &replanMemoryLedger{memoryEffectLedger: &memoryEffectLedger{state: legacy}}
	handler := &staticCompleteRouteHandler{batch: build(ptr(""))}
	executor := completeRouteExecutor(now.Add(time.Hour), handler, ledger, &memoryEffectSink{})
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "gitlab"}
	executor.Credentials.Decryptor = preparedGitHubTokenDecryptor{}
	executor.BudgetLimits = map[CostClass]int{claim.CostClass: 1}
	result, err := executor.Execute(context.Background(), session, descriptor)
	return ledger, result, err, len(descriptor.Destinations)
}

func TestLegacyLedgerRetryAcrossTheDescriptionChangeCommits(t *testing.T) {
	ledger, result, err, destinations := runLegacyLedgerRetry(t, false)
	if ledger.resets != 1 {
		t.Fatalf("resets=%d, want the untouched legacy ledger discarded once", ledger.resets)
	}
	if err != nil {
		t.Fatalf("retry across the change: %v", err)
	}
	if result.Effects.Written != destinations {
		t.Fatalf("result=%+v, want every destination written", result.Effects)
	}
}

// A writing effect may already have a row in the sink: the ledger is not
// discarded, so the readback still settles it.
func TestLegacyLedgerWithAWritingEffectIsNotDiscarded(t *testing.T) {
	ledger, _, _, _ := runLegacyLedgerRetry(t, true)
	if ledger.resets != 0 {
		t.Fatalf("resets=%d, want a ledger with a writing effect kept", ledger.resets)
	}
}

// The Projects v2 query does not ask for a body, so its row states no
// description; the repository row for the same item does, and must survive the
// merge, a stated empty included.
func TestProjectsV2MergeKeepsTheRepositoryRowDescription(t *testing.T) {
	for name, stated := range map[string]*string{"cleared": ptr(""), "text": ptr("repo body")} {
		repository := githubWorkItemRows{WorkItems: []githubWorkItemRow{{WorkItemID: "gh:acme/api#1", Description: stated}}}
		projects := githubWorkItemRows{WorkItems: []githubWorkItemRow{{WorkItemID: "gh:acme/api#1"}}}
		merged := mergeGitHubProjectV2Rows(repository, projects)
		if got := merged.WorkItems[0].Description; got == nil || *got != *stated {
			t.Fatalf("%s: merged description=%v, want %q", name, valueOrNil(got), *stated)
		}
	}
	repository := githubWorkItemRows{WorkItems: []githubWorkItemRow{{WorkItemID: "gh:acme/api#1", Description: ptr("old")}}}
	projects := githubWorkItemRows{WorkItems: []githubWorkItemRow{{WorkItemID: "gh:acme/api#1", Description: ptr("board body")}}}
	if got := mergeGitHubProjectV2Rows(repository, projects).WorkItems[0].Description; got == nil || *got != "board body" {
		t.Fatalf("a board row that states a description must win, got %v", valueOrNil(got))
	}
}

func TestGitLabEpicWorkItemStatesAClearedDescription(t *testing.T) {
	for _, c := range descriptionCases("description") {
		t.Run(c.name, func(t *testing.T) {
			var payload gitlabEpicPayload
			raw := `{` + c.payload + `"iid": 4, "title": "E", "state": "opened",
				"created_at": "2026-10-01T00:00:00Z", "updated_at": "2026-10-02T00:00:00Z", "labels": []}`
			if err := json.Unmarshal([]byte(raw), &payload); err != nil {
				t.Fatal(err)
			}
			row, _, _, err := normalizeGitLabEpicWorkItem(
				nativeTestClaim("gitlab", "work-items"), "acme", payload, nil, loadRealStatusMapping(t), nil,
				time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC),
			)
			if err != nil {
				t.Fatal(err)
			}
			checkStatedDescription(t, row.Description, c)
		})
	}
}
