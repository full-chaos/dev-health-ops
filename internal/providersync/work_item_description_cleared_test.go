package providersync

import (
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
	for name, want := range map[string]*string{"null": ptr(""), "empty": ptr(""), "empty document": ptr(""), "text": ptr("body")} {
		got := build(t, func(f map[string]any) {
			switch name {
			case "null":
				f["description"] = nil
			case "empty":
				f["description"] = ""
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
// addition that lets the sink clear a stored description.
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
