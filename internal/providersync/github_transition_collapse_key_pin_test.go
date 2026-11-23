package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

// The settle step reads stored GitHub transitions without from_status_raw and
// actor (gitHubProjectV2StoredTransitionsQuery selects neither), then
// transitionsAfterWrite collapses rows by instant + transitionTieOrder. The
// readers count two events that differ only by those fields as two; settle
// would see one. That is safe only while no GitHub transition producer writes
// them, which this cell pins over every GitHub transition kind.
func TestGitHubTransitionProducersWriteNoRawFromOrActor(t *testing.T) {
	claim := nativeTestClaim("github", "work-items")
	normalizedAt := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	created := time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)
	login := "octo-synthetic"
	resolver := func(githubWorkItemUserPayload) string { return "github:" + login }

	type produced struct {
		source string
		row    githubWorkItemTransitionRow
	}
	var rows []produced

	timeline := func(pullRequest bool, events []githubWorkItemEventPayload) {
		t.Helper()
		raws := make([]json.RawMessage, 0, len(events))
		for _, event := range events {
			event.Actor = &githubWorkItemUserPayload{Login: &login}
			raw, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			raws = append(raws, raw)
		}
		source, item := "issue timeline", "gh:acme/api#1"
		if pullRequest {
			source, item = "pull request timeline", "gh:acme/api#2"
		}
		for _, resolve := range []githubIdentityResolver{nil, resolver} {
			out, _, err := normalizeGitHubWorkItemEvents(claim, item, pullRequest, raws, created, resolve, normalizedAt)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range out {
				rows = append(rows, produced{source: source, row: row})
			}
		}
	}
	at := func(value string) *string { return &value }
	timeline(false, []githubWorkItemEventPayload{
		{Event: "labeled", CreatedAt: at("2026-07-03T00:00:00Z"), Label: &githubWorkItemLabelPayload{Name: "in progress"}},
		{Event: "unlabeled", CreatedAt: at("2026-07-04T00:00:00Z"), Label: &githubWorkItemLabelPayload{Name: "in progress"}},
		{Event: "closed", CreatedAt: at("2026-07-05T00:00:00Z")},
		{Event: "reopened", CreatedAt: at("2026-07-06T00:00:00Z")},
	})
	timeline(true, []githubWorkItemEventPayload{
		{Event: "closed", CreatedAt: at("2026-07-03T00:00:00Z")},
		{Event: "reopened", CreatedAt: at("2026-07-04T00:00:00Z")},
		{Event: "merged", CreatedAt: at("2026-07-05T00:00:00Z")},
	})

	board := func(state, closedAt, status string) {
		t.Helper()
		closed := ""
		if closedAt != "" {
			closed = `,"closedAt":"` + closedAt + `"`
		}
		item := `{"id":"PVTI_1","createdAt":"2026-07-02T00:00:00Z",
	"content":{"__typename":"Issue","id":"I_1","number":9,"title":"T","state":"` + state + `",
	"createdAt":"2026-07-02T00:00:00Z","updatedAt":"2026-07-06T00:00:00Z"` + closed + `,
	"author":{"login":"` + login + `"},"repository":{"nameWithOwner":"acme/api"},"labels":{"nodes":[]},"assignees":{"nodes":[]}},
	"fieldValues":{"nodes":[{"__typename":"ProjectV2ItemFieldSingleSelectValue","name":"` + status +
			`","updatedAt":"2026-07-07T00:00:00Z","field":{"name":"Status"}}]}}`
		var payload gitHubProjectV2ItemPayload
		if err := json.Unmarshal([]byte(item), &payload); err != nil {
			t.Fatal(err)
		}
		row, kept, err := normalizeGitHubProjectV2Item(claim, payload, "ghprojv2:acme/1", nil, normalizedAt)
		if err != nil || !kept {
			t.Fatalf("board item kept=%v err=%v", kept, err)
		}
		event, hasEvent, _, err := githubProjectV2StatusEvent(claim, payload, row.WorkItemID, normalizedAt)
		if err != nil || !hasEvent {
			t.Fatalf("board %q: hasEvent=%v err=%v", status, hasEvent, err)
		}
		rows = append(rows, produced{source: "projects v2 board", row: event})
	}
	board("open", "", "In Progress")
	board("open", "", "Synthetic Column")
	board("closed", "2026-07-05T00:00:00Z", "Synthetic Column")

	wantKinds := map[string]bool{
		"issue timeline/in progress": false, "issue timeline/unlabeled:in progress": false,
		"issue timeline/closed": false, "issue timeline/reopened": false,
		"pull request timeline/closed": false, "pull request timeline/reopened": false,
		"pull request timeline/merged":  false,
		"projects v2 board/In Progress": false, "projects v2 board/Synthetic Column": false,
	}
	for _, produced := range rows {
		row := produced.row
		kind := produced.source + "/" + derefString(row.ToStatusRaw)
		if _, known := wantKinds[kind]; known {
			wantKinds[kind] = true
		}
		if derefString(row.FromStatusRaw) != "" || derefString(row.Actor) != "" {
			t.Errorf("%s at %s writes from_status_raw=%q actor=%q: the settle stored read drops both, so "+
				"transitionsAfterWrite would collapse events the readers count as two; select them in "+
				"gitHubProjectV2StoredTransitionsQuery first", kind, row.OccurredAt.Format(time.RFC3339),
				derefString(row.FromStatusRaw), derefString(row.Actor))
		}
	}
	for kind, seen := range wantKinds {
		if !seen {
			t.Errorf("no %s transition produced: the pin no longer covers that kind", kind)
		}
	}
}
