package providersync

import (
	"encoding/json"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func tieOrdinalsByContent(rows []workItemTransitionStoredRow) map[string]uint16 {
	ordinals := make(map[string]uint16, len(rows))
	for _, row := range rankTransitionTies(rows) {
		ordinals[transitionInstantKey(row)+workItemKeySeparator+transitionTieOrder(row)] = row.TieOrdinal
	}
	return ordinals
}

func reversedRows[T any](rows []T) []T {
	out := slices.Clone(rows)
	slices.Reverse(out)
	return out
}

func storedTieRow(item string, at time.Time, from, to, toRaw string, synced time.Time) workItemTransitionStoredRow {
	return workItemTransitionStoredRow{
		WorkItemID: item, OccurredAt: at, FromStatus: from, ToStatus: to, ToStatusRaw: toRaw,
		OrgID: "org-acme", LastSynced: synced,
	}
}

func TestRankTransitionTiesIsIndependentOfBatchOrder(t *testing.T) {
	tie := time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	rows := []workItemTransitionStoredRow{
		storedTieRow("gh:acme/api#1", tie.Add(-time.Hour), "unknown", "done", "closed", synced),
		storedTieRow("gh:acme/api#1", tie, "done", "todo", "reopened", synced),
		storedTieRow("gh:acme/api#1", tie, "todo", "in_progress", "in progress", synced),
		storedTieRow("gh:acme/api#1", tie, "in_progress", "blocked", "blocked", synced),
	}
	want := tieOrdinalsByContent(rows)
	for name, batch := range map[string][]workItemTransitionStoredRow{
		"reversed": reversedRows(rows),
		"rotated":  append(slices.Clone(rows[2:]), rows[:2]...),
	} {
		for run := 0; run < 20; run++ {
			if got := tieOrdinalsByContent(batch); !mapsEqual(got, want) {
				t.Fatalf("%s batch, run %d: ordinals %v, want %v (same events, other order)", name, run, got, want)
			}
		}
	}
	seen := map[uint16]int{}
	for key, ordinal := range want {
		if key[len(key)-len("done"+workItemKeySeparator+"unknown"):] == "done"+workItemKeySeparator+"unknown" {
			if ordinal != 0 {
				t.Fatalf("the only transition of its instant has ordinal %d, want 0", ordinal)
			}
			continue
		}
		seen[ordinal]++
	}
	if len(seen) != 3 || seen[0] != 1 || seen[1] != 1 || seen[2] != 1 {
		t.Fatalf("tie ordinals %v, want 0, 1 and 2 once each", seen)
	}
}

func mapsEqual(left, right map[string]uint16) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if other, ok := right[key]; !ok || other != value {
			return false
		}
	}
	return true
}

func TestRankTransitionTiesKeepsTheLastCopyOfOneTransition(t *testing.T) {
	tie := time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)
	older := storedTieRow("linear:ENG-1", tie, "todo", "in_progress", "In Progress", time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	newer := older
	newer.LastSynced = time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	ranked := rankTransitionTies([]workItemTransitionStoredRow{older, newer})
	if len(ranked) != 1 || !ranked[0].LastSynced.Equal(newer.LastSynced) || ranked[0].TieOrdinal != 0 {
		t.Fatalf("ranked %+v, want the newer copy alone at ordinal 0", ranked)
	}
}

// Each provider's normalizer, run twice over the same source events (the second time with the events in the order a
// re-fetch may serve them, where the endpoint allows another order), gives every transition the same ordinal.
// GitHub (issue events) and GitLab (resource label/state events) take no sort parameter and serve one order, so their
// re-fetch is the same order; Jira histories and Linear history entries are ranked from each entry's own statuses,
// so their reversed order is checked too. A Projects v2 item has one Status event.
func TestTieOrdinalIsStablePerProviderAcrossResyncs(t *testing.T) {
	mapping, err := LoadStatusMapping(filepath.Join("..", "..", "src", "dev_health_ops", "config", "status_mapping.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	first := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	second := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	at := func(value string) *string { return &value }
	ordinals := func(t *testing.T, rows []workItemTransitionStoredRow) map[string]uint16 {
		t.Helper()
		for index := range rows {
			rows[index].LastSynced = time.Time{}
		}
		return tieOrdinalsByContent(rows)
	}
	same := func(t *testing.T, name string, left, right map[string]uint16, ties int) {
		t.Helper()
		if !mapsEqual(left, right) {
			t.Fatalf("%s: ordinals %v then %v", name, left, right)
		}
		maxOrdinal := uint16(0)
		for _, ordinal := range left {
			maxOrdinal = max(maxOrdinal, ordinal)
		}
		if int(maxOrdinal) != ties-1 {
			t.Fatalf("%s: highest ordinal %d, want %d (%v)", name, maxOrdinal, ties-1, left)
		}
	}

	t.Run("github", func(t *testing.T) {
		var events []json.RawMessage
		for _, event := range []githubWorkItemEventPayload{
			{Event: "closed", CreatedAt: at("2026-03-01T10:00:00Z")},
			{Event: "reopened", CreatedAt: at("2026-03-02T09:00:00Z")},
			{Event: "labeled", CreatedAt: at("2026-03-02T09:00:00Z"), Label: &githubWorkItemLabelPayload{Name: "in progress"}},
		} {
			raw, _ := json.Marshal(event)
			events = append(events, raw)
		}
		run := func(normalizedAt time.Time) map[string]uint16 {
			rows, _, err := normalizeGitHubWorkItemEvents(nativeTestClaim("github", "work-items"), "gh:acme/api#1", false, events, created, nil, normalizedAt)
			if err != nil {
				t.Fatal(err)
			}
			return ordinals(t, projectRows(rows, projectWorkItemTransition))
		}
		same(t, "github", run(first), run(second), 2)
	})
	t.Run("gitlab", func(t *testing.T) {
		run := func(normalizedAt time.Time) map[string]uint16 {
			rows, _ := normalizeGitLabIssueTransitions(nativeTestClaim("gitlab", "work-items"), "gitlab:acme/api#1",
				[]gitlabLabelEventPayload{
					{Action: "add", CreatedAt: at("2026-03-02T09:00:00Z"), LabelName: "doing"},
					{Action: "add", CreatedAt: at("2026-03-02T09:00:00Z"), LabelName: "blocked"},
				}, []gitlabStateEventPayload{}, "opened", created, mapping, nil, normalizedAt)
			return ordinals(t, projectRows(rows, projectWorkItemTransition))
		}
		same(t, "gitlab", run(first), run(second), 2)
	})
	t.Run("jira", func(t *testing.T) {
		histories := []any{
			map[string]any{"id": "10001", "created": "2026-03-02T09:00:00.000+0000", "items": []any{
				map[string]any{"field": "status", "fromString": "To Do", "toString": "In Progress"}}},
			map[string]any{"id": "10002", "created": "2026-03-02T09:00:00.000+0000", "items": []any{
				map[string]any{"field": "status", "fromString": "In Progress", "toString": "Done"}}},
		}
		run := func(served []any, normalizedAt time.Time) map[string]uint16 {
			rows := normalizeJiraTransitions(nativeTestClaim("jira", "work-items"), "OPS-1", map[string]any{"histories": served},
				nil, created, mapping, nil, false, false, normalizedAt)
			return ordinals(t, projectRows(rows, projectWorkItemTransition))
		}
		same(t, "jira", run(histories, first), run(reversedRows(histories), second), 2)
	})
	t.Run("linear", func(t *testing.T) {
		entries := []linearHistoryEntry{
			{ID: "h1", CreatedAt: "2026-03-02T09:00:00.000Z", FromState: &linearStatePayload{Name: "Todo", Type: "unstarted"},
				ToState: &linearStatePayload{Name: "In Progress", Type: "started"}},
			{ID: "h2", CreatedAt: "2026-03-02T09:00:00.000Z", FromState: &linearStatePayload{Name: "In Progress", Type: "started"},
				ToState: &linearStatePayload{Name: "Done", Type: "completed"}},
		}
		run := func(served []linearHistoryEntry, normalizedAt time.Time) map[string]uint16 {
			rows := normalizeLinearTransitions(nativeTestClaim("linear", "work-items"), "linear:ENG-1", served, normalizedAt)
			return ordinals(t, projectRows(rows, projectLinearTransition))
		}
		same(t, "linear", run(entries, first), run(reversedRows(entries), second), 2)
	})
	t.Run("projects v2", func(t *testing.T) {
		column, set := "In Progress", "2026-03-02T09:00:00Z"
		run := func(normalizedAt time.Time) map[string]uint16 {
			var item gitHubProjectV2ItemPayload
			value := gitHubProjectV2FieldValuePayload{Typename: "ProjectV2ItemFieldSingleSelectValue", Name: &column, UpdatedAt: &set}
			value.Field.Name = "Status"
			item.FieldValues.Nodes = []gitHubProjectV2FieldValuePayload{value}
			event, ok, _, err := githubProjectV2StatusEvent(nativeTestClaim("github", "work-items"), item, "gh:acme/api#1", normalizedAt)
			if err != nil || !ok {
				t.Fatalf("board event: ok=%v err=%v", ok, err)
			}
			return ordinals(t, projectRows([]githubWorkItemTransitionRow{event}, projectWorkItemTransition))
		}
		same(t, "projects v2", run(first), run(second), 1)
	})
}
