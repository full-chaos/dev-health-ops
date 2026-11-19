//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type tieWriter struct {
	write   func(ctx context.Context, t *testing.T) error
	inspect func(ctx context.Context, t *testing.T) (EffectInspection, error)
}

func githubFamilyTieWriter(conn driver.Conn, rows []githubWorkItemTransitionRow) tieWriter {
	adapter := GitHubWorkItemTransitionsClickHouseAdapter{Conn: conn}
	values := func() []any {
		out := make([]any, 0, len(rows))
		for _, row := range rows {
			out = append(out, row)
		}
		return out
	}
	return tieWriter{
		write: func(ctx context.Context, t *testing.T) error {
			identity, effect := workItemEffect(t, "work_item_transitions", values()...)
			return adapter.WriteGitHubWorkItemEffect(ctx, identity, effect)
		},
		inspect: func(ctx context.Context, t *testing.T) (EffectInspection, error) {
			identity, effect := workItemEffect(t, "work_item_transitions", values()...)
			return adapter.InspectGitHubWorkItemEffect(ctx, identity, effect)
		},
	}
}

func linearTieWriter(conn driver.Conn, rows []linearWorkItemTransitionRow) tieWriter {
	adapter := LinearWorkItemTransitionsClickHouseAdapter{Conn: conn}
	effectOf := func(t *testing.T) (LinearWorkItemEffectIdentity, EffectBatch) {
		t.Helper()
		encoded := make([]json.RawMessage, 0, len(rows))
		for _, row := range rows {
			raw, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			encoded = append(encoded, raw)
		}
		effect, err := BuildEffectBatch("work_item_transitions", EffectReadbackRequired, encoded)
		if err != nil {
			t.Fatal(err)
		}
		identity, err := newLinearWorkItemEffectIdentity(nativeTestClaim("linear", "work-items"), effect)
		if err != nil {
			t.Fatal(err)
		}
		return identity, effect
	}
	return tieWriter{
		write: func(ctx context.Context, t *testing.T) error {
			identity, effect := effectOf(t)
			return adapter.WriteLinearWorkItemEffect(ctx, identity, effect)
		},
		inspect: func(ctx context.Context, t *testing.T) (EffectInspection, error) {
			identity, effect := effectOf(t)
			return adapter.InspectLinearWorkItemEffect(ctx, identity, effect)
		},
	}
}

func storedTransitionsAfterMerge(ctx context.Context, t *testing.T, conn driver.Conn, orgID, workItemID string) uint64 {
	t.Helper()
	if err := conn.Exec(ctx, `OPTIMIZE TABLE work_item_transitions FINAL`); err != nil {
		t.Fatal(err)
	}
	var stored uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM work_item_transitions WHERE org_id = ? AND work_item_id = ?`,
		orgID, workItemID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	return stored
}

// tieInstantCases are the provider shapes in which two status changes of one item share an occurred_at, each through
// its real normalizer: every transition must be stored, survive a merge, and read back Exact.
func tieInstantCases(t *testing.T, conn driver.Conn) []struct {
	name, orgID, workItemID string
	want                    uint64
	writer                  tieWriter
} {
	t.Helper()
	mapping, err := LoadStatusMapping(filepath.Join("..", "..", "src", "dev_health_ops", "config", "status_mapping.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	normalizedAt := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	at := func(value string) *string { return &value }

	var events []json.RawMessage
	for _, event := range []githubWorkItemEventPayload{
		{Event: "closed", CreatedAt: at("2026-03-01T10:00:00Z")},
		{Event: "reopened", CreatedAt: at("2026-03-02T09:00:00Z")},
		{Event: "labeled", CreatedAt: at("2026-03-02T09:00:00Z"), Label: &githubWorkItemLabelPayload{Name: "in progress"}},
	} {
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, raw)
	}
	github, _, err := normalizeGitHubWorkItemEvents(nativeTestClaim("github", "work-items"), "gh:acme/api#14190", false,
		events, created, nil, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}

	gitlab, _ := normalizeGitLabIssueTransitions(nativeTestClaim("gitlab", "work-items"), "gitlab:acme/api#14190",
		[]gitlabLabelEventPayload{
			{Action: "add", CreatedAt: at("2026-03-02T09:00:00Z"), LabelName: "doing"},
			{Action: "add", CreatedAt: at("2026-03-02T09:00:00Z"), LabelName: "blocked"},
		},
		[]gitlabStateEventPayload{}, "opened", created, mapping, nil, normalizedAt)

	jira := normalizeJiraTransitions(nativeTestClaim("jira", "work-items"), "OPS-14190", map[string]any{"histories": []any{
		map[string]any{"id": "10001", "created": "2026-03-02T09:00:00.000+0000", "items": []any{
			map[string]any{"field": "status", "fromString": "To Do", "toString": "In Progress"}}},
		map[string]any{"id": "10002", "created": "2026-03-02T09:00:00.000+0000", "items": []any{
			map[string]any{"field": "status", "fromString": "In Progress", "toString": "Done"}}},
	}}, nil, created, mapping, nil, false, false, normalizedAt)

	linear := normalizeLinearTransitions(nativeTestClaim("linear", "work-items"), "linear:ENG-14190", []linearHistoryEntry{
		{ID: "h1", CreatedAt: "2026-03-02T09:00:00.000Z", FromState: &linearStatePayload{Name: "Todo", Type: "unstarted"},
			ToState: &linearStatePayload{Name: "In Progress", Type: "started"}},
		{ID: "h2", CreatedAt: "2026-03-02T09:00:00.000Z", FromState: &linearStatePayload{Name: "In Progress", Type: "started"},
			ToState: &linearStatePayload{Name: "Done", Type: "completed"}},
	}, normalizedAt)

	for name, got := range map[string][2]int{"github": {len(github), 3}, "gitlab": {len(gitlab), 2}, "jira": {len(jira), 2}, "linear": {len(linear), 2}} {
		if got[0] != got[1] {
			t.Fatalf("%s normalizer: %d transitions, want %d", name, got[0], got[1])
		}
	}
	githubOrg := nativeTestClaim("github", "work-items").OrgID
	return []struct {
		name, orgID, workItemID string
		want                    uint64
		writer                  tieWriter
	}{
		{"github reopen and label in one second", githubOrg, "gh:acme/api#14190", 3, githubFamilyTieWriter(conn, github)},
		{"gitlab two status labels in one second", githubOrg, "gitlab:acme/api#14190", 2, githubFamilyTieWriter(conn, gitlab)},
		{"jira two changelog histories at one time", githubOrg, "jira:OPS-14190", 2, githubFamilyTieWriter(conn, jira)},
		{"linear two history entries at one time", nativeTestClaim("linear", "work-items").OrgID, "linear:ENG-14190", 2,
			linearTieWriter(conn, linear)},
	}
}

func TestTransitionWritersKeepEveryTransitionOfATieInstant(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, c := range tieInstantCases(t, conn) {
		t.Run(c.name, func(t *testing.T) {
			if err := c.writer.write(ctx, t); err != nil {
				t.Fatal(err)
			}
			if stored := storedTransitionsAfterMerge(ctx, t, conn, c.orgID, c.workItemID); stored != c.want {
				t.Fatalf("after OPTIMIZE FINAL: %d rows, want %d", stored, c.want)
			}
			if inspection, err := c.writer.inspect(ctx, t); err != nil || inspection != EffectExact {
				t.Fatalf("inspection=%s err=%v, want exact", inspection, err)
			}
		})
	}
}
