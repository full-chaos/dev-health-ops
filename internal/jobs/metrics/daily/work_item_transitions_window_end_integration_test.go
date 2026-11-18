//go:build integration

package daily

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestTransitionReadsKeepTheLastMillisecondsBeforeTheWindowEnd reads the transitions of one day with both readers that
// bound occurred_at (a sort-key column of work_item_transitions) strictly below the window end. The end bound must keep
// the DateTime64 precision: a transition 500 ms before the end is read, one at the end or after it is not.
func TestTransitionReadsKeepTheLastMillisecondsBeforeTheWindowEnd(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	orgID := "org-window-end-" + uuid.NewString()
	repoID := uuid.New()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	end := day.AddDate(0, 0, 1)
	seedWorkItemEngineItem(t, ctx, conn, orgID, repoID, "gh:acme/api#1", "quality", day.AddDate(0, 0, -2), nil, nil, nil)

	insert := func(status string, occurredAt time.Time) {
		t.Helper()
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_transitions (
			repo_id, work_item_id, occurred_at, provider, from_status, to_status,
			from_status_raw, to_status_raw, actor, org_id, last_synced)`)
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Append(uuid.Nil, "gh:acme/api#1", occurredAt, "", "todo", status, "todo", status, "actor", orgID, day); err != nil {
			t.Fatal(err)
		}
		if err := batch.Send(); err != nil {
			t.Fatal(err)
		}
	}
	insert("early", day.Add(9*time.Hour))
	insert("last_second", end.Add(-500*time.Millisecond))
	insert("last_millisecond", end.Add(-time.Millisecond))
	insert("at_end", end)
	insert("after_end", end.Add(500*time.Millisecond))

	want := "early last_millisecond last_second"
	stateTransitions, err := LoadWorkItemStateTransitions(ctx, conn, orgID, repoID, end)
	if err != nil {
		t.Fatal(err)
	}
	scopeTransitions, err := loadWorkItemScopeTransitions(ctx, conn, orgID, end, nil, false, false)
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string][]workItemStateTransition{"LoadWorkItemStateTransitions": stateTransitions, "loadWorkItemScopeTransitions": scopeTransitions} {
		to := make([]string, 0, len(got))
		for _, transition := range got {
			to = append(to, transition.ToStatus)
		}
		sort.Strings(to)
		if joined := strings.Join(to, " "); joined != want {
			t.Errorf("%s read %q, want %q", name, joined, want)
		}
	}
}
