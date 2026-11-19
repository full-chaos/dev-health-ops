//go:build integration

package providersync

import (
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// The stored form of the Projects v2 merge, read the way the daily state
// family reads it. Two syncs of one board item: run A has the repository
// close in its REST window, run B does not (the issue was not updated in its
// window) and carries the board row alone. The stored transitions are the
// union of every run; the stored status is the newest row. Both run orders
// must leave a stored status equal to the last stored transition.
func TestChaos14184RedStoredStatusMatchesLastStoredTransitionInEitherRunOrder(t *testing.T) {
	runA := func(at time.Time) githubWorkItemRows {
		repository := chaos14184RepositoryAt(t, at, chaos14184ClosedIssue,
			`{"event":"closed","created_at":"2026-10-03T00:00:00Z"}`)
		return mergeGitHubProjectV2Rows(repository, chaos14184Board(t, chaos14184BoardInProgressClosedIssue, at))
	}
	runB := func(at time.Time) githubWorkItemRows {
		return mergeGitHubProjectV2Rows(chaos14184EmptyRows(), chaos14184Board(t, chaos14184BoardInProgressClosedIssue, at))
	}
	first := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	second := first.Add(time.Hour)
	for _, order := range []struct {
		name string
		runs []githubWorkItemRows
	}{
		{"A_then_B", []githubWorkItemRows{runA(first), runB(second)}},
		{"B_then_A", []githubWorkItemRows{runB(first), runA(second)}},
	} {
		t.Run(order.name, func(t *testing.T) {
			ctx, conn := newWorkItemEffectsConn(t)
			for _, rows := range order.runs {
				chaos14184Write(t, conn, rows)
			}
			orgID := workItemTestOrgID(t)
			start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
			end := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
			items, err := daily.LoadWorkItemStateWorkItems(ctx, conn, orgID, uuid.Nil, start, end)
			if err != nil || len(items) != 1 {
				t.Fatalf("items=%d err=%v", len(items), err)
			}
			transitions, err := daily.LoadWorkItemStateTransitions(ctx, conn, orgID, uuid.Nil, end)
			if err != nil || len(transitions) == 0 {
				t.Fatalf("transitions=%d err=%v", len(transitions), err)
			}
			sort.SliceStable(transitions, func(i, j int) bool { return transitions[i].OccurredAt.Before(transitions[j].OccurredAt) })
			last := transitions[len(transitions)-1]
			if last.ToStatus != items[0].Status {
				t.Fatalf("stored status=%q but last stored transition (%s) to_status=%q",
					items[0].Status, last.OccurredAt.Format(time.RFC3339), last.ToStatus)
			}
		})
	}
}

func chaos14184EmptyRows() githubWorkItemRows {
	return githubWorkItemRows{WorkItems: []githubWorkItemRow{}, StatusTransitions: []githubWorkItemTransitionRow{}}
}

func chaos14184Write(t *testing.T, conn driver.Conn, rows githubWorkItemRows) {
	t.Helper()
	ctx := t.Context()
	items := make([]any, 0, len(rows.WorkItems))
	for _, row := range rows.WorkItems {
		items = append(items, row)
	}
	transitions := make([]any, 0, len(rows.StatusTransitions))
	for _, row := range rows.StatusTransitions {
		transitions = append(transitions, row)
	}
	for _, write := range []struct {
		destination string
		adapter     GitHubWorkItemEffectAdapter
		rows        []any
	}{
		{"work_items", GitHubWorkItemsClickHouseAdapter{Conn: conn}, items},
		{"work_item_transitions", GitHubWorkItemTransitionsClickHouseAdapter{Conn: conn}, transitions},
	} {
		if len(write.rows) == 0 {
			continue
		}
		identity, effect := workItemEffect(t, write.destination, write.rows...)
		if err := write.adapter.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
			t.Fatalf("write %s: %v", write.destination, err)
		}
	}
}
