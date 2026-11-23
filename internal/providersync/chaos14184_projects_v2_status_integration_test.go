//go:build integration

package providersync

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// I1: the route's board merge step on a real ClickHouse, the stored history
// read through the effect connection. Two syncs of one board item: run A has
// the repository close in its REST window, run B does not (the issue was not
// updated in its window) and carries the board row alone. In both run orders
// the stored status, read the way the daily state family reads it, is the last
// stored transition in the shared order.
func TestChaos14184StoredStatusMatchesLastStoredTransitionInEitherRunOrder(t *testing.T) {
	item := chaos14184Item("CLOSED", "2026-10-03T00:00:00Z", "In Progress", "2026-10-02T00:00:00Z")
	type run func(at time.Time) (githubWorkItemRows, githubWorkItemRows)
	runA := func(at time.Time) (githubWorkItemRows, githubWorkItemRows) {
		return chaos14184RepositoryAt(t, at, chaos14184ClosedIssue, chaos14184CloseEvent), chaos14184Board(t, item, at)
	}
	runB := func(at time.Time) (githubWorkItemRows, githubWorkItemRows) {
		return chaos14184EmptyRows(), chaos14184Board(t, item, at)
	}
	first := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	second := first.Add(time.Hour)
	statuses := map[string]string{}
	for _, order := range []struct {
		name string
		runs []run
	}{
		{"A_then_B", []run{runA, runB}},
		{"B_then_A", []run{runB, runA}},
	} {
		t.Run(order.name, func(t *testing.T) {
			ctx, conn := newWorkItemEffectsConn(t)
			orgID := workItemTestOrgID(t)
			reader := GitHubProjectV2StoredTransitionsClickHouseReader{Conn: conn}
			for index, step := range order.runs {
				repository, board := step([]time.Time{first, second}[index])
				rows, err := mergeGitHubProjectV2RowsWithStored(ctx, reader, orgID, repository, board)
				if err != nil {
					t.Fatal(err)
				}
				chaos14184Write(t, conn, rows)
			}
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
			ordered := make([]workitemmetrics.Transition, 0, len(transitions))
			for _, transition := range transitions {
				ordered = append(ordered, workitemmetrics.Transition{
					WorkItemID: transition.WorkItemID, OccurredAt: transition.OccurredAt,
					FromStatus: transition.FromStatus, ToStatus: transition.ToStatus,
				})
			}
			ordered = workitemmetrics.OrderTransitions(ordered)
			last := ordered[len(ordered)-1]
			if last.ToStatus != items[0].Status || items[0].Status != "done" {
				t.Fatalf("stored status=%q but last stored transition (%s) to_status=%q, want done",
					items[0].Status, last.OccurredAt.Format(time.RFC3339), last.ToStatus)
			}
			statuses[order.name] = items[0].Status
		})
	}
	if statuses["A_then_B"] != statuses["B_then_A"] {
		t.Fatalf("run order changes the status: %v", statuses)
	}
}

// The stored read on a real ClickHouse: one row per distinct event of the
// asked items in the asked org. Re-synced copies collapse; other items and
// other orgs are not read.
func TestChaos14184StoredTransitionsReadsDistinctEventsOfTheAskedItems(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := workItemTestOrgID(t)
	at := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	done, progress := "Done", "In Progress"
	later := at.Add(time.Minute)
	event := func(org, id string, occurred time.Time, from, to string, raw *string, synced time.Time) githubWorkItemTransitionRow {
		return githubWorkItemTransitionRow{
			WorkItemID: id, Provider: "github", OccurredAt: occurred, ToStatusRaw: raw,
			FromStatus: from, ToStatus: to, OrgID: org, LastSynced: synced,
		}
	}
	synced := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	writes := [][]githubWorkItemTransitionRow{
		{event(orgID, "gh:acme/api#9", at, "unknown", "done", &done, synced)},
		// A re-sync copy of the same event.
		{event(orgID, "gh:acme/api#9", at, "unknown", "done", &done, synced.Add(time.Hour))},
		{event(orgID, "gh:acme/api#9", later, "done", "in_progress", &progress, synced)},
		// Not asked: another item.
		{event(orgID, "gh:acme/api#10", at, "unknown", "done", &done, synced)},
	}
	for _, rows := range writes {
		chaos14184Write(t, conn, githubWorkItemRows{StatusTransitions: rows})
	}
	chaos14184WriteOtherOrg(t, conn, event("org-other", "gh:acme/api#9", at, "unknown", "canceled", nil, synced))

	got, err := GitHubProjectV2StoredTransitionsClickHouseReader{Conn: conn}.StoredTransitions(ctx, orgID, []string{"gh:acme/api#9"})
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(got, func(i, j int) bool { return got[i].OccurredAt.Before(got[j].OccurredAt) })
	want := []githubWorkItemTransitionRow{
		{WorkItemID: "gh:acme/api#9", OccurredAt: at, FromStatus: "unknown", ToStatus: "done", ToStatusRaw: &done, storedTieOrdinals: []uint32{0}},
		{WorkItemID: "gh:acme/api#9", OccurredAt: later, FromStatus: "done", ToStatus: "in_progress", ToStatusRaw: &progress, storedTieOrdinals: []uint32{0}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stored=%+v\nwant=%+v", got, want)
	}
	if _, err := (GitHubProjectV2StoredTransitionsClickHouseReader{}).StoredTransitions(ctx, orgID, []string{"x"}); err == nil {
		t.Fatal("a reader with no connection read")
	}
}

func chaos14184WriteOtherOrg(t *testing.T, conn driver.Conn, row githubWorkItemTransitionRow) {
	t.Helper()
	identity, effect := workItemEffect(t, "work_item_transitions", row)
	identity.OrgID = row.OrgID
	if err := (GitHubWorkItemTransitionsClickHouseAdapter{Conn: conn}).WriteGitHubWorkItemEffect(t.Context(), identity, effect); err != nil {
		t.Fatalf("write other org: %v", err)
	}
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
