//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// boardTieEvent is a Projects v2 board Status event as the board fetch emits it: from githubProjectV2StatusEvent,
// with its board's slot.
func boardTieEvent(t *testing.T, item, projectScopeID, column, set string, normalizedAt time.Time) githubWorkItemTransitionRow {
	t.Helper()
	var payload gitHubProjectV2ItemPayload
	value := gitHubProjectV2FieldValuePayload{Typename: "ProjectV2ItemFieldSingleSelectValue", Name: &column, UpdatedAt: &set}
	value.Field.Name = "Status"
	payload.FieldValues.Nodes = []gitHubProjectV2FieldValuePayload{value}
	event, ok, _, err := githubProjectV2StatusEvent(nativeTestClaim("github", "work-items"), payload, item, normalizedAt)
	if err != nil || !ok {
		t.Fatalf("board event: ok=%v err=%v", ok, err)
	}
	event.BoardSlot = int(boardTieSlot(projectScopeID))
	return event
}

func reopenInOneSecond(t *testing.T, item string, normalizedAt time.Time) []githubWorkItemTransitionRow {
	t.Helper()
	at := func(value string) *string { return &value }
	var events []json.RawMessage
	for _, event := range []githubWorkItemEventPayload{
		{Event: "closed", CreatedAt: at("2026-03-01T10:00:00Z")},
		{Event: "reopened", CreatedAt: at("2026-03-02T09:00:00Z")},
	} {
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, raw)
	}
	rows, _, err := normalizeGitHubWorkItemEvents(nativeTestClaim("github", "work-items"), item, false, events,
		time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), nil, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// D1 (C-A): a board-only run after a mixed run. Run 1 holds the repo reopen and the board's Status event (an emoji
// column, which sorts after "reopened") in one second; run 2 holds the board event alone, as every run does for a
// board item whose issue is outside the window. Both rows must stay, the reopen included.
func TestBoardOnlyRunKeepsTheRepoTieRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := nativeTestClaim("github", "work-items").OrgID
	item := "gh:acme/api#6553"
	run1At, run2At := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	board := func(at time.Time) githubWorkItemTransitionRow {
		return boardTieEvent(t, item, "ghprojv2:acme#3", "\U0001F3D7 In progress", "2026-03-02T09:00:00Z", at)
	}
	run1 := append(reopenInOneSecond(t, item, run1At), board(run1At))
	if err := githubFamilyTieWriter(conn, run1).write(ctx, t); err != nil {
		t.Fatal(err)
	}
	run2 := githubFamilyTieWriter(conn, []githubWorkItemTransitionRow{board(run2At)})
	if err := run2.write(ctx, t); err != nil {
		t.Fatal(err)
	}
	got := boardCellRows(ctx, t, conn, orgID, item)
	want := []string{"done->todo(reopened)", "unknown->unknown(\U0001F3D7 In progress)"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("after the board-only run: %v, want %v", got, want)
	}
	if inspection, err := run2.inspect(ctx, t); err != nil || inspection != EffectExact {
		t.Fatalf("inspection of the board-only run=%s err=%v, want exact", inspection, err)
	}
}

// C-B: one item on boards A and B, both Status values set in one ms, and a repo reopen in that second (run 1). Run 2
// is degraded: board A's items are missing and board B's event comes alone. Board A's value ("Blocked") sorts before
// the reopen and board B's (an emoji column) after it, so a rank over the run's own rows would move B onto A. All 3
// rows must stay.
func TestDegradedBoardRunKeepsTheOtherBoardsRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := nativeTestClaim("github", "work-items").OrgID
	item := "gh:acme/api#6554"
	run1At, run2At := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	boardA := func(at time.Time) githubWorkItemTransitionRow {
		return boardTieEvent(t, item, "ghprojv2:acme#3", "Blocked", "2026-03-02T09:00:00Z", at)
	}
	boardB := func(at time.Time) githubWorkItemTransitionRow {
		return boardTieEvent(t, item, "ghprojv2:acme#4", "\U0001F3D7 In progress", "2026-03-02T09:00:00Z", at)
	}
	run1 := append(reopenInOneSecond(t, item, run1At), boardA(run1At), boardB(run1At))
	if err := githubFamilyTieWriter(conn, run1).write(ctx, t); err != nil {
		t.Fatal(err)
	}
	run2 := githubFamilyTieWriter(conn, []githubWorkItemTransitionRow{boardB(run2At)})
	if err := run2.write(ctx, t); err != nil {
		t.Fatal(err)
	}
	got := boardCellRows(ctx, t, conn, orgID, item)
	want := []string{"done->todo(reopened)", "unknown->blocked(Blocked)", "unknown->unknown(\U0001F3D7 In progress)"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("after the degraded run: %v, want %v", got, want)
	}
	if inspection, err := run2.inspect(ctx, t); err != nil || inspection != EffectExact {
		t.Fatalf("inspection of the degraded run=%s err=%v, want exact", inspection, err)
	}
}

// MW-9 cell 3: two timeline events at one instant with ONE normalized pair and a different raw value and actor are two
// transitions; both are stored.
func TestTieOfOneNormalizedPairWithDifferentRawKeepsBoth(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := nativeTestClaim("github", "work-items").OrgID
	item := "gh:acme/api#6555"
	at := time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	doing, wip, alice, bob := "doing", "wip", "alice", "bob"
	rows := []githubWorkItemTransitionRow{
		{WorkItemID: item, Provider: "github", OccurredAt: at, ToStatusRaw: &doing, FromStatus: "todo", ToStatus: "in_progress", Actor: &alice, OrgID: orgID, LastSynced: synced},
		{WorkItemID: item, Provider: "github", OccurredAt: at, ToStatusRaw: &wip, FromStatus: "todo", ToStatus: "in_progress", Actor: &bob, OrgID: orgID, LastSynced: synced},
	}
	writer := githubFamilyTieWriter(conn, rows)
	if err := writer.write(ctx, t); err != nil {
		t.Fatal(err)
	}
	if got := boardCellRows(ctx, t, conn, orgID, item); len(got) != 2 {
		t.Fatalf("stored %v, want 2 rows", got)
	}
	if inspection, err := writer.inspect(ctx, t); err != nil || inspection != EffectExact {
		t.Fatalf("inspection=%s err=%v, want exact", inspection, err)
	}
}

// G3, case (b): two items of one board resolve to one work item (as the repository-identity rename can make them),
// with different Status values set in one ms. The board keeps ONE of them, the same item its work_items row comes from:
// one board row stored, the read-back Exact, and its value is the row's status.
func TestBoardItemsOfOneWorkItemWriteOneBoardRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	item := func(id, status string) string {
		return `{"id":"` + id + `","content":{"__typename":"Issue","number":5,"title":"t","state":"OPEN","createdAt":"2026-08-01T08:00:00Z","updatedAt":"2026-08-02T08:00:00Z","repository":{"nameWithOwner":"acme/api"},"labels":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"assignees":{"nodes":[]}},` +
			`"fieldValues":{"nodes":[{"__typename":"ProjectV2ItemFieldSingleSelectValue","name":"` + status + `","updatedAt":"2026-08-03T09:30:00Z","field":{"name":"Status"}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}`
	}
	doer := &gitHubProjectV2Doer{t: t, replies: []string{
		`{"data":{"organization":{"projectV2":{"items":{"nodes":[` + item("PVTI_A", "Todo") + `,` + item("PVTI_B", "Done") +
			`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
	}}
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{map[string]any{"org_login": "acme", "project_number": 3}}}
	result, err := (GitHubProjectV2Fetcher{}).Fetch(
		context.Background(), claim, providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		githubProjectV2TestClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows.WorkItems) != 1 {
		t.Fatalf("work items %+v, want one", result.Rows.WorkItems)
	}
	orgID := nativeTestClaim("github", "work-items").OrgID
	events := append([]githubWorkItemTransitionRow{}, result.Rows.StatusTransitions...)
	for index := range events {
		events[index].OrgID = orgID
	}
	writer := githubFamilyTieWriter(conn, events)
	if err := writer.write(ctx, t); err != nil {
		t.Fatal(err)
	}
	got := boardCellRows(ctx, t, conn, orgID, "gh:acme/api#5")
	want := []string{"unknown->" + result.Rows.WorkItems[0].Status + "(" + derefString(result.Rows.WorkItems[0].StatusRaw) + ")"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("stored %v, want %v (the board row of the item the work_items row comes from)", got, want)
	}
	if inspection, err := writer.inspect(ctx, t); err != nil || inspection != EffectExact {
		t.Fatalf("inspection=%s err=%v, want exact", inspection, err)
	}
}

func boardCellRows(ctx context.Context, t *testing.T, conn driver.Conn, orgID, item string) []string {
	t.Helper()
	if err := conn.Exec(ctx, `OPTIMIZE TABLE work_item_transitions FINAL`); err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(ctx, `SELECT from_status, to_status, to_status_raw FROM work_item_transitions WHERE org_id = ? AND work_item_id = ? AND occurred_at >= toDateTime64('2026-03-02 00:00:00', 3, 'UTC')`, orgID, item)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var from, to, raw string
		if err := rows.Scan(&from, &to, &raw); err != nil {
			t.Fatal(err)
		}
		out = append(out, from+"->"+to+"("+raw+")")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}
