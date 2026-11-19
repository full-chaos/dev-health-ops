//go:build integration

package providersync

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// The INSERT of a build before migration 170: no tie_ordinal column, so the row reads tie_ordinal 0.
const boardTieZeroPre170Insert = `INSERT INTO work_item_transitions (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, last_synced, org_id, source_id)`

func writeBoardTieZeroPre170(ctx context.Context, t *testing.T, conn driver.Conn, row githubWorkItemTransitionRow) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, boardTieZeroPre170Insert)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(projectWorkItemTransition(row).values()[:12]...); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

// boardTieZeroCase stores a board's Status event as a build before 170 wrote it (tie_ordinal 0, set at preSet, with
// the to_status of an earlier status map), then the board's current event (column Done, set at nowSet) through the 170
// writer at the board's slot. It returns the stored history the Projects v2 route reads, in OrderTransitions order of
// occurred_at, and the current event.
func boardTieZeroCase(
	t *testing.T, item, preSet, oldToStatus, nowSet string, extra ...githubWorkItemTransitionRow,
) ([]githubWorkItemTransitionRow, githubWorkItemTransitionRow) {
	t.Helper()
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := nativeTestClaim("github", "work-items").OrgID
	run1At, run2At := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	pre := boardTieEvent(t, item, "ghprojv2:acme#3", "Done", preSet, run1At)
	pre.BoardSlot = 0
	pre.ToStatus = oldToStatus
	writeBoardTieZeroPre170(ctx, t, conn, pre)
	for _, row := range extra {
		writeBoardTieZeroPre170(ctx, t, conn, row)
	}
	now := boardTieEvent(t, item, "ghprojv2:acme#3", "Done", nowSet, run2At)
	if err := githubFamilyTieWriter(conn, []githubWorkItemTransitionRow{now}).write(ctx, t); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `OPTIMIZE TABLE work_item_transitions FINAL`); err != nil {
		t.Fatal(err)
	}
	stored, err := GitHubProjectV2StoredTransitionsClickHouseReader{Conn: conn}.StoredTransitions(ctx, orgID, []string{item})
	if err != nil {
		t.Fatal(err)
	}
	return stored, now
}

func boardTieZeroLabels(stored []githubWorkItemTransitionRow) string {
	out := make([]string, 0, len(stored))
	for _, row := range stored {
		out = append(out, row.OccurredAt.UTC().Format("01-02T15:04")+" "+row.FromStatus+"->"+row.ToStatus+"("+derefString(row.ToStatusRaw)+")")
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}

// A status-map change after 170: the pre-170 row at 0 (unknown) and the board's slot row (done) hold one board event.
// The read serves one transition, the current one.
func TestBoardTieZeroRowAfterStatusMapChangeIsOneTransition(t *testing.T) {
	stored, _ := boardTieZeroCase(t, "gh:acme/api#14270", "2026-03-02T09:00:00Z", "unknown", "2026-03-02T09:00:00Z")
	if got, want := boardTieZeroLabels(stored), "03-02T09:00 unknown->done(Done)"; got != want {
		t.Fatalf("stored history %q, want %q", got, want)
	}
}

// The pre-170 row (in_progress) sorts after the current one (done) in OrderTransitions; it must not set the item's
// board status (settleGitHubProjectV2Status through lastGitHubTransition).
func TestBoardTieZeroRowDoesNotWinTheBoardStatus(t *testing.T) {
	stored, now := boardTieZeroCase(t, "gh:acme/api#14271", "2026-03-02T09:00:00Z", "in_progress", "2026-03-02T09:00:00Z")
	last, ok := lastGitHubTransition(stored)
	if !ok || last.ToStatus != now.ToStatus {
		t.Fatalf("served status %q of %q, want %q", last.ToStatus, boardTieZeroLabels(stored), now.ToStatus)
	}
}

// While the content is identical the two rows are one transition.
func TestBoardTieZeroRowWithTheSameContentIsOneTransition(t *testing.T) {
	stored, _ := boardTieZeroCase(t, "gh:acme/api#14272", "2026-03-02T09:00:00Z", "done", "2026-03-02T09:00:00Z")
	if got, want := boardTieZeroLabels(stored), "03-02T09:00 unknown->done(Done)"; got != want {
		t.Fatalf("stored history %q, want %q", got, want)
	}
}

// A tie_ordinal 0 board row with NO slot row at its instant is history (the board value changed later) and is kept,
// although the board has a slot row at another instant.
func TestBoardTieZeroRowWithNoSlotRowAtItsInstantIsKept(t *testing.T) {
	stored, _ := boardTieZeroCase(t, "gh:acme/api#14273", "2026-03-01T09:00:00Z", "in_progress", "2026-03-02T09:00:00Z")
	want := "03-01T09:00 unknown->in_progress(Done), 03-02T09:00 unknown->done(Done)"
	if got := boardTieZeroLabels(stored); got != want {
		t.Fatalf("stored history %q, want %q", got, want)
	}
}

// A GitHub timeline transition of the board event's shape (the issue's first event, from "unknown", no actor) at the
// slot row's instant is kept by its raw value.
func TestTimelineTieZeroRowAtTheSlotInstantIsKept(t *testing.T) {
	item := "gh:acme/api#14274"
	closed := "closed"
	timeline := githubWorkItemTransitionRow{
		WorkItemID: item, Provider: "github", OccurredAt: time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC),
		ToStatusRaw: &closed, FromStatus: "unknown", ToStatus: "done",
		OrgID: nativeTestClaim("github", "work-items").OrgID, LastSynced: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
	}
	stored, _ := boardTieZeroCase(t, item, "2026-02-01T09:00:00Z", "in_progress", "2026-03-02T09:00:00Z", timeline)
	want := "02-01T09:00 unknown->in_progress(Done), 03-02T09:00 unknown->done(Done), 03-02T09:00 unknown->done(closed)"
	if got := boardTieZeroLabels(stored); got != want {
		t.Fatalf("stored history %q, want %q", got, want)
	}
}
