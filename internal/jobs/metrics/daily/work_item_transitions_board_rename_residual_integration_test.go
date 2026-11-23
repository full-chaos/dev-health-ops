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

// TestTransitionReadsServeTwoForARenamedBoardOptionPinnedAcceptedResidual pins an ACCEPTED residual (CHAOS-14289, acp
// ruling (b) of 2026-11-20), not a wanted behavior: a Projects v2 board Status option renamed after migration 170
// leaves the pre-170 tie_ordinal 0 row (raw "Done") and the slot row (raw "Finished") of one board event with
// different raw values, so StaleBoardTieZeroSQL has no positive evidence and both readers serve TWO transitions. No
// stored column separates a renamed board option from a first label on legacy rows. The controls: the same raw value
// serves one transition (TestTransitionReadsDropAStaleBoardTieZeroRow), and a real label (actor set) beside a slot
// row of its raw value is kept with the slot row.
func TestTransitionReadsServeTwoForARenamedBoardOptionPinnedAcceptedResidual(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	orgID := "org-board-rename-" + uuid.NewString()
	repoID := uuid.New()
	item := "gh:acme/api#14289"
	day := time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)
	end := day.AddDate(0, 0, 1)
	seedWorkItemEngineItem(t, ctx, conn, orgID, repoID, item, "quality", day.AddDate(0, 0, -10), nil, nil, nil)

	insert := func(columns string, values ...any) {
		t.Helper()
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_transitions (
			repo_id, work_item_id, occurred_at, provider, from_status, to_status,
			from_status_raw, to_status_raw, actor, org_id, last_synced`+columns+`)`)
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Append(values...); err != nil {
			t.Fatal(err)
		}
		if err := batch.Send(); err != nil {
			t.Fatal(err)
		}
	}
	pre170, post170 := time.Date(2026, 3, 3, 10, 0, 0, 0, time.UTC), time.Date(2026, 3, 3, 11, 0, 0, 0, time.UTC)
	renamed, label := day.Add(9*time.Hour), day.Add(8*time.Hour)
	// The renamed option: legacy 0 row raw "Done", slot row raw "Finished", one board event.
	insert("", uuid.Nil, item, renamed, "github", "unknown", "in_progress", "", "Done", "", orgID, pre170)
	insert(", tie_ordinal", uuid.Nil, item, renamed, "github", "unknown", "done", "", "Finished", "", orgID, post170, uint32(1<<31|12345))
	// Control: a real label (actor set) beside a slot row of its raw value: both kept.
	insert("", uuid.Nil, item, label, "github", "unknown", "blocked", "", "Hold", "alice", orgID, pre170)
	insert(", tie_ordinal", uuid.Nil, item, label, "github", "unknown", "review", "", "Hold", "", orgID, post170, uint32(1<<31|777))

	want := []string{
		"08:00 unknown->blocked", "08:00 unknown->review",
		"09:00 unknown->done", "09:00 unknown->in_progress",
	}
	stateTransitions, err := LoadWorkItemStateTransitions(ctx, conn, orgID, repoID, end)
	if err != nil {
		t.Fatal(err)
	}
	scopeTransitions, err := loadWorkItemScopeTransitions(ctx, conn, orgID, end, nil, false, false)
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string][]workItemStateTransition{"LoadWorkItemStateTransitions": stateTransitions, "loadWorkItemScopeTransitions": scopeTransitions} {
		read := make([]string, 0, len(got))
		for _, transition := range got {
			read = append(read, transition.OccurredAt.UTC().Format("15:04")+" "+transition.FromStatus+"->"+transition.ToStatus)
		}
		sort.Strings(read)
		if joined, wantJoined := strings.Join(read, ", "), strings.Join(want, ", "); joined != wantJoined {
			t.Errorf("%s read %q, want %q", name, joined, wantJoined)
		}
	}
}
