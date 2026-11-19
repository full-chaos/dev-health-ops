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

// TestTransitionReadsDropAStaleBoardTieZeroRow: a Projects v2 board Status event stored before migration 170
// (tie_ordinal 0, an earlier status map's to_status) and the board's slot row of the same event (tie_ordinal >= 2^31)
// are one transition, the slot row's, in both daily readers. A board row at 0 with no slot row at its instant is kept.
func TestTransitionReadsDropAStaleBoardTieZeroRow(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	orgID := "org-board-tie-zero-" + uuid.NewString()
	repoID := uuid.New()
	item := "gh:acme/api#14270"
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
	history := day.Add(-24 * time.Hour)
	current := day.Add(9 * time.Hour)
	pre170, post170 := time.Date(2026, 3, 3, 10, 0, 0, 0, time.UTC), time.Date(2026, 3, 3, 11, 0, 0, 0, time.UTC)
	insert("", uuid.Nil, item, history, "github", "unknown", "in_progress", "", "Doing", "", orgID, pre170)
	insert("", uuid.Nil, item, current, "github", "unknown", "in_progress", "", "Done", "", orgID, pre170)
	insert(", tie_ordinal", uuid.Nil, item, current, "github", "unknown", "done", "", "Done", "", orgID, post170, uint32(1<<31|12345))

	want := "unknown->done unknown->in_progress"
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
			read = append(read, transition.FromStatus+"->"+transition.ToStatus)
		}
		sort.Strings(read)
		if joined := strings.Join(read, " "); joined != want {
			t.Errorf("%s read %q, want %q", name, joined, want)
		}
	}
}
