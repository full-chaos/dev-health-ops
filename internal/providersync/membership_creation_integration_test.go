//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-7361, through the REAL presence view on a migrated ClickHouse. The
// production sequence is replayed: an issue created inside a project is first
// synced by the OLD producer (no creation ADD: the creation row is dropped
// from the real producer's output), then by the NEW one. For both providers
// the presence view must keep the same project_id AND project_key and stay
// one row; the only change is source column -> transition (named behaviour
// change). A second sync of the new output must not change anything.

type presenceSnapshot struct {
	ProjectID, ProjectKey, Source string
	ObservedAt                    time.Time
}

func readPresence(t *testing.T, ctx context.Context, conn driver.Conn, orgID, subjectID string) []presenceSnapshot {
	t.Helper()
	result, err := conn.Query(ctx,
		`SELECT project_id, project_key, source, observed_at FROM project_membership_presence `+
			`WHERE org_id = ? AND subject_kind = 'work_item' AND subject_id = ? ORDER BY project_id`,
		orgID, subjectID)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	rows := []presenceSnapshot{}
	for result.Next() {
		var row presenceSnapshot
		if err := result.Scan(&row.ProjectID, &row.ProjectKey, &row.Source, &row.ObservedAt); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	if err := result.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

// withoutCreationRow is the OLD producer's output: the real batch minus the
// creation ADD (a ("", P) row at the issue's creation time).
func withoutCreationRow(t *testing.T, effects []EffectBatch, created time.Time) []EffectBatch {
	t.Helper()
	out := make([]EffectBatch, 0, len(effects))
	for _, effect := range effects {
		if effect.Destination == "project_membership_transitions" {
			kept := effect
			kept.Rows = nil
			for _, raw := range effect.Rows {
				var row projectmembership.Row
				if err := json.Unmarshal(raw, &row); err != nil {
					t.Fatal(err)
				}
				if row.FromProjectID == "" && row.OccurredAt.Equal(created) {
					continue
				}
				kept.Rows = append(kept.Rows, raw)
			}
			effect = kept
		}
		out = append(out, effect)
	}
	return out
}

func directEffectsOnly(effects []EffectBatch) []EffectBatch {
	out := []EffectBatch{}
	for _, effect := range effects {
		switch effect.Destination {
		case "work_items", "projects", "project_membership_transitions":
			out = append(out, effect)
		}
	}
	return out
}

func assertCreationAddKeepsPresence(
	t *testing.T, ctx context.Context, conn driver.Conn, claim Claim,
	commit func(effects []EffectBatch, at time.Time),
	batch CompleteRouteBatch, subjectID, wantProject, wantKey string, created time.Time,
) {
	t.Helper()
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	commit(withoutCreationRow(t, directEffectsOnly(batch.Effects), created), now)
	before := readPresence(t, ctx, conn, claim.OrgID, subjectID)
	if len(before) != 1 || before[0].ProjectID != wantProject || before[0].ProjectKey != wantKey || before[0].Source != "work_item_column" {
		t.Fatalf("before (old producer): presence=%+v want one work_item_column row %s/%q", before, wantProject, wantKey)
	}
	commit(directEffectsOnly(batch.Effects), now.Add(time.Hour))
	after := readPresence(t, ctx, conn, claim.OrgID, subjectID)
	if len(after) != 1 || after[0].ProjectID != before[0].ProjectID || after[0].ProjectKey != before[0].ProjectKey {
		t.Fatalf("after (new producer): presence=%+v, want the same project_id/project_key as before=%+v and ONE row", after, before)
	}
	if after[0].Source != "transition" || !after[0].ObservedAt.Equal(created.UTC().Truncate(time.Millisecond)) {
		t.Fatalf("after: %+v, want source=transition observed_at=creation time %v", after[0], created)
	}
	// A second sync of the same output changes nothing.
	commit(directEffectsOnly(batch.Effects), now.Add(2*time.Hour))
	again := readPresence(t, ctx, conn, claim.OrgID, subjectID)
	if len(again) != 1 || again[0] != after[0] {
		t.Fatalf("second sync changed presence: %+v -> %+v", after, again)
	}
	var transitions uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM project_membership_transitions FINAL WHERE org_id = ? AND subject_id = ?`,
		claim.OrgID, subjectID).Scan(&transitions); err != nil {
		t.Fatal(err)
	}
	if transitions != 1 {
		t.Fatalf("transitions after two syncs of the creation ADD = %d, want 1 (no second ADD)", transitions)
	}
	// The stored work_items.created_at equals the ADD's occurred_at at the
	// stored precision (the backfill-derivation fact).
	var storedCreated, addOccurred time.Time
	if err := conn.QueryRow(ctx,
		`SELECT created_at FROM work_items FINAL WHERE org_id = ? AND work_item_id = ?`,
		claim.OrgID, subjectID).Scan(&storedCreated); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx,
		`SELECT occurred_at FROM project_membership_transitions FINAL WHERE org_id = ? AND subject_id = ?`,
		claim.OrgID, subjectID).Scan(&addOccurred); err != nil {
		t.Fatal(err)
	}
	if !storedCreated.Equal(addOccurred) {
		t.Fatalf("work_items.created_at %v != creation ADD occurred_at %v in ClickHouse", storedCreated, addOccurred)
	}
}

func TestLinearCreationAddKeepsPresenceIdenticalThroughTheRealView(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	claim := nativeTestClaim("linear", "work-items")
	batch := linearCreationBatch(t, linearIssue("2026-07-25T09:00:00.123Z", "P", ``), true)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	commit := func(effects []EffectBatch, at time.Time) {
		t.Helper()
		sink := linearMigratedClickHouseSink(conn, lease)
		committer := EffectCommitter{Ledger: &memoryEffectLedger{}, Sink: sink, Readback: sink, Now: func() time.Time { return at }}
		if _, err := committer.Commit(ctx, claim, effects, at); err != nil {
			t.Fatal(err)
		}
	}
	assertCreationAddKeepsPresence(t, ctx, conn, claim, commit, batch, "linear:ENG-7", "P", "",
		time.Date(2026, 7, 25, 9, 0, 0, 123000000, time.UTC))
}

func TestJiraCreationAddKeepsPresenceIdenticalThroughTheRealView(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	claim := jiraAtlassianClaim()
	batch := jiraCreationBatch(t, "2026-07-30T08:00:00.123Z", ``)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	commit := func(effects []EffectBatch, at time.Time) {
		t.Helper()
		sink := NewJiraAtlassianClickHouseEffects(conn, lease)
		committer := EffectCommitter{Ledger: &memoryEffectLedger{}, Sink: sink, Readback: sink, Now: func() time.Time { return at }}
		if _, err := committer.Commit(ctx, claim, effects, at); err != nil {
			t.Fatal(err)
		}
	}
	assertCreationAddKeepsPresence(t, ctx, conn, claim, commit, batch, "jira:OPS-9", "10001", "OPS",
		time.Date(2026, 7, 30, 8, 0, 0, 123000000, time.UTC))
}
