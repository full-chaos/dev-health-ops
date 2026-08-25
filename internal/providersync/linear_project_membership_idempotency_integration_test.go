//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

// This is the round-5 codex follow-up's PROOF, not a fix: team-lead's ruling
// (CHAOS-4193 lease-retry gap, follow-up filed as CHAOS-4247) is that both new
// destinations are replay-idempotent BY CONSTRUCTION, so no enforcement
// machinery is needed in this PR -- project_membership_transitions is
// ReplacingMergeTree(last_synced) keyed by a content-determined event_id, and
// `projects` rows are ensured onto a ReplacingMergeTree keyed (org_id,
// provider, id).
//
// Two distinct replay shapes are both exercised, per a task-route codex
// finding that the first draft only proved the second and never touched the
// ledger's own recovery/readback control flow:
//
//  1. A genuine interrupted-then-recovered commit: the lease fails right
//     after the `projects` write physically lands but before its
//     acknowledgement, leaving that effect GenerationBlockWriting. Recovery
//     reuses the SAME ledger, so it drives commitPrepared's
//     EffectReadbackRequired branch and existenceOnlyProjectCatalogReadback
//     for real -- this is the exact readback path round 2/5 fixed.
//  2. An independent expired-lease reclaim: a completely FRESH, unrelated
//     ledger with no memory of attempt 1 replays the identical content. If
//     the first ledger's own bookkeeping were doing the convergence work, a
//     second unrelated ledger would defeat it, and only the ENGINE's
//     natural-key convergence is left to make this pass.
//
// Both phases assert the FULL persisted row content against known expected
// values (not just counts), since every row's content is fixed once, up
// front, and reused unchanged across every commit call in this test -- a
// replay that silently corrupted a field while preserving counts must still
// fail here.
func TestLinearProjectMembershipReplayAfterExpiredLeaseIsIdempotentAtClickHouse(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	claim := nativeTestClaim("linear", "work-items")
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	occurredAt := now.Add(-time.Hour)

	workItemID := "linear:ENG-idem-1"
	fromID, toID := "project-idem-old", "project-idem-new"
	actor := "linear@example.com"
	eventID := "linear:hist-idem-1"

	item := LinearWorkItemRow{
		WorkItemID: workItemID, Provider: "linear", Title: "Replay idempotency proof",
		Type: "task", Status: "in_progress", StatusRaw: stringPtr("In Progress"),
		ProjectID: stringPtr(toID), ProjectName: stringPtr("Idem New"),
		CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now,
		Labels: []string{}, OrgID: claim.OrgID, LastSynced: now,
	}
	membership := projectmembership.Row{
		OrgID: claim.OrgID, RepoID: uuid.Nil, SubjectKind: projectmembership.SubjectWorkItem,
		SubjectID: workItemID, Provider: "linear",
		FromProjectID: fromID, ToProjectID: toID,
		FromProjectKey: "", ToProjectKey: "",
		Actor: actor, OccurredAt: occurredAt, LastSynced: now,
		EventID: eventID,
	}
	catalogFrom := linearEnsureProjectsRow(claim.OrgID, fromID, "", now)
	catalogTo := linearEnsureProjectsRow(claim.OrgID, toID, "Idem New", now)

	itemRaw, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	membershipRaw, err := json.Marshal(membership)
	if err != nil {
		t.Fatal(err)
	}
	catalogFromRaw, err := json.Marshal(catalogFrom)
	if err != nil {
		t.Fatal(err)
	}
	catalogToRaw, err := json.Marshal(catalogTo)
	if err != nil {
		t.Fatal(err)
	}

	// Same content, built ONCE and reused for every commit call in this test
	// (all three phases) -- that is what makes "unchanged" a meaningful
	// assertion rather than a tautology.
	effects, err := BuildLinearWorkItemEffects(LinearWorkItemEffectRows{
		WorkItems:          []json.RawMessage{itemRaw},
		ProjectMemberships: []json.RawMessage{membershipRaw},
		Projects:           []json.RawMessage{catalogFromRaw, catalogToRaw},
	})
	if err != nil {
		t.Fatal(err)
	}

	type transitionSnapshot struct {
		FromProjectID, ToProjectID   string
		FromProjectKey, ToProjectKey string
		Actor, EventID               string
		OccurredAt                   time.Time
	}
	queryTransition := func() transitionSnapshot {
		t.Helper()
		var row transitionSnapshot
		if err := conn.QueryRow(ctx,
			`SELECT from_project_id, to_project_id, from_project_key, to_project_key, actor, event_id, occurred_at `+
				`FROM project_membership_transitions FINAL WHERE org_id = ? AND subject_kind = ? AND subject_id = ?`,
			claim.OrgID, projectmembership.SubjectWorkItem, workItemID,
		).Scan(&row.FromProjectID, &row.ToProjectID, &row.FromProjectKey, &row.ToProjectKey,
			&row.Actor, &row.EventID, &row.OccurredAt); err != nil {
			t.Fatal(err)
		}
		return row
	}
	wantTransition := transitionSnapshot{
		FromProjectID: fromID, ToProjectID: toID,
		FromProjectKey: "", ToProjectKey: "",
		Actor: actor, EventID: eventID, OccurredAt: occurredAt.UTC(),
	}

	type catalogSnapshot struct {
		ID, Name, ProjectKey string
		IsActive             uint8
	}
	queryCatalog := func() []catalogSnapshot {
		t.Helper()
		result, err := conn.Query(ctx,
			`SELECT id, name, project_key, is_active FROM projects FINAL `+
				`WHERE org_id = ? AND provider = 'linear' AND id IN (?, ?) ORDER BY id`,
			claim.OrgID, fromID, toID,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()
		rows := make([]catalogSnapshot, 0)
		for result.Next() {
			var row catalogSnapshot
			var key *string
			if err := result.Scan(&row.ID, &row.Name, &key, &row.IsActive); err != nil {
				t.Fatal(err)
			}
			if key != nil {
				row.ProjectKey = *key
			}
			rows = append(rows, row)
		}
		if err := result.Err(); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	// "project-idem-new" sorts before "project-idem-old" ('n' < 'o').
	wantCatalog := []catalogSnapshot{
		{ID: toID, Name: "Idem New", ProjectKey: "", IsActive: 1},
		{ID: fromID, Name: "", ProjectKey: "", IsActive: 1},
	}

	type presenceRow struct {
		ProjectID string
		Source    string
	}
	queryPresence := func() []presenceRow {
		t.Helper()
		result, err := conn.Query(ctx,
			`SELECT project_id, source FROM project_membership_presence `+
				`WHERE org_id = ? AND subject_kind = ? AND subject_id = ? ORDER BY project_id`,
			claim.OrgID, projectmembership.SubjectWorkItem, workItemID,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()
		rows := make([]presenceRow, 0)
		for result.Next() {
			var row presenceRow
			if err := result.Scan(&row.ProjectID, &row.Source); err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
		}
		if err := result.Err(); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	wantPresence := []presenceRow{{ProjectID: toID, Source: "transition"}}

	assertState := func(label string) {
		t.Helper()
		if transition := queryTransition(); transition != wantTransition {
			t.Fatalf("%s: transition=%+v want=%+v", label, transition, wantTransition)
		}
		catalog := queryCatalog()
		if len(catalog) != len(wantCatalog) || catalog[0] != wantCatalog[0] || catalog[1] != wantCatalog[1] {
			t.Fatalf("%s: catalog=%+v want=%+v", label, catalog, wantCatalog)
		}
		presence := queryPresence()
		if len(presence) != len(wantPresence) || presence[0] != wantPresence[0] {
			t.Fatalf("%s: presence=%+v want=%+v", label, presence, wantPresence)
		}
	}

	// Phase 1: a genuine interrupted-then-recovered commit. The lease fails
	// on its 4th Assert call, which -- effects commit in ALPHABETICAL
	// destination order (effectBatchLess), and WriteEffect asserts once
	// before and once after the real write -- lands as the ACK assert for
	// "projects" (2nd of 8: project_membership_transitions,
	// projects, sprints, work_item_dependencies, work_item_interactions,
	// work_item_reopen_events, work_item_transitions, work_items). The
	// catalog rows physically land in ClickHouse before the ack fails,
	// leaving that effect GenerationBlockWriting with nothing else attempted.
	crashingLease := &linearLifecycleCountingLease{failAt: 4}
	crashingSink := linearMigratedClickHouseSink(conn, crashingLease)
	ledger := &memoryEffectLedger{}
	firstAttempt := EffectCommitter{
		Ledger: ledger, Sink: crashingSink, Readback: crashingSink,
		Now: func() time.Time { return now },
	}
	if _, err := firstAttempt.Commit(ctx, claim, effects, now); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("first commit error=%v, want ErrLeaseLost", err)
	}

	// Recovery reuses the SAME ledger with a healthy lease -- this is what
	// actually exercises commitPrepared's EffectReadbackRequired branch and
	// existenceOnlyProjectCatalogReadback, not just a second from-scratch
	// write.
	healthyLease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	recoverySink := linearMigratedClickHouseSink(conn, healthyLease)
	recovery := EffectCommitter{
		Ledger: ledger, Sink: recoverySink, Readback: recoverySink,
		Now: func() time.Time { return now.Add(time.Minute) },
	}
	recoveryResult, err := recovery.Commit(ctx, claim, effects, now)
	if err != nil {
		t.Fatalf("recovery commit: %v", err)
	}
	// 1 destination (project_membership_transitions) fully acknowledged in
	// attempt 1 -> Skipped. 1 destination (projects) was written but not
	// acknowledged -> readback confirms it via existence, MarkedCommitted.
	// The remaining 6 (five empty, plus work_items) were never attempted in
	// attempt 1 -> Written fresh here.
	if recoveryResult.Skipped != 1 || recoveryResult.MarkedCommitted != 1 || recoveryResult.Written != 6 {
		t.Fatalf("recovery result=%+v, want {Skipped:1 MarkedCommitted:1 Written:6 ...}", recoveryResult)
	}
	assertState("after crash-recovery (phase 1)")

	// Phase 2: an independent expired-lease reclaim -- a completely FRESH
	// ledger with no memory of phase 1 at all, replaying the exact same
	// content against the SAME live ClickHouse.
	replaySink := linearMigratedClickHouseSink(conn, healthyLease)
	replay := EffectCommitter{
		Ledger: &memoryEffectLedger{}, Sink: replaySink, Readback: replaySink,
		Now: func() time.Time { return now.Add(2 * time.Minute) },
	}
	if _, err := replay.Commit(ctx, claim, effects, now); err != nil {
		t.Fatalf("replay commit: %v", err)
	}
	assertState("after independent replay (phase 2)")
}
