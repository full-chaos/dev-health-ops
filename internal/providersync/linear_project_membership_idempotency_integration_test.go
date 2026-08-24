//go:build integration

package providersync

import (
	"context"
	"encoding/json"
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
// provider, id). This test is the proof that ruling asked for: an
// expired-lease reclaim restarts a unit from a completely fresh ledger with
// no memory of the earlier attempt, so it replays the SAME committer.Commit
// call against a SECOND, unrelated ledger. If ClickHouse's FINAL state
// (transition count, catalog count, presence view) is not byte-identical
// after that replay, the ledger's own dedup was doing the work the ruling
// assumed the ENGINE does -- and that overturns the ruling rather than being
// something to patch around here.
func TestLinearProjectMembershipReplayAfterExpiredLeaseIsIdempotentAtClickHouse(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	claim := nativeTestClaim("linear", "work-items")
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	occurredAt := now.Add(-time.Hour)

	workItemID := "linear:ENG-idem-1"
	fromID, toID := "project-idem-old", "project-idem-new"

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
		Actor: "linear@example.com", OccurredAt: occurredAt, LastSynced: now,
		EventID: "linear:hist-idem-1",
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

	effects, err := BuildLinearWorkItemEffects(LinearWorkItemEffectRows{
		WorkItems:          []json.RawMessage{itemRaw},
		ProjectMemberships: []json.RawMessage{membershipRaw},
		Projects:           []json.RawMessage{catalogFromRaw, catalogToRaw},
	})
	if err != nil {
		t.Fatal(err)
	}

	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := linearMigratedClickHouseSink(conn, lease)

	commit := func() {
		t.Helper()
		ledger := &memoryEffectLedger{}
		committer := EffectCommitter{
			Ledger: ledger, Sink: sink, Readback: sink,
			Now: func() time.Time { return now },
		}
		if _, err := committer.Commit(ctx, claim, effects, now); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	countTransitions := func() uint64 {
		t.Helper()
		var count uint64
		if err := conn.QueryRow(ctx,
			`SELECT count() FROM project_membership_transitions FINAL `+
				`WHERE org_id = ? AND subject_kind = ? AND subject_id = ?`,
			claim.OrgID, projectmembership.SubjectWorkItem, workItemID,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	countProjects := func() uint64 {
		t.Helper()
		var count uint64
		if err := conn.QueryRow(ctx,
			`SELECT count() FROM projects FINAL WHERE org_id = ? AND provider = 'linear' AND id IN (?, ?)`,
			claim.OrgID, fromID, toID,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
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

	commit()
	firstTransitions, firstProjects := countTransitions(), countProjects()
	firstPresence := queryPresence()
	if firstTransitions != 1 {
		t.Fatalf("transitions after first commit=%d, want 1", firstTransitions)
	}
	if firstProjects != 2 {
		t.Fatalf("projects after first commit=%d, want 2", firstProjects)
	}
	if len(firstPresence) != 1 || firstPresence[0].ProjectID != toID {
		t.Fatalf("presence after first commit=%+v, want one active row for %s", firstPresence, toID)
	}

	// Simulate an expired-lease reclaim: a completely FRESH ledger, no memory
	// of the first attempt, replaying the exact same content against the
	// SAME live ClickHouse. This is the strongest form of the proof -- if the
	// first ledger's own bookkeeping were doing the work, a brand new,
	// unrelated ledger would defeat it, and only the ENGINE's natural-key
	// convergence is left to make this pass.
	commit()
	secondTransitions, secondProjects := countTransitions(), countProjects()
	secondPresence := queryPresence()

	if secondTransitions != firstTransitions {
		t.Fatalf("transitions changed on replay: first=%d second=%d", firstTransitions, secondTransitions)
	}
	if secondProjects != firstProjects {
		t.Fatalf("projects changed on replay: first=%d second=%d", firstProjects, secondProjects)
	}
	if len(secondPresence) != len(firstPresence) || secondPresence[0] != firstPresence[0] {
		t.Fatalf("presence changed on replay: first=%+v second=%+v", firstPresence, secondPresence)
	}
}
