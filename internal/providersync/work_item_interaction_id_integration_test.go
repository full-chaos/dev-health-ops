//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// CHAOS-8790: state the system exists to reach. For EVERY provider, two
// comments of one work item with the same timestamp, built by the provider's
// real normalizer and written by the real adapter into the table the real
// migration chain built, are two rows of the reader contract
// (work_item_interactions_current), and a re-sync of both is still two.

func interactionViewCount(t *testing.T, ctx context.Context, conn driver.Conn, org, workItem string) uint64 {
	t.Helper()
	var count uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM work_item_interactions_current WHERE org_id = ? AND work_item_id = ?",
		org, workItem).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func writeInteractionRows(t *testing.T, ctx context.Context, conn driver.Conn, org string, rows []githubWorkItemInteractionRow) {
	t.Helper()
	identity, effect := workItemEffect(t, "work_item_interactions", interactionAny(rows)...)
	identity.OrgID = org
	if err := (GitHubWorkItemInteractionsClickHouseAdapter{Conn: conn}).WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatal(err)
	}
}

func interactionAny(rows []githubWorkItemInteractionRow) []any {
	out := make([]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, row)
	}
	return out
}

func TestEveryProviderStoresTwoSameTimestampCommentsAsTwoRows(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, provider := range interactionIDProviders() {
		t.Run(provider.provider, func(t *testing.T) {
			org := nativeTestClaim(provider.provider, "work-items").OrgID
			rows := provider.build(t, []any{"c-1", "c-2"})
			if len(rows) != 2 {
				t.Fatalf("normalizer gave %d rows", len(rows))
			}
			workItem := rows[0].WorkItemID
			writeInteractionRows(t, ctx, conn, org, rows)
			if got := interactionViewCount(t, ctx, conn, org, workItem); got != 2 {
				t.Fatalf("two same-timestamp comments = %d rows, want 2", got)
			}
			// a re-sync (later last_synced) of both comments
			for index := range rows {
				rows[index].LastSynced = rows[index].LastSynced.Add(time.Hour)
			}
			writeInteractionRows(t, ctx, conn, org, rows)
			if got := interactionViewCount(t, ctx, conn, org, workItem); got != 2 {
				t.Fatalf("after a re-sync = %d rows, want 2", got)
			}
		})
	}
}

// A comment written before the key carried the id (a legacy row) is replaced
// in the reader contract by the keyed row of the same comment: never counted
// twice, and shown while it is the only fact. The slot is
// (org_id, work_item_id, occurred_at, interaction_type): a legacy row of any
// OTHER slot stays visible, so each part of the slot is asserted apart.
func TestALegacyRowIsNotCountedTwiceOnceItsCommentIsResyncedWithAnID(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	legacy := func(org, workItem, provider, kind string, at time.Time) {
		t.Helper()
		if err := conn.Exec(ctx,
			`INSERT INTO work_item_interactions (work_item_id, provider, interaction_type, occurred_at, actor, body_length, last_synced, org_id) VALUES (?, ?, ?, ?, NULL, 5, ?, ?)`,
			workItem, provider, kind, at, at.Add(-time.Hour), org); err != nil {
			t.Fatal(err)
		}
	}
	for _, provider := range interactionIDProviders() {
		t.Run(provider.provider, func(t *testing.T) {
			org := nativeTestClaim(provider.provider, "work-items").OrgID
			otherOrg := org + "-other"
			rows := provider.build(t, []any{"c-1"})
			workItem, at := rows[0].WorkItemID, rows[0].OccurredAt
			legacy(org, workItem, provider.provider, "comment", at)
			if got := interactionViewCount(t, ctx, conn, org, workItem); got != 1 {
				t.Fatalf("a legacy row alone = %d rows, want 1", got)
			}
			// legacy rows of every OTHER slot: another time, another type,
			// another work item, another organization
			legacy(org, workItem, provider.provider, "comment", at.Add(time.Second))
			legacy(org, workItem, provider.provider, "reaction", at)
			legacy(org, workItem+"-other", provider.provider, "comment", at)
			legacy(otherOrg, workItem, provider.provider, "comment", at)

			writeInteractionRows(t, ctx, conn, org, rows)

			// the keyed row replaces ONLY the legacy row of its own slot
			if got := interactionViewCount(t, ctx, conn, org, workItem); got != 3 {
				t.Fatalf("keyed row + legacy rows of two other slots = %d rows, want 3 (keyed, +1s, reaction)", got)
			}
			if got := interactionViewCount(t, ctx, conn, org, workItem+"-other"); got != 1 {
				t.Fatalf("another work item's legacy row = %d rows, want 1", got)
			}
			if got := interactionViewCount(t, ctx, conn, otherOrg, workItem); got != 1 {
				t.Fatalf("another organization's legacy row = %d rows, want 1", got)
			}
			var legacyShown uint64
			if err := conn.QueryRow(ctx,
				"SELECT count() FROM work_item_interactions_current WHERE org_id = ? AND work_item_id = ? AND interaction_type = 'comment' AND occurred_at = ? AND interaction_id = ''",
				org, workItem, at).Scan(&legacyShown); err != nil {
				t.Fatal(err)
			}
			if legacyShown != 0 {
				t.Fatalf("the legacy row of the re-synced comment is still shown")
			}
			var raw uint64
			if err := conn.QueryRow(ctx,
				"SELECT count() FROM work_item_interactions FINAL WHERE org_id = ? AND work_item_id = ?",
				org, workItem).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			if raw != 4 {
				t.Fatalf("the table holds %d rows, want 4 (the view hides the legacy row, it does not delete it)", raw)
			}
		})
	}
}

// The adapter refuses a row with no id before anything reaches ClickHouse.
func TestTheAdapterRefusesARowWithoutAnID(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	org := nativeTestClaim("github", "work-items").OrgID
	row := workItemInteractionTestRow(org, time.Now().UTC())
	row.InteractionID = ""
	identity, effect := workItemEffect(t, "work_item_interactions", row)
	if err := (GitHubWorkItemInteractionsClickHouseAdapter{Conn: conn}).WriteGitHubWorkItemEffect(ctx, identity, effect); err == nil {
		t.Fatal("a row with an empty interaction_id was accepted")
	}
	var count uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM work_item_interactions WHERE org_id = ?", org).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("%d rows reached the table", count)
	}
}

// P1 (review round 1): GitLab note ids above 2^53, through the real route,
// the real adapter and the migrated table, are two rows.
func TestGitLabNoteIDsAbove2Pow53AreTwoStoredRows(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	rows := gitLabNotesInteractionRows(t, noteJSON(idTwoPow53), noteJSON(idTwoPow53Plus1))
	if len(rows) != 2 {
		t.Fatalf("route gave %d rows", len(rows))
	}
	org := nativeTestClaim("gitlab", "work-items").OrgID
	writeInteractionRows(t, ctx, conn, org, rows)
	if got := interactionViewCount(t, ctx, conn, org, rows[0].WorkItemID); got != 2 {
		t.Fatalf("persisted rows = %d, want 2", got)
	}
}
