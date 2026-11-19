//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Rows stored before the Duplicate and Triage mapping carry 'unknown' for those states. A later sync of the same
// Linear history writes the new statuses at the same (org, repo, item, occurred_at) key, so a FINAL read returns only
// the new row for each move. Stored rows are written in the prod form: the Linear INSERT writes provider linear, repo_id is
// the nil uuid, occurred_at has the milliseconds of a Linear history node.
func TestLinearResyncReplacesTheStoredDuplicateAndTriageRows(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	claim := nativeTestClaim("linear", "work-items")
	storedAt := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	resyncAt := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	at := func(value string) time.Time {
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	raw := func(value string) *string { return &value }
	type move struct {
		item, at, fromName, fromType, toName, toType string
		mainFrom, mainTo, fixFrom, fixTo             string
	}
	moves := []move{
		{"linear:ENG-1", "2026-05-18T18:25:00.402Z", "Triage", "triage", "Backlog", "backlog", "unknown", "backlog", "backlog", "backlog"},
		{"linear:ENG-1", "2026-05-18T21:24:19.876Z", "Backlog", "backlog", "Canceled", "canceled", "backlog", "canceled", "backlog", "canceled"},
		{"linear:ENG-1", "2026-05-18T21:24:40.125Z", "Canceled", "canceled", "Duplicate", "duplicate", "canceled", "unknown", "canceled", "canceled"},
		{"linear:ENG-2", "2026-11-18T09:27:39.366Z", "Backlog", "backlog", "Duplicate", "duplicate", "backlog", "unknown", "backlog", "canceled"},
		{"linear:ENG-2", "2026-11-18T10:02:11.007Z", "Duplicate", "duplicate", "Todo", "unstarted", "unknown", "todo", "canceled", "todo"},
	}

	// Rows as main stored them.
	var stored []json.RawMessage
	for _, m := range moves {
		row := linearWorkItemTransitionRow{
			WorkItemID: m.item, Provider: "linear", OccurredAt: at(m.at),
			FromStatusRaw: raw(m.fromName), ToStatusRaw: raw(m.toName), FromStatus: m.mainFrom, ToStatus: m.mainTo,
			OrgID: claim.OrgID, LastSynced: storedAt,
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		stored = append(stored, encoded)
	}
	writeLinearTransitionsForResyncCell(t, ctx, conn, claim, stored)

	// The same histories normalized with this change.
	var resynced []json.RawMessage
	for _, item := range []struct{ id, state string }{
		{"linear:ENG-1", `{"name":"Duplicate","type":"duplicate"}`},
		{"linear:ENG-2", `{"name":"Todo","type":"unstarted"}`},
	} {
		var nodes []map[string]any
		for index, m := range moves {
			if m.item != item.id {
				continue
			}
			nodes = append(nodes, map[string]any{
				"id": item.id + "-" + string(rune('a'+index)), "createdAt": m.at,
				"fromState": map[string]string{"name": m.fromName, "type": m.fromType},
				"toState":   map[string]string{"name": m.toName, "type": m.toType},
			})
		}
		history, err := json.Marshal(nodes)
		if err != nil {
			t.Fatal(err)
		}
		var payload linearWorkItemPayload
		if err := json.Unmarshal([]byte(`{"id":"`+item.id+`","identifier":"`+item.id[len("linear:"):]+`","title":"T",
			"createdAt":"2026-05-18T18:22:23Z","updatedAt":"2026-11-18T10:02:12Z","state":`+item.state+`,
			"history":{"nodes":`+string(history)+`,"pageInfo":{"hasNextPage":false,"endCursor":""}}}`), &payload); err != nil {
			t.Fatal(err)
		}
		_, transitions, err := normalizeLinearWorkItem(claim, payload, resyncAt)
		if err != nil {
			t.Fatal(err)
		}
		for _, transition := range transitions {
			encoded, err := json.Marshal(transition)
			if err != nil {
				t.Fatal(err)
			}
			resynced = append(resynced, encoded)
		}
	}
	if len(resynced) != len(moves) {
		t.Fatalf("resync normalized %d transitions, want %d", len(resynced), len(moves))
	}
	writeLinearTransitionsForResyncCell(t, ctx, conn, claim, resynced)

	var physical uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM work_item_transitions WHERE org_id = ?`, claim.OrgID).Scan(&physical); err != nil {
		t.Fatal(err)
	}
	t.Logf("physical rows before FINAL: %d (stored %d + resynced %d, less any background merge)", physical, len(moves), len(moves))
	rows, err := conn.Query(ctx, `
SELECT work_item_id, occurred_at, provider, toString(repo_id), from_status, to_status, from_status_raw, to_status_raw, last_synced
FROM work_item_transitions FINAL
WHERE org_id = ?
ORDER BY work_item_id, occurred_at`, claim.OrgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var item, provider, repoID, from, to, fromRaw, toRaw string
		var occurred, synced time.Time
		if err := rows.Scan(&item, &occurred, &provider, &repoID, &from, &to, &fromRaw, &toRaw, &synced); err != nil {
			t.Fatal(err)
		}
		if index >= len(moves) {
			t.Fatalf("FINAL returned more than %d rows: extra %s %s %s -> %s", len(moves), item, occurred.Format(time.RFC3339Nano), from, to)
		}
		m := moves[index]
		t.Logf("FINAL %s %s provider=%q repo=%s %s -> %s (%s -> %s) last_synced=%s",
			item, occurred.UTC().Format(time.RFC3339Nano), provider, repoID, from, to, fromRaw, toRaw, synced.UTC().Format(time.RFC3339))
		if item != m.item || !occurred.Equal(at(m.at)) || provider != "linear" || repoID != "00000000-0000-0000-0000-000000000000" ||
			from != m.fixFrom || to != m.fixTo || fromRaw != m.fromName || toRaw != m.toName || !synced.Equal(resyncAt) {
			t.Errorf("row %d: got %s %s provider=%q %s -> %s synced %s; want %s %s provider=\"linear\" %s -> %s synced %s", index,
				item, occurred.UTC().Format(time.RFC3339Nano), provider, from, to, synced.UTC(), m.item, m.at, m.fixFrom, m.fixTo, resyncAt)
		}
		index++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if index != len(moves) {
		t.Fatalf("FINAL returned %d rows, want %d", index, len(moves))
	}
}

func writeLinearTransitionsForResyncCell(t *testing.T, ctx context.Context, conn driver.Conn, claim Claim, rows []json.RawMessage) {
	t.Helper()
	effect, err := BuildEffectBatch("work_item_transitions", EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := newLinearWorkItemEffectIdentity(claim, effect)
	if err != nil {
		t.Fatal(err)
	}
	if err := (LinearWorkItemTransitionsClickHouseAdapter{Conn: conn}).WriteLinearWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatal(err)
	}
}
