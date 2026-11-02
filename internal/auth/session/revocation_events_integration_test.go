//go:build integration

package session

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionevent"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

// revocationRow is one revocation event as the stream carries it.
type revocationRow struct {
	aggregateType, aggregateID, eventType string
	reason                                string
	revokedBefore                         time.Time
}

// revocationRows reads every session, principal and organization scope event of the outbox, parsed against
// provisioning-event.v1 (a row that is not the wire contract fails the test).
func (e *env) revocationRows(t *testing.T, ctx context.Context) []revocationRow {
	t.Helper()
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := e.admin.Query(ctx, `SELECT json_build_object(
	'schema_version', 'provisioning-event.v1', 'aggregate_type', aggregate_type, 'aggregate_id', aggregate_id,
	'event_type', event_type, 'idempotency_key', idempotency_key,
	'created_at', to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'), 'payload', payload)::text
FROM auth.auth_outbox_events WHERE event_type IN ('session.revoked', 'principal.sessions_revoked', 'organization.sessions_revoked') ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []revocationRow
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		event, err := contracts.ParseProvisioningEvent(root, []byte(raw))
		if err != nil {
			t.Fatalf("outbox row is not provisioning-event.v1: %v\n%s", err, raw)
		}
		row := revocationRow{aggregateType: event.AggregateType, aggregateID: event.AggregateID, eventType: event.EventType}
		var change struct {
			Reason        string `json:"reason"`
			RevokedBefore string `json:"revoked_before"`
		}
		if err := unmarshalChange(event, &change); err != nil {
			t.Fatal(err)
		}
		row.reason = change.Reason
		if change.RevokedBefore != "" {
			if row.revokedBefore, err = time.Parse(time.RFC3339Nano, change.RevokedBefore); err != nil {
				t.Fatal(err)
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func (e *env) sessionEvents(t *testing.T, ctx context.Context, id uuid.UUID) []revocationRow {
	t.Helper()
	var out []revocationRow
	for _, r := range e.revocationRows(t, ctx) {
		if r.aggregateType == "session" && r.aggregateID == sessionid.Wire(id) {
			out = append(out, r)
		}
	}
	return out
}

func (e *env) revokedAt(t *testing.T, ctx context.Context, id uuid.UUID) time.Time {
	t.Helper()
	var at time.Time
	if err := e.admin.QueryRow(ctx, `SELECT revoked_at FROM auth.sessions WHERE id = $1`, id.String()).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at
}

// One explicit revoke is one session.revoked at revision 1; the repeat revoke changes nothing and writes none.
func TestRevokeWritesOneSessionEvent(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))
	for i, want := range []bool{true, false} {
		if ok, err := e.store.Revoke(ctx, issued.Session.ID, Actor{}); err != nil || ok != want {
			t.Fatalf("Revoke %d = %v, %v; want %v", i, ok, err, want)
		}
	}
	events := e.sessionEvents(t, ctx, issued.Session.ID)
	if len(events) != 1 || events[0].reason != sessionevent.ReasonCaller {
		t.Fatalf("events %+v, want one session.revoked with reason caller", events)
	}
	var counters int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.aggregate_revisions WHERE aggregate_type = 'session'`).Scan(&counters); err != nil || counters != 0 {
		t.Fatalf("%d aggregate_revisions rows for a one-shot session (err %v), want none", counters, err)
	}
}

// A reused refresh credential revokes its session with the refresh_reuse reason, once under 16 concurrent exchanges.
func TestReuseWritesOneRefreshReuseEvent(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))
	e.rotatedAgo(t, ctx, issued, GraceWindow+time.Second)
	parallel(16, func(int) { _, _ = e.store.Exchange(ctx, issued.Credential) })
	events := e.sessionEvents(t, ctx, issued.Session.ID)
	if len(events) != 1 || events[0].reason != sessionevent.ReasonRefreshReuse {
		t.Fatalf("events %+v, want one session.revoked with reason refresh_reuse", events)
	}
}

// A bulk end is ONE scope event whose revoked_before is the instant written as revoked_at on every session it ended, and
// no per-session event; another principal's session is untouched.
func TestRevokeAllWritesOneScopeEvent(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	principal := e.principal(t, ctx)
	var ids []uuid.UUID
	for i := 0; i < 3; i++ {
		ids = append(ids, e.create(t, ctx, principal).Session.ID)
	}
	other := e.create(t, ctx, e.principal(t, ctx))
	if n, err := e.store.RevokeAll(ctx, principal, Actor{}); err != nil || n != 3 {
		t.Fatalf("RevokeAll = %d, %v; want 3", n, err)
	}
	rows := e.revocationRows(t, ctx)
	if len(rows) != 1 || rows[0].eventType != "principal.sessions_revoked" || rows[0].reason != sessionevent.ReasonCaller {
		t.Fatalf("events %+v, want one principal.sessions_revoked with reason caller", rows)
	}
	for _, id := range ids {
		if at := e.revokedAt(t, ctx, id); !at.Equal(rows[0].revokedBefore) {
			t.Fatalf("session %s revoked_at %s, event revoked_before %s: not the same instant", id, at, rows[0].revokedBefore)
		}
	}
	if e.sessionRevoked(t, ctx, other.Session.ID) {
		t.Fatal("RevokeAll revoked another principal's session")
	}
	if rows := e.revocationRows(t, ctx); len(rows) != 1 {
		t.Fatalf("%d events", len(rows))
	}
}

// Revoking a web session tells the stream of the web session and of its live LINKED client session (it ends by read of the
// parent, no row written), not of an unlinked MCP client's; revoking the client's own row afterwards is the same fact:
// it commits and writes no second row.
func testRevokingAWebSessionTellsTheStreamOfItsLinkedClientSessions(t *testing.T, ctx context.Context, d *delegationEnv) {
	c := d.clientCase(t, ctx)
	linked, err := c.client(ctx, c.web.Session.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	mcp, err := c.client(ctx, c.web.Session.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := c.owned.Revoke(ctx, c.web.Session.ID, Actor{}); err != nil || !ok {
		t.Fatalf("Revoke web = %v, %v", ok, err)
	}
	got := map[string]int{}
	for _, r := range c.eventsOf(t, ctx, c.web.Session.ID, linked.ID, mcp.ID) {
		got[r.aggregateID]++
	}
	if len(got) != 2 || got[sessionid.Wire(c.web.Session.ID)] != 1 || got[sessionid.Wire(linked.ID)] != 1 {
		t.Fatalf("events by session %v, want the web and the linked client once each (the unlinked MCP session lives on)", got)
	}
	if _, err := c.owned.Revoke(ctx, linked.ID, Actor{}); err != nil {
		t.Fatalf("Revoke of the client row after its parent: %v", err)
	}
	if n := len(c.eventsOf(t, ctx, c.web.Session.ID, linked.ID, mcp.ID)); n != 2 {
		t.Fatalf("%d events after revoking the client row, want still 2", n)
	}
}

func (d *delegationEnv) eventsOf(t *testing.T, ctx context.Context, ids ...uuid.UUID) []revocationRow {
	t.Helper()
	wanted := map[string]bool{}
	for _, id := range ids {
		wanted[sessionid.Wire(id)] = true
	}
	var out []revocationRow
	for _, r := range d.env.revocationRows(t, ctx) {
		if r.aggregateType == "session" && wanted[r.aggregateID] {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].aggregateID < out[j].aggregateID })
	return out
}

func unmarshalChange(event contracts.ProvisioningEvent, into any) error {
	raw, err := json.Marshal(event.Payload.Change)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, into)
}
