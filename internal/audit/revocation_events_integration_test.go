//go:build integration

package audit

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// TestRevocationEventsCommitAsProvisioningEvents: the session, signing-key and
// scope revocation events of provisioning-event.v1 (CHAOS-11816) pass Commit,
// get contiguous per-aggregate revisions from aggregate_revisions (the 0046
// CHECK admits signing_key) and each outbox row parses as the wire contract.
// session is one-shot: revision 1, no aggregate_revisions row, and a second
// revoke of the same session (a child revoked with its parent, then on its own
// row) commits and writes no second row; a plain insert would abort it on the
// unique idempotency key and fail the logout.
func TestRevocationEventsCommitAsProvisioningEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	runtime := runtimeRolePool(t, ctx, env.pool)
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.NewString()
	if _, err := env.pool.Exec(ctx, `INSERT INTO auth.principals (id, kind) VALUES ($1, 'user')`, actor); err != nil {
		t.Fatal(err)
	}
	session, key, principal, org := "ses_"+uuid.NewString(), "key_"+uuid.NewString(), "prn_"+uuid.NewString(), "org_"+uuid.NewString()
	scope := map[string]any{"revoked_before": "2026-09-03T09:59:59.123456Z", "reason": "password_changed"}
	events := []OutboxEvent{
		{AggregateType: "session", AggregateID: session, EventType: "session.revoked", Change: map[string]any{"status": "revoked", "reason": "caller"}},
		{AggregateType: "signing_key", AggregateID: key, EventType: "signing_key.compromised", Change: map[string]any{"kid": "example.key-2026-09"}},
		{AggregateType: "principal", AggregateID: principal, EventType: "principal.sessions_revoked", Change: scope},
		{AggregateType: "organization", AggregateID: org, EventType: "organization.sessions_revoked", Change: scope},
	}
	for round := 0; round < 2; round++ {
		err := Commit(ctx, runtime, env.schema, Mutation{
			Apply: func(ctx context.Context, tx TxOps) error {
				_, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug) VALUES (gen_random_uuid(), $1, $1)`, "rev-"+uuid.NewString())
				return err
			},
			Audit:  AuditEvent{EventType: "organization.updated", Outcome: OutcomeAllowed, ActorPrincipalID: &actor},
			Events: events,
		})
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
	}
	cursor := contracts.NewConsumerCursor()
	rows := envelopeRows(t, ctx, env)
	if want := 2*len(events) - 1; len(rows) != want {
		t.Fatalf("%d outbox rows, want %d (the second session.revoked is the same fact and writes no row)", len(rows), want)
	}
	for _, r := range rows {
		event, err := contracts.ParseProvisioningEvent(root, []byte(r.raw))
		if err != nil {
			t.Fatalf("row %d is not provisioning-event.v1: %v\n%s", r.id, err, r.raw)
		}
		if d := cursor.Observe(event); d != contracts.Apply {
			t.Errorf("%s rev %d: consumer disposition %s, want apply", event.AggregateType, event.Payload.Revision, d)
		}
	}
	var sessionCounters int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM auth.aggregate_revisions WHERE aggregate_type = 'session'`).Scan(&sessionCounters); err != nil {
		t.Fatal(err)
	}
	if sessionCounters != 0 {
		t.Errorf("%d aggregate_revisions rows for the one-shot session aggregate, want none", sessionCounters)
	}
	for _, e := range events {
		if e.AggregateType == "session" {
			continue
		}
		var counter int64
		if err := env.pool.QueryRow(ctx, `SELECT revision FROM auth.aggregate_revisions WHERE aggregate_type = $1 AND aggregate_id = $2`,
			e.AggregateType, e.AggregateID).Scan(&counter); err != nil {
			t.Fatalf("%s has no revision row: %v", e.AggregateType, err)
		}
		if counter != 2 {
			t.Errorf("%s counter = %d, want 2", e.AggregateType, counter)
		}
	}
}
