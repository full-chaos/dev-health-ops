package revisionfeed

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// pushRow is an external_push_credential row exactly as audit.AppendPushLifecycle
// writes it: the payload member of the wire event, the aggregate and the key in
// their columns.
func pushRow(t *testing.T, id string, revision int64, eventType string, change contracts.ExternalPushLifecycleChange) audit.Event {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"revision": revision, "organization_id": "org_EXAMPLE0000000000000001",
		"binding": "source", "source_id": "src_EXAMPLE0000000000000001", "change": change})
	if err != nil {
		t.Fatal(err)
	}
	return audit.Event{ID: 9, AggregateType: contracts.ExternalPushCredentialAggregate, AggregateID: id, EventType: eventType,
		Payload: payload, IdempotencyKey: "external_push_credential:" + id + ":" + itoa(revision),
		CreatedAt: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)}
}

func itoa(n int64) string { b, _ := json.Marshal(n); return string(b) }

// TestAnExternalPushRowRidesTheRevisionFeed: with AUTH_REVISION_FEED on, a
// push lifecycle row is delivered, applied by the consumer as a per-aggregate
// revision, and neither rejected nor stalled. The feed carries only the
// aggregate key and the revision, so the aggregate type needs no registration.
func TestAnExternalPushRowRidesTheRevisionFeed(t *testing.T) {
	client := &fakeClient{evalResult: 1}
	sink, err := NewSink(client)
	if err != nil {
		t.Fatal(err)
	}
	created := pushRow(t, "cred-1", 1, contracts.ExternalPushCreated, contracts.ExternalPushLifecycleChange{
		Status: "active", Fingerprint: "0123456789abcdef", Scopes: []string{"ingest:write"}})
	revoked := pushRow(t, "cred-1", 2, contracts.ExternalPushRevoked, contracts.ExternalPushLifecycleChange{Status: "revoked"})
	for _, row := range []audit.Event{created, revoked} {
		if err := sink.Deliver(context.Background(), row); err != nil {
			t.Fatalf("Deliver %s: %v (reason %q)", row.EventType, err, reasonOf(err))
		}
	}
	calls := client.calls()
	if len(calls) != 2 || calls[1].args[0] != "external_push_credential:cred-1" || calls[1].args[1] != "2" {
		t.Fatalf("script calls = %+v", calls)
	}

	consumer, rec := newTestConsumer(t)
	for i, row := range []audit.Event{created, revoked, revoked} {
		got, err := consumer.HandleOutbox(context.Background(), row)
		if err != nil {
			t.Fatalf("HandleOutbox %d: %v", i, err)
		}
		if want := []contracts.Disposition{contracts.Apply, contracts.Apply, contracts.Ignore}[i]; got != want {
			t.Fatalf("row %d: disposition %s, want %s", i, got, want)
		}
	}
	invalidated := rec.snapshot()
	if len(invalidated) != 2 || invalidated[1].typ != contracts.ExternalPushCredentialAggregate || invalidated[1].id != "cred-1" || invalidated[1].revision != 2 {
		t.Fatalf("invalidations = %+v, want the push aggregate at revisions 1 and 2", invalidated)
	}
}
