package revisionfeed

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

func outboxEvent(key string, payload string) audit.Event {
	return audit.Event{
		ID: 7, AggregateType: "membership", AggregateID: "mbr_EXAMPLE0000000000000001", EventType: "membership.changed",
		Payload: json.RawMessage(payload), IdempotencyKey: key, Attempts: 1,
		CreatedAt: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC),
	}
}

func TestFromOutboxMapsAnEventWhoseKeyAgreesWithItsRevision(t *testing.T) {
	got, err := FromOutbox(outboxEvent("membership:mbr_EXAMPLE0000000000000001:4",
		`{"revision":4,"organization_id":"org_1","actor_principal_id":"prn_1","request_id":"req_1","change":{"role":"admin"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != contracts.ProvisioningEventSurface || got.AggregateType != "membership" ||
		got.AggregateID != "mbr_EXAMPLE0000000000000001" || got.EventType != "membership.changed" ||
		got.Payload.Revision != 4 || got.Payload.OrganizationID != "org_1" || got.DedupeKey() != got.IdempotencyKey {
		t.Fatalf("mapped event %+v", got)
	}
	if !got.CreatedAt.Time.Equal(time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("created_at %v", got.CreatedAt.Time)
	}
}

func TestFromOutboxRefusesAKeyThatDisagreesWithTheRevision(t *testing.T) {
	for name, tc := range map[string]struct{ key, payload string }{
		"revision differs":   {"membership:mbr_EXAMPLE0000000000000001:5", `{"revision":4,"change":{}}`},
		"other aggregate id": {"membership:mbr_OTHER:4", `{"revision":4,"change":{}}`},
		"other aggregate":    {"principal:mbr_EXAMPLE0000000000000001:4", `{"revision":4,"change":{}}`},
		"empty key":          {"", `{"revision":4,"change":{}}`},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := FromOutbox(outboxEvent(tc.key, tc.payload))
			if !errors.Is(err, ErrKeyDisagrees) {
				t.Fatalf("err = %v, want ErrKeyDisagrees", err)
			}
		})
	}
}

func TestFromOutboxRefusesAPayloadThatIsNoRevision(t *testing.T) {
	for name, payload := range map[string]string{
		"not json":       `{`,
		"no revision":    `{"change":{}}`,
		"zero revision":  `{"revision":0,"change":{}}`,
		"decimal form":   `{"revision":4.0,"change":{}}`,
		"exponent form":  `{"revision":4e0,"change":{}}`,
		"negative":       `{"revision":-1,"change":{}}`,
		"string":         `{"revision":"4","change":{}}`,
		"empty document": ``,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := FromOutbox(outboxEvent("membership:mbr_EXAMPLE0000000000000001:4", payload))
			if !errors.Is(err, ErrNotAProvisioningEvent) {
				t.Fatalf("err = %v, want ErrNotAProvisioningEvent", err)
			}
		})
	}
}

func TestFromOutboxErrorsNeverQuoteThePayloadOrTheID(t *testing.T) {
	e := outboxEvent("membership:mbr_EXAMPLE0000000000000001:9", `{"revision":4,"change":{"note":"SENTINEL_PAYLOAD_VALUE"}}`)
	e.AggregateID = "mbr_SENTINEL_ID_0001"
	_, err := FromOutbox(e)
	if err == nil {
		t.Fatal("want an error")
	}
	for _, leak := range []string{"SENTINEL_PAYLOAD_VALUE", "SENTINEL_ID_0001", "EXAMPLE0000000000000001"} {
		if strings.Contains(err.Error(), leak) {
			t.Fatalf("the error holds %q: %v", leak, err)
		}
	}
}

func TestHandleOutboxAppliesAMappedEvent(t *testing.T) {
	c, rec := newTestConsumer(t)
	got, err := c.HandleOutbox(t.Context(), outboxEvent("membership:mbr_EXAMPLE0000000000000001:1", `{"revision":1,"change":{}}`))
	if err != nil || got != contracts.Apply {
		t.Fatalf("HandleOutbox = %q, %v", got, err)
	}
	if calls := rec.snapshot(); len(calls) != 1 || calls[0].typ != "membership" || calls[0].revision != 1 {
		t.Fatalf("calls %+v", calls)
	}
	if _, err := c.HandleOutbox(t.Context(), outboxEvent("membership:mbr_EXAMPLE0000000000000001:2", `{"revision":1,"change":{}}`)); !errors.Is(err, ErrKeyDisagrees) {
		t.Fatalf("a disagreeing key: err = %v", err)
	}
	if n := len(rec.snapshot()); n != 1 {
		t.Fatalf("a refused event invalidated: %d calls", n)
	}
}
