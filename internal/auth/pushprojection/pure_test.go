package pushprojection

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/outboxprojection"
)

const rowOrg = "7f1c0d2e-0000-4000-8000-0000000000a1"

func validRow(t *testing.T) OutboxRow {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"revision": 2, "organization_id": "org_" + rowOrg, "source_id": "src-1",
		"change": map[string]any{"scopes": []string{"ingest:write"}}})
	if err != nil {
		t.Fatal(err)
	}
	return OutboxRow{ID: 41, AggregateID: "0a1b2c3d-0000-4000-8000-000000000001", EventType: contracts.ExternalPushScoped,
		IdempotencyKey: "external_push_credential:0a1b2c3d-0000-4000-8000-000000000001:2", Payload: payload,
		CreatedAt: time.Date(2026, 11, 1, 10, 0, 0, 123456000, time.UTC)}
}

func TestEventFromRowBuildsTheMappedPushEvent(t *testing.T) {
	in, err := EventFromRow(validRow(t))
	if err != nil {
		t.Fatal(err)
	}
	e := in.Event
	if in.Domain != outboxprojection.DomainExternalPush || e.AggregateType != contracts.ExternalPushCredentialAggregate ||
		e.EventType != contracts.ExternalPushScoped || e.Payload.Revision != 2 || e.Payload.SourceID != "src-1" ||
		e.Payload.OrganizationID != "org_"+rowOrg || len(e.Payload.Change.Scopes) != 1 || !e.CreatedAt.Time.Equal(validRow(t).CreatedAt) {
		t.Fatalf("event %+v", in)
	}
	if e.IdempotencyKey != validRow(t).IdempotencyKey {
		t.Fatalf("idempotency key %q", e.IdempotencyKey)
	}
}

func TestEventFromRowRefusesWhatIsNotAPushEvent(t *testing.T) {
	for name, edit := range map[string]func(*OutboxRow){
		"payload not json":    func(r *OutboxRow) { r.Payload = []byte("{") },
		"payload is an array": func(r *OutboxRow) { r.Payload = []byte("[]") },
		"revision is a decimal": func(r *OutboxRow) {
			r.Payload = []byte(`{"revision":1.5,"organization_id":"o","source_id":"s","change":{}}`)
		},
		"revision missing": func(r *OutboxRow) { r.Payload = []byte(`{"organization_id":"o","source_id":"s","change":{}}`) },
		"no aggregate id":  func(r *OutboxRow) { r.AggregateID = "" },
	} {
		row := validRow(t)
		edit(&row)
		if _, err := EventFromRow(row); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	noSource := validRow(t)
	noSource.Payload = []byte(`{"revision":2,"organization_id":"org_` + rowOrg + `","change":{}}`)
	in, err := EventFromRow(noSource)
	if err != nil {
		t.Fatalf("a row with no source_id decodes (the engine refuses it as malformed): %v", err)
	}
	if in.Check() == nil {
		t.Fatal("an event with no source_id must fail Input.Check")
	}
}

func TestRetryDelayDoublesAndIsCapped(t *testing.T) {
	want := []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second, 160 * time.Second, 300 * time.Second, 300 * time.Second}
	for attempts, d := range want {
		if got := RetryDelay(attempts); got != d {
			t.Errorf("RetryDelay(%d) = %s, want %s", attempts, got, d)
		}
	}
	if RetryDelay(-3) != 5*time.Second || RetryDelay(1<<20) != 300*time.Second {
		t.Error("RetryDelay is not total")
	}
}

func TestConfigRefusesWhatCannotRunARead(t *testing.T) {
	base := Config{Horizon: time.Hour, BatchLimit: 50, PollInterval: time.Second}
	if err := base.validateShape(); err != nil {
		t.Fatalf("a valid shape: %v", err)
	}
	for name, edit := range map[string]func(*Config){
		"no horizon":           func(c *Config) { c.Horizon = 0 },
		"horizon over a day":   func(c *Config) { c.Horizon = 25 * time.Hour },
		"batch zero":           func(c *Config) { c.BatchLimit = 0 },
		"batch over the cap":   func(c *Config) { c.BatchLimit = 1001 },
		"poll under the floor": func(c *Config) { c.PollInterval = time.Millisecond },
	} {
		c := base
		edit(&c)
		if err := c.validateShape(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestFloorKeyIsTheAggregateKeyOfTheFence(t *testing.T) {
	if got := FloorKey("0a1b"); got != "external_push_credential:0a1b" {
		t.Fatalf("FloorKey = %q", got)
	}
	if FloorInputName != "push-projection-store" {
		t.Fatalf("FloorInputName = %q", FloorInputName)
	}
}
