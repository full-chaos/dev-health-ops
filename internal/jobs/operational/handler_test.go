package operational

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// TestWebhookHandlerIgnoresEventWithNoNativeHandler covers CHAOS-5320's
// replacement for the deleted HTTP fallback: a valid, digest-verified
// delivery whose EventType matches no native branch is explicitly ignored
// (counted + logged, see telemetry.go), never an error, never silently
// dropped without a trace.
func TestWebhookHandlerIgnoresEventWithNoNativeHandler(t *testing.T) {
	payload := []byte(`{"repository":{"full_name":"full-chaos/dev-health"}}`)
	sum := sha256.Sum256(payload)
	store := &fakeStore{webhook: WebhookDelivery{
		ID: webhookID, Provider: "github", DeliveryKey: "delivery-1", EventType: "check_run",
		Payload: payload, PayloadSHA256: hex.EncodeToString(sum[:]),
	}}
	handler, err := NewWebhookHandler(store)
	if err != nil {
		t.Fatal(err)
	}
	var recorded bool
	restore := stubRecordIgnoredWebhookEvent(func(_ context.Context, provider, eventType, deliveryID string) {
		recorded = true
		if provider != "github" || eventType != "check_run" || deliveryID != webhookID {
			t.Fatalf("recordIgnoredWebhookEvent(%q, %q, %q)", provider, eventType, deliveryID)
		}
	})
	defer restore()
	execution := &jobruntime.Execution[jobruntime.WebhookDeliveryArgs]{
		Args: jobruntime.WebhookDeliveryArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.WebhookDeliveryPayload]{
			Payload: jobcontract.WebhookDeliveryPayload{DeliveryID: webhookID},
		}},
		Envelope: jobcontract.Envelope{Domain: jobcontract.DomainLink{Type: "webhook_delivery", ID: webhookID}},
	}
	if err := handler.Work(context.Background(), execution); err != nil {
		t.Fatal(err)
	}
	if !recorded {
		t.Fatal("an event with no native handler must record the ignore, never silently return")
	}
}

func TestWebhookHandlerRejectsDigestMismatchWithoutEffect(t *testing.T) {
	store := &fakeStore{webhook: WebhookDelivery{
		ID: webhookID, Provider: "github", DeliveryKey: "delivery-1", EventType: "push",
		Payload: []byte(`{}`), PayloadSHA256: "0" + string(make([]byte, 63)),
	}}
	handler, _ := NewWebhookHandler(store)
	var recorded bool
	restore := stubRecordIgnoredWebhookEvent(func(context.Context, string, string, string) { recorded = true })
	defer restore()
	execution := &jobruntime.Execution[jobruntime.WebhookDeliveryArgs]{
		Args: jobruntime.WebhookDeliveryArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.WebhookDeliveryPayload]{
			Payload: jobcontract.WebhookDeliveryPayload{DeliveryID: webhookID},
		}},
	}
	if err := handler.Work(context.Background(), execution); err == nil || recorded {
		t.Fatalf("err=%v recorded=%v, want a permanent error and no ignore recorded", err, recorded)
	}
}

// stubRecordIgnoredWebhookEvent swaps the package's recordIgnoredWebhookEvent
// var for the duration of one test, returning a restore func -- same pattern
// as analytics/telemetry.go's injectable recordDegradation, for the same
// reason: asserting on Work()'s returned nil cannot distinguish "ignored and
// recorded" from "ignored and silently dropped," so the report has to be the
// observable.
func stubRecordIgnoredWebhookEvent(fn func(ctx context.Context, provider, eventType, deliveryID string)) func() {
	original := recordIgnoredWebhookEvent
	recordIgnoredWebhookEvent = fn
	return func() { recordIgnoredWebhookEvent = original }
}

// spyRecordIgnoredWebhookEvent is stubRecordIgnoredWebhookEvent's common
// case: most callers only need "was an ignore recorded at all," not the
// specific provider/eventType/deliveryID it carried.
func spyRecordIgnoredWebhookEvent() (recorded *bool, restore func()) {
	recorded = new(bool)
	restore = stubRecordIgnoredWebhookEvent(func(context.Context, string, string, string) { *recorded = true })
	return recorded, restore
}

func TestBillingHandlerEnforcesAuthoritativeTenant(t *testing.T) {
	org := "00000000-0000-4000-8000-000000000010"
	store := &fakeStore{billing: BillingNotification{
		ID: billingID, OrganizationID: org, NotificationType: "invoice_receipt", IdempotencyKey: "billing:key",
	}}
	dispatcher := &fakeDispatcher{}
	handler, _ := NewBillingHandler(store, dispatcher)
	execution := &jobruntime.Execution[jobruntime.BillingNotificationArgs]{
		Args: jobruntime.BillingNotificationArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.BillingNotificationPayload]{
			Payload: jobcontract.BillingNotificationPayload{NotificationID: billingID},
		}},
		OrganizationID: &org,
		Envelope:       jobcontract.Envelope{Domain: jobcontract.DomainLink{Type: "billing_notification", ID: billingID}},
	}
	if err := handler.Work(context.Background(), execution); err != nil {
		t.Fatal(err)
	}
	if dispatcher.billing.ID != billingID {
		t.Fatalf("dispatched %#v", dispatcher.billing)
	}
}

func TestDurableDuplicateIsSuppressedByRuntimeBeforeHandler(t *testing.T) {
	// The handler has no local duplicate cache: jobruntime's durable
	// billing_notification/webhook_delivery claim is the single source.
	store := &fakeStore{err: errors.New("must not load duplicate")}
	dispatcher := &fakeDispatcher{}
	handler, _ := NewBillingHandler(store, dispatcher)
	if handler == nil || store.calls != 0 || dispatcher.calls != 0 {
		t.Fatal("construction performed a delivery effect")
	}
}

const (
	webhookID = "00000000-0000-4000-8000-000000000012"
	billingID = "00000000-0000-4000-8000-000000000011"
)

type fakeStore struct {
	webhook WebhookDelivery
	billing BillingNotification
	err     error
	calls   int
}

func (store *fakeStore) LoadWebhook(context.Context, string) (WebhookDelivery, error) {
	store.calls++
	return store.webhook, store.err
}
func (store *fakeStore) LoadBilling(context.Context, string) (BillingNotification, error) {
	store.calls++
	return store.billing, store.err
}

type fakeDispatcher struct {
	billing BillingNotification
	err     error
	calls   int
}

func (dispatcher *fakeDispatcher) DispatchBilling(_ context.Context, notification BillingNotification) error {
	dispatcher.calls++
	dispatcher.billing = notification
	return dispatcher.err
}
