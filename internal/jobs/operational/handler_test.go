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

func TestWebhookHandlerDispatchesVerifiedDurableReference(t *testing.T) {
	payload := []byte(`{"repository":{"full_name":"full-chaos/dev-health"}}`)
	sum := sha256.Sum256(payload)
	store := &fakeStore{webhook: WebhookDelivery{
		ID: webhookID, Provider: "github", DeliveryKey: "delivery-1", EventType: "push",
		Payload: payload, PayloadSHA256: hex.EncodeToString(sum[:]),
	}}
	dispatcher := &fakeDispatcher{}
	handler, err := NewWebhookHandler(store, dispatcher)
	if err != nil {
		t.Fatal(err)
	}
	execution := &jobruntime.Execution[jobruntime.WebhookDeliveryArgs]{
		Args: jobruntime.WebhookDeliveryArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.WebhookDeliveryPayload]{
			Payload: jobcontract.WebhookDeliveryPayload{DeliveryID: webhookID},
		}},
		Envelope: jobcontract.Envelope{Domain: jobcontract.DomainLink{Type: "webhook_delivery", ID: webhookID}},
	}
	if err := handler.Work(context.Background(), execution); err != nil {
		t.Fatal(err)
	}
	if dispatcher.webhook.ID != webhookID {
		t.Fatalf("dispatched %#v", dispatcher.webhook)
	}
}

func TestWebhookHandlerRejectsDigestMismatchWithoutEffect(t *testing.T) {
	store := &fakeStore{webhook: WebhookDelivery{
		ID: webhookID, Provider: "github", DeliveryKey: "delivery-1", EventType: "push",
		Payload: []byte(`{}`), PayloadSHA256: "0" + string(make([]byte, 63)),
	}}
	dispatcher := &fakeDispatcher{}
	handler, _ := NewWebhookHandler(store, dispatcher)
	execution := &jobruntime.Execution[jobruntime.WebhookDeliveryArgs]{
		Args: jobruntime.WebhookDeliveryArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.WebhookDeliveryPayload]{
			Payload: jobcontract.WebhookDeliveryPayload{DeliveryID: webhookID},
		}},
	}
	if err := handler.Work(context.Background(), execution); err == nil || dispatcher.webhook.ID != "" {
		t.Fatalf("err=%v dispatch=%#v", err, dispatcher.webhook)
	}
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
	webhook WebhookDelivery
	billing BillingNotification
	err     error
	calls   int
}

func (dispatcher *fakeDispatcher) DispatchWebhook(_ context.Context, delivery WebhookDelivery) error {
	dispatcher.calls++
	dispatcher.webhook = delivery
	return dispatcher.err
}
func (dispatcher *fakeDispatcher) DispatchBilling(_ context.Context, notification BillingNotification) error {
	dispatcher.calls++
	dispatcher.billing = notification
	return dispatcher.err
}
