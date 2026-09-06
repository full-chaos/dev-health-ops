package operational

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// fakeInstallationStore embeds fakeStore so it satisfies DeliveryStore, and
// additionally implements InstallationWriter -- the shape WebhookHandler.Work
// type-asserts for.
type fakeInstallationStore struct {
	fakeStore
	eventType string
	payload   []byte
	result    GithubAppEventResult
	err       error
	calls     int
}

func (store *fakeInstallationStore) UpsertGithubAppEvent(
	_ context.Context, eventType string, payload []byte, _ time.Time,
) (GithubAppEventResult, error) {
	store.calls++
	store.eventType = eventType
	store.payload = payload
	return store.result, store.err
}

func webhookExecution(deliveryID string) *jobruntime.Execution[jobruntime.WebhookDeliveryArgs] {
	return &jobruntime.Execution[jobruntime.WebhookDeliveryArgs]{
		Args: jobruntime.WebhookDeliveryArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.WebhookDeliveryPayload]{
			Payload: jobcontract.WebhookDeliveryPayload{DeliveryID: deliveryID},
		}},
		Envelope: jobcontract.Envelope{Domain: jobcontract.DomainLink{Type: "webhook_delivery", ID: deliveryID}},
	}
}

func TestWebhookHandlerRoutesInstallationEventsNativelyWithoutHTTPDispatch(t *testing.T) {
	payload := []byte(`{"action":"created","installation":{"id":123}}`)
	sum := sha256.Sum256(payload)
	installStore := &fakeInstallationStore{
		fakeStore: fakeStore{webhook: WebhookDelivery{
			ID: webhookID, Provider: "github", DeliveryKey: "delivery-1", EventType: "installation",
			Payload: payload, PayloadSHA256: hex.EncodeToString(sum[:]),
		}},
		result: GithubAppEventResult{Processed: true, InstallationID: 123, Action: "created"},
	}
	dispatcher := &fakeDispatcher{}
	handler, err := NewWebhookHandler(installStore, dispatcher)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), webhookExecution(webhookID)); err != nil {
		t.Fatal(err)
	}
	if installStore.calls != 1 || installStore.eventType != "installation" {
		t.Fatalf("native writer calls=%d eventType=%q", installStore.calls, installStore.eventType)
	}
	if dispatcher.calls != 0 {
		t.Fatalf("HTTP dispatcher was called %d times; installation events must never reach it", dispatcher.calls)
	}
}

func TestWebhookHandlerRoutesMarketplacePurchaseNativelyWithoutHTTPDispatch(t *testing.T) {
	payload := []byte(`{}`)
	sum := sha256.Sum256(payload)
	installStore := &fakeInstallationStore{
		fakeStore: fakeStore{webhook: WebhookDelivery{
			ID: webhookID, Provider: "github", DeliveryKey: "delivery-1", EventType: "marketplace_purchase",
			Payload: payload, PayloadSHA256: hex.EncodeToString(sum[:]),
		}},
		result: GithubAppEventResult{Processed: true, Action: "marketplace_purchase"},
	}
	dispatcher := &fakeDispatcher{}
	handler, _ := NewWebhookHandler(installStore, dispatcher)
	if err := handler.Work(context.Background(), webhookExecution(webhookID)); err != nil {
		t.Fatal(err)
	}
	if installStore.calls != 1 || dispatcher.calls != 0 {
		t.Fatalf("native calls=%d dispatcher calls=%d", installStore.calls, dispatcher.calls)
	}
}

func TestWebhookHandlerNativeInstallationFailureIsRetryable(t *testing.T) {
	payload := []byte(`{"action":"created","installation":{"id":123}}`)
	sum := sha256.Sum256(payload)
	installStore := &fakeInstallationStore{
		fakeStore: fakeStore{webhook: WebhookDelivery{
			ID: webhookID, Provider: "github", DeliveryKey: "delivery-1", EventType: "installation",
			Payload: payload, PayloadSHA256: hex.EncodeToString(sum[:]),
		}},
		err: errUnavailableForTest,
	}
	dispatcher := &fakeDispatcher{}
	handler, _ := NewWebhookHandler(installStore, dispatcher)
	err := handler.Work(context.Background(), webhookExecution(webhookID))
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("Work = %v, want category %s", err, jobruntime.CategoryRetryable)
	}
	if strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("a store failure must be retryable, not permanent: %v", err)
	}
	if dispatcher.calls != 0 {
		t.Fatalf("dispatcher must not be called on a native-path failure, got %d calls", dispatcher.calls)
	}
}

// TestWebhookHandlerFallsBackToHTTPDispatchWithoutInstallationWriter pins the
// optional-capability contract: a store that does NOT implement
// InstallationWriter (fakeStore, used by every other test in this package)
// must still dispatch installation/marketplace_purchase events over HTTP
// exactly as before -- this is the backward-compatible fallback the type
// assertion in Work exists to provide.
func TestWebhookHandlerFallsBackToHTTPDispatchWithoutInstallationWriter(t *testing.T) {
	payload := []byte(`{"action":"created","installation":{"id":123}}`)
	sum := sha256.Sum256(payload)
	store := &fakeStore{webhook: WebhookDelivery{
		ID: webhookID, Provider: "github", DeliveryKey: "delivery-1", EventType: "installation",
		Payload: payload, PayloadSHA256: hex.EncodeToString(sum[:]),
	}}
	dispatcher := &fakeDispatcher{}
	handler, _ := NewWebhookHandler(store, dispatcher)
	if err := handler.Work(context.Background(), webhookExecution(webhookID)); err != nil {
		t.Fatal(err)
	}
	if dispatcher.webhook.ID != webhookID {
		t.Fatalf("expected the fallback HTTP dispatch, got %#v", dispatcher.webhook)
	}
}

func TestWebhookHandlerNonGithubAppEventsStillDispatchOverHTTP(t *testing.T) {
	payload := []byte(`{"repository":{"full_name":"full-chaos/dev-health"}}`)
	sum := sha256.Sum256(payload)
	installStore := &fakeInstallationStore{
		fakeStore: fakeStore{webhook: WebhookDelivery{
			ID: webhookID, Provider: "github", DeliveryKey: "delivery-1", EventType: "push",
			Payload: payload, PayloadSHA256: hex.EncodeToString(sum[:]),
		}},
	}
	dispatcher := &fakeDispatcher{}
	handler, _ := NewWebhookHandler(installStore, dispatcher)
	if err := handler.Work(context.Background(), webhookExecution(webhookID)); err != nil {
		t.Fatal(err)
	}
	if installStore.calls != 0 {
		t.Fatalf("a push event must never reach the installation writer, got %d calls", installStore.calls)
	}
	if dispatcher.webhook.ID != webhookID {
		t.Fatalf("expected push to dispatch over HTTP, got %#v", dispatcher.webhook)
	}
}

var errUnavailableForTest = &staticError{"native installation store is unavailable"}

type staticError struct{ text string }

func (err *staticError) Error() string { return err.text }
