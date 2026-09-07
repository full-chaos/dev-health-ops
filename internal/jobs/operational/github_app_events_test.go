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
	handler, err := NewWebhookHandler(installStore)
	if err != nil {
		t.Fatal(err)
	}
	recorded, restore := spyRecordIgnoredWebhookEvent()
	defer restore()
	if err := handler.Work(context.Background(), webhookExecution(webhookID)); err != nil {
		t.Fatal(err)
	}
	if installStore.calls != 1 || installStore.eventType != "installation" {
		t.Fatalf("native writer calls=%d eventType=%q", installStore.calls, installStore.eventType)
	}
	if *recorded {
		t.Fatal("installation events must never be recorded as ignored")
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
	handler, _ := NewWebhookHandler(installStore)
	recorded, restore := spyRecordIgnoredWebhookEvent()
	defer restore()
	if err := handler.Work(context.Background(), webhookExecution(webhookID)); err != nil {
		t.Fatal(err)
	}
	if installStore.calls != 1 || *recorded {
		t.Fatalf("native calls=%d recorded=%v", installStore.calls, *recorded)
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
	handler, _ := NewWebhookHandler(installStore)
	recorded, restore := spyRecordIgnoredWebhookEvent()
	defer restore()
	err := handler.Work(context.Background(), webhookExecution(webhookID))
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("Work = %v, want category %s", err, jobruntime.CategoryRetryable)
	}
	if strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("a store failure must be retryable, not permanent: %v", err)
	}
	if *recorded {
		t.Fatal("a retryable native-path failure must not also be recorded as ignored")
	}
}

// TestWebhookHandlerIgnoresInstallationEventWithoutInstallationWriter pins
// the optional-capability contract: a store that does NOT implement
// InstallationWriter (fakeStore, used by every other test in this package)
// falls through installation/marketplace_purchase's native branch, and --
// since CHAOS-5320 removed the HTTP fallback -- lands on the explicit-ignore
// path instead, same as any other event type with no matching writer.
func TestWebhookHandlerIgnoresInstallationEventWithoutInstallationWriter(t *testing.T) {
	payload := []byte(`{"action":"created","installation":{"id":123}}`)
	sum := sha256.Sum256(payload)
	store := &fakeStore{webhook: WebhookDelivery{
		ID: webhookID, Provider: "github", DeliveryKey: "delivery-1", EventType: "installation",
		Payload: payload, PayloadSHA256: hex.EncodeToString(sum[:]),
	}}
	handler, _ := NewWebhookHandler(store)
	recorded, restore := spyRecordIgnoredWebhookEvent()
	defer restore()
	if err := handler.Work(context.Background(), webhookExecution(webhookID)); err != nil {
		t.Fatal(err)
	}
	if !*recorded {
		t.Fatal("expected the ignore path since fakeStore implements neither InstallationWriter nor SyncDispatchWriter")
	}
}

// TestWebhookHandlerIgnoresPushEventWithoutSyncDispatchWriter is
// fakeInstallationStore's twin of the test above: it implements
// InstallationWriter but not SyncDispatchWriter, so a "push" event (native
// per isNativeSyncDispatchEvent) still falls through to the ignore path.
func TestWebhookHandlerIgnoresPushEventWithoutSyncDispatchWriter(t *testing.T) {
	payload := []byte(`{"repository":{"full_name":"full-chaos/dev-health"}}`)
	sum := sha256.Sum256(payload)
	installStore := &fakeInstallationStore{
		fakeStore: fakeStore{webhook: WebhookDelivery{
			ID: webhookID, Provider: "github", DeliveryKey: "delivery-1", EventType: "push",
			Payload: payload, PayloadSHA256: hex.EncodeToString(sum[:]),
		}},
	}
	handler, _ := NewWebhookHandler(installStore)
	recorded, restore := spyRecordIgnoredWebhookEvent()
	defer restore()
	if err := handler.Work(context.Background(), webhookExecution(webhookID)); err != nil {
		t.Fatal(err)
	}
	if installStore.calls != 0 {
		t.Fatalf("a push event must never reach the installation writer, got %d calls", installStore.calls)
	}
	if !*recorded {
		t.Fatal("expected the ignore path since fakeInstallationStore does not implement SyncDispatchWriter")
	}
}

var errUnavailableForTest = &staticError{"native installation store is unavailable"}

type staticError struct{ text string }

func (err *staticError) Error() string { return err.text }
