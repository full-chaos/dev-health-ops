package operational

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// fakeSyncDispatchStore embeds fakeStore so it satisfies DeliveryStore, and
// additionally implements SyncDispatchWriter -- the shape WebhookHandler.Work
// type-asserts for.
type fakeSyncDispatchStore struct {
	fakeStore
	provider  string
	eventType string
	result    SyncDispatchResult
	err       error
	calls     int
}

func (store *fakeSyncDispatchStore) TriggerScopedSync(
	_ context.Context, provider, eventType string, _ []byte, _ time.Time,
) (SyncDispatchResult, error) {
	store.calls++
	store.provider = provider
	store.eventType = eventType
	return store.result, store.err
}

func webhookDeliveryFor(provider, eventType string, payload []byte) WebhookDelivery {
	sum := sha256.Sum256(payload)
	return WebhookDelivery{
		ID: webhookID, Provider: provider, DeliveryKey: "delivery-1", EventType: eventType,
		Payload: payload, PayloadSHA256: hex.EncodeToString(sum[:]),
		CreatedAt: time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC),
	}
}

func TestIsNativeSyncDispatchEvent(t *testing.T) {
	cases := []struct {
		provider, eventType string
		want                bool
	}{
		{"github", "push", true},
		{"github", "pull_request", true},
		{"github", "issue_created", true},
		{"github", "issue_updated", true},
		{"github", "issue_closed", true},
		{"github", "deployment", true},
		{"github", "workflow_run", true},
		{"github", "installation", false},
		{"github", "marketplace_purchase", false},
		{"github", "release", false},
		{"github", "issue_comment", false},
		{"gitlab", "push", true},
		{"gitlab", "merge_request", true},
		{"gitlab", "issue_created", true},
		{"gitlab", "pipeline", true},
		{"gitlab", "tag_push", false},
		// CHAOS-5320: jira is no longer excluded -- issue_created/updated/
		// deleted are the exact set _process_jira_event handles today
		// (JIRA_EVENT_MAP's three canonical values). The raw provider
		// webhookEvent string ("jira:issue_created") is never what reaches
		// this function -- delivery.EventType is the already-canonicalised
		// value (map_jira_event's output), same as github/gitlab.
		{"jira", "issue_created", true},
		{"jira", "issue_updated", true},
		{"jira", "issue_deleted", true},
		{"jira", "jira:issue_created", false},
		{"jira", "unknown", false},
	}
	for _, testCase := range cases {
		if got := isNativeSyncDispatchEvent(testCase.provider, testCase.eventType); got != testCase.want {
			t.Errorf("isNativeSyncDispatchEvent(%q, %q) = %v, want %v", testCase.provider, testCase.eventType, got, testCase.want)
		}
	}
}

func TestWebhookHandlerRoutesRecognisedGithubEventsNativelyWithoutHTTPDispatch(t *testing.T) {
	payload := []byte(`{"repository":{"id":1,"full_name":"full-chaos/dev-health"}}`)
	store := &fakeSyncDispatchStore{
		fakeStore: fakeStore{webhook: webhookDeliveryFor("github", "push", payload)},
		result:    SyncDispatchResult{Processed: true, OccurrenceID: "sha256:abc"},
	}
	handler, err := NewWebhookHandler(store)
	if err != nil {
		t.Fatal(err)
	}
	recorded, restore := spyRecordIgnoredWebhookEvent()
	defer restore()
	if err := handler.Work(context.Background(), webhookExecution(webhookID)); err != nil {
		t.Fatal(err)
	}
	if store.calls != 1 || store.provider != "github" || store.eventType != "push" {
		t.Fatalf("dispatch calls=%d provider=%q eventType=%q", store.calls, store.provider, store.eventType)
	}
	if *recorded {
		t.Fatal("a recognised sync event must never be recorded as ignored")
	}
}

func TestWebhookHandlerRoutesRecognisedGitlabEventsNativelyWithoutHTTPDispatch(t *testing.T) {
	payload := []byte(`{"project":{"id":42,"path_with_namespace":"group/project"}}`)
	store := &fakeSyncDispatchStore{
		fakeStore: fakeStore{webhook: webhookDeliveryFor("gitlab", "merge_request", payload)},
		result:    SyncDispatchResult{Processed: true, OccurrenceID: "sha256:def"},
	}
	handler, _ := NewWebhookHandler(store)
	recorded, restore := spyRecordIgnoredWebhookEvent()
	defer restore()
	if err := handler.Work(context.Background(), webhookExecution(webhookID)); err != nil {
		t.Fatal(err)
	}
	if store.calls != 1 || *recorded {
		t.Fatalf("dispatch calls=%d recorded=%v", store.calls, *recorded)
	}
}

func TestWebhookHandlerUnroutableSyncIsPermanentNotRetried(t *testing.T) {
	payload := []byte(`{"repository":{"id":1,"full_name":"full-chaos/dev-health"}}`)
	store := &fakeSyncDispatchStore{
		fakeStore: fakeStore{webhook: webhookDeliveryFor("github", "push", payload)},
		result:    SyncDispatchResult{Processed: false, Reason: "webhook_sync_unroutable:no_sync_configuration"},
	}
	handler, _ := NewWebhookHandler(store)
	recorded, restore := spyRecordIgnoredWebhookEvent()
	defer restore()
	err := handler.Work(context.Background(), webhookExecution(webhookID))
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("Work = %v, want category %s", err, jobruntime.CategoryPermanent)
	}
	if !errors.Is(err, ErrWebhookSyncUnroutable) {
		t.Fatalf("Work error must wrap ErrWebhookSyncUnroutable: %v", err)
	}
	cause := errors.Unwrap(err)
	if cause == nil || !strings.Contains(cause.Error(), "no_sync_configuration") {
		t.Fatalf("Work error's unwrapped cause must name the unroutable reason: %v", cause)
	}
	if *recorded {
		t.Fatal("an unroutable sync is a permanent error, never also recorded as an ignore (no fallback exists)")
	}
}

func TestWebhookHandlerSyncDispatchFailureIsRetryable(t *testing.T) {
	payload := []byte(`{"repository":{"id":1,"full_name":"full-chaos/dev-health"}}`)
	store := &fakeSyncDispatchStore{
		fakeStore: fakeStore{webhook: webhookDeliveryFor("github", "push", payload)},
		err:       errUnavailableForTest,
	}
	handler, _ := NewWebhookHandler(store)
	recorded, restore := spyRecordIgnoredWebhookEvent()
	defer restore()
	err := handler.Work(context.Background(), webhookExecution(webhookID))
	if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("Work = %v, want category %s", err, jobruntime.CategoryRetryable)
	}
	if *recorded {
		t.Fatal("a retryable native-path failure must not also be recorded as ignored")
	}
}

// TestWebhookHandlerIgnoresUnrecognisedJiraEvent covers a jira
// delivery.EventType that isn't one of jiraSyncEventTypes' three canonical
// values -- the raw, un-canonicalised webhookEvent string
// ("jira:issue_updated") never actually reaches this function in production
// (delivery.EventType is already map_jira_event's OUTPUT by the time a
// webhook_deliveries row exists, same as github/gitlab), but the ignore path
// this exercises (an unmatched jira event type is recorded, never silently
// dropped) is real regardless of how it's reached. See
// TestWebhookHandlerRoutesRecognisedJiraEventsNativelyWithoutHTTPDispatch
// for the CHAOS-5320 native path this PR adds.
func TestWebhookHandlerIgnoresUnrecognisedJiraEvent(t *testing.T) {
	payload := []byte(`{"issue":{"key":"CHAOS-1"}}`)
	store := &fakeSyncDispatchStore{
		fakeStore: fakeStore{webhook: webhookDeliveryFor("jira", "jira:issue_updated", payload)},
	}
	handler, _ := NewWebhookHandler(store)
	recorded, restore := spyRecordIgnoredWebhookEvent()
	defer restore()
	if err := handler.Work(context.Background(), webhookExecution(webhookID)); err != nil {
		t.Fatal(err)
	}
	if store.calls != 0 {
		t.Fatalf("an unrecognised jira event type must never reach SyncDispatchWriter, got %d calls", store.calls)
	}
	if !*recorded {
		t.Fatal("expected the ignore path for an unrecognised jira event type")
	}
}

// TestWebhookHandlerRoutesRecognisedJiraEventsNativelyWithoutHTTPDispatch is
// jira's twin of the existing github/gitlab "routes natively" tests
// (CHAOS-5320): a recognised, already-canonicalised jira event type is
// routed to SyncDispatchWriter, never recorded as an ignore.
func TestWebhookHandlerRoutesRecognisedJiraEventsNativelyWithoutHTTPDispatch(t *testing.T) {
	payload := []byte(`{"issue":{"key":"CHAOS-1","fields":{"project":{"key":"CHAOS","name":"Full Chaos"}}}}`)
	store := &fakeSyncDispatchStore{
		fakeStore: fakeStore{webhook: webhookDeliveryFor("jira", "issue_updated", payload)},
		result:    SyncDispatchResult{Processed: true, OccurrenceID: "sha256:jira1"},
	}
	handler, _ := NewWebhookHandler(store)
	recorded, restore := spyRecordIgnoredWebhookEvent()
	defer restore()
	if err := handler.Work(context.Background(), webhookExecution(webhookID)); err != nil {
		t.Fatal(err)
	}
	if store.calls != 1 || *recorded {
		t.Fatalf("dispatch calls=%d recorded=%v", store.calls, *recorded)
	}
}

func TestWebhookHandlerIgnoresUnrecognisedGithubEvent(t *testing.T) {
	payload := []byte(`{"action":"created"}`)
	store := &fakeSyncDispatchStore{
		fakeStore: fakeStore{webhook: webhookDeliveryFor("github", "issue_comment", payload)},
	}
	handler, _ := NewWebhookHandler(store)
	recorded, restore := spyRecordIgnoredWebhookEvent()
	defer restore()
	if err := handler.Work(context.Background(), webhookExecution(webhookID)); err != nil {
		t.Fatal(err)
	}
	if store.calls != 0 {
		t.Fatalf("an unrecognised event type must never reach SyncDispatchWriter, got %d calls", store.calls)
	}
	if !*recorded {
		t.Fatal("expected the ignore path for an unrecognised github event type")
	}
}

func TestScheduledSyncOccurrenceIdentityIsDeterministic(t *testing.T) {
	scheduledFor := time.Date(2026, 9, 6, 12, 0, 0, 123456000, time.UTC)
	first := scheduledSyncOccurrenceIdentity("11111111-1111-1111-1111-111111111111", scheduledFor)
	second := scheduledSyncOccurrenceIdentity("11111111-1111-1111-1111-111111111111", scheduledFor)
	if first != second {
		t.Fatalf("identity must be a pure function of (configID, scheduledFor): %q != %q", first, second)
	}
	if !strings.HasPrefix(first, "sha256:") {
		t.Fatalf("identity = %q, want a sha256: prefix", first)
	}
	other := scheduledSyncOccurrenceIdentity("22222222-2222-2222-2222-222222222222", scheduledFor)
	if first == other {
		t.Fatal("identity must differ for a different config_id")
	}
	laterTime := scheduledSyncOccurrenceIdentity("11111111-1111-1111-1111-111111111111", scheduledFor.Add(time.Second))
	if first == laterTime {
		t.Fatal("identity must differ for a different scheduled_for")
	}
}
