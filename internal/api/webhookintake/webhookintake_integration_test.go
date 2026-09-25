//go:build integration

package webhookintake

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func bytesReader(body []byte) *bytes.Reader { return bytes.NewReader(body) }

// testContractRoot matches internal/schedulerservice's own convention: this
// package is two directories below the repo root.
const testContractRoot = "../../../contracts/jobs/v1"

func createWebhookIntakeTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	// The migrated schema (CHAOS-6769 ledger): the hand-written webhook_deliveries, worker_job_outbox,
	// pagerduty_webhook_bindings (no integration_source_id), organizations, org_licenses,
	// feature_flags and org_feature_overrides drifted from the real tables' types, NOT NULL columns
	// and foreign keys.
	pgschema.Apply(ctx, t, pool)
}

func newTestProducer(t *testing.T, pool *pgxpool.Pool) *joboutbox.Producer {
	t.Helper()
	registry, err := jobruntime.Load(testContractRoot)
	if err != nil {
		t.Fatalf("load job registry: %v", err)
	}
	producer, err := joboutbox.NewProducer(pool, registry)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	return producer
}

func seedFeatureEnabled(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID uuid.UUID) {
	t.Helper()
	pgseed.Org(ctx, t, pool, orgID.String(), "team")
	// The migrations register canonical_incident_ingestion; the test needs it enabled at the team floor.
	if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key = 'canonical_incident_ingestion'`); err != nil {
		t.Fatalf("reset feature flag: %v", err)
	}
	pgseed.FeatureFlag(ctx, t, pool, uuid.NewString(), "canonical_incident_ingestion", "team", true)
}

func newTestDecryptor(t *testing.T) providerfoundation.FernetDecryptor {
	t.Helper()
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("test-master-key-for-webhookintake"), "")
	if err != nil {
		t.Fatal(err)
	}
	return decryptor
}

func seedPagerDutyBinding(t *testing.T, ctx context.Context, pool *pgxpool.Pool, decryptor providerfoundation.FernetDecryptor, orgID uuid.UUID, status, subscriptionID, secret string) uuid.UUID {
	t.Helper()
	encrypted, err := decryptor.Encrypt([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	// A binding points at one of the org's integration sources (real NOT NULL foreign key).
	integrationID, sourceID := uuid.NewString(), uuid.NewString()
	pgseed.Integration(ctx, t, pool, integrationID, orgID.String(), "pagerduty")
	pgseed.IntegrationSource(ctx, t, pool, sourceID, orgID.String(), integrationID, "pagerduty", "service-"+sourceID)
	// The real table refuses an active binding without a credential
	// (ck_pagerduty_webhook_bindings_active_credential_required); the hand-written one allowed it.
	credentialID := uuid.NewString()
	pgseed.Credential(ctx, t, pool, credentialID, orgID.String(), "pagerduty")
	if _, err := pool.Exec(ctx, `
INSERT INTO pagerduty_webhook_bindings (id, org_id, integration_source_id, credential_id, status, provider_subscription_id,
	signing_secret_encrypted, signing_secret_key_version, created_at, updated_at)
VALUES ($1, $2, $3::uuid, $4::uuid, $5, $6, $7, 'v1', now(), now())`, id, orgID, sourceID, credentialID, status, subscriptionID, encrypted.Reveal()); err != nil {
		t.Fatalf("seed pagerduty binding: %v", err)
	}
	return id
}

func githubSign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// TestGitHubWebhookAcceptedEndToEnd is this ticket's proof shape: a real
// signed request through the full handler against real Postgres, asserting
// on the actual persisted webhook_deliveries row AND the actual
// worker_job_outbox row's decoded args -- the same fields
// internal/jobs/operational.WebhookHandler.Work/PostgresStore.LoadWebhook
// read, so a producer/consumer field-name drift fails this test.
func TestGitHubWebhookAcceptedEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(ctx) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createWebhookIntakeTables(t, ctx, pool)
	producer := newTestProducer(t, pool)

	deps := Deps{Pool: pool, Producer: producer, Secrets: Secrets{GitHub: "s3cr3t"}}
	handler := deps.handleGitHubWebhook()

	body := []byte(`{"repository":{"full_name":"acme/repo","owner":{"login":"acme"}},"organization":{"login":"acme"}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/github", bytesReader(body))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-GitHub-Delivery", "delivery-1")
	req.Header.Set("X-Hub-Signature-256", githubSign("s3cr3t", body))
	recorder := httptest.NewRecorder()
	handler(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Status  string `json:"status"`
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Status != "accepted" {
		t.Fatalf("response = %+v", response)
	}

	var provider, deliveryKey, eventTypeCol string
	if err := pool.QueryRow(ctx, `SELECT provider, delivery_key, event_type FROM webhook_deliveries WHERE id = $1`, response.EventID).
		Scan(&provider, &deliveryKey, &eventTypeCol); err != nil {
		t.Fatalf("query webhook_deliveries: %v", err)
	}
	if provider != "github" || deliveryKey != "delivery-1" || eventTypeCol != "push" {
		t.Fatalf("provider=%s deliveryKey=%s eventType=%s", provider, deliveryKey, eventTypeCol)
	}

	var argsRaw []byte
	var jobKind string
	if err := pool.QueryRow(ctx, `SELECT job_kind, args FROM worker_job_outbox WHERE dedupe_key LIKE 'webhook:%'`).
		Scan(&jobKind, &argsRaw); err != nil {
		t.Fatalf("query worker_job_outbox: %v", err)
	}
	if jobKind != jobcontract.KindWebhookDelivery {
		t.Fatalf("job_kind = %s", jobKind)
	}
	var envelope struct {
		Payload struct {
			DeliveryID string `json:"delivery_id"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(argsRaw, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Payload.DeliveryID != response.EventID {
		t.Fatalf("outbox delivery_id = %s, want %s", envelope.Payload.DeliveryID, response.EventID)
	}
}

// TestGitHubWebhookRedeliveryIsIdempotent proves a provider retry (same
// delivery id, same payload) resolves to the SAME durable row and does not
// duplicate the outbox publish (dedupe_key ON CONFLICT DO NOTHING).
func TestGitHubWebhookRedeliveryIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(ctx) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createWebhookIntakeTables(t, ctx, pool)
	producer := newTestProducer(t, pool)
	deps := Deps{Pool: pool, Producer: producer, Secrets: Secrets{GitHub: "s3cr3t"}}
	handler := deps.handleGitHubWebhook()

	body := []byte(`{"repository":{"full_name":"acme/repo"}}`)
	send := func() string {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/github", bytesReader(body))
		req.Header.Set("X-GitHub-Event", "push")
		req.Header.Set("X-GitHub-Delivery", "delivery-redeliver")
		req.Header.Set("X-Hub-Signature-256", githubSign("s3cr3t", body))
		recorder := httptest.NewRecorder()
		handler(recorder, req)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
		var response struct {
			EventID string `json:"event_id"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		return response.EventID
	}
	first := send()
	second := send()
	if first != second {
		t.Fatalf("redelivery resolved to a different row: %s vs %s", first, second)
	}
	var deliveryRows, outboxRows int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM webhook_deliveries WHERE id = $1`, first).Scan(&deliveryRows)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox`).Scan(&outboxRows)
	if deliveryRows != 1 || outboxRows != 1 {
		t.Fatalf("deliveryRows=%d outboxRows=%d, want 1 and 1", deliveryRows, outboxRows)
	}
}

func TestGitHubWebhookInvalidSignatureRejectedNoRowWritten(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(ctx) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createWebhookIntakeTables(t, ctx, pool)
	producer := newTestProducer(t, pool)
	deps := Deps{Pool: pool, Producer: producer, Secrets: Secrets{GitHub: "s3cr3t"}}
	handler := deps.handleGitHubWebhook()

	body := []byte(`{"repository":{"full_name":"acme/repo"}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/github", bytesReader(body))
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("X-GitHub-Delivery", "delivery-bad-sig")
	req.Header.Set("X-Hub-Signature-256", "sha256=0000000000000000000000000000000000000000000000000000000000000000")
	recorder := httptest.NewRecorder()
	handler(recorder, req)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", recorder.Code)
	}
	if recorder.Body.String() != `{"detail":"Invalid signature"}` {
		t.Fatalf("body = %s", recorder.Body.String())
	}
	var count int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM webhook_deliveries`).Scan(&count)
	if count != 0 {
		t.Fatalf("expected no row written on a rejected signature, found %d", count)
	}
}

// TestPagerDutyWebhookAcceptedEndToEnd is the write-route proof for the
// PagerDuty leg: a real signed request through the full handler against
// real Postgres AND real Valkey, asserting on the actual XADD stream entry
// the Go consumer (internal/jobs/pagerduty/stream.go's parse()) reads.
func TestPagerDutyWebhookAcceptedEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pgInstance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pgInstance.Close(ctx) })
	pool, err := pgxpool.New(ctx, pgInstance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createWebhookIntakeTables(t, ctx, pool)

	valkeyInstance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = valkeyInstance.Close(ctx) })
	client, err := valkey.Open(ctx, valkey.DefaultConfig(valkeyInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)

	orgID := uuid.New()
	seedFeatureEnabled(t, ctx, pool, orgID)
	decryptor := newTestDecryptor(t)
	bindingID := seedPagerDutyBinding(t, ctx, pool, decryptor, orgID, "active", "sub-1", "pd-secret")

	deps := Deps{Pool: pool, Valkey: client, Decryptor: decryptor, limiters: newRateLimiters(nil, nil)}
	handler := deps.handlePagerDutyWebhook()

	body := []byte(`{"event":{"id":"E1","event_type":"incident.triggered","occurred_at":"2026-01-01T00:00:00Z","data":{"incident":{"id":"I1"}}}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/pagerduty/"+bindingID.String(), bytesReader(body))
	req.SetPathValue("binding_id", bindingID.String())
	req.Header.Set("X-Webhook-Subscription", "sub-1")
	req.Header.Set("X-PagerDuty-Signature", pagerdutySign("pd-secret", body))
	recorder := httptest.NewRecorder()
	handler(recorder, req)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	entries, err := client.Do(ctx, client.B().Xrange().Key(pagerdutyStreamName(bindingID.String())).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("stream entries = %d, want 1", len(entries))
	}
	fields := entries[0].FieldValues
	if fields["binding_id"] != bindingID.String() || fields["event_id"] != "E1" {
		t.Fatalf("fields = %+v", fields)
	}
}

// TestPagerDutyWebhookReplayReturnsSameOutcomeNoSecondStreamEntry pins the
// oracle condition team-lead set: a replayed delivery (same body) gives the
// same 202 outcome both times, with exactly one stream entry, never two.
func TestPagerDutyWebhookReplayReturnsSameOutcomeNoSecondStreamEntry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pgInstance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pgInstance.Close(ctx) })
	pool, err := pgxpool.New(ctx, pgInstance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createWebhookIntakeTables(t, ctx, pool)

	valkeyInstance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = valkeyInstance.Close(ctx) })
	client, err := valkey.Open(ctx, valkey.DefaultConfig(valkeyInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)

	orgID := uuid.New()
	seedFeatureEnabled(t, ctx, pool, orgID)
	decryptor := newTestDecryptor(t)
	bindingID := seedPagerDutyBinding(t, ctx, pool, decryptor, orgID, "active", "sub-2", "pd-secret-2")

	deps := Deps{Pool: pool, Valkey: client, Decryptor: decryptor, limiters: newRateLimiters(nil, nil)}
	handler := deps.handlePagerDutyWebhook()

	body := []byte(`{"event":{"id":"E-replay","event_type":"incident.triggered","occurred_at":"2026-01-01T00:00:00Z","data":{}}}`)
	send := func() int {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/pagerduty/"+bindingID.String(), bytesReader(body))
		req.SetPathValue("binding_id", bindingID.String())
		req.Header.Set("X-Webhook-Subscription", "sub-2")
		req.Header.Set("X-PagerDuty-Signature", pagerdutySign("pd-secret-2", body))
		recorder := httptest.NewRecorder()
		handler(recorder, req)
		return recorder.Code
	}
	first := send()
	second := send()
	if first != http.StatusAccepted || second != http.StatusAccepted {
		t.Fatalf("first=%d second=%d, want 202 both times", first, second)
	}
	entries, err := client.Do(ctx, client.B().Xrange().Key(pagerdutyStreamName(bindingID.String())).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("stream entries = %d, want exactly 1 (the replay must not write a second)", len(entries))
	}
}
