//go:build integration

package apiservice

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/webhookintake"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"
)

// webhookintakeRepoRoot mirrors internal/api/policy's own live oracle
// helper: internal/apiservice is one directory below the repo root, since
// this file must live here (not in internal/api/webhookintake, which
// apiservice imports -- an import cycle otherwise).
func webhookintakeRepoRoot(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve package path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
}

func githubVenueSign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// blankVenueEventID replaces the response's own event_id field (a fresh
// row-id UUID each plane's INSERT mints independently) with a fixed
// placeholder, and blanks celery_available -- a documented, intentional
// divergence (the Python venue process has celery importable and reports
// true; the Go port hardcodes false always, since it has no such import to
// attempt -- see webhookintake/health.go) -- so the venue diff compares the
// fields that must actually agree (status, message, HTTP status/headers)
// without failing on the two values neither plane can or should make equal.
func blankVenueEventID(_ venueoracle.Request, body string) string {
	var decoded map[string]any
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		return body
	}
	changed := false
	if _, ok := decoded["event_id"]; ok {
		decoded["event_id"] = "<event_id>"
		changed = true
	}
	if _, ok := decoded["celery_available"]; ok {
		decoded["celery_available"] = "<celery_available>"
		changed = true
	}
	if !changed {
		return body
	}
	rewritten, err := json.Marshal(decoded)
	if err != nil {
		return body
	}
	return string(rewritten)
}

// TestWebhookIntakeVenueOracleGitHubGitLabJiraHealth is the write-route
// differential proof CHAOS-6247 needs: the REAL Python api
// (internal/testsupport/venueoracle's TestClient-driven "serve" mode) and
// the REAL Go api answer the identical signed fixture, on two copies of the
// same migrated Postgres, and the responses AND the persisted
// webhook_deliveries/worker_job_outbox rows are diffed -- not just read
// side by side.
//
// PagerDuty's own venue oracle (the state machine, replay claim, and
// per-binding decryption) is TestWebhookIntakeVenueOraclePagerDuty, below.
func TestWebhookIntakeVenueOracleGitHubGitLabJiraHealth(t *testing.T) {
	ctx := context.Background()
	root := webhookintakeRepoRoot(t)

	const (
		githubSecret = "venue-github-secret"
		gitlabSecret = "venue-gitlab-secret"
		jiraSecret   = "venue-jira-secret"
		jwtKey       = "venue-oracle-jwt-signing-key-32-bytes-min"
	)

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		PythonEnv: []string{
			"GITHUB_WEBHOOK_SECRET=" + githubSecret,
			"GITLAB_WEBHOOK_TOKEN=" + gitlabSecret,
			"JIRA_WEBHOOK_SECRET=" + jiraSecret,
		},
		// No JWT principals needed: webhook intake authenticates by
		// provider secret, not by session/access token.
		Seed: func(*testing.T, context.Context, *pgxpool.Pool, *venueoracle.Venue) map[string]map[string]any {
			return nil
		},
	})
	t.Setenv("GITHUB_WEBHOOK_SECRET", githubSecret)
	t.Setenv("GITLAB_WEBHOOK_TOKEN", gitlabSecret)
	t.Setenv("JIRA_WEBHOOK_SECRET", jiraSecret)

	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatalf("open Go api database: %v", err)
	}
	t.Cleanup(pool.Close)

	jobRegistry, err := webhookintake.LoadJobRegistry(filepath.Join(root, "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatalf("load job registry: %v", err)
	}
	producer, err := joboutbox.NewProducer(pool, jobRegistry)
	if err != nil {
		t.Fatalf("new job outbox producer: %v", err)
	}

	logger := quietLogger()
	deps := Deps{Pool: pool, Producer: producer}
	server, err := NewServer(config.Config{APIAddress: "127.0.0.1:0"}, logger, Routes(deps, logger))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatalf("start server: %v (api role gaps: %s)", err, venue.DiagnoseAPIRole(t, ctx))
	}
	t.Cleanup(func() { _ = server.Shutdown(ctx) })
	goBase := "http://" + server.Address()

	githubBody := []byte(`{"repository":{"full_name":"acme/venue-repo","owner":{"login":"acme"}},"organization":{"login":"acme"}}`)
	gitlabBody := []byte(`{"object_kind":"push","project":{"path_with_namespace":"acme/venue-repo","namespace":"acme"}}`)
	jiraBody := []byte(`{"webhookEvent":"jira:issue_created","issue":{"key":"VEN-1","fields":{"project":{"key":"VEN"}}},"timestamp":1700000000000}`)

	requests := []venueoracle.Request{
		{
			Name:   "github push accepted",
			Method: "POST", Path: "/api/v1/webhooks/github",
			Headers: map[string]string{
				"X-GitHub-Event": "push", "X-GitHub-Delivery": "venue-github-delivery-1",
				"X-Hub-Signature-256": githubVenueSign(githubSecret, githubBody),
				"Content-Type":        "application/json",
			},
			Body: venueoracle.B64(string(githubBody)),
		},
		{
			Name:   "github invalid signature",
			Method: "POST", Path: "/api/v1/webhooks/github",
			Headers: map[string]string{
				"X-GitHub-Event": "push", "X-GitHub-Delivery": "venue-github-delivery-bad",
				"X-Hub-Signature-256": "sha256=" + hex.EncodeToString(make([]byte, 32)),
				"Content-Type":        "application/json",
			},
			Body: venueoracle.B64(string(githubBody)),
		},
		{
			Name:   "gitlab push accepted",
			Method: "POST", Path: "/api/v1/webhooks/gitlab",
			Headers: map[string]string{
				"X-Gitlab-Event": "Push Hook", "X-Gitlab-Token": gitlabSecret,
				"Content-Type": "application/json",
			},
			Body: venueoracle.B64(string(gitlabBody)),
		},
		{
			Name:   "jira issue created accepted",
			Method: "POST", Path: "/api/v1/webhooks/jira",
			Headers: map[string]string{
				"X-Hub-Signature": func() string {
					mac := hmac.New(sha256.New, []byte(jiraSecret))
					mac.Write(jiraBody)
					return hex.EncodeToString(mac.Sum(nil))
				}(),
				"Content-Type": "application/json",
			},
			Body: venueoracle.B64(string(jiraBody)),
		},
		{Name: "webhooks health", Method: "GET", Path: "/api/v1/webhooks/health"},
	}

	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{Normalize: blankVenueEventID})
	t.Log(receipt)

	pythonRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB),
		`SELECT provider, event_type, raw_event_type, org_ref, repo_name FROM webhook_deliveries ORDER BY provider, delivery_key`)
	goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB),
		`SELECT provider, event_type, raw_event_type, org_ref, repo_name FROM webhook_deliveries ORDER BY provider, delivery_key`)
	if pythonRows != goRows {
		t.Errorf("webhook_deliveries rows differ:\n python: %s\n go:     %s", pythonRows, goRows)
	}

	pythonOutbox := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB),
		`SELECT job_kind FROM worker_job_outbox ORDER BY job_kind, id`)
	goOutbox := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB),
		`SELECT job_kind FROM worker_job_outbox ORDER BY job_kind, id`)
	if pythonOutbox != goOutbox {
		t.Errorf("worker_job_outbox rows differ:\n python: %s\n go:     %s", pythonOutbox, goOutbox)
	}

	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proofDir, "webhookintake-venue-oracle"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func pagerdutyVenueSign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

// pagerdutyVenueSeed names the fixture rows seedPagerDutyVenue writes: an
// organization licensed for canonical_incident_ingestion, one that is not,
// and one binding per required oracle case. The caller generates these ids
// before Start (Seed's own return value is reserved for mint specs), then
// seedPagerDutyVenue runs INSIDE Start's Seed hook -- against the SOURCE
// database, as a superuser, before the CREATE DATABASE ... TEMPLATE copy --
// so one write reaches both planes.
type pagerdutyVenueSeed struct {
	orgLicensed, orgGated                         string
	bindingActive, bindingCandidate, bindingGated string
}

func newPagerDutyVenueSeed() pagerdutyVenueSeed {
	return pagerdutyVenueSeed{
		orgLicensed:      uuid.New().String(),
		orgGated:         uuid.New().String(),
		bindingActive:    uuid.New().String(),
		bindingCandidate: uuid.New().String(),
		bindingGated:     uuid.New().String(),
	}
}

// seedPagerDutyVenue writes the fixture rows for seed, encrypting each
// binding's signing secret with the Python plane's OWN core.encryption.
// encrypt_value (venue.CallPython) under Options.PythonEnv's
// SETTINGS_ENCRYPTION_KEY -- never this test's own Go encryptor -- so the
// seeded ciphertext is a genuine cross-language artifact, proving the Go
// DECRYPT path against Python's real ENCRYPT path, not a Go-side
// round-trip.
func seedPagerDutyVenue(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue, seed pagerdutyVenueSeed, secretActive, secretCandidate, secretGated string) {
	t.Helper()
	results := venue.CallPython(t,
		venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{secretActive}},
		venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{secretCandidate}},
		venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{secretGated}},
	)
	var encryptedActive, encryptedCandidate, encryptedGated string
	if err := json.Unmarshal(results[0], &encryptedActive); err != nil {
		t.Fatalf("decode encrypted active secret: %v", err)
	}
	if err := json.Unmarshal(results[1], &encryptedCandidate); err != nil {
		t.Fatalf("decode encrypted candidate secret: %v", err)
	}
	if err := json.Unmarshal(results[2], &encryptedGated); err != nil {
		t.Fatalf("decode encrypted gated secret: %v", err)
	}

	integrationLicensed := uuid.New().String()
	integrationGated := uuid.New().String()
	sourceActive := uuid.New().String()
	sourceCandidate := uuid.New().String()
	sourceGated := uuid.New().String()
	overrideID := uuid.New().String()
	credentialActive := uuid.New().String()
	credentialGated := uuid.New().String()

	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier) VALUES ($1::uuid, $2, $3, 'team')`,
			[]any{seed.orgLicensed, "venue-pagerduty-licensed", "Venue PagerDuty Licensed"}},
		{`INSERT INTO organizations (id, slug, name, tier) VALUES ($1::uuid, $2, $3, 'community')`,
			[]any{seed.orgGated, "venue-pagerduty-gated", "Venue PagerDuty Gated"}},
		// canonical_incident_ingestion is already seeded by Alembic 0048,
		// globally enabled at min_tier "community" -- every org qualifies by
		// tier alone, so "gated off" here is an explicit per-org override
		// disabling that already-enabled feature for orgGated, exactly the
		// real-world shape ("compliance/support" override) the engine
		// supports, rather than a second feature_flags row this venue would
		// own (its UNIQUE(key) would collide with the real seed anyway).
		{`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, created_at, updated_at)
SELECT $1::uuid, $2::uuid, feature_flags.id, false, now(), now() FROM feature_flags WHERE feature_flags.key = 'canonical_incident_ingestion'`,
			[]any{overrideID, seed.orgGated}},
		{`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, 'pagerduty', 'Venue PagerDuty', '{}'::json, true, now(), now())`,
			[]any{integrationLicensed, seed.orgLicensed}},
		{`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, 'pagerduty', 'Venue PagerDuty', '{}'::json, true, now(), now())`,
			[]any{integrationGated, seed.orgGated}},
		{`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1::uuid, $2, $3::uuid, 'pagerduty', 'pagerduty_service', 'venue-active', 'venue-active', 'venue-active', '{}'::json, true, now(), now())`,
			[]any{sourceActive, seed.orgLicensed, integrationLicensed}},
		{`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1::uuid, $2, $3::uuid, 'pagerduty', 'pagerduty_service', 'venue-candidate', 'venue-candidate', 'venue-candidate', '{}'::json, true, now(), now())`,
			[]any{sourceCandidate, seed.orgLicensed, integrationLicensed}},
		{`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1::uuid, $2, $3::uuid, 'pagerduty', 'pagerduty_service', 'venue-gated', 'venue-gated', 'venue-gated', '{}'::json, true, now(), now())`,
			[]any{sourceGated, seed.orgGated, integrationGated}},
		// ck_pagerduty_webhook_bindings_active_credential_required: an
		// "active" row needs a real credential_id -- the candidate row does
		// not (it is not active yet).
		{`INSERT INTO integration_credentials (id, org_id, provider, name, created_at, updated_at)
VALUES ($1::uuid, $2, 'pagerduty', 'Venue PagerDuty Credential', now(), now())`,
			[]any{credentialActive, seed.orgLicensed}},
		{`INSERT INTO integration_credentials (id, org_id, provider, name, created_at, updated_at)
VALUES ($1::uuid, $2, 'pagerduty', 'Venue PagerDuty Credential', now(), now())`,
			[]any{credentialGated, seed.orgGated}},
		{`INSERT INTO pagerduty_webhook_bindings (id, org_id, integration_source_id, credential_id, provider_subscription_id, signing_secret_encrypted, signing_secret_key_version, status, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, 'sub-active', $5, 'v1', 'active', now(), now())`,
			[]any{seed.bindingActive, seed.orgLicensed, sourceActive, credentialActive, encryptedActive}},
		{`INSERT INTO pagerduty_webhook_bindings (id, org_id, integration_source_id, provider_subscription_id, signing_secret_encrypted, signing_secret_key_version, status, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3::uuid, 'sub-candidate', $4, 'v1', 'candidate', now(), now())`,
			[]any{seed.bindingCandidate, seed.orgLicensed, sourceCandidate, encryptedCandidate}},
		{`INSERT INTO pagerduty_webhook_bindings (id, org_id, integration_source_id, credential_id, provider_subscription_id, signing_secret_encrypted, signing_secret_key_version, status, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, 'sub-gated', $5, 'v1', 'active', now(), now())`,
			[]any{seed.bindingGated, seed.orgGated, sourceGated, credentialGated, encryptedGated}},
	}
	for _, statement := range statements {
		if _, err := admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed %s: %v", statement.sql, err)
		}
	}
}

// replayKeys reads every Valkey string key matching pattern as "key=value",
// sorted -- venueoracle.StreamEntries only reads Redis streams, and the
// PagerDuty replay claim is a plain string key (SET ... NX/XX), so this
// test reads it directly.
func replayKeys(t *testing.T, ctx context.Context, uri, pattern string) string {
	t.Helper()
	options, err := valkeygo.ParseURL(uri)
	if err != nil {
		t.Fatal(err)
	}
	client, err := valkeygo.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	keys, err := client.Do(ctx, client.B().Keys().Pattern(pattern).Build()).AsStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(keys)
	var lines []string
	for _, key := range keys {
		value, err := client.Do(ctx, client.B().Get().Key(key).Build()).ToString()
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, key+"="+value)
	}
	return strings.Join(lines, " | ")
}

// TestWebhookIntakeVenueOraclePagerDuty is the PagerDuty leg team-lead
// required in this same PR: the state machine, replay claim, and per-binding
// decryption are exactly where a differential oracle earns its keep. Both
// planes share ONE SETTINGS_ENCRYPTION_KEY fixture, and every binding's
// signing_secret_encrypted is produced by the PYTHON side's own
// encrypt_value (seedPagerDutyVenue, via venue.CallPython) -- never this
// test's own Go encryptor -- so the seeded ciphertext is a genuine
// cross-language artifact, proving the Go DECRYPT path against Python's
// real ENCRYPT path, not a Go-side round-trip.
func TestWebhookIntakeVenueOraclePagerDuty(t *testing.T) {
	ctx := context.Background()
	root := webhookintakeRepoRoot(t)

	const (
		jwtKey          = "venue-oracle-jwt-signing-key-32-bytes-min"
		encryptionKey   = "venue-pagerduty-settings-encryption-key-fixture"
		secretActive    = "pd-secret-active"
		secretCandidate = "pd-secret-candidate"
		secretGated     = "pd-secret-gated"
	)

	seed := newPagerDutyVenueSeed()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		PythonEnv: []string{
			"SETTINGS_ENCRYPTION_KEY=" + encryptionKey,
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			seedPagerDutyVenue(t, ctx, admin, venue, seed, secretActive, secretCandidate, secretGated)
			return nil
		},
	})

	// One shared fixture key: Python encrypted the bindings' secrets above
	// (inside Seed, via venue.CallPython), this decrypts them.
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(encryptionKey), "")
	if err != nil {
		t.Fatalf("new decryptor: %v", err)
	}

	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatalf("open Go api database: %v", err)
	}
	t.Cleanup(pool.Close)
	goValkey, err := valkey.Open(ctx, valkey.DefaultConfig(venue.ValkeyURI))
	if err != nil {
		t.Fatalf("open Go valkey: %v", err)
	}
	t.Cleanup(goValkey.Close)

	logger := quietLogger()
	deps := Deps{Pool: pool, Valkey: goValkey, Decryptor: decryptor}
	server, err := NewServer(config.Config{APIAddress: "127.0.0.1:0"}, logger, Routes(deps, logger))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatalf("start server: %v (api role gaps: %s)", err, venue.DiagnoseAPIRole(t, ctx))
	}
	t.Cleanup(func() { _ = server.Shutdown(ctx) })
	goBase := "http://" + server.Address()

	activeBody := []byte(`{"event":{"id":"PD-ACTIVE-1","event_type":"incident.triggered","occurred_at":"2026-01-02T03:04:05Z","data":{"incident":{"id":"I1"}}}}`)
	pingBody := []byte(`{"event":{"id":"PD-PING-1","event_type":"pagey.ping","occurred_at":"2026-01-02T03:04:05Z","data":{}}}`)
	replayBody := []byte(`{"event":{"id":"PD-REPLAY-1","event_type":"incident.triggered","occurred_at":"2026-01-02T03:04:05Z","data":{}}}`)
	gatedBody := []byte(`{"event":{"id":"PD-GATED-1","event_type":"incident.triggered","occurred_at":"2026-01-02T03:04:05Z","data":{}}}`)

	requests := []venueoracle.Request{
		{
			Name:   "pagerduty valid signature active",
			Method: "POST", Path: "/api/v1/webhooks/pagerduty/" + seed.bindingActive,
			Headers: map[string]string{
				"X-Webhook-Subscription": "sub-active",
				"X-PagerDuty-Signature":  pagerdutyVenueSign(secretActive, activeBody),
				"Content-Type":           "application/json",
			},
			Body: venueoracle.B64(string(activeBody)),
		},
		{
			Name:   "pagerduty candidate ping to ready",
			Method: "POST", Path: "/api/v1/webhooks/pagerduty/" + seed.bindingCandidate,
			Headers: map[string]string{
				"X-Webhook-Subscription": "sub-candidate",
				"X-PagerDuty-Signature":  pagerdutyVenueSign(secretCandidate, pingBody),
				"Content-Type":           "application/json",
			},
			Body: venueoracle.B64(string(pingBody)),
		},
		{
			Name:   "pagerduty replayed delivery (first)",
			Method: "POST", Path: "/api/v1/webhooks/pagerduty/" + seed.bindingActive,
			Headers: map[string]string{
				"X-Webhook-Subscription": "sub-active",
				"X-PagerDuty-Signature":  pagerdutyVenueSign(secretActive, replayBody),
				"Content-Type":           "application/json",
			},
			Body: venueoracle.B64(string(replayBody)),
		},
		{
			Name:   "pagerduty replayed delivery (replay)",
			Method: "POST", Path: "/api/v1/webhooks/pagerduty/" + seed.bindingActive,
			Headers: map[string]string{
				"X-Webhook-Subscription": "sub-active",
				"X-PagerDuty-Signature":  pagerdutyVenueSign(secretActive, replayBody),
				"Content-Type":           "application/json",
			},
			Body: venueoracle.B64(string(replayBody)),
		},
		{
			Name:   "pagerduty bad signature",
			Method: "POST", Path: "/api/v1/webhooks/pagerduty/" + seed.bindingActive,
			Headers: map[string]string{
				"X-Webhook-Subscription": "sub-active",
				"X-PagerDuty-Signature":  "v1=" + hex.EncodeToString(make([]byte, 32)),
				"Content-Type":           "application/json",
			},
			Body: venueoracle.B64(string(activeBody)),
		},
		{
			Name:   "pagerduty gated feature",
			Method: "POST", Path: "/api/v1/webhooks/pagerduty/" + seed.bindingGated,
			Headers: map[string]string{
				"X-Webhook-Subscription": "sub-gated",
				"X-PagerDuty-Signature":  pagerdutyVenueSign(secretGated, gatedBody),
				"Content-Type":           "application/json",
			},
			Body: venueoracle.B64(string(gatedBody)),
		},
	}

	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{})
	t.Log(receipt)

	pythonStream := venueoracle.StreamEntries(t, ctx, venue.PythonValkeyURI, "pagerduty-webhooks:*", "received_at")
	goStream := venueoracle.StreamEntries(t, ctx, venue.ValkeyURI, "pagerduty-webhooks:*", "received_at")
	if pythonStream != goStream {
		t.Errorf("pagerduty-webhooks stream entries differ:\n python: %s\n go:     %s", pythonStream, goStream)
	}

	pythonBindings := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB),
		`SELECT provider_subscription_id, status FROM pagerduty_webhook_bindings ORDER BY provider_subscription_id`)
	goBindings := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB),
		`SELECT provider_subscription_id, status FROM pagerduty_webhook_bindings ORDER BY provider_subscription_id`)
	if pythonBindings != goBindings {
		t.Errorf("pagerduty_webhook_bindings rows differ:\n python: %s\n go:     %s", pythonBindings, goBindings)
	}

	pythonReplay := replayKeys(t, ctx, venue.PythonValkeyURI, "pagerduty-webhook-replay:*")
	goReplay := replayKeys(t, ctx, venue.ValkeyURI, "pagerduty-webhook-replay:*")
	if pythonReplay != goReplay {
		t.Errorf("pagerduty-webhook-replay keys differ:\n python: %s\n go:     %s", pythonReplay, goReplay)
	}

	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proofDir, "webhookintake-pagerduty-venue-oracle"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}
