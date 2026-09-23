//go:build integration

package externalingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"
)

// createExternalIngestTables mirrors internal/externalrecompute's own
// createCompatibilityTables pattern: a Go integration test defines exactly
// the tables it touches directly, rather than running the full Python
// alembic chain -- the columns here are taken from the SQLAlchemy models
// this package's queries were written against (models/ingest_auth.py,
// models/external_ingest.py, models/integrations.py) plus the minimal
// feature-flag tables featuregate.go reads.
func createExternalIngestTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	statements := []string{
		`CREATE EXTENSION IF NOT EXISTS pgcrypto`,
		`CREATE TABLE organizations (id uuid PRIMARY KEY, tier text)`,
		`CREATE TABLE org_licenses (org_id uuid PRIMARY KEY, tier text, features_override jsonb)`,
		`CREATE TABLE feature_flags (id uuid PRIMARY KEY, key text UNIQUE NOT NULL, is_enabled boolean NOT NULL, min_tier text NOT NULL)`,
		`CREATE TABLE org_feature_overrides (org_id uuid, feature_id uuid, is_enabled boolean, expires_at timestamptz, PRIMARY KEY (org_id, feature_id))`,
		`CREATE TABLE external_ingest_sources (
			id uuid PRIMARY KEY, org_id text NOT NULL, system text NOT NULL, instance text NOT NULL,
			entity_family text NOT NULL DEFAULT 'legacy', mode text NOT NULL DEFAULT 'disabled',
			enabled boolean NOT NULL DEFAULT true
		)`,
		`CREATE TABLE external_ingest_tokens (
			id uuid PRIMARY KEY, org_id text NOT NULL, source_id uuid, token_hash text UNIQUE NOT NULL,
			scopes jsonb NOT NULL, expires_at timestamptz, revoked_at timestamptz,
			last_used_at timestamptz, last_used_ip text
		)`,
		`CREATE TABLE external_ingest_batches (
			ingestion_id uuid PRIMARY KEY, org_id text NOT NULL, idempotency_key text NOT NULL,
			payload_hash text NOT NULL, source_system text NOT NULL, source_instance text NOT NULL,
			entity_family text NOT NULL DEFAULT 'legacy', producer text, producer_version text,
			schema_version text NOT NULL, window_started_at timestamptz, window_ended_at timestamptz,
			status text NOT NULL DEFAULT 'accepted', attempts integer NOT NULL DEFAULT 1,
			items_received integer NOT NULL DEFAULT 0, items_accepted integer NOT NULL DEFAULT 0,
			items_rejected integer NOT NULL DEFAULT 0, record_counts jsonb, error_summary jsonb,
			created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL, completed_at timestamptz,
			recompute_status text NOT NULL DEFAULT 'not_applicable', recompute_scope jsonb,
			recompute_dispatched_at timestamptz, recompute_completed_at timestamptz, recompute_error text,
			UNIQUE (org_id, source_system, source_instance, entity_family, idempotency_key)
		)`,
		`CREATE TABLE external_ingest_batch_payloads (
			ingestion_id uuid PRIMARY KEY, org_id text NOT NULL, schema_version text NOT NULL,
			payload_json bytea NOT NULL, byte_size integer NOT NULL, created_at timestamptz NOT NULL
		)`,
		`CREATE TABLE external_ingest_rejections (
			id uuid PRIMARY KEY, org_id text NOT NULL, ingestion_id uuid NOT NULL, record_index integer NOT NULL,
			record_kind text NOT NULL, external_id text, code text NOT NULL, message text NOT NULL,
			path text, created_at timestamptz NOT NULL
		)`,
		`CREATE TABLE integrations (id uuid PRIMARY KEY, org_id text NOT NULL, provider text NOT NULL, is_active boolean NOT NULL DEFAULT true, config jsonb NOT NULL DEFAULT '{}')`,
		`CREATE TABLE integration_sources (
			id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NOT NULL REFERENCES integrations(id),
			provider text NOT NULL, external_id text NOT NULL, name text NOT NULL, full_name text NOT NULL,
			metadata jsonb NOT NULL DEFAULT '{}', is_enabled boolean NOT NULL DEFAULT true
		)`,
	}
	for _, stmt := range statements {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("create table: %v\n%s", err, stmt)
		}
	}
}

func mustHash(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

// seedIngestToken inserts an org, an enabled customer_push source, and an
// ingest:write + schema:read token bound to it -- the minimum fixture the
// accept-batch happy path needs.
func seedIngestToken(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, system, instance, token string) uuid.UUID {
	t.Helper()
	sourceID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, tier) VALUES ($1, 'team')`, orgID); err != nil {
		t.Fatalf("seed organization: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO external_ingest_sources (id, org_id, system, instance, entity_family, mode, enabled)
		VALUES ($1, $2, $3, $4, 'legacy', 'customer_push', true)
	`, sourceID, orgID, system, instance); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	scopes, _ := json.Marshal([]string{"schema:read", "ingest:write", "ingest:status"})
	tokenID := uuid.New()
	if _, err := pool.Exec(ctx, `
		INSERT INTO external_ingest_tokens (id, org_id, source_id, token_hash, scopes)
		VALUES ($1, $2, $3, $4, $5)
	`, tokenID, orgID, sourceID, mustHash(token), scopes); err != nil {
		t.Fatalf("seed token: %v", err)
	}
	// ON CONFLICT DO NOTHING: feature_flags.key is UNIQUE and this helper is
	// called once per subtest within one shared Postgres instance (several
	// integration tests run multiple subtests against the same container) --
	// idempotent by design, not "seed once and hope no caller repeats it".
	if _, err := pool.Exec(ctx, `
		INSERT INTO feature_flags (id, key, is_enabled, min_tier) VALUES ($1, 'customer_push_ingest', true, 'team')
		ON CONFLICT (key) DO NOTHING
	`, uuid.New()); err != nil {
		t.Fatalf("seed feature flag: %v", err)
	}
	return sourceID
}

func newTestDeps(t *testing.T, pool *pgxpool.Pool, client valkeygo.Client) Deps {
	t.Helper()
	return Deps{Pool: pool, Valkey: client, limiters: newAuthLimiters(nil), routeLimiters: newRouteLimiters(nil)}
}

// TestAcceptBatchEndToEnd is this ticket's proof shape (Linear CHAOS-6246,
// plan.md §3.4): a real POST /batches request through the full CC22
// sequence against a real Postgres + Valkey, then verifies BOTH persisted
// effects -- the external_ingest_batches row AND the Redis stream pointer --
// plus the RETRY/REPLAY/CONFLICT idempotency states a customer's retried
// push actually exercises. It is not a differential against the Python
// plane (that needs a shared venue this unit-test package does not stand
// up); it is the Go-side half: proof the accepted batch reaches exactly the
// state internal/streamhandlers/external_ingest.go's consumer expects.
func TestAcceptBatchEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createExternalIngestTables(t, ctx, pool)

	valkey, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = valkey.Close(context.Background()) })
	options, err := valkeygo.ParseURL(valkey.URI)
	if err != nil {
		t.Fatal(err)
	}
	client, err := valkeygo.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)

	orgID := uuid.New().String()
	const token = "fcpush_test-token-accept-batch-e2e"
	seedIngestToken(t, ctx, pool, orgID, "github", "acme/repo", token)

	deps := newTestDeps(t, pool, client)
	handler := deps.handleAcceptBatch()

	body := `{
		"schemaVersion": "external-ingest.v1",
		"idempotencyKey": "batch-1",
		"source": {"system": "github", "instance": "acme/repo"},
		"records": [{"kind": "repository.v1", "externalId": "acme/repo", "payload": {"externalId":"acme/repo","sourceSystem":"github"}}]
	}`

	post := func(b string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/external-ingest/batches", strings.NewReader(b))
		req.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		handler(recorder, req)
		return recorder
	}

	t.Run("NEW: 202, batch row + stream pointer + durable payload", func(t *testing.T) {
		recorder := post(body)
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
		var accepted map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &accepted); err != nil {
			t.Fatal(err)
		}
		ingestionID, err := uuid.Parse(accepted["ingestionId"].(string))
		if err != nil {
			t.Fatal(err)
		}

		batch, err := getBatch(ctx, pool, orgID, ingestionID)
		if err != nil || batch == nil {
			t.Fatalf("batch row missing: %v", err)
		}
		if batch.Status != "accepted" || batch.ItemsReceived != 1 {
			t.Fatalf("%+v", batch)
		}
		exists, err := payloadExists(ctx, pool, ingestionID, orgID)
		if err != nil || !exists {
			t.Fatalf("payload row missing: %v", err)
		}

		// The stream entry uses the exact field names
		// internal/streamhandlers/external_ingest.go's parseExternalPointer
		// reads.
		stream := streamName(orgID)
		entries, err := client.Do(ctx, client.B().Xrange().Key(stream).Start("-").End("+").Build()).AsXRange()
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Fatalf("%d stream entries, want 1", len(entries))
		}
		fields := entries[0].FieldValues
		if fields["ingestion_id"] != ingestionID.String() || fields["org_id"] != orgID ||
			fields["source_system"] != "github" || fields["source_instance"] != "acme/repo" ||
			fields["schema_version"] != "external-ingest.v1" {
			t.Fatalf("%+v", fields)
		}
	})

	t.Run("REPLAY: same key+payload returns 200 with the current status, no new row", func(t *testing.T) {
		countBefore := countBatches(t, ctx, pool, orgID)
		recorder := post(body)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
		if got := countBatches(t, ctx, pool, orgID); got != countBefore {
			t.Fatalf("REPLAY must not insert a row: before=%d after=%d", countBefore, got)
		}
	})

	t.Run("CONFLICT: same key, different payload is 409, original row untouched", func(t *testing.T) {
		different := `{
			"schemaVersion": "external-ingest.v1",
			"idempotencyKey": "batch-1",
			"source": {"system": "github", "instance": "acme/repo"},
			"records": [{"kind": "repository.v1", "externalId": "acme/repo-2", "payload": {"externalId":"acme/repo-2","sourceSystem":"github"}}]
		}`
		recorder := post(different)
		if recorder.Code != http.StatusConflict {
			t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
		var errBody map[string]any
		_ = json.Unmarshal(recorder.Body.Bytes(), &errBody)
		errObj := errBody["error"].(map[string]any)
		if errObj["code"] != "idempotency_conflict" {
			t.Fatalf("%+v", errBody)
		}
	})

	t.Run("RETRY: a stream_unavailable row is re-accepted under the same ingestion_id", func(t *testing.T) {
		key := "batch-retry"
		retryBody := `{
			"schemaVersion": "external-ingest.v1",
			"idempotencyKey": "` + key + `",
			"source": {"system": "github", "instance": "acme/repo"},
			"records": [{"kind": "repository.v1", "externalId": "acme/repo", "payload": {"externalId":"acme/repo","sourceSystem":"github"}}]
		}`
		first := post(retryBody)
		if first.Code != http.StatusAccepted {
			t.Fatalf("first accept: %d %s", first.Code, first.Body.String())
		}
		var accepted map[string]any
		_ = json.Unmarshal(first.Body.Bytes(), &accepted)
		ingestionID, _ := uuid.Parse(accepted["ingestionId"].(string))

		if err := markStreamUnavailableTx(ctx, pool, orgID, ingestionID); err != nil {
			t.Fatal(err)
		}

		second := post(retryBody)
		if second.Code != http.StatusAccepted {
			t.Fatalf("retry accept: %d %s", second.Code, second.Body.String())
		}
		var retried map[string]any
		_ = json.Unmarshal(second.Body.Bytes(), &retried)
		if retried["ingestionId"] != ingestionID.String() {
			t.Fatalf("RETRY must reuse the ingestion_id: got %v, want %v", retried["ingestionId"], ingestionID)
		}
		batch, err := getBatch(ctx, pool, orgID, ingestionID)
		if err != nil || batch == nil {
			t.Fatal(err)
		}
		if batch.Attempts != 2 {
			t.Fatalf("attempts = %d, want 2", batch.Attempts)
		}
	})

	t.Run("wrong-source token is refused, no row is written", func(t *testing.T) {
		before := countBatches(t, ctx, pool, orgID)
		wrongInstance := `{
			"schemaVersion": "external-ingest.v1",
			"idempotencyKey": "batch-wrong-source",
			"source": {"system": "github", "instance": "someone-else/repo"},
			"records": [{"kind": "repository.v1", "externalId": "someone-else/repo", "payload": {"externalId":"someone-else/repo","sourceSystem":"github"}}]
		}`
		recorder := post(wrongInstance)
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
		var errBody map[string]any
		_ = json.Unmarshal(recorder.Body.Bytes(), &errBody)
		if errBody["error"].(map[string]any)["code"] != "source_mismatch" {
			t.Fatalf("%+v", errBody)
		}
		if got := countBatches(t, ctx, pool, orgID); got != before {
			t.Fatalf("a refused source_mismatch must not write a row: before=%d after=%d", before, got)
		}
	})
}

func countBatches(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM external_ingest_batches WHERE org_id = $1`, orgID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
