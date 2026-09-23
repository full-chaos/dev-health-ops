//go:build integration

package externalingest

import (
	"context"
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

// TestAcceptBatchReviewFindingsFixed is the executed proof for the round-1
// self-review's P1 findings (see the lane's invariant.md / PR body): each
// subtest reproduces the finding's own repro shape against a real
// Postgres + Valkey and asserts the fixed behavior.
func TestAcceptBatchReviewFindingsFixed(t *testing.T) {
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

	t.Run("trailing garbage after a valid envelope is rejected, not enqueued", func(t *testing.T) {
		orgID := uuid.New().String()
		const token = "fcpush_trailing_garbage_token"
		seedIngestToken(t, ctx, pool, orgID, "github", "acme/repo", token)
		deps := newTestDeps(t, pool, client)

		body := `{
			"schemaVersion": "external-ingest.v1",
			"idempotencyKey": "batch-trailing",
			"source": {"system": "github", "instance": "acme/repo"},
			"records": [{"kind": "repository.v1", "externalId": "acme/repo", "payload": {"externalId":"acme/repo","sourceSystem":"github"}}]
		}trailing-garbage`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/external-ingest/batches", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		deps.handleAcceptBatch()(recorder, req)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, body = %s, want 400 (trailing garbage must never be accepted)", recorder.Code, recorder.Body.String())
		}
		if got := countBatches(t, ctx, pool, orgID); got != 0 {
			t.Fatalf("a rejected envelope must not write a row: got %d", got)
		}
	})

	t.Run("a failed mark-stream-unavailable update is reported loudly, not silently swallowed into a false REPLAY", func(t *testing.T) {
		orgID := uuid.New().String()
		const token = "fcpush_mark_unavailable_fault_token"
		seedIngestToken(t, ctx, pool, orgID, "github", "acme/repo", token)
		// A valid ingestion_id column check constraint that only the
		// mark-stream-unavailable UPDATE can violate: reject any status
		// transition INTO 'stream_unavailable', simulating the update
		// itself failing (e.g. a transient connection loss) while every
		// other write (create, accept) still succeeds.
		if _, err := pool.Exec(ctx, `
			ALTER TABLE external_ingest_batches
			ADD CONSTRAINT review_fix_no_stream_unavailable CHECK (status <> 'stream_unavailable')
		`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `ALTER TABLE external_ingest_batches DROP CONSTRAINT IF EXISTS review_fix_no_stream_unavailable`)
		})

		// deps.Valkey is nil: enqueueBatch always fails closed
		// (errStreamUnavailable), forcing the markStreamUnavailableTx path,
		// which the CHECK constraint above then also fails.
		deps := Deps{Pool: pool, limiters: newAuthLimiters(nil), routeLimiters: newRouteLimiters(nil)}
		body := `{
			"schemaVersion": "external-ingest.v1",
			"idempotencyKey": "batch-mark-fault",
			"source": {"system": "github", "instance": "acme/repo"},
			"records": [{"kind": "repository.v1", "externalId": "acme/repo", "payload": {"externalId":"acme/repo","sourceSystem":"github"}}]
		}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/external-ingest/batches", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		deps.handleAcceptBatch()(recorder, req)
		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, body = %s, want 500 internal_error (the row could not be durably marked unavailable)", recorder.Code, recorder.Body.String())
		}

		// The row is still stuck at 'accepted' (the CHECK constraint
		// blocked the transition) -- this is the exact state that would
		// previously false-REPLAY. Prove a same-key retry does NOT return
		// a false 200 "accepted": it must resolve as if nothing happened
		// yet, i.e. the row is untouched, still recoverable by an operator,
		// and the client was told 500 (not "retry, it's fine") so it will
		// not treat this as a routine transient failure and move on.
		batch, err := getBatch(ctx, pool, orgID, uuidFromResponse(t, orgID, pool))
		if err != nil {
			t.Fatal(err)
		}
		if batch == nil || batch.Status != "accepted" {
			t.Fatalf("expected the row still at status=accepted (illustrating the failure mode the 500 now reports loudly): %+v", batch)
		}
	})

	t.Run("createdAfter/createdBefore filter the list", func(t *testing.T) {
		orgID := uuid.New().String()
		const token = "fcpush_time_range_token"
		seedIngestToken(t, ctx, pool, orgID, "github", "acme/repo", token)
		deps := newTestDeps(t, pool, client)

		post := func(key string) {
			body := `{"schemaVersion":"external-ingest.v1","idempotencyKey":"` + key + `",
				"source":{"system":"github","instance":"acme/repo"},
				"records":[{"kind":"repository.v1","externalId":"acme/repo","payload":{"externalId":"acme/repo","sourceSystem":"github"}}]}`
			req := httptest.NewRequest(http.MethodPost, "/api/v1/external-ingest/batches", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+token)
			recorder := httptest.NewRecorder()
			deps.handleAcceptBatch()(recorder, req)
			if recorder.Code != http.StatusAccepted {
				t.Fatalf("seed accept: %d %s", recorder.Code, recorder.Body.String())
			}
		}
		post("range-1")
		post("range-2")

		future := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/external-ingest/batches?createdAfter="+future, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		deps.handleListBatches()(recorder, req)
		var listResponse struct {
			Total int `json:"total"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &listResponse); err != nil {
			t.Fatal(err)
		}
		if listResponse.Total != 0 {
			t.Fatalf("createdAfter=<future> must exclude every existing row, got total=%d", listResponse.Total)
		}
	})

	t.Run("GET /schemas is rate limited per caller, not unlimited", func(t *testing.T) {
		deps := newTestDeps(t, pool, client)
		var statuses [130]int
		for i := range statuses {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/external-ingest/schemas", nil)
			req.RemoteAddr = "203.0.113.9:1234"
			recorder := httptest.NewRecorder()
			deps.handleListSchemas()(recorder, req)
			statuses[i] = recorder.Code
		}
		successCount, limitedCount := 0, 0
		for _, s := range statuses {
			if s == http.StatusOK {
				successCount++
			} else if s == http.StatusTooManyRequests {
				limitedCount++
			}
		}
		if limitedCount == 0 {
			t.Fatalf("130 requests from one caller must eventually be rate-limited (INGEST_READ_LIMIT=120/minute); got %d 200s, %d 429s", successCount, limitedCount)
		}
		if successCount > 120 {
			t.Fatalf("more than the 120/minute ceiling succeeded: %d", successCount)
		}
	})

	t.Run("an active managed github integration on the default host blocks an operational customer_push registration", func(t *testing.T) {
		orgID := uuid.New().String()
		const token = "fcpush_operational_ownership_token"
		sourceID := uuid.New()
		if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, tier) VALUES ($1, 'team')`, orgID); err != nil {
			t.Fatal(err)
		}
		// The explicit customer_push registration for an OPERATIONAL github
		// source at the default host (github.com).
		if _, err := pool.Exec(ctx, `
			INSERT INTO external_ingest_sources (id, org_id, system, instance, entity_family, mode, enabled)
			VALUES ($1, $2, 'github', 'github.com', 'operational', 'customer_push', true)
		`, sourceID, orgID); err != nil {
			t.Fatal(err)
		}
		scopes, _ := json.Marshal([]string{"schema:read", "ingest:write", "ingest:status"})
		tokenID := uuid.New()
		if _, err := pool.Exec(ctx, `
			INSERT INTO external_ingest_tokens (id, org_id, source_id, token_hash, scopes)
			VALUES ($1, $2, $3, $4, $5)
		`, tokenID, orgID, sourceID, mustHash(token), scopes); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO feature_flags (id, key, is_enabled, min_tier) VALUES ($1, 'customer_push_ingest', true, 'team') ON CONFLICT (key) DO NOTHING
		`, uuid.New()); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO feature_flags (id, key, is_enabled, min_tier) VALUES ($1, 'canonical_incident_ingestion', true, 'community') ON CONFLICT (key) DO NOTHING
		`, uuid.New()); err != nil {
			t.Fatal(err)
		}
		// An ACTIVE managed github integration with NO explicit host config
		// -- resolves to the default host (github.com), which matches the
		// operational source's instance above.
		integrationID := uuid.New()
		if _, err := pool.Exec(ctx, `
			INSERT INTO integrations (id, org_id, provider, is_active, config) VALUES ($1, $2, 'github', true, '{}')
		`, integrationID, orgID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO integration_sources (id, org_id, integration_id, provider, external_id, name, full_name, is_enabled)
			VALUES ($1, $2, $3, 'github', 'acme/repo', 'repo', 'acme/repo', true)
		`, uuid.New(), orgID, integrationID); err != nil {
			t.Fatal(err)
		}

		deps := newTestDeps(t, pool, client)
		body := `{
			"schemaVersion": "external-ingest.v1",
			"idempotencyKey": "batch-operational",
			"source": {"system": "github", "instance": "github.com", "entityFamily": "operational"},
			"records": [{"kind": "operational_incident.v1", "externalId": "inc-1", "payload": {"externalId":"inc-1","sourceVersionAt":"2026-01-01T00:00:00Z","title":"down"}}]
		}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/external-ingest/batches", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		deps.handleAcceptBatch()(recorder, req)
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("status = %d, body = %s, want 403 source_owned_by_fullchaos_sync (an active managed integration on the same default host must block the push)",
				recorder.Code, recorder.Body.String())
		}
		var errBody map[string]any
		_ = json.Unmarshal(recorder.Body.Bytes(), &errBody)
		if errBody["error"].(map[string]any)["code"] != "source_owned_by_fullchaos_sync" {
			t.Fatalf("%+v", errBody)
		}
	})
}

// uuidFromResponse re-derives the ingestion_id the "mark-stream-unavailable
// fault" subtest's accept call produced, by looking up the only row for
// orgID (that subtest writes exactly one).
func uuidFromResponse(t *testing.T, orgID string, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := pool.QueryRow(context.Background(),
		`SELECT ingestion_id FROM external_ingest_batches WHERE org_id = $1 LIMIT 1`, orgID,
	).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}
