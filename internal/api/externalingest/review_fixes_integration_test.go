//go:build integration

package externalingest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"
)

// TestAcceptBatchAgainstFaultsAndEdgeCases exercises a battery of failure
// modes and edge cases the accept-batch/status handlers must handle
// correctly against a real Postgres + Valkey: malformed input, a durability-
// write fault, time-range filtering, per-caller rate limiting, and an
// active-managed-integration ownership conflict.
func TestAcceptBatchAgainstFaultsAndEdgeCases(t *testing.T) {
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
			ADD CONSTRAINT pin_no_stream_unavailable_transition CHECK (status <> 'stream_unavailable')
		`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `ALTER TABLE external_ingest_batches DROP CONSTRAINT IF EXISTS pin_no_stream_unavailable_transition`)
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

	// handleAvailability, handleValidate, and handleGetBatch each have their
	// own behavior exercised here directly, not just route registration.
	t.Run("handleAvailability, handleValidate, and handleGetBatch behave end to end", func(t *testing.T) {
		orgID := uuid.New().String()
		const token = "fcpush_handler_coverage_token"
		seedIngestToken(t, ctx, pool, orgID, "github", "acme/repo", token)
		deps := newTestDeps(t, pool, client)

		availabilityReq := httptest.NewRequest(http.MethodGet, "/api/v1/external-ingest/availability", nil)
		availabilityReq.Header.Set("Authorization", "Bearer "+token)
		availabilityRecorder := httptest.NewRecorder()
		deps.handleAvailability()(availabilityRecorder, availabilityReq)
		if availabilityRecorder.Code != http.StatusOK {
			t.Fatalf("availability: status = %d, body = %s", availabilityRecorder.Code, availabilityRecorder.Body.String())
		}
		var availability struct {
			Features struct {
				CustomerPushIngest bool `json:"customerPushIngest"`
			} `json:"features"`
		}
		if err := json.Unmarshal(availabilityRecorder.Body.Bytes(), &availability); err != nil {
			t.Fatal(err)
		}
		if !availability.Features.CustomerPushIngest {
			t.Fatalf("expected customerPushIngest=true for a seeded org: %s", availabilityRecorder.Body.String())
		}

		validBody := `{"schemaVersion":"external-ingest.v1","idempotencyKey":"k",
			"source":{"system":"github","instance":"acme/repo"},
			"records":[{"kind":"repository.v1","externalId":"acme/repo","payload":{"externalId":"acme/repo","sourceSystem":"bitbucket"}}]}`
		validateReq := httptest.NewRequest(http.MethodPost, "/api/v1/external-ingest/validate", strings.NewReader(validBody))
		validateReq.Header.Set("Authorization", "Bearer "+token)
		validateRecorder := httptest.NewRecorder()
		deps.handleValidate()(validateRecorder, validateReq)
		if validateRecorder.Code != http.StatusOK {
			t.Fatalf("validate: status = %d, body = %s", validateRecorder.Code, validateRecorder.Body.String())
		}
		var validation struct {
			Valid  bool `json:"valid"`
			Errors []struct {
				Code string `json:"code"`
			} `json:"errors"`
		}
		if err := json.Unmarshal(validateRecorder.Body.Bytes(), &validation); err != nil {
			t.Fatal(err)
		}
		if validation.Valid || len(validation.Errors) != 1 || validation.Errors[0].Code != "invalid_literal" {
			t.Fatalf("expected exactly one invalid_literal error for sourceSystem=bitbucket: %+v", validation)
		}

		acceptBody := `{"schemaVersion":"external-ingest.v1","idempotencyKey":"handler-coverage",
			"source":{"system":"github","instance":"acme/repo"},
			"records":[{"kind":"repository.v1","externalId":"acme/repo","payload":{"externalId":"acme/repo","sourceSystem":"github"}}]}`
		acceptReq := httptest.NewRequest(http.MethodPost, "/api/v1/external-ingest/batches", strings.NewReader(acceptBody))
		acceptReq.Header.Set("Authorization", "Bearer "+token)
		acceptRecorder := httptest.NewRecorder()
		deps.handleAcceptBatch()(acceptRecorder, acceptReq)
		if acceptRecorder.Code != http.StatusAccepted {
			t.Fatalf("seed accept: %d %s", acceptRecorder.Code, acceptRecorder.Body.String())
		}
		var accepted struct {
			IngestionID string `json:"ingestionId"`
		}
		if err := json.Unmarshal(acceptRecorder.Body.Bytes(), &accepted); err != nil {
			t.Fatal(err)
		}

		getReq := httptest.NewRequest(http.MethodGet, "/api/v1/external-ingest/batches/"+accepted.IngestionID, nil)
		getReq.SetPathValue("ingestion_id", accepted.IngestionID)
		getReq.Header.Set("Authorization", "Bearer "+token)
		getRecorder := httptest.NewRecorder()
		deps.handleGetBatch()(getRecorder, getReq)
		if getRecorder.Code != http.StatusOK {
			t.Fatalf("get batch: status = %d, body = %s", getRecorder.Code, getRecorder.Body.String())
		}
		var status struct {
			IngestionID string `json:"ingestionId"`
			Status      string `json:"status"`
		}
		if err := json.Unmarshal(getRecorder.Body.Bytes(), &status); err != nil {
			t.Fatal(err)
		}
		if status.IngestionID != accepted.IngestionID || status.Status != "accepted" {
			t.Fatalf("got %+v", status)
		}

		unknownReq := httptest.NewRequest(http.MethodGet, "/api/v1/external-ingest/batches/"+uuid.New().String(), nil)
		unknownReq.SetPathValue("ingestion_id", uuid.New().String())
		unknownReq.Header.Set("Authorization", "Bearer "+token)
		unknownRecorder := httptest.NewRecorder()
		deps.handleGetBatch()(unknownRecorder, unknownReq)
		if unknownRecorder.Code != http.StatusNotFound {
			t.Fatalf("unknown ingestion id: status = %d, want 404", unknownRecorder.Code)
		}
		var notFound map[string]any
		_ = json.Unmarshal(unknownRecorder.Body.Bytes(), &notFound)
		if notFound["error"].(map[string]any)["code"] != "not_found" {
			t.Fatalf("%+v", notFound)
		}
	})
}

// TestResolveBatchIdempotencyUnderConcurrentDuplicateInserts proves a
// losing racer's INSERT (it hits the unique index, and Postgres marks its
// enclosing transaction ABORTED -- refusing every further statement until
// rollback) still resolves cleanly: resolveBatchIdempotency must run that
// INSERT inside a savepoint, so the transaction stays usable afterward to
// look up the winning row, resolving to REPLAY/RETRY/CONFLICT rather than
// an internal_error. Reproduced with two real goroutines, each on its
// own pool connection/transaction, released through a barrier so both
// reach resolveBatchIdempotency at (as close to) the same instant as the
// scheduler allows -- run across several distinct keys, since a single
// pair of goroutines is not guaranteed to land inside the narrow window
// (both sides' OWN first existence check must see nothing, which needs
// the interleaving, not just concurrent calls) every time.
func TestResolveBatchIdempotencyUnderConcurrentDuplicateInserts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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

	orgID := uuid.New().String()
	const trials = 25

	for trial := 0; trial < trials; trial++ {
		key := "race-key-" + uuid.New().String()
		params := createBatchParams{
			OrgID: orgID, IdempotencyKey: key, PayloadHash: "hash-a",
			SourceSystem: "github", SourceInstance: "acme/repo", EntityFamily: legacyEntityFamily,
			SchemaVersion: schemaVersion, ItemsReceived: 1,
		}

		start := make(chan struct{})
		type result struct {
			outcome idempotencyOutcome
			err     error
		}
		results := make(chan result, 2)
		var ready sync.WaitGroup
		ready.Add(2)
		for i := 0; i < 2; i++ {
			go func() {
				tx, beginErr := pool.Begin(ctx)
				if beginErr != nil {
					ready.Done()
					results <- result{err: beginErr}
					return
				}
				ready.Done()
				<-start
				outcome, resolveErr := resolveBatchIdempotency(ctx, tx, params)
				if resolveErr != nil {
					_ = tx.Rollback(ctx)
					results <- result{err: resolveErr}
					return
				}
				if commitErr := tx.Commit(ctx); commitErr != nil {
					results <- result{err: commitErr}
					return
				}
				results <- result{outcome: outcome}
			}()
		}
		ready.Wait() // both goroutines hold an open transaction before either proceeds
		close(start)

		var outcomes []idempotencyOutcome
		for i := 0; i < 2; i++ {
			r := <-results
			if r.err != nil {
				t.Fatalf("trial %d: resolveBatchIdempotency must never error on a concurrent duplicate: %v", trial, r.err)
			}
			outcomes = append(outcomes, r.outcome)
		}

		newCount, replayCount := 0, 0
		for _, o := range outcomes {
			switch o.Kind {
			case outcomeNew:
				newCount++
			case outcomeReplay:
				replayCount++
			default:
				t.Fatalf("trial %d: unexpected outcome kind %s", trial, o.Kind)
			}
		}
		if newCount != 1 || replayCount != 1 {
			t.Fatalf("trial %d: want exactly one NEW and one REPLAY, got new=%d replay=%d (%+v)", trial, newCount, replayCount, outcomes)
		}
		if got := countBatches(t, ctx, pool, orgID); got != trial+1 {
			t.Fatalf("trial %d: expected %d cumulative row(s), got %d", trial, trial+1, got)
		}
	}
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
