//go:build integration

package apiservice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ingestTokenHash mirrors auth.go's own sha256-hex token_hash computation
// (unexported there, so this is a second, trivial copy rather than a
// package split for one function).
func ingestTokenHash(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

// externalIngestVenueSeed names the fixture rows seedExternalIngestVenue
// writes: one organization (team tier, so customer_push_ingest -- min_tier
// "team", Alembic 0036 -- qualifies by tier alone, no override needed,
// matching the PagerDuty oracle's canonical_incident_ingestion pattern),
// one enabled customer_push source, one token bound to it carrying every
// scope this package's routes check (schema:read, ingest:write,
// ingest:status), and PRE-SEEDED external_ingest_batches rows with fixed,
// known ids -- an accept_batch response's own ingestionId is minted
// independently by each plane's own INSERT (the same reason webhookintake's
// blankVenueEventID exists), so a get-by-id request against a freshly
// ACCEPTED batch cannot use the same id on both planes; seeding the rows
// directly (inside Start's Seed hook, before the CREATE DATABASE ...
// TEMPLATE copy) gives both planes the identical row and id from the start.
//
// seededFailedBatchID and seededRecomputeBatchID cover the two response
// shapes round 1 review found this venue oracle's original 13 cases never
// reached: a mark_failed-shaped error_summary ({system_failure, reason} --
// a DIFFERENT shape than _build_error_summary()'s, which a fixed-shape
// converter silently dropped) and a recompute row carrying a persisted
// scope + a matching recompute_jobs log row.
type externalIngestVenueSeed struct {
	orgID, sourceID, token                                     string
	seededBatchID, seededFailedBatchID, seededRecomputeBatchID string
	recomputeDispatchedAt                                      string
}

func newExternalIngestVenueSeed() externalIngestVenueSeed {
	return externalIngestVenueSeed{
		orgID:                  uuid.New().String(),
		sourceID:               uuid.New().String(),
		token:                  "fcpush_venue-oracle-" + uuid.New().String(),
		seededBatchID:          uuid.New().String(),
		seededFailedBatchID:    uuid.New().String(),
		seededRecomputeBatchID: uuid.New().String(),
		// A fixed literal, not now(): the batch row and the job row below
		// must carry the IDENTICAL timestamp for listRecomputeJobs'
		// dispatched_at equality join (recompute_status.py's
		// get_recompute_jobs joins the same way) to match either row to
		// the other on both planes.
		recomputeDispatchedAt: "2026-09-10T12:00:00+00:00",
	}
}

// seedExternalIngestVenue writes the fixture rows for seed against the
// REAL Alembic-migrated schema (unlike accept_batch_integration_test.go's
// own hand-rolled tables, which exist so that unit test can run without a
// live venue -- this runs inside venueoracle.Start's Seed hook, against the
// admin pool, before the CREATE DATABASE ... TEMPLATE copy, so one write
// reaches both planes).
func seedExternalIngestVenue(t *testing.T, ctx context.Context, admin *pgxpool.Pool, seed externalIngestVenueSeed) {
	t.Helper()
	tokenHash := ingestTokenHash(seed.token)
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier) VALUES ($1::uuid, $2, $3, 'team')`,
			[]any{seed.orgID, "venue-external-ingest", "Venue External Ingest"}},
		{`INSERT INTO external_ingest_sources (id, org_id, system, instance, entity_family, mode, enabled, created_at, updated_at)
VALUES ($1::uuid, $2, 'github', 'acme/venue-repo', 'legacy', 'customer_push', true, now(), now())`,
			[]any{seed.sourceID, seed.orgID}},
		{`INSERT INTO external_ingest_tokens (id, org_id, source_id, name, token_hash, token_prefix, scopes, created_at)
VALUES ($1::uuid, $2, $3::uuid, 'venue oracle token', $4, 'fcpush_venue', $5::jsonb, now())`,
			[]any{uuid.New().String(), seed.orgID, seed.sourceID, tokenHash, `["schema:read","ingest:write","ingest:status"]`}},
		{`INSERT INTO external_ingest_batches (ingestion_id, org_id, idempotency_key, payload_hash, source_system, source_instance, schema_version, items_received, created_at, updated_at)
VALUES ($1::uuid, $2, 'venue-seeded-batch', 'venue-seeded-hash', 'github', 'acme/venue-repo', 'external-ingest.v1', 1, now(), now())`,
			[]any{seed.seededBatchID, seed.orgID}},
		// mark_failed's shape (status.py:723): {"system_failure": true,
		// "reason": ...} -- NOT _build_error_summary()'s
		// {total_rejected, stored_rejections, truncated, top_codes} shape.
		// Planted directly (mirroring the seeded-batch case above, not by
		// driving the real worker failure path) since the row shape, not
		// the path that produced it, is what the writer must render
		// byte-identically.
		{`INSERT INTO external_ingest_batches (ingestion_id, org_id, idempotency_key, payload_hash, source_system, source_instance, schema_version, status, items_received, items_accepted, items_rejected, error_summary, completed_at, created_at, updated_at)
VALUES ($1::uuid, $2, 'venue-seeded-failed-batch', 'venue-seeded-failed-hash', 'github', 'acme/venue-repo', 'external-ingest.v1', 'failed', 1, 0, 1, $3::jsonb, now(), now(), now())`,
			[]any{seed.seededFailedBatchID, seed.orgID, `{"system_failure": true, "reason": "worker failed"}`}},
		// A recompute row carrying a persisted scope (status.py's
		// RecomputeScopeResponse: repoIds, teamIds, windowStartedAt,
		// windowEndedAt, cappedDays, cappedRepos) plus one matching
		// external_ingest_recompute_jobs row -- recompute_status.py's
		// get_recompute_jobs joins jobs to a batch's dispatch by
		// (org_id, source_system, source_instance, dispatched_at), not by
		// ingestion_id, so the job row below shares seed.orgID/github/
		// acme/venue-repo/recomputeDispatchedAt with this batch row.
		//
		// windowStartedAt deliberately carries a NON-UTC offset (+05:30)
		// and a non-zero, trailing-zero microsecond fraction (.123400);
		// windowEndedAt carries the same offset with no fraction at all
		// (round 2 review): the producer writes
		// `scope.window_start.isoformat()`, which preserves whatever aware
		// offset the value had, and Pydantic's datetime serializer never
		// trims a non-zero microsecond field to fewer than six digits or
		// omits a non-UTC offset -- a values-all-Z-and-round-seconds seed
		// (the round 1 shape) can't distinguish pytime's exact
		// fromisoformat/Pydantic pairing from a naive UTC-forcing,
		// trailing-zero-trimming formatter; this one can.
		{`INSERT INTO external_ingest_batches (ingestion_id, org_id, idempotency_key, payload_hash, source_system, source_instance, schema_version, items_received, recompute_status, recompute_scope, recompute_dispatched_at, created_at, updated_at)
VALUES ($1::uuid, $2, 'venue-seeded-recompute-batch', 'venue-seeded-recompute-hash', 'github', 'acme/venue-repo', 'external-ingest.v1', 1, 'dispatched', $3::jsonb, $4::timestamptz, now(), now())`,
			[]any{seed.seededRecomputeBatchID, seed.orgID,
				`{"repoIds":["repo-1","repo-2"],"teamIds":["team-1"],"windowStartedAt":"2026-09-01T00:00:00.123400+05:30","windowEndedAt":"2026-09-08T00:00:00+05:30","cappedDays":true,"cappedRepos":false}`,
				seed.recomputeDispatchedAt},
		},
		{`INSERT INTO external_ingest_recompute_jobs (id, org_id, source_system, source_instance, celery_task_name, celery_task_id, queue, repo_id, status, dispatched_at)
VALUES ($1::uuid, $2, 'github', 'acme/venue-repo', 'run_daily_metrics', 'task-123', 'metrics', 'repo-1', 'dispatched', $3::timestamptz)`,
			[]any{uuid.New().String(), seed.orgID, seed.recomputeDispatchedAt}},
	}
	for _, statement := range statements {
		if _, err := admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed %s: %v", statement.sql, err)
		}
	}
}

// blankExternalIngestVolatileFields blanks response fields a fresh
// accept/replay/list mints independently on each plane (ingestionId,
// createdAt, updatedAt) -- the seeded-batch get-by-id case needs no
// blanking (both planes read the identical seeded row), but running this
// normalizer over every response is harmless there: blanking an
// already-identical value changes nothing about whether the two sides
// match.
func blankExternalIngestVolatileFields(_ venueoracle.Request, body string) string {
	decoded, err := pyjson.DecodeString(body)
	if err != nil {
		return body
	}
	object, ok := decoded.(*pyjson.Object)
	if !ok {
		return body
	}
	changed := false
	for _, key := range []string{"ingestionId", "createdAt", "updatedAt"} {
		if _, ok := object.Get(key); ok {
			object.Set(key, "<"+key+">")
			changed = true
		}
	}
	if items, ok := object.Get("items"); ok {
		if list, ok := items.([]pyjson.Value); ok {
			for _, item := range list {
				if itemObject, ok := item.(*pyjson.Object); ok {
					for _, key := range []string{"ingestionId", "createdAt"} {
						if _, ok := itemObject.Get(key); ok {
							itemObject.Set(key, "<"+key+">")
							changed = true
						}
					}
				}
			}
		}
	}
	if !changed {
		return body
	}
	rewritten, err := pyjson.Marshal(object)
	if err != nil {
		return body
	}
	return string(rewritten)
}

// TestExternalIngestVenueOracle is CHAOS-6321's proof shape: the REAL
// Python api and the REAL Go api answer identical requests against every
// /api/v1/external-ingest/* route this change touched, on two copies of
// one Alembic-migrated Postgres, diffed byte for byte -- both the success
// shapes (list/get schemas, availability, validate, accept, list/get
// batches) and every error class writeIngestError/ValidationErrorItem now
// render through policy.WriteJSON + an ordered *pyjson.Object instead of a
// map[string]any.
//
// GET /schemas/{version}'s document body is NOT diffed here: that response
// stays a stdlib map[string]any writer by design (bundle.go's schemaDocument
// doc comment, a NAMED LIMIT this change does not touch), so a byte-for-byte
// diff against it would only prove the two planes' JSON Schema GENERATORS
// agree, not anything this PR changed. The route is still called, once, to
// prove it answers 200 with a matching ETag on both planes (the cheap,
// relevant half of that route this PR's scope covers).
func TestExternalIngestVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := webhookintakeRepoRoot(t)

	seed := newExternalIngestVenueSeed()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: "venue-oracle-jwt-signing-key-32-bytes-min",
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seedExternalIngestVenue(t, ctx, admin, seed)
			return nil
		},
	})

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
	deps := Deps{Pool: pool, Valkey: goValkey}
	server, err := NewServer(config.Config{APIAddress: "127.0.0.1:0"}, logger, Routes(deps, logger))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatalf("start server: %v (api role gaps: %s)", err, venue.DiagnoseAPIRole(t, ctx))
	}
	t.Cleanup(func() { _ = server.Shutdown(ctx) })
	goBase := "http://" + server.Address()

	auth := map[string]string{"Authorization": "Bearer " + seed.token}
	jsonHeaders := func(extra map[string]string) map[string]string {
		headers := map[string]string{"Content-Type": "application/json"}
		for k, v := range extra {
			headers[k] = v
		}
		return headers
	}

	// The SAME idempotencyKey for the NEW-accept case and the replay case
	// below -- a customer's exact retry, which router.py's REPLAY path
	// answers 200 with the full status envelope (writeReplayStatus ->
	// batchStatusResponse), not the narrower 202 accept shape.
	acceptBody := `{"schemaVersion":"external-ingest.v1","idempotencyKey":"venue-batch-1",` +
		`"source":{"system":"github","instance":"acme/venue-repo"},` +
		`"records":[{"kind":"repository.v1","externalId":"acme/venue-repo",` +
		`"payload":{"externalId":"acme/venue-repo","sourceSystem":"github"}}]}`

	requests := []venueoracle.Request{
		{Name: "list schemas", Method: "GET", Path: "/api/v1/external-ingest/schemas"},
		{Name: "get schema unknown version", Method: "GET", Path: "/api/v1/external-ingest/schemas/external-ingest.v99"},
		// pythonRepr's quote-delimiter switch (round 1 review, reproduced
		// live): a version containing a single quote and no double quote
		// makes CPython's repr switch to double quotes rather than escape
		// the embedded ' -- a naive always-single-quote repr diverges here
		// where the quote-free "v99" case above never could.
		{Name: "get schema unknown version with quote", Method: "GET", Path: "/api/v1/external-ingest/schemas/bad'version"},
		{Name: "availability authenticated", Method: "GET", Path: "/api/v1/external-ingest/availability", Headers: auth},
		{Name: "availability unauthenticated", Method: "GET", Path: "/api/v1/external-ingest/availability"},
		{
			Name: "validate accepted", Method: "POST", Path: "/api/v1/external-ingest/validate",
			Headers: jsonHeaders(auth),
			Body: venueoracle.B64(`{"schemaVersion":"external-ingest.v1","idempotencyKey":"venue-validate-1",` +
				`"source":{"system":"github","instance":"acme/venue-repo"},` +
				`"records":[{"kind":"repository.v1","externalId":"acme/venue-repo",` +
				`"payload":{"externalId":"acme/venue-repo","sourceSystem":"github"}}]}`),
		},
		{
			Name: "validate unknown kind", Method: "POST", Path: "/api/v1/external-ingest/validate",
			Headers: jsonHeaders(auth),
			Body: venueoracle.B64(`{"schemaVersion":"external-ingest.v1","idempotencyKey":"venue-validate-2",` +
				`"source":{"system":"github","instance":"acme/venue-repo"},` +
				`"records":[{"kind":"not_a_real_kind.v1","externalId":"x","payload":{}}]}`),
		},
		{
			// Same pythonRepr quote-delimiter case as the schema-version one
			// above, hit through validate.go's toPyJSON message path instead
			// of errors.go's.
			Name: "validate unknown kind with quote", Method: "POST", Path: "/api/v1/external-ingest/validate",
			Headers: jsonHeaders(auth),
			Body: venueoracle.B64(`{"schemaVersion":"external-ingest.v1","idempotencyKey":"venue-validate-3",` +
				`"source":{"system":"github","instance":"acme/venue-repo"},` +
				`"records":[{"kind":"not_a_real_kind's.v1","externalId":"x","payload":{}}]}`),
		},
		{
			Name: "accept new batch", Method: "POST", Path: "/api/v1/external-ingest/batches",
			Headers: jsonHeaders(auth), Body: venueoracle.B64(acceptBody),
		},
		{
			// The exact same body again: the REPLAY path.
			Name: "accept replayed batch", Method: "POST", Path: "/api/v1/external-ingest/batches",
			Headers: jsonHeaders(auth), Body: venueoracle.B64(acceptBody),
		},
		{
			Name: "accept unknown record kind", Method: "POST", Path: "/api/v1/external-ingest/batches",
			Headers: jsonHeaders(auth),
			Body: venueoracle.B64(`{"schemaVersion":"external-ingest.v1","idempotencyKey":"venue-batch-2",` +
				`"source":{"system":"github","instance":"acme/venue-repo"},` +
				`"records":[{"kind":"not_a_real_kind.v1","externalId":"x","payload":{}}]}`),
		},
		{Name: "list batches", Method: "GET", Path: "/api/v1/external-ingest/batches", Headers: auth},
		{Name: "get batches unauthenticated", Method: "GET", Path: "/api/v1/external-ingest/batches"},
		// The SEEDED batch (fixed id, identical row on both planes from the
		// start -- see externalIngestVenueSeed's doc comment): a real
		// "get one batch" round-trip without the per-plane-minted-id problem
		// a freshly accepted batch would have.
		{Name: "get seeded batch by id", Method: "GET", Path: "/api/v1/external-ingest/batches/" + seed.seededBatchID, Headers: auth},
		// error_summary's second real shape (mark_failed's {system_failure,
		// reason}, round 1 review): a fixed-shape converter that only knew
		// _build_error_summary()'s four keys silently dropped both of these.
		{Name: "get seeded failed batch by id", Method: "GET", Path: "/api/v1/external-ingest/batches/" + seed.seededFailedBatchID, Headers: auth},
		// recompute.scope + recompute.jobs (round 1 review): a persisted
		// scope and one matching recompute_jobs row, both previously
		// hardcoded to null/[] regardless of what was stored.
		{Name: "get seeded recompute batch by id", Method: "GET", Path: "/api/v1/external-ingest/batches/" + seed.seededRecomputeBatchID, Headers: auth},
		{Name: "get batch unknown id", Method: "GET", Path: "/api/v1/external-ingest/batches/" + uuid.New().String(), Headers: auth},
	}

	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{Normalize: blankExternalIngestVolatileFields})
	t.Log(receipt)

	// GET /schemas/{version}'s document body is a NAMED LIMIT (bundle.go's
	// schemaDocument doc comment): it stays a stdlib map[string]any writer,
	// so a full-body Diff would only prove the two planes' JSON Schema
	// GENERATORS produce the same document, not anything this PR touched.
	// This checks the relevant half instead: both planes answer 200 with a
	// matching ETag for the identical request.
	schemaRequest := venueoracle.Request{Name: "get schema known version", Method: "GET", Path: "/api/v1/external-ingest/schemas/external-ingest.v1"}
	schemaPython := venue.ServePython(t, []venueoracle.Request{schemaRequest})[0]
	schemaGo := venueoracle.Do(t, goBase, schemaRequest)
	if schemaPython.Status != schemaGo.Status {
		t.Errorf("get schema known version: status python=%d go=%d", schemaPython.Status, schemaGo.Status)
	}
	if schemaPython.Headers["etag"] != schemaGo.Headers["etag"] || schemaPython.Headers["etag"] == "" {
		t.Errorf("get schema known version: etag python=%q go=%q", schemaPython.Headers["etag"], schemaGo.Headers["etag"])
	}

	pythonBatches := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB),
		`SELECT idempotency_key, status, items_received, items_accepted, items_rejected FROM external_ingest_batches ORDER BY idempotency_key`)
	goBatches := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB),
		`SELECT idempotency_key, status, items_received, items_accepted, items_rejected FROM external_ingest_batches ORDER BY idempotency_key`)
	if pythonBatches != goBatches {
		t.Errorf("external_ingest_batches rows differ:\n python: %s\n go:     %s", pythonBatches, goBatches)
	}
	// venueoracle.Start already wrote this test's own proof file.
}
