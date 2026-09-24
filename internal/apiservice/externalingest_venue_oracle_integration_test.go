//go:build integration

package apiservice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
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
		// created_at/updated_at are a FIXED literal, not now(): its
		// microseconds (254860) end in a trailing zero on purpose --
		// formatOptionalRFC3339 used to format this through Go's
		// RFC3339Nano, which trims trailing zero fractional digits, where
		// Pydantic's JSON datetime serializer always renders exactly six.
		// now() lands on a value with a trailing zero roughly 1 run in 10,
		// which is exactly how this bug flaked the required venue-oracles
		// check instead of failing it outright -- a fixed value with a
		// trailing zero makes a regression fail every run, not 1 in 10.
		{`INSERT INTO external_ingest_batches (ingestion_id, org_id, idempotency_key, payload_hash, source_system, source_instance, schema_version, items_received, created_at, updated_at)
VALUES ($1::uuid, $2, 'venue-seeded-batch', 'venue-seeded-hash', 'github', 'acme/venue-repo', 'external-ingest.v1', 1, '2026-09-15T08:30:22.254860+00:00'::timestamptz, '2026-09-15T08:30:22.254860+00:00'::timestamptz)`,
			[]any{seed.seededBatchID, seed.orgID}},
		// mark_failed's shape (status.py:723): {"system_failure": true,
		// "reason": ...} -- NOT _build_error_summary()'s
		// {total_rejected, stored_rejections, truncated, top_codes} shape.
		// Planted directly (mirroring the seeded-batch case above, not by
		// driving the real worker failure path) since the row shape, not
		// the path that produced it, is what the writer must render
		// byte-identically.
		// Same fixed-literal, trailing-zero-microsecond reasoning as the
		// plain seeded batch above, extended to completed_at too.
		{`INSERT INTO external_ingest_batches (ingestion_id, org_id, idempotency_key, payload_hash, source_system, source_instance, schema_version, status, items_received, items_accepted, items_rejected, error_summary, completed_at, created_at, updated_at)
VALUES ($1::uuid, $2, 'venue-seeded-failed-batch', 'venue-seeded-failed-hash', 'github', 'acme/venue-repo', 'external-ingest.v1', 'failed', 1, 0, 1, $3::jsonb, '2026-09-15T08:30:22.254860+00:00'::timestamptz, '2026-09-15T08:30:22.254860+00:00'::timestamptz, '2026-09-15T08:30:22.254860+00:00'::timestamptz)`,
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
VALUES ($1::uuid, $2, 'venue-seeded-recompute-batch', 'venue-seeded-recompute-hash', 'github', 'acme/venue-repo', 'external-ingest.v1', 1, 'dispatched', $3::jsonb, $4::timestamptz, '2026-09-15T08:30:22.254860+00:00'::timestamptz, '2026-09-15T08:30:22.254860+00:00'::timestamptz)`,
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
// seededByIDRequestNames names the three "get seeded ... by id" cases:
// both planes read the IDENTICAL row (seeded once, before the CREATE
// DATABASE ... TEMPLATE copy, with a FIXED created_at/updated_at literal),
// so their ingestionId/createdAt/updatedAt are not volatile at all and
// blanking them would hide a real divergence -- exactly what happened
// before this map existed: a round-1 review of CHAOS-6359 reverted only
// batchStatusResponse's inline createdAt/updatedAt formatting (leaving the
// shared formatOptionalRFC3339 helper fixed) and the live oracle still
// PASSED, because these three cases' createdAt/updatedAt were blanked
// before comparison.
var seededByIDRequestNames = map[string]bool{
	"get seeded batch by id":           true,
	"get seeded failed batch by id":    true,
	"get seeded recompute batch by id": true,
}

func blankExternalIngestVolatileFields(request venueoracle.Request, body string) string {
	// Replaced in the raw text, so every other byte stays as the plane
	// wrote it (venueoracle.RedactJSON).
	seeded := seededByIDRequestNames[request.Name]
	return venueoracle.RedactJSON(body, func(path []string, _ string) (string, bool) {
		switch {
		case len(path) == 1 && !seeded && (path[0] == "ingestionId" || path[0] == "createdAt" || path[0] == "updatedAt"):
			return `"<` + path[0] + `>"`, true
		case len(path) == 3 && path[0] == "items" && path[1] == "[]" && (path[2] == "ingestionId" || path[2] == "createdAt"):
			return `"<` + path[2] + `>"`, true
		}
		return "", false
	})
}

// seededListItemField finds the items[] entry whose ingestionId is
// ingestionID in a GET /batches response body and returns its named string
// field, or "" (and fails the test) if either is missing.
func seededListItemField(t *testing.T, body, ingestionID, field string) string {
	t.Helper()
	decoded, err := pyjson.DecodeString(body)
	if err != nil {
		t.Fatalf("decode list batches body: %v", err)
	}
	object, ok := decoded.(*pyjson.Object)
	if !ok {
		t.Fatalf("list batches body is not an object: %s", body)
	}
	items, ok := object.Get("items")
	if !ok {
		t.Fatalf("list batches body has no items: %s", body)
	}
	list, ok := items.([]pyjson.Value)
	if !ok {
		t.Fatalf("list batches items is not an array: %s", body)
	}
	for _, item := range list {
		itemObject, ok := item.(*pyjson.Object)
		if !ok {
			continue
		}
		id, ok := itemObject.Get("ingestionId")
		if !ok {
			continue
		}
		idStr, _ := id.(string)
		if idStr != ingestionID {
			continue
		}
		value, ok := itemObject.Get(field)
		if !ok {
			t.Fatalf("list batches item %q has no field %q", ingestionID, field)
		}
		valueStr, _ := value.(string)
		return valueStr
	}
	t.Fatalf("list batches: no item with ingestionId %q", ingestionID)
	return ""
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
// GET /schemas/{version}'s document is diffed here like every other route:
// the body is compared as raw text (key order, separators, no trailing
// newline), so it fails on any byte the two planes render differently.
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
		{Name: "get schema known version", Method: "GET", Path: "/api/v1/external-ingest/schemas/external-ingest.v1"},
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
		// Malformed envelopes (the shared exact envelope validation): the
		// 400 carries errors=[dict(e) ...]; where Python's json.dumps cannot
		// write them, both planes give the unhandled 500.
		malformedEnvelope("validate JSON syntax error", "/api/v1/external-ingest/validate", auth, `{"schemaVersion": `),
		malformedEnvelope("validate trailing data", "/api/v1/external-ingest/validate", auth, `{} x`),
		malformedEnvelope("validate shape errors", "/api/v1/external-ingest/validate", auth,
			`{"schemaVersion":1,"idempotencyKey":"","source":{"system":"x","instance":"","extra":1},"records":[1,{"kind":1,"externalId":"","payload":[],"q":1}],"zzz":1}`),
		malformedEnvelope("validate null payload", "/api/v1/external-ingest/validate", auth,
			`{"schemaVersion":"external-ingest.v1","idempotencyKey":"k","source":{"system":"github","instance":"acme/venue-repo"},"records":[{"kind":"repository.v1","externalId":"e","payload":null}]}`),
		malformedEnvelope("validate storyPoints integer past float64 range", "/api/v1/external-ingest/validate", auth,
			`{"schemaVersion":"external-ingest.v1","idempotencyKey":"k","source":{"system":"github","instance":"acme/venue-repo"},"records":[{"kind":"work_item.v1","externalId":"w","payload":{"externalKey":"1","provider":"github","title":"T","type":"issue","status":"todo","createdAt":"2026-07-22T10:00:00Z","repositoryExternalId":"acme/venue-repo","storyPoints":`+"1"+strings.Repeat("0", 400)+`}}]}`),
		malformedEnvelope("validate storyPoints integer just under float64 range", "/api/v1/external-ingest/validate", auth,
			`{"schemaVersion":"external-ingest.v1","idempotencyKey":"k","source":{"system":"github","instance":"acme/venue-repo"},"records":[{"kind":"work_item.v1","externalId":"w","payload":{"externalKey":"1","provider":"github","title":"T","type":"issue","status":"todo","createdAt":"2026-07-22T10:00:00Z","repositoryExternalId":"acme/venue-repo","storyPoints":`+"1"+strings.Repeat("0", 308)+`}}]}`),
		malformedEnvelope("validate storyPoints negative integer past float64 range", "/api/v1/external-ingest/validate", auth,
			`{"schemaVersion":"external-ingest.v1","idempotencyKey":"k","source":{"system":"github","instance":"acme/venue-repo"},"records":[{"kind":"work_item.v1","externalId":"w","payload":{"externalKey":"1","provider":"github","title":"T","type":"issue","status":"todo","createdAt":"2026-07-22T10:00:00Z","repositoryExternalId":"acme/venue-repo","storyPoints":`+"-1"+strings.Repeat("0", 400)+`}}]}`),
		malformedEnvelope("validate empty records", "/api/v1/external-ingest/validate", auth,
			`{"schemaVersion":"external-ingest.v1","idempotencyKey":"k","source":{"system":"github","instance":"acme/venue-repo"},"records":[]}`),
		malformedEnvelope("validate field names and record errors", "/api/v1/external-ingest/validate", auth,
			`{"schema_version":"external-ingest.v1","idempotency_key":"k","source":{"system":"github","instance":"acme/venue-repo"},"records":[{"kind":"repository.v1","external_id":"e","payload":{"externalId":"e","sourceSystem":"github","tags":["a",1],"settings":{"a":[1]}}}]}`),
		malformedEnvelope("validate window ends before it starts", "/api/v1/external-ingest/validate", auth,
			`{"schemaVersion":"external-ingest.v1","idempotencyKey":"k","source":{"system":"github","instance":"acme/venue-repo"},"window":{"startedAt":"2026-01-02T00:00:00Z","endedAt":"2026-01-01T00:00:00Z"},"records":[{"kind":"repository.v1","externalId":"e","payload":{}}]}`),
		malformedEnvelope("validate naive and aware window", "/api/v1/external-ingest/validate", auth,
			`{"schemaVersion":"external-ingest.v1","idempotencyKey":"k","source":{"system":"github","instance":"acme/venue-repo"},"window":{"startedAt":"2026-01-02T00:00:00","endedAt":"2026-01-01T00:00:00Z"},"records":[{"kind":"repository.v1","externalId":"e","payload":{}}]}`),
		malformedEnvelope("accept shape errors", "/api/v1/external-ingest/batches", auth, `{"records":[]}`),
		malformedEnvelope("accept JSON syntax error", "/api/v1/external-ingest/batches", auth, `[1,]`),
		// jiter refuses an integer part (sign included) past 4300 characters:
		// Python answers its unhandled 500 and stores no batch row.
		malformedEnvelope("validate integer part at jiter's limit", "/api/v1/external-ingest/validate", auth, `{"extra":`+strings.Repeat("1", 4300)+`}`),
		malformedEnvelope("validate integer part over jiter's limit", "/api/v1/external-ingest/validate", auth, `{"extra":`+strings.Repeat("1", 4301)+`}`),
		malformedEnvelope("validate signed integer part over jiter's limit", "/api/v1/external-ingest/validate", auth, `{"extra":-`+strings.Repeat("1", 4300)+`}`),
		malformedEnvelope("validate float integer part over jiter's limit", "/api/v1/external-ingest/validate", auth, `{"extra":`+strings.Repeat("1", 4301)+`.5}`),
		malformedEnvelope("accept integer part over jiter's limit", "/api/v1/external-ingest/batches", auth,
			`{"schemaVersion":"external-ingest.v1","idempotencyKey":"venue-huge-int","source":{"system":"github","instance":"acme/api"},"records":[{"kind":"repository.v1","externalId":"e","payload":{"n":`+strings.Repeat("1", 4301)+`}}]}`),
		malformedEnvelope("accept integer part at jiter's limit", "/api/v1/external-ingest/batches", auth,
			`{"schemaVersion":"external-ingest.v1","idempotencyKey":"venue-huge-int-ok","source":{"system":"github","instance":"acme/api"},"records":[{"kind":"repository.v1","externalId":"e","payload":{"n":`+strings.Repeat("1", 4300)+`}}]}`),
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
		{Name: "list batches createdAfter exponent", Method: "GET", Path: "/api/v1/external-ingest/batches?createdAfter=1e5", Headers: auth},
		{Name: "list batches createdAfter unix seconds", Method: "GET", Path: "/api/v1/external-ingest/batches?createdAfter=1767323045", Headers: auth},
		{Name: "list batches createdAfter naive iso", Method: "GET", Path: "/api/v1/external-ingest/batches?createdAfter=2026-01-01T00:00:00", Headers: auth},
		{Name: "list batches createdBefore garbage with bad page", Method: "GET", Path: "/api/v1/external-ingest/batches?createdBefore=bogus&limit=0&offset=-1", Headers: auth},
		{Name: "list batches limit not an integer", Method: "GET", Path: "/api/v1/external-ingest/batches?limit=abc", Headers: auth},
		{Name: "list batches limit over the cap", Method: "GET", Path: "/api/v1/external-ingest/batches?limit=201", Headers: auth},
		{Name: "list batches limit leading zeros then minus", Method: "GET", Path: "/api/v1/external-ingest/batches?limit=0-5", Headers: auth},
		{Name: "list batches limit leading zeros and underscores", Method: "GET", Path: "/api/v1/external-ingest/batches?limit=0__5", Headers: auth},
		{Name: "list batches limit leading underscores beyond 4300 chars", Method: "GET", Path: "/api/v1/external-ingest/batches?limit=" + strings.Repeat("0_", 2150) + "5", Headers: auth},
		{Name: "list batches empty status filter", Method: "GET", Path: "/api/v1/external-ingest/batches?status=", Headers: auth},
		{Name: "list batches offset past int64", Method: "GET", Path: "/api/v1/external-ingest/batches?offset=99999999999999999999", Headers: auth},
		{Name: "list batches repeated createdAfter", Method: "GET", Path: "/api/v1/external-ingest/batches?createdAfter=bogus&createdAfter=2026-01-01T00:00:00Z", Headers: auth},
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

	// batchListItem's own createdAt formatting (handlers.go, separate from
	// batchStatusResponse's) is only reached through GET /batches -- and
	// that list mixes freshly-accepted rows (genuinely per-plane-volatile
	// createdAt, which blankExternalIngestVolatileFields must still blank
	// in the Diff above) with the seeded ones. This pins the one list item
	// this test CAN check exactly: the seeded batch's own entry, by its
	// known fixed id, unblanked.
	listRequest := venueoracle.Request{Name: "list batches (createdAt check)", Method: "GET", Path: "/api/v1/external-ingest/batches", Headers: auth}
	listPython := venue.ServePython(t, []venueoracle.Request{listRequest})[0]
	listGo := venueoracle.Do(t, goBase, listRequest)
	pythonSeededCreatedAt := seededListItemField(t, listPython.Body, seed.seededBatchID, "createdAt")
	goSeededCreatedAt := seededListItemField(t, listGo.Body, seed.seededBatchID, "createdAt")
	if pythonSeededCreatedAt != goSeededCreatedAt || pythonSeededCreatedAt == "" {
		t.Errorf("list batches: seeded item createdAt python=%q go=%q", pythonSeededCreatedAt, goSeededCreatedAt)
	}

	pythonBatches := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB),
		`SELECT idempotency_key, status, items_received, items_accepted, items_rejected, coalesce(record_counts::text, '<null>'),
			coalesce(error_summary::text, '<null>'), coalesce(recompute_scope::text, '<null>') FROM external_ingest_batches ORDER BY idempotency_key`)
	goBatches := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB),
		`SELECT idempotency_key, status, items_received, items_accepted, items_rejected, coalesce(record_counts::text, '<null>'),
			coalesce(error_summary::text, '<null>'), coalesce(recompute_scope::text, '<null>') FROM external_ingest_batches ORDER BY idempotency_key`)
	if pythonBatches != goBatches {
		t.Errorf("external_ingest_batches rows differ:\n python: %s\n go:     %s", pythonBatches, goBatches)
	}
	// venueoracle.Start already wrote this test's own proof file.
}

// malformedEnvelope is a JSON POST of body to path with the ingest token.
func malformedEnvelope(name, path string, auth map[string]string, body string) venueoracle.Request {
	headers := map[string]string{"Content-Type": "application/json"}
	for key, value := range auth {
		headers[key] = value
	}
	return venueoracle.Request{Name: name, Method: "POST", Path: path, Headers: headers, Body: venueoracle.B64(body)}
}
