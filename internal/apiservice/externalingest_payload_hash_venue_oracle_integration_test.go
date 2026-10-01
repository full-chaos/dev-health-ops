//go:build integration

package apiservice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
	"github.com/jackc/pgx/v5/pgxpool"
)

// payloadHashIngestTokenHash mirrors auth.go's own sha256-hex token_hash
// computation (unexported there; a second trivial copy, same reason
// externalingest's own venue oracle test carries one -- no package split
// for one function). Named distinctly from that file's own
// ingestTokenHash: this test's branch predates CHAOS-6321's merge and does
// not have that file yet, so nothing to collide with today, but this file
// is expected to coexist with it once this branch rebases past main.
func payloadHashIngestTokenHash(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

// payloadHashVenueSeed is this test's own minimal org/source/token fixture
// -- CHAOS-6350 is a payload-hash-only concern, so it seeds nothing this
// PR's own externalingest_venue_oracle_integration_test.go (not yet on
// this branch, landed by CHAOS-6321 after this branch forked) will need to
// duplicate once the two coexist.
type payloadHashVenueSeed struct {
	orgID, sourceID, token string
	// operationalSourceID/operationalToken are a SECOND source+token pair,
	// entity_family='operational' -- requireMatchingSource checks
	// system+instance+entity_family against the token's bound source, so
	// the "operational" case can't reuse the legacy-family token/source
	// above.
	operationalSourceID, operationalToken string
}

func newPayloadHashVenueSeed() payloadHashVenueSeed {
	return payloadHashVenueSeed{
		orgID:               stableVenueID("ph-orgID").String(),
		sourceID:            stableVenueID("ph-sourceID").String(),
		token:               "fcpush_venue-payloadhash-" + stableVenueID("ph-token").String(),
		operationalSourceID: stableVenueID("ph-operationalSourceID").String(),
		operationalToken:    "fcpush_venue-payloadhash-ops-" + stableVenueID("ph-operationalToken").String(),
	}
}

func seedPayloadHashVenue(t *testing.T, ctx context.Context, admin *pgxpool.Pool, seed payloadHashVenueSeed) {
	t.Helper()
	tokenHash := payloadHashIngestTokenHash(seed.token)
	operationalTokenHash := payloadHashIngestTokenHash(seed.operationalToken)
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier) VALUES ($1::uuid, $2, $3, 'team')`,
			[]any{seed.orgID, "venue-payload-hash", "Venue Payload Hash"}},
		{`INSERT INTO external_ingest_sources (id, org_id, system, instance, entity_family, mode, enabled, created_at, updated_at)
VALUES ($1::uuid, $2, 'github', 'acme/payload-hash-repo', 'legacy', 'customer_push', true, now(), now())`,
			[]any{seed.sourceID, seed.orgID}},
		{`INSERT INTO external_ingest_tokens (id, org_id, source_id, name, token_hash, token_prefix, scopes, created_at)
VALUES ($1::uuid, $2, $3::uuid, 'venue payload-hash token', $4, 'fcpush_venue', $5::jsonb, now())`,
			[]any{stableVenueID("ph-token-row").String(), seed.orgID, seed.sourceID, tokenHash, `["schema:read","ingest:write","ingest:status"]`}},
		{`INSERT INTO external_ingest_sources (id, org_id, system, instance, entity_family, mode, enabled, created_at, updated_at)
VALUES ($1::uuid, $2, 'github', 'acme/payload-hash-repo-ops', 'operational', 'customer_push', true, now(), now())`,
			[]any{seed.operationalSourceID, seed.orgID}},
		{`INSERT INTO external_ingest_tokens (id, org_id, source_id, name, token_hash, token_prefix, scopes, created_at)
VALUES ($1::uuid, $2, $3::uuid, 'venue payload-hash operational token', $4, 'fcpush_venue', $5::jsonb, now())`,
			[]any{stableVenueID("ph-ops-token-row").String(), seed.orgID, seed.operationalSourceID, operationalTokenHash, `["schema:read","ingest:write","ingest:status"]`}},
	}
	for _, statement := range statements {
		if _, err := admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed %s: %v", statement.sql, err)
		}
	}
}

// acceptedBatchRow is the subset of an external_ingest_batches row
// readAcceptedRow reads back and plantRow re-inserts -- enough to
// reconstruct a fresh accept's row on the OTHER plane's database, which is
// what makes the cross-plane retry check in
// TestExternalIngestPayloadHashVenueOracle possible: the venue harness
// gives each plane its OWN database (a template copy taken before any
// test-time accept, CHAOS-6321's venueoracle.Start doc comment), so a row
// ACCEPTED on one plane never exists on the other's database by itself --
// in real production this is the SAME Postgres table both languages read
// during a mixed rollout, so planting one plane's row into the other's
// database here is what actually reproduces that scenario.
type acceptedBatchRow struct {
	IngestionID, OrgID, IdempotencyKey, PayloadHash, SourceSystem, SourceInstance, EntityFamily, SchemaVersion string
	Producer, ProducerVersion                                                                                  *string
	WindowStartedAt, WindowEndedAt                                                                             *time.Time
	ItemsReceived                                                                                              int
	CreatedAt, UpdatedAt                                                                                       time.Time
}

func readAcceptedRow(t *testing.T, ctx context.Context, adminPool *pgxpool.Pool, idempotencyKey string) acceptedBatchRow {
	t.Helper()
	var row acceptedBatchRow
	err := adminPool.QueryRow(ctx, `
		SELECT ingestion_id::text, org_id, idempotency_key, payload_hash, source_system, source_instance,
		       entity_family, schema_version, producer, producer_version,
		       window_started_at, window_ended_at, items_received, created_at, updated_at
		FROM external_ingest_batches WHERE idempotency_key = $1
	`, idempotencyKey).Scan(
		&row.IngestionID, &row.OrgID, &row.IdempotencyKey, &row.PayloadHash, &row.SourceSystem, &row.SourceInstance,
		&row.EntityFamily, &row.SchemaVersion, &row.Producer, &row.ProducerVersion,
		&row.WindowStartedAt, &row.WindowEndedAt, &row.ItemsReceived, &row.CreatedAt, &row.UpdatedAt,
	)
	if err != nil {
		t.Fatalf("read accepted row %q: %v", idempotencyKey, err)
	}
	return row
}

// plantRow inserts row into adminPool's database exactly as a fresh accept
// would have (status='accepted', attempts=1, items_accepted=0,
// items_rejected=0, recompute_status='not_applicable') -- the SAME
// ingestion_id, org_id and payload_hash the donor plane wrote, so the
// receiving plane's own idempotency lookup finds a row whose stored hash
// came from the OTHER language's compute_payload_hash/computePayloadHash,
// not its own.
func plantRow(t *testing.T, ctx context.Context, adminPool *pgxpool.Pool, row acceptedBatchRow) {
	t.Helper()
	_, err := adminPool.Exec(ctx, `
		INSERT INTO external_ingest_batches (
			ingestion_id, org_id, idempotency_key, payload_hash, source_system, source_instance, entity_family,
			producer, producer_version, schema_version, window_started_at, window_ended_at, status, attempts,
			items_received, items_accepted, items_rejected, created_at, updated_at, recompute_status
		) VALUES ($1::uuid,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'accepted',1,$13,0,0,$14,$15,'not_applicable')
	`, row.IngestionID, row.OrgID, row.IdempotencyKey, row.PayloadHash, row.SourceSystem, row.SourceInstance,
		row.EntityFamily, row.Producer, row.ProducerVersion, row.SchemaVersion,
		row.WindowStartedAt, row.WindowEndedAt, row.ItemsReceived, row.CreatedAt, row.UpdatedAt)
	if err != nil {
		t.Fatalf("plant row %q: %v", row.IdempotencyKey, err)
	}
}

// crossPlaneIdempotencyKey renames body's idempotencyKey to avoid colliding
// with the same-plane accept this test already did under the original key.
func crossPlaneIdempotencyKey(body, original, renamed string) string {
	return strings.Replace(body, `"idempotencyKey":"`+original+`"`, `"idempotencyKey":"`+renamed+`"`, 1)
}

// TestExternalIngestPayloadHashVenueOracle is CHAOS-6350's proof: the REAL
// Python api and the REAL Go api, given the identical accept batch body,
// write the SAME external_ingest_batches.payload_hash on two copies of one
// Alembic-migrated Postgres -- and, because the two planes then agree on
// what "the same payload" means, posting the identical body a SECOND time
// to each plane's OWN server answers REPLAY (200), not CONFLICT (409),
// on both. Digest equality alone would not catch a bug where both planes
// happen to answer REPLAY for the wrong reason; asserting the actual
// status code this bug corrupts is the closer proof.
//
// The corpus targets the three divergences CHAOS-6350's design comment
// names, not just the ticket's headline camelCase-vs-snake_case one:
//   - an integer-valued payload field (Int must not coerce to Float)
//   - a NaN-valued payload field (model_dump(mode="json") -> null)
//   - a window with a non-UTC offset and a trailing-zero-but-nonzero
//     microsecond value (Pydantic's datetime JSON form, not UTC-forced
//     RFC3339Nano)
//   - entity_family absent (defaults to "legacy", popped), explicit
//     "legacy" (popped the same way), and "operational" (kept)
//   - producer/producerVersion present and absent
//   - a non-ASCII string in a payload value
func TestExternalIngestPayloadHashVenueOracle(t *testing.T) {
	ctx := context.Background()
	golden := venueoracle.OpenGolden(t, venueGolden("external-ingest-payload-hash", t.Name(), "f40780b89d4ab0e4e52e9b83aea83123428da02ad78349ba88e689d80a8220b5"))
	root := golden.PythonRoot(t, webhookintakeRepoRoot(t))

	seed := newPayloadHashVenueSeed()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Golden: golden,
		Root:   root,
		JWTKey: "venue-oracle-jwt-signing-key-32-bytes-min",
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seedPayloadHashVenue(t, ctx, admin, seed)
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

	sourceAdmin, err := pgxpool.New(ctx, venue.AdminURI(t, venue.SourceDB))
	if err != nil {
		t.Fatalf("open source admin pool: %v", err)
	}
	t.Cleanup(sourceAdmin.Close)
	goAdmin, err := pgxpool.New(ctx, venue.AdminURI(t, venue.GoDB))
	if err != nil {
		t.Fatalf("open go admin pool: %v", err)
	}
	t.Cleanup(goAdmin.Close)

	headersFor := func(token string) map[string]string {
		return map[string]string{"Content-Type": "application/json", "Authorization": "Bearer " + token}
	}

	cases := []struct {
		name, idempotencyKey, body, token string
	}{
		{"integer payload field", "venue-hash-int", `{"schemaVersion":"external-ingest.v1","idempotencyKey":"venue-hash-int",` +
			`"source":{"system":"github","instance":"acme/payload-hash-repo"},` +
			`"records":[{"kind":"repository.v1","externalId":"acme/payload-hash-repo",` +
			`"payload":{"externalId":"acme/payload-hash-repo","sourceSystem":"github","stars":42}}]}`, seed.token},
		{"nan payload field", "venue-hash-nan", `{"schemaVersion":"external-ingest.v1","idempotencyKey":"venue-hash-nan",` +
			`"source":{"system":"github","instance":"acme/payload-hash-repo"},` +
			`"records":[{"kind":"repository.v1","externalId":"acme/payload-hash-repo",` +
			`"payload":{"externalId":"acme/payload-hash-repo","sourceSystem":"github","score":NaN}}]}`, seed.token},
		{"non-utc offset window with trailing-zero microseconds", "venue-hash-window", `{"schemaVersion":"external-ingest.v1","idempotencyKey":"venue-hash-window",` +
			`"source":{"system":"github","instance":"acme/payload-hash-repo"},` +
			`"window":{"startedAt":"2026-09-01T00:00:00.123400+05:30","endedAt":"2026-09-08T00:00:00+05:30"},` +
			`"records":[{"kind":"repository.v1","externalId":"acme/payload-hash-repo",` +
			`"payload":{"externalId":"acme/payload-hash-repo","sourceSystem":"github"}}]}`, seed.token},
		{"entity family operational", "venue-hash-operational", `{"schemaVersion":"external-ingest.v1","idempotencyKey":"venue-hash-operational",` +
			`"source":{"system":"github","instance":"acme/payload-hash-repo-ops","entityFamily":"operational"},` +
			`"records":[{"kind":"operational_service.v1","externalId":"acme/payload-hash-repo-ops",` +
			`"payload":{"externalId":"acme/payload-hash-repo-ops","sourceSystem":"github","name":"svc"}}]}`, seed.operationalToken},
		{"entity family explicit legacy", "venue-hash-legacy", `{"schemaVersion":"external-ingest.v1","idempotencyKey":"venue-hash-legacy",` +
			`"source":{"system":"github","instance":"acme/payload-hash-repo","entityFamily":"legacy"},` +
			`"records":[{"kind":"repository.v1","externalId":"acme/payload-hash-repo",` +
			`"payload":{"externalId":"acme/payload-hash-repo","sourceSystem":"github"}}]}`, seed.token},
		{"producer and producerVersion present", "venue-hash-producer", `{"schemaVersion":"external-ingest.v1","idempotencyKey":"venue-hash-producer",` +
			`"source":{"system":"github","instance":"acme/payload-hash-repo","producer":"ci","producerVersion":"1.2.3"},` +
			`"records":[{"kind":"repository.v1","externalId":"acme/payload-hash-repo",` +
			`"payload":{"externalId":"acme/payload-hash-repo","sourceSystem":"github"}}]}`, seed.token},
		{"non-ascii payload value", "venue-hash-unicode", `{"schemaVersion":"external-ingest.v1","idempotencyKey":"venue-hash-unicode",` +
			`"source":{"system":"github","instance":"acme/payload-hash-repo"},` +
			`"records":[{"kind":"repository.v1","externalId":"acme/payload-hash-repo",` +
			`"payload":{"externalId":"acme/payload-hash-repo","sourceSystem":"github","description":"café über 😀"}}]}`, seed.token},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := venueoracle.Request{
				Name: tc.name, Method: "POST", Path: "/api/v1/external-ingest/batches",
				Headers: headersFor(tc.token), Body: venueoracle.B64(tc.body),
			}
			python := golden.Python(t, venue, []venueoracle.Request{request})[0]
			golden.Consumed(t, python)
			goResp := venueoracle.Do(t, goBase, request)
			if python.Status != goResp.Status {
				t.Fatalf("accept status: python=%d go=%d\npython body: %s\ngo body:     %s",
					python.Status, goResp.Status, python.Body, goResp.Body)
			}

			// The digest VALUE itself, read back from each plane's own
			// database, per case -- not just inferred from a same-plane
			// REPLAY (a status code that matches for the wrong reason
			// would not be caught by that alone) and not deferred to one
			// bulk diff at the end (which would not name the failing
			// case).
			pythonRow := frozenAcceptedRow(t, golden, "python row "+tc.idempotencyKey, func() acceptedBatchRow {
				return readAcceptedRow(t, ctx, sourceAdmin, tc.idempotencyKey)
			})
			goRow := readAcceptedRow(t, ctx, goAdmin, tc.idempotencyKey)
			if pythonRow.PayloadHash != goRow.PayloadHash {
				t.Fatalf("payload_hash digest differs: python=%s go=%s", pythonRow.PayloadHash, goRow.PayloadHash)
			}

			// Same-plane replay: the identical body again, against EACH
			// plane's own server -- both must answer REPLAY (200), not a
			// fresh 202 or a 409 CONFLICT.
			replayRequest := request
			replayRequest.Name = tc.name + " replay"
			pythonReplay := golden.Python(t, venue, []venueoracle.Request{replayRequest})[0]
			golden.Consumed(t, pythonReplay)
			goReplay := venueoracle.Do(t, goBase, request)
			if pythonReplay.Status != 200 {
				t.Errorf("python replay: want 200, got %d\nbody: %s", pythonReplay.Status, pythonReplay.Body)
			}
			if goReplay.Status != 200 {
				t.Errorf("go replay: want 200, got %d (CONFLICT/fresh-accept means the payload hash diverged)\nbody: %s", goReplay.Status, goReplay.Body)
			}

			// Cross-plane retry, the actual acceptance case: a batch
			// accepted on ONE plane, its row planted into the OTHER
			// plane's database (simulating the one shared production
			// Postgres table a mixed rollout actually has -- see
			// plantRow's doc comment), retried against that OTHER plane's
			// server. Both directions, under their own idempotency keys so
			// neither collides with the same-plane rows above.
			toGoKey := tc.idempotencyKey + "-cross-go"
			toGoBody := crossPlaneIdempotencyKey(tc.body, tc.idempotencyKey, toGoKey)
			toGoRequest := venueoracle.Request{Name: tc.name + " to go", Method: "POST", Path: "/api/v1/external-ingest/batches", Headers: headersFor(tc.token), Body: venueoracle.B64(toGoBody)}
			golden.Consumed(t, golden.Python(t, venue, []venueoracle.Request{toGoRequest})...)
			planted := frozenAcceptedRow(t, golden, "python row "+toGoKey, func() acceptedBatchRow {
				return readAcceptedRow(t, ctx, sourceAdmin, toGoKey)
			})
			// A just-accepted row: the Go plane treats an accepted batch idle for
			// EXTERNAL_INGEST_ACCEPTED_STALE_MINUTES as a retry, and a frozen row
			// is as old as its recording.
			planted.CreatedAt, planted.UpdatedAt = time.Now().UTC(), time.Now().UTC()
			plantRow(t, ctx, goAdmin, planted)
			if retry := venueoracle.Do(t, goBase, toGoRequest); retry.Status != 200 {
				t.Errorf("retry on go of a python-accepted batch: want 200 (REPLAY), got %d\nbody: %s", retry.Status, retry.Body)
			}

			toPythonKey := tc.idempotencyKey + "-cross-py"
			toPythonBody := crossPlaneIdempotencyKey(tc.body, tc.idempotencyKey, toPythonKey)
			toPythonRequest := venueoracle.Request{Name: tc.name + " to python", Method: "POST", Path: "/api/v1/external-ingest/batches", Headers: headersFor(tc.token), Body: venueoracle.B64(toPythonBody)}
			venueoracle.Do(t, goBase, toPythonRequest)
			if golden.Recording() {
				// The Python plane's answer depends on the row the Go plane wrote;
				// a frozen run holds that answer and has no Python database.
				plantRow(t, ctx, sourceAdmin, readAcceptedRow(t, ctx, goAdmin, toPythonKey))
			}
			retry := golden.Python(t, venue, []venueoracle.Request{toPythonRequest})[0]
			golden.Consumed(t, retry)
			if retry.Status != 200 {
				t.Errorf("retry on python of a go-accepted batch: want 200 (REPLAY), got %d\nbody: %s", retry.Status, retry.Body)
			}
		})
	}

	hashesQuery := `SELECT idempotency_key, payload_hash FROM external_ingest_batches ORDER BY idempotency_key`
	golden.CompareRows(t, "external_ingest_batches.payload_hash", func() string {
		return venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), hashesQuery)
	}, venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), hashesQuery))

	// This test's comparison shape (digest VALUES read back per case, a
	// same-plane replay check, and a cross-plane plant+retry both ways)
	// does not fit venueoracle.Diff: every Python answer is declared
	// inspected (Consumed) where it is checked, so the lifecycle skips Diff.
	golden.SkipDiff(t)
	golden.Finish(t)
	if !golden.Recording() {
		venueoracle.WriteGoOnlyProof(t, "Go payload hash, replay and cross-plane retry against the Python plane's answers frozen on build "+venuePythonBuild)
	}
}

// frozenAcceptedRow is the accepted row the Python plane wrote: read from its
// database while recording, from the golden afterwards.
func frozenAcceptedRow(t *testing.T, golden *venueoracle.Golden, name string, read func() acceptedBatchRow) acceptedBatchRow {
	t.Helper()
	text := golden.InspectRows(t, name, func() string {
		raw, err := json.Marshal(read())
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	})
	var row acceptedBatchRow
	if err := json.Unmarshal([]byte(text), &row); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return row
}
