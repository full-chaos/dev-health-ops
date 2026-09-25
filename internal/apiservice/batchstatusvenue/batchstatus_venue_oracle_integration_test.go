//go:build integration

package batchstatusvenue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// shape is one stored value of one of the three JSON columns. text "" with
// sqlNull set is SQL NULL; anything else is the JSON text stored as-is.
type shape struct {
	name    string
	text    string
	sqlNull bool
}

// column is which of the batch row's JSON columns a scenario fills; the other
// two are left at a valid object so a raise is attributable to this one.
type column string

const (
	recordCounts   column = "record_counts"
	errorSummary   column = "error_summary"
	recomputeScope column = "recompute_scope"
)

type scenario struct {
	col   column
	shape shape
	id    string
}

// scopeKeys are the keys status.py's _recompute_scope_response reads.
var scopeKeys = []string{"repoIds", "teamIds", "windowStartedAt", "windowEndedAt", "cappedDays", "cappedRepos"}

// values are the JSON values drawn for a field: every JSON type, values Go's
// typed decode cannot hold (1e400, 1e-400, -0), timestamps in every form
// datetime.fromisoformat takes or refuses, lists of strings and of not-strings.
var values = []string{
	`null`, `true`, `false`, `0`, `1`, `-0`, `5`, `2.5`, `1e400`, `1e-400`, `123456789012345678901234567890`,
	`""`, `"abc"`, `"x"`, ` "  "`, `"repo-1"`, `[]`, `["a","b"]`, `["a",1]`, `[1,2]`, `[null]`, `[["a"]]`, `{}`, `{"a":1}`, `[[1,2]]`,
	`"2026-09-01T00:00:00Z"`, `"2026-09-01T00:00:00+05:30"`, `"2026-09-01T00:00:00.123400+05:30"`, `"2026-09-01T00:00:00"`,
	`"2026-09-01"`, `"2026-09-01 12:00:00"`, `"20260901T000000"`, `"garbage"`, `"0001-01-01T00:00:00"`, `"9999-12-31T23:59:59Z"`,
	`"2026-13-01"`, `"2026-09-01T25:00:00"`,
}

// fixed are the shapes one column's stored value takes at top level.
var fixed = []shape{
	{name: "sql null", sqlNull: true},
	{name: "json null", text: `null`},
	{name: "empty object", text: `{}`},
	{name: "empty array", text: `[]`},
	{name: "array of pairs", text: `[["repoIds",["a"]]]`},
	{name: "array of one non-pair", text: `[1]`},
	{name: "array of a string", text: `["ab"]`},
	{name: "string", text: `"abc"`},
	{name: "string holding an object", text: `"{\"repoIds\":[\"a\"]}"`},
	{name: "string holding an array", text: `"[]"`},
	{name: "empty string", text: `""`},
	{name: "number", text: `5`},
	{name: "zero", text: `0`},
	{name: "true", text: `true`},
	{name: "false", text: `false`},
	{name: "huge number", text: `1e400`},
	{name: "object", text: `{"repoIds":["r1"],"teamIds":["t1"],"windowStartedAt":"2026-09-01T00:00:00Z","windowEndedAt":"2026-09-08T00:00:00Z","cappedDays":true,"cappedRepos":false}`},
	{name: "object with unrelated keys", text: `{"x":1,"y":[1,2,{"z":null}]}`},
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve package path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func tokenHash(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

// draw is one seeded random object for a column: some of the scope keys with
// values from the pool, extra keys, and once in eight a top-level non-object.
func draw(r *rand.Rand, col column) shape {
	if r.Intn(8) == 0 {
		return shape{name: "random top level", text: values[r.Intn(len(values))]}
	}
	keys := scopeKeys
	if col != recomputeScope {
		keys = []string{"total", "byKind", "system_failure", "reason", "rejected"}
	}
	var members []string
	for _, key := range keys {
		if r.Intn(3) == 0 {
			continue
		}
		members = append(members, strconv.Quote(key)+":"+values[r.Intn(len(values))])
	}
	if r.Intn(3) == 0 {
		members = append(members, `"extra":`+values[r.Intn(len(values))])
	}
	return shape{name: "random object", text: "{" + strings.Join(members, ",") + "}"}
}

func seededInt(name string, fallback int64) int64 {
	if raw := os.Getenv(name); raw != "" {
		if value, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return value
		}
	}
	return fallback
}

// TestBatchStatusShapeVenueOracle answers GET batch status for one row per
// (column, shape): the fixed shapes in each of the three columns, and drawn
// objects (seeded: BATCHSTATUS_ORACLE_SEED, default 6766). Both
// planes read the identical rows; every DIFF fails the test.
func TestBatchStatusShapeVenueOracle(t *testing.T) {
	// The peer is the loopback address (Go plane) or the FastAPI TestClient host
	// (Python plane): trust it as a proxy so each request's X-Forwarded-For is
	// its own rate-limit bucket.
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1,testclient")
	ctx := context.Background()
	orgID, sourceID := uuid.New().String(), uuid.New().String()
	token := "fcpush_venue-oracle-" + uuid.New().String()

	var scenarios []scenario
	add := func(col column, s shape) {
		scenarios = append(scenarios, scenario{col: col, shape: s, id: uuid.New().String()})
	}
	for _, col := range []column{recordCounts, errorSummary, recomputeScope} {
		for _, s := range fixed {
			add(col, s)
		}
	}
	r := rand.New(rand.NewSource(seededInt("BATCHSTATUS_ORACLE_SEED", 6766)))
	for i := 0; i < 60; i++ {
		col := []column{recordCounts, errorSummary, recomputeScope, recomputeScope}[r.Intn(4)]
		add(col, draw(r, col))
	}
	// One scope field at a time, with every value: the reads are lazy per field.
	for _, key := range scopeKeys {
		for _, value := range values {
			add(recomputeScope, shape{name: "scope " + key + "=" + value, text: `{"` + key + `":` + value + `}`})
		}
	}

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   repoRoot(t),
		JWTKey: "venue-oracle-jwt-signing-key-32-bytes-min",
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			exec(`INSERT INTO organizations (id, slug, name, tier) VALUES ($1::uuid, 'venue-batch-status', 'Venue Batch Status', 'team')`, orgID)
			exec(`INSERT INTO external_ingest_sources (id, org_id, system, instance, entity_family, mode, enabled, created_at, updated_at)
VALUES ($1::uuid, $2, 'github', 'acme/venue-repo', 'legacy', 'customer_push', true, now(), now())`, sourceID, orgID)
			exec(`INSERT INTO external_ingest_tokens (id, org_id, source_id, name, token_hash, token_prefix, scopes, created_at)
VALUES ($1::uuid, $2, $3::uuid, 'venue oracle token', $4, 'fcpush_venue', $5::jsonb, now())`,
				uuid.New().String(), orgID, sourceID, tokenHash(token), `["schema:read","ingest:write","ingest:status"]`)
			for i, sc := range scenarios {
				cols := map[column]any{recordCounts: `{}`, errorSummary: `{}`, recomputeScope: `{}`}
				if sc.shape.sqlNull {
					cols[sc.col] = nil
				} else {
					cols[sc.col] = sc.shape.text
				}
				exec(`INSERT INTO external_ingest_batches (ingestion_id, org_id, idempotency_key, payload_hash, source_system, source_instance, schema_version,
	items_received, status, record_counts, error_summary, recompute_status, recompute_scope, producer, created_at, updated_at)
VALUES ($1::uuid, $2, $3, $4, 'github', 'acme/venue-repo', 'external-ingest.v1', 1, 'accepted', $5::jsonb, $6::jsonb, 'pending', $7::jsonb, $8,
	'2026-09-15T08:30:22.254860+00:00'::timestamptz, '2026-09-15T08:30:22.254860+00:00'::timestamptz)`,
					sc.id, orgID, fmt.Sprintf("shape-%03d", i), fmt.Sprintf("hash-%03d", i),
					cols[recordCounts], cols[errorSummary], cols[recomputeScope], fmt.Sprintf("p-%03d", i))
			}
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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := apiservice.NewServer(config.Config{APIAddress: "127.0.0.1:0"}, logger, apiservice.Routes(apiservice.Deps{Pool: pool, Valkey: goValkey}, logger))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatalf("start server: %v (api role gaps: %s)", err, venue.DiagnoseAPIRole(t, ctx))
	}
	t.Cleanup(func() { _ = server.Shutdown(ctx) })
	goBase := "http://" + server.Address()

	auth := map[string]string{"Authorization": "Bearer " + token}
	requests := make([]venueoracle.Request, 0, len(scenarios))
	for i, sc := range scenarios {
		// Each scenario is its own client address: the ingest-auth ceiling is 100
		// attempts a minute per address and this suite sends more than that.
		headers := map[string]string{"Authorization": auth["Authorization"], "X-Forwarded-For": fmt.Sprintf("10.%d.%d.%d", i/65025, (i/255)%255, i%255+1)}
		requests = append(requests, venueoracle.Request{
			Name:   fmt.Sprintf("%s %s", sc.col, sc.shape.name),
			Method: "GET", Path: "/api/v1/external-ingest/batches/" + sc.id, Headers: headers,
		})
	}
	// The other two routes that read the same row through _row_to_batch: the list
	// (filtered to one row by its producer, so one bad row cannot fail the others)
	// and a POST replay of the row's idempotency key (an existing batch is loaded
	// before anything is compared). Every fixed shape of the three columns.
	posts := 0
	for i, sc := range scenarios {
		if i >= len(fixed)*3 {
			break
		}
		address := fmt.Sprintf("10.%d.%d.%d", 200+i/65025, (i/255)%255, i%255+1)
		requests = append(requests, venueoracle.Request{
			Name: fmt.Sprintf("list %s %s", sc.col, sc.shape.name), Method: "GET",
			Path:    "/api/v1/external-ingest/batches?producer=" + fmt.Sprintf("p-%03d", i),
			Headers: map[string]string{"Authorization": auth["Authorization"], "X-Forwarded-For": address},
		})
		if i%3 != 0 || posts >= 50 {
			continue
		}
		posts++
		body := fmt.Sprintf(`{"schemaVersion":"external-ingest.v1","idempotencyKey":"shape-%03d","source":{"system":"github","instance":"acme/venue-repo"},`+
			`"records":[{"kind":"repository.v1","externalId":"acme/venue-repo","payload":{"externalId":"acme/venue-repo","sourceSystem":"github"}}]}`, i)
		requests = append(requests, venueoracle.Request{
			Name: fmt.Sprintf("accept existing %s %s", sc.col, sc.shape.name), Method: "POST", Path: "/api/v1/external-ingest/batches",
			Headers: map[string]string{"Authorization": auth["Authorization"], "Content-Type": "application/json", "X-Forwarded-For": address},
			Body:    venueoracle.B64(body),
		})
	}
	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{})
	t.Log(receipt)
}
