//go:build integration

package apiservice

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The venue oracle (internal/testsupport/venueoracle) for this service's
// routes: the REAL Python api and the REAL dho api (configure(), as the api
// role after the River migration) answer the same requests against two
// copies of one seeded Postgres database. Responses must match byte for
// byte, and the rows a write touches must match afterwards.

const venueKey = "venue-oracle-signing-key-0123456789abcdef"

func venueRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

// TestVenueOracleProtectedRoutes is the write-and-read differential.
func TestVenueOracleProtectedRoutes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	sent := &sentReports{}
	endpoint := httptest.NewServer(sent)
	t.Cleanup(endpoint.Close)
	var seed venueFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(),
		PythonEnv: []string{"EXPECTED_WORKER_GROUPS=" + venueWorkerGroups, "TELEMETRY_ENDPOINT=" + endpoint.URL + "/py"},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			// Python's collect_usage_stats counts a Postgres repos table the
			// Alembic schema does not create; a stand-in lets its /report run
			// at all, so the rest of the report path can be compared.
			if _, err := admin.Exec(ctx, `CREATE TABLE public.repos (id uuid PRIMARY KEY)`); err != nil {
				t.Fatal(err)
			}
			seed = venueSeed(t, ctx, admin)
			return seed.tokenSpecs()
		},
	})

	cfg := config.Config{
		APIAddress: "127.0.0.1:0", RiverDatabaseSchema: "river", APIDatabaseRole: venue.Roles["api"],
		APIDatabaseURI: secrets.NewValue(venue.GoAPIDatabaseURI(t)),
		APIJWTSecret:   secrets.NewValue(venueKey), APIJWTIssuer: "dev-health-ops", APIJWTAudience: "dev-health-api",
		CORSAllowedOrigins: []string{"http://localhost:3000"},
		ValkeyURI:          secrets.NewValue(venue.ValkeyURI),
		TelemetryEndpoint:  endpoint.URL + "/go",
	}
	groups := venueGroupList()
	cfg.APIExpectedWorkerGroups = &groups
	base := startVenueAPI(t, ctx, cfg, venue)

	requests := venueRequests(seed, venue.Tokens)
	var receipt strings.Builder
	receipt.WriteString(venueoracle.Diff(t, base, requests, venue.ServePython(t, requests), venueoracle.DiffOptions{
		Normalize: func(_ venueoracle.Request, body string) string { return normalizeRuled(body) },
		Inspect: func(request venueoracle.Request, goResponse venueoracle.Response) {
			if strings.HasPrefix(request.Name, "report: ") && goResponse.Status == http.StatusOK {
				assertPerTableCounts(t, ctx, venue.AdminURI(t, venue.GoDB), goResponse.Body)
			}
		},
		// A HEAD probe has no body to normalize, but its content-length is the
		// length of the ruled GET body.
		SkipContentLength: func(request venueoracle.Request) bool {
			return request.Method == http.MethodHead && (request.Path == "/health" || request.Path == "/health/workers")
		},
	}))
	// The rows the writes touched are identical on both copies.
	compareRows(t, ctx, venue, &receipt, "organizations", `SELECT id::text, slug, name, coalesce(description, '<null>'), tier, is_active,
		updated_at > created_at FROM organizations ORDER BY slug`)
	compareRows(t, ctx, venue, &receipt, "settings", `SELECT org_id, category, key, CASE WHEN key = 'telemetry_last_report_at' AND value ~ '^2026-09-01' THEN value
		WHEN key = 'telemetry_last_report_at' AND value <> 'garbage' THEN 'reported' ELSE coalesce(value, '<null>') END, is_encrypted,
		coalesce(description, '<null>') FROM settings ORDER BY org_id, category, key`)
	compareRows(t, ctx, venue, &receipt, "audit_logs", `SELECT org_id::text, coalesce(user_id::text, '<null>'), action, resource_type, resource_id, description,
		regexp_replace(changes::text, '"collected_at": "[^"]*"', '"collected_at": "<t>"'), request_metadata::text, status
		FROM audit_logs ORDER BY org_id`)
	pyStream := venueoracle.StreamEntries(t, ctx, venue.PythonValkeyURI, "product-telemetry:*", "ingestion_id")
	goStream := venueoracle.StreamEntries(t, ctx, venue.ValkeyURI, "product-telemetry:*", "ingestion_id")
	if pyStream != goStream || pyStream == "" {
		t.Errorf("product-telemetry stream entries differ:\n python %s\n go     %s", pyStream, goStream)
	}
	fmt.Fprintf(&receipt, "product-telemetry stream entries: %s\n", venueoracle.Mark(pyStream == goStream && pyStream != ""))
	pySent, goSent := sent.bodies("/py"), sent.bodies("/go")
	if pySent != goSent || pySent == "" {
		t.Errorf("sent telemetry reports differ:\n python %s\n go     %s", pySent, goSent)
	}
	fmt.Fprintf(&receipt, "telemetry reports sent: %s\n", venueoracle.Mark(pySent == goSent && pySent != ""))
	if path := os.Getenv("DEV_HEALTH_VENUE_RECEIPT"); path != "" {
		_ = os.WriteFile(path, []byte(receipt.String()), 0o600)
	}
	t.Log("\n" + receipt.String())
}

// startVenueAPI runs configure() as a deploy does and returns the api's
// base URL once readiness is true as the api role.
func startVenueAPI(t *testing.T, ctx context.Context, cfg config.Config, venue *venueoracle.Venue) string {
	t.Helper()
	registry := health.NewRegistry(5 * time.Second)
	components, err := configure(ctx, cfg, registry, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	for _, component := range components {
		if err := component.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for index := len(components) - 1; index >= 0; index-- {
			_ = components[index].Shutdown(context.Background())
		}
	})
	if ready := registry.CheckRequired(ctx); !ready.Ready {
		t.Fatalf("dho api not ready as the api role: %+v %s", ready, venue.DiagnoseAPIRole(t, ctx))
	}
	for _, component := range components {
		if server, ok := component.(interface{ Address() string }); ok {
			return "http://" + server.Address()
		}
	}
	t.Fatal("configure started no HTTP server")
	return ""
}

func compareRows(t *testing.T, ctx context.Context, venue *venueoracle.Venue, receipt *strings.Builder, name, query string) {
	t.Helper()
	pyRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), query)
	goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), query)
	if pyRows != goRows || pyRows == "" {
		t.Errorf("%s after the writes differ (or are empty):\n python %s\n go     %s", name, pyRows, goRows)
	}
	fmt.Fprintf(receipt, "%s rows after writes: %s\n", name, venueoracle.Mark(pyRows == goRows && pyRows != ""))
}

// venueWorkerGroups is EXPECTED_WORKER_GROUPS on both planes: a blank entry
// and a duplicate included.
const venueWorkerGroups = "ops, sync,, heavy ,ops"

func venueGroupList() []string {
	var out []string
	for _, group := range strings.Split(venueWorkerGroups, ",") {
		if group = strings.TrimSpace(group); group != "" {
			out = append(out, group)
		}
	}
	return out
}

// normalizeRuled blanks values that differ by decision or by construction,
// not by defect: the rate limiter backend (the Go api has none, "noop";
// Python reports its own), the Celery leg of /health/workers (the Go api
// reports "retired"; Python inspects its broker), a random ingestion id,
// the build version and the report time.
func normalizeRuled(body string) string {
	for _, field := range []string{`"rate_limiter":"`, `"celery":"`, `"ingestion_id":"`, `"version":"`, `"collected_at":"`} {
		for start := 0; ; {
			index := strings.Index(body[start:], field)
			if index < 0 {
				break
			}
			valueStart := start + index + len(field)
			end := strings.Index(body[valueStart:], `"`)
			if end < 0 {
				break
			}
			body = body[:valueStart] + "<ruled>" + body[valueStart+end:]
			start = valueStart + len("<ruled>")
		}
	}
	return normalizeCounts(body)
}

// normalizeCounts blanks /telemetry/report's seven totals: Go counts per
// table where Python's query multiplies the tables (ruled); the Go values
// are checked against direct counts instead.
func normalizeCounts(body string) string {
	for _, field := range []string{"total_organizations", "active_organizations", "total_users", "active_users",
		"total_repos", "total_sync_configs", "active_syncs_24h"} {
		key := `"` + field + `":`
		if index := strings.Index(body, key); index >= 0 {
			start := index + len(key)
			for start < len(body) && body[start] == ' ' {
				start++
			}
			end := start
			for end < len(body) && body[end] >= '0' && body[end] <= '9' {
				end++
			}
			body = body[:start] + `"<count>"` + body[end:]
		}
	}
	return body
}

// sentReports records the bodies POSTed to the TELEMETRY_ENDPOINT stand-in,
// by path (/py or /go).
type sentReports struct {
	mu     sync.Mutex
	byPath map[string][]string
}

func (s *sentReports) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	if s.byPath == nil {
		s.byPath = map[string][]string{}
	}
	s.byPath[r.URL.Path] = append(s.byPath[r.URL.Path], normalizeRuled(string(raw)))
	s.mu.Unlock()
	w.WriteHeader(http.StatusAccepted)
}

func (s *sentReports) bodies(path string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.byPath[path], " | ")
}

// assertPerTableCounts checks /telemetry/report's totals against per-table
// counts read directly (the ruled non-parity with Python's cross product).
func assertPerTableCounts(t *testing.T, ctx context.Context, uri, body string) {
	t.Helper()
	var report map[string]any
	if err := json.Unmarshal([]byte(body), &report); err != nil {
		t.Fatalf("report body: %v", err)
	}
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for field, query := range map[string]string{
		"total_organizations":  `SELECT count(*) FROM organizations`,
		"total_users":          `SELECT count(*) FROM users`,
		"active_users":         `SELECT count(*) FROM users WHERE is_active`,
		"total_sync_configs":   `SELECT count(*) FROM sync_configurations`,
		"active_organizations": `SELECT count(*) FROM organizations WHERE is_active`,
		"total_repos":          `SELECT 0::bigint`, // ruled: repositories live in ClickHouse
		"active_syncs_24h":     `SELECT count(*) FROM sync_configurations WHERE is_active AND last_sync_at >= now() - interval '24 hours'`,
	} {
		var want int64
		if err := pool.QueryRow(ctx, query).Scan(&want); err != nil {
			t.Fatal(err)
		}
		got, ok := report[field].(float64)
		if !ok {
			t.Errorf("report %s = %v, want a number", field, report[field])
			continue
		}
		if int64(got) != want {
			t.Errorf("report %s = %v, want the per-table count %d", field, report[field], want)
		}
	}
}
