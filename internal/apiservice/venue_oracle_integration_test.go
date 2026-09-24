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
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The venue oracle (internal/testsupport/venueoracle) for this service's
// routes: the REAL Python api and the REAL dho api (configure(), as the api
// role after the River migration) answer the same requests against two
// copies of one seeded Postgres database. Responses must match byte for
// byte, and the rows a write touches must match afterwards.

const venueKey = "venue-oracle-signing-key-0123456789abcdef"

// timestampFieldPattern matches created_at/updated_at/last_drift_sync_at
// JSON string values (team/identity admin responses, CHAOS-6310) so the
// venue oracle diff can blank them before comparing bodies.
var timestampFieldPattern = regexp.MustCompile(`"(created_at|updated_at|last_drift_sync_at)":"[^"]*"`)

// importedDiscoveredAtPattern matches discovered_at values written by the
// wall-clock (POST /teams/import) rather than by seedDriftReview, whose rows
// all carry a fixed 2026-09-01 date and stay compared exactly.
var importedDiscoveredAtPattern = regexp.MustCompile(`"discovered_at":"(?:2026-09-(?:0[2-9]|[1-3][0-9])|2026-1[0-2]|2027|202[89]|20[3-9][0-9])[^"]*"`)

// venueEncryptionKey is the one SETTINGS_ENCRYPTION_KEY both planes share:
// the Python plane encrypts the seeded credentials, the Go plane decrypts.
const venueEncryptionKey = "venue-protected-routes-settings-encryption-key"

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
	members := newMembersStub(t)
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(),
		PythonEnv: []string{"EXPECTED_WORKER_GROUPS=" + venueWorkerGroups, "TELEMETRY_ENDPOINT=" + endpoint.URL + "/py", "SETTINGS_ENCRYPTION_KEY=" + venueEncryptionKey},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			// Python's collect_usage_stats counts a Postgres repos table the
			// Alembic schema does not create; a stand-in lets its /report run
			// at all, so the rest of the report path can be compared.
			if _, err := admin.Exec(ctx, `CREATE TABLE public.repos (id uuid PRIMARY KEY)`); err != nil {
				t.Fatal(err)
			}
			seed = venueSeed(t, ctx, admin)
			seedMemberCredentials(t, ctx, admin, venue, seed.orgA, members.server.URL)
			return seed.tokenSpecs()
		},
	})

	cfg := config.Config{
		APIAddress: "127.0.0.1:0", RiverDatabaseSchema: "river", APIDatabaseRole: venue.Roles["api"],
		APIDatabaseURI:   secrets.NewValue(venue.GoAPIDatabaseURI(t)),
		APIClickHouseURI: secrets.NewValue(venue.GoAPIClickHouseURI(t)),
		APIJWTSecret:     secrets.NewValue(venueKey), APIJWTIssuer: "dev-health-ops", APIJWTAudience: "dev-health-api",
		CORSAllowedOrigins:    []string{"http://localhost:3000"},
		ValkeyURI:             secrets.NewValue(venue.ValkeyURI),
		TelemetryEndpoint:     endpoint.URL + "/go",
		SettingsEncryptionKey: secrets.NewValue(venueEncryptionKey),
	}
	groups := venueGroupList()
	cfg.APIExpectedWorkerGroups = &groups
	base := startVenueAPI(t, ctx, cfg, venue)

	// team_sync_policies has no Python or Go write route in this venue's
	// sequence (policies are set through the drift-review routes, out of
	// scope here), so the FLAG_FOR_REVIEW import case seeds the same policy
	// row into BOTH ClickHouse databases before any request runs: team
	// "qa4" (created below by the CRUD sequence) is under FLAG_FOR_REVIEW,
	// so importing it must write drift changes and leave the catalog row.
	for _, database := range []string{venue.PythonClickHouseDB, venue.GoClickHouseDB} {
		seedConn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.AdminClickHouseURI(t, database)))
		if err != nil {
			t.Fatal(err)
		}
		if err := seedConn.Exec(ctx, `INSERT INTO team_sync_policies (org_id, team_id, sync_policy, managed_fields, updated_by, updated_at)
			VALUES ($1, 'qa4', 1, ['name', 'description', 'members', 'project_keys', 'repo_patterns'], 'venue-seed', now64(6))`, seed.orgA.String()); err != nil {
			t.Fatal(err)
		}
		_ = seedConn.Close()
	}

	seedDriftReview(t, ctx, venue, seed.orgA.String(), seed.orgB.String())

	requests := venueRequests(seed, venue.Tokens)
	var receipt strings.Builder
	pythonResponses := venue.ServePython(t, requests)
	pythonProviderRequests := members.take()
	receipt.WriteString(venueoracle.Diff(t, base, requests, pythonResponses, venueoracle.DiffOptions{
		// Composes TWO independent normalizations, each scoped to its own
		// known cause: normalizeRuled's own rules, and the team/identity
		// admin timestamp blanking (CHAOS-6310 -- each plane mints its own
		// ReplacingMergeTree row's now(), a construction-time value never
		// meant to agree, exactly what Normalize's own doc comment
		// describes). r1 finding #6 (the apostrophe-quoting gap) is fixed
		// as of r2 -- unknownTeamIDsDetail now uses the shared
		// pythonparity.StrRepr encoder (CHAOS-6322, #2850) -- so the
		// byte-for-byte body comparison no longer needs a per-request
		// exception for it.
		Normalize: func(request venueoracle.Request, body string) string {
			body = normalizeRuled(body)
			body = timestampFieldPattern.ReplaceAllString(body, `"$1":"<time>"`)
			body = importedDiscoveredAtPattern.ReplaceAllString(body, `"discovered_at":"<time>"`)
			return body
		},
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
	// The provider traffic the member routes caused: the same requests
	// (method, path, sorted query) reached the stub from both planes.
	goProviderRequests := members.take()
	providerSame := strings.Join(pythonProviderRequests, "\n") == strings.Join(goProviderRequests, "\n") && len(goProviderRequests) > 0
	if !providerSame {
		t.Errorf("member-route provider requests differ:\n python:\n%s\n go:\n%s",
			strings.Join(pythonProviderRequests, "\n"), strings.Join(goProviderRequests, "\n"))
	}
	fmt.Fprintf(&receipt, "member-route provider requests (%d): %s\n", len(goProviderRequests), venueoracle.Mark(providerSame))
	// The rows the writes touched are identical on both copies.
	compareRows(t, ctx, venue, &receipt, "organizations", `SELECT id::text, slug, name, coalesce(description, '<null>'), tier, is_active,
		updated_at > created_at, settings::text FROM organizations ORDER BY slug`)
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
	// CHAOS-6310: the team + identity admin CRUD routes write ClickHouse,
	// not Postgres -- same shape, a ClickHouse reader instead of a
	// Postgres one. FINAL resolves each plane's own ReplacingMergeTree
	// merge state, the same discipline the Python readers use.
	compareCHRows(t, ctx, venue, &receipt, "teams",
		`SELECT id, name, coalesce(description, '<null>'), members, manual_members, project_keys,
			repo_patterns, is_active, provider, native_team_key FROM teams FINAL
		WHERE org_id != '' ORDER BY id`)
	// CHAOS-6311: POST /teams/import's drift-projector writes, compared as
	// raw text. Timestamps are excluded (each plane mints its own now()).
	compareCHRows(t, ctx, venue, &receipt, "team_provider_observations",
		`SELECT provider, native_team_key, team_id, coalesce(name, '<null>'), coalesce(description, '<null>'),
			members_json, project_keys_json, repo_patterns_json, is_active, coalesce(parent_team_id, '<null>')
		FROM team_provider_observations FINAL WHERE org_id != '' ORDER BY provider, native_team_key`)
	compareCHRows(t, ctx, venue, &receipt, "team_drift_changes",
		`SELECT change_id, entity_type, entity_id, provider, coalesce(native_team_key, '<null>'), change_type,
			coalesce(field, '<null>'), old_value_json, new_value_json, status, coalesce(decided_by, '<null>')
		FROM team_drift_changes FINAL WHERE org_id != '' ORDER BY change_id`)
	compareCHRows(t, ctx, venue, &receipt, "team_memberships",
		`SELECT provider, team_id, member_id, coalesce(raw_provider_user_id, '<null>'), coalesce(raw_email, '<null>'), identity_facets,
			source, is_primary, specificity, priority, valid_from, valid_to IS NULL, toUInt8(ifNull(valid_to > valid_from, 0))
		FROM team_memberships FINAL WHERE org_id != '' ORDER BY provider, team_id, member_id, source, valid_from`)
	compareCHRows(t, ctx, venue, &receipt, "manual_attribution_fallbacks",
		`SELECT provider, scope_type, scope_id, team_id, team_name, reason, priority, valid_from, valid_to IS NULL,
			coalesce(created_by, '<null>'), created_at FROM manual_attribution_fallbacks FINAL WHERE org_id != '' ORDER BY provider, scope_type, scope_id`)
	compareCHRows(t, ctx, venue, &receipt, "team_drift_changes (seeded review rows, decided fields)",
		`SELECT change_id, entity_type, entity_id, status, coalesce(decided_by, '<null>'), decided_at IS NOT NULL, first_seen_at, last_seen_at > toDateTime64('2026-09-10', 6)
		FROM team_drift_changes FINAL WHERE org_id != '' AND change_id LIKE 'c-%' ORDER BY org_id, change_id`)
	compareCHRows(t, ctx, venue, &receipt, "team_sync_policies",
		`SELECT team_id, sync_policy, managed_fields, coalesce(updated_by, '<null>')
		FROM team_sync_policies FINAL WHERE org_id != '' ORDER BY team_id`)
	compareCHRows(t, ctx, venue, &receipt, "identities",
		`SELECT canonical_id, coalesce(display_name, '<null>'), coalesce(email, '<null>'),
			provider_identities, team_ids, is_active FROM identities FINAL
		WHERE org_id != '' ORDER BY canonical_id`)
	// Last, since they break both planes' database: the acr store made
	// unusable.
	receipt.WriteString(venueACRStoreDown(t, ctx, venue, base, seed.orgA.String()))
	if path := os.Getenv("DEV_HEALTH_VENUE_RECEIPT"); path != "" {
		_ = os.WriteFile(path, []byte(receipt.String()), 0o600)
	}
	t.Log("\n" + receipt.String())
}

func venueACRStoreDown(t *testing.T, ctx context.Context, venue *venueoracle.Venue, base, orgID string) string {
	t.Helper()
	admin, err := pgxpool.New(ctx, venue.AdminURI(t, "postgres"))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	auth := map[string]string{"Authorization": "Bearer " + venueACRToken}
	health := venueoracle.Request{Name: "acr store unreadable: health", Method: http.MethodGet, Path: "/api/v1/internal/acr/health", Headers: auth}

	// 1. The table each plane's health reads is gone: Python reads acr's
	// credential row, Go the organizations table the entitlement route
	// reads first. Both answer 503.
	for database, table := range map[string]string{venue.SourceDB: "internal_service_credentials", venue.GoDB: "organizations"} {
		conn, err := pgxpool.New(ctx, venue.AdminURI(t, database))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %q RENAME TO %q`, table, table+"_gone")); err != nil {
			t.Fatal(err)
		}
		conn.Close()
	}
	requests := []venueoracle.Request{health}
	out := venueoracle.Diff(t, base, requests, venue.ServePython(t, requests), venueoracle.DiffOptions{
		Inspect: func(request venueoracle.Request, goResponse venueoracle.Response) {
			// SAME alone would also hold if both planes answered 200.
			if goResponse.Status != http.StatusServiceUnavailable {
				t.Errorf("%s: go answered %d, want 503", request.Name, goResponse.Status)
			}
		},
	})

	// 2. Postgres unreachable: the Go database refuses connections and its
	// open ones are ended. Python answers this with an unhandled 500 (a
	// crash in its credential lookup, not a contract), so only the Go
	// answer is checked: 503 with Python's handled body on both routes.
	if _, err := admin.Exec(ctx, fmt.Sprintf(`ALTER DATABASE %q ALLOW_CONNECTIONS false`, venue.GoDB)); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, venue.GoDB); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/v1/internal/acr/health", "/api/v1/internal/acr/entitlements/" + orgID} {
		response, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		same := response.StatusCode == http.StatusServiceUnavailable && string(body) == `{"detail":"Service unavailable"}`
		if !same {
			t.Errorf("acr store unreachable: GET %s: go answered %d %q, want 503 {\"detail\":\"Service unavailable\"}", path, response.StatusCode, body)
		}
		out += fmt.Sprintf("acr store unreachable: GET %s go=%d %s\n", path, response.StatusCode, venueoracle.Mark(same))
	}
	return out
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

// compareCHRows is compareRows' ClickHouse sibling (CHAOS-6310): each
// plane has its own ClickHouse database (venue.PythonClickHouseDB /
// venue.GoClickHouseDB), never a shared one, for the same reason the
// Postgres pair is two databases -- comparing state one plane's writes
// could otherwise corrupt for the other.
func compareCHRows(t *testing.T, ctx context.Context, venue *venueoracle.Venue, receipt *strings.Builder, name, query string) {
	t.Helper()
	pyRows := venueoracle.CHRows(t, ctx, venue.AdminClickHouseURI(t, venue.PythonClickHouseDB), query)
	goRows := venueoracle.CHRows(t, ctx, venue.AdminClickHouseURI(t, venue.GoClickHouseDB), query)
	if pyRows != goRows || pyRows == "" {
		t.Errorf("%s after the writes differ (or are empty):\n python %s\n go     %s", name, pyRows, goRows)
	}
	fmt.Fprintf(receipt, "%s rows after writes: %s\n", name, venueoracle.Mark(pyRows == goRows && pyRows != ""))
}
