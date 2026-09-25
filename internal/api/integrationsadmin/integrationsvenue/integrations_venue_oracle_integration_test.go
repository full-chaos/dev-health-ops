//go:build integration

package integrationsvenue

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestIntegrationsAdminVenueOracle sends the generic integration admin routes
// the same requests on the real Python api and the Go api, over two copies of
// one seeded database, and requires byte-identical answers, then requires the
// integrations, integration_sources and integration_datasets tables to end
// in the same state (raw column text, JSON columns as stored). The requests
// run in one order on each plane, so a write is visible to the reads after
// it.
func TestIntegrationsAdminVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-integrations-32b!"
	v := newIDs()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			seed(t, ctx, admin, v)
			return map[string]map[string]any{
				"adminA":     {"user_id": v.adminA.String(), "email": "admin-a@example.com", "org_id": v.orgA.String(), "role": "admin"},
				"memberA":    {"user_id": v.memberA.String(), "email": "member-a@example.com", "org_id": v.orgA.String(), "role": "member"},
				"adminB":     {"user_id": v.adminB.String(), "email": "admin-b@example.com", "org_id": v.orgB.String(), "role": "admin"},
				"adminC":     {"user_id": v.adminC.String(), "email": "admin-c@example.com", "org_id": v.orgC.String(), "role": "admin"},
				"adminD":     {"user_id": v.adminD.String(), "email": "admin-d@example.com", "org_id": v.orgD.String(), "role": "admin"},
				"adminNoOrg": {"user_id": v.adminNoOrg.String(), "email": "admin-noorg@example.com", "org_id": "", "role": "admin"},
				"superNoOrg": {"user_id": v.superNoOrg.String(), "email": "super-noorg@example.com", "org_id": "", "role": "member", "is_superuser": true},
			}
		},
	})
	base := startGoServer(t, ctx, venue, jwtKey)

	requests := integrationRequests(venue, v)
	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{Normalize: normalize})
	t.Logf("receipt (%d requests):\n%s", len(requests), receipt)

	// The rows both planes wrote, as raw column text. A row a request created
	// has a random id and creation time on each plane, so those columns are
	// compared only for the seeded rows.
	for _, table := range []struct{ name, query string }{
		{"integrations (seeded rows)", `SELECT id, org_id, provider, credential_id, name, config::text, is_active, schedule_cron, timezone, created_at,
updated_at >= '2026-06-01'::timestamptz AS touched FROM integrations WHERE name LIKE 'seed-%' ORDER BY name`},
		{"integrations (all rows)", `SELECT org_id, provider, credential_id, name, config::text, is_active, schedule_cron, timezone
FROM integrations ORDER BY org_id, name, provider, config::text`},
		{"integration_sources", `SELECT id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata::text, is_enabled,
discovered_at, last_seen_at, last_sync_at, last_sync_success, last_sync_error FROM integration_sources ORDER BY id`},
		{"integration_datasets", `SELECT org_id, integration_id, dataset_key, is_enabled, options::text, unavailable_reason, unavailable_since, unavailable_last_seen_at
FROM integration_datasets ORDER BY integration_id, dataset_key`},
	} {
		pythonRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), table.query)
		goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), table.query)
		if pythonRows != goRows {
			t.Errorf("%s differ\n python:\n%s\n go:\n%s", table.name, pythonRows, goRows)
		}
		if strings.TrimSpace(pythonRows) == "" {
			t.Errorf("%s: no rows compared", table.name)
		}
	}
}

var (
	anyUUID     = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	createdTime = regexp.MustCompile(`"(created_at|updated_at)":"[^"]*"`)
)

// normalize blanks what differs by construction in a "clock: " request: the
// random ids of the rows it created and the times each plane read from its
// own clock. Each distinct uuid becomes "<uuid#N>" by its first appearance,
// so the same row named twice in one body still reads as the same row, and
// two different rows never do.
func normalize(request venueoracle.Request, body string) string {
	if !strings.HasPrefix(request.Name, "clock: ") {
		return body
	}
	seen := map[string]int{}
	body = anyUUID.ReplaceAllStringFunc(body, func(id string) string {
		if _, ok := seen[id]; !ok {
			seen[id] = len(seen) + 1
		}
		return fmt.Sprintf("<uuid#%d>", seen[id])
	})
	return createdTime.ReplaceAllString(body, `"$1":"<time>"`)
}

func startGoServer(t *testing.T, ctx context.Context, venue *venueoracle.Venue, jwtKey string) string {
	t.Helper()
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatalf("go pool: %v", err)
	}
	t.Cleanup(pool.Close)
	verifier, err := edgetoken.New(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatalf("authenticator: %v", err)
	}
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	routes := apiservice.Routes(apiservice.Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, logger)}, logger)
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, routes, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}

// repoRoot walks up from this file to the directory holding src/dev_health_ops.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test's source file")
	}
	directory := filepath.Dir(file)
	for {
		if info, err := os.Stat(filepath.Join(directory, "src", "dev_health_ops")); err == nil && info.IsDir() {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatalf("no src/dev_health_ops above %s", file)
		}
		directory = parent
	}
}

var _ = fmt.Sprint
var _ = uuid.Nil
