//go:build integration

package discoveryvenue

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	venueKey       = "venue-oracle-integration-discover-key-32-bytes!"
	venuePinnedNow = "2026-09-24T12:34:56.123456+00:00"
	discoverPath   = "/api/v1/admin/integrations/"
)

type ids struct {
	orgA, orgB                                             uuid.UUID
	adminA, memberA, adminB, adminNoOrg                    uuid.UUID
	credGood, credBad, credGoodB                           uuid.UUID
	intJira, intScoped, intConfigScoped, intBad, intNoCred uuid.UUID
	intLinear, intB, intEmpty, intRename                   uuid.UUID
	cfgJira, cfgScoped, cfgB                               uuid.UUID
	srcAcm, srcOld, srcDupLower, srcDupUpper, srcRename    uuid.UUID
}

func newIDs() ids {
	var v ids
	for _, target := range []*uuid.UUID{&v.orgA, &v.orgB, &v.adminA, &v.memberA, &v.adminB, &v.adminNoOrg, &v.credGood, &v.credBad,
		&v.credGoodB, &v.intJira, &v.intScoped, &v.intConfigScoped, &v.intBad, &v.intNoCred, &v.intLinear, &v.intB, &v.intEmpty,
		&v.cfgJira, &v.cfgScoped, &v.cfgB, &v.intRename, &v.srcAcm, &v.srcOld, &v.srcDupLower, &v.srcDupUpper, &v.srcRename} {
		*target = uuid.New()
	}
	return v
}

// fakeJira serves the recorded project list (internal/api/syncadmin/testdata/
// jira_recorded) as /rest/api/3/project/search windows over TLS, the only
// scheme Python's Jira client uses. The bad credential's token gets Jira's 401.
func fakeJira(t *testing.T, root string) (*httptest.Server, string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "internal", "api", "syncadmin", "testdata", "jira_recorded", "search_*.json"))
	if err != nil || len(files) != 6 {
		t.Fatalf("recorded jira pages: %v (%d)", err, len(files))
	}
	start := func(name string) int {
		n, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(filepath.Base(name), "search_"), ".json"))
		return n
	}
	sort.Slice(files, func(i, j int) bool { return start(files[i]) < start(files[j]) })
	var projects []json.RawMessage
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var page struct {
			Values []json.RawMessage `json:"values"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			t.Fatal(err)
		}
		projects = append(projects, page.Values...)
	}
	badAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("venue@example.com:bad-token"))
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		if r.Header.Get("Authorization") == badAuth {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"errorMessages":["Client must be authenticated to access this resource."],"errors":{}}`))
			return
		}
		if r.URL.Path != "/rest/api/3/project/search" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"errorMessages":["Not found"]}`))
			return
		}
		from, _ := strconv.Atoi(r.URL.Query().Get("startAt"))
		size, _ := strconv.Atoi(r.URL.Query().Get("maxResults"))
		if size <= 0 {
			size = 50
		}
		end := from + size
		if end > len(projects) {
			end = len(projects)
		}
		values := []json.RawMessage{}
		if from < len(projects) {
			values = projects[from:end]
		}
		isLast := end >= len(projects)
		page := map[string]any{
			"self":       fmt.Sprintf("https://%s/rest/api/3/project/search?maxResults=%d&startAt=%d", r.Host, size, from),
			"maxResults": size, "startAt": from, "total": len(projects), "isLast": isLast, "values": values,
		}
		if !isLast {
			page["nextPage"] = fmt.Sprintf("https://%s/rest/api/3/project/search?maxResults=%d&startAt=%d", r.Host, size, end)
		}
		_ = json.NewEncoder(w).Encode(page)
	}))
	t.Cleanup(server.Close)
	certFile := filepath.Join(t.TempDir(), "fake-jira.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	return server, certFile
}

// TestIntegrationDiscoverVenueOracle sends POST .../integrations/{id}/discover
// to the real Python api and the Go api, on one pinned clock and one fake
// Jira, and requires the same answers and the same integration_sources rows.
// A row's random id (uuid4 on each plane) is replaced by its place of first
// appearance in the body it is named in.
//
// Named limits: a Jira project renamed in Jira (same project id, new key) is
// updated in place by Python, with its watermarks moved, and created as a
// second row by the Go service (not seeded; the same gap is in the worker's
// discovery). And (lead ruling for the sync config create path, the same
// service): an integration without a credential is skipped here where Python
// would try the JIRA_* environment credentials or, for GitHub and GitLab,
// discover anonymously; a credential Python loads by id and org although it is
// inactive or another org's is refused by the Go resolver. Neither is seeded.
func TestIntegrationDiscoverVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-integration-discover!"
	v := newIDs()
	jira, certFile := fakeJira(t, root)
	t.Setenv("NO_PROXY", "127.0.0.1,localhost")
	t.Setenv("no_proxy", "127.0.0.1,localhost")

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		PythonEnv: []string{
			"SETTINGS_ENCRYPTION_KEY=" + venueKey,
			"NO_PROXY=127.0.0.1,localhost", "no_proxy=127.0.0.1,localhost",
			"REQUESTS_CA_BUNDLE=" + certFile,
			"VENUE_PINNED_NOW=" + venuePinnedNow,
			"VENUE_PINNED_NOW_MODULES=dev_health_ops.sync.discovery,dev_health_ops.models.integrations",
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			seed(t, ctx, admin, venue, v, "https://"+jira.Listener.Addr().String())
			token := func(user, org uuid.UUID, email, role string) map[string]any {
				return map[string]any{"user_id": user.String(), "email": email, "org_id": org.String(), "role": role}
			}
			return map[string]map[string]any{
				"adminA":     token(v.adminA, v.orgA, "disc-admin-a@example.com", "admin"),
				"memberA":    token(v.memberA, v.orgA, "disc-member-a@example.com", "member"),
				"adminB":     token(v.adminB, v.orgB, "disc-admin-b@example.com", "admin"),
				"adminNoOrg": {"user_id": v.adminNoOrg.String(), "email": "disc-noorg@example.com", "org_id": "", "role": "admin"},
			}
		},
	})
	pinned, err := time.Parse(time.RFC3339Nano, venuePinnedNow)
	if err != nil {
		t.Fatal(err)
	}
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(venueKey), "")
	if err != nil {
		t.Fatal(err)
	}
	jiraClient := jira.Client()
	jiraClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	base := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Now = func() time.Time { return pinned }
		deps.Decryptor = decryptor
		deps.SyncJiraHTTP = jiraClient
	})

	requests := discoverRequests(venue, v)
	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{Normalize: normalize})
	t.Logf("receipt (%d requests):\n%s", len(requests), receipt)

	// integration_sources as raw column text; a created row's id is random on
	// each plane, so rows are compared by their identity columns.
	query := `SELECT org_id, integration_id, provider, source_type, external_id, name, full_name, metadata::text, is_enabled,
discovered_at, last_seen_at, last_sync_at, last_sync_success, last_sync_error
FROM integration_sources ORDER BY org_id, integration_id, provider, external_id`
	pythonRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), query)
	goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), query)
	if pythonRows != goRows {
		t.Errorf("integration_sources differ\n python:\n%.4000s\n go:\n%.4000s", pythonRows, goRows)
	}
	// Rows are joined by " | ": the seed and the discoveries must have
	// produced hundreds of them, or SAME could be two empty tables agreeing.
	if rows := strings.Count(pythonRows, " | ") + 1; rows < 300 {
		t.Errorf("only %d source rows compared", rows)
	}
}

var anyUUID = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

// normalize names each distinct uuid by its first appearance in the body, so
// the same row named twice still reads as one row and two rows never do.
func normalize(_ venueoracle.Request, body string) string {
	seen := map[string]int{}
	return anyUUID.ReplaceAllStringFunc(body, func(id string) string {
		if _, ok := seen[id]; !ok {
			seen[id] = len(seen) + 1
		}
		return fmt.Sprintf("<uuid#%d>", seen[id])
	})
}

func startGoServer(t *testing.T, ctx context.Context, venue *venueoracle.Venue, jwtKey string, adjust func(*apiservice.Deps)) string {
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
	deps := apiservice.Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, logger)}
	adjust(&deps)
	routes := apiservice.Routes(deps, logger)
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, routes, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}

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
