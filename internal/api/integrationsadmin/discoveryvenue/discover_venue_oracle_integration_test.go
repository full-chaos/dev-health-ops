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
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
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
	orgA, orgB, orgC                                       uuid.UUID
	adminA, memberA, adminB, adminC, adminNoOrg            uuid.UUID
	credGood, credBad, credGoodB, credGoodC                uuid.UUID
	intRecover, cfgRecover, srcRecover                     uuid.UUID
	credNoURL, credNoEmail, intNoURL, intNoEmail, cfgNoURL uuid.UUID
	intJira, intScoped, intConfigScoped, intBad, intNoCred uuid.UUID
	intLinear, intB, intEmpty, intRename                   uuid.UUID
	cfgJira, cfgScoped, cfgB                               uuid.UUID
	srcAcm, srcOld, srcDupLower, srcDupUpper, srcRename    uuid.UUID
}

func newIDs() ids {
	var v ids
	for _, target := range []*uuid.UUID{&v.credNoURL, &v.credNoEmail, &v.intNoURL, &v.intNoEmail, &v.cfgNoURL, &v.orgA, &v.orgB, &v.orgC, &v.adminC, &v.credGoodC, &v.intRecover, &v.cfgRecover, &v.srcRecover, &v.adminA, &v.memberA, &v.adminB, &v.adminNoOrg, &v.credGood, &v.credBad,
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
				"adminC":     token(v.adminC, v.orgC, "disc-admin-c@example.com", "admin"),
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
	// The dho api's operator /metrics, the Go api's scrape surface: the
	// discovery counter is read from it after the requests.
	registry := health.NewRegistry(0)
	base := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Now = func() time.Time { return pinned }
		deps.Decryptor = decryptor
		deps.SyncJiraHTTP = jiraClient
		if err := apiservice.RegisterOperatorMetrics(registry, deps); err != nil {
			t.Fatal(err)
		}
	})
	operatorServer, err := health.NewServer(health.ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "api"})
	if err != nil {
		t.Fatal(err)
	}
	operator := httptest.NewServer(operatorServer.Handler())
	t.Cleanup(operator.Close)

	requests := discoverRequests(venue, v)
	scrape := venueoracle.Request{Name: "metrics", Method: http.MethodGet, Path: "/metrics"}
	python := venue.ServePython(t, append(append([]venueoracle.Request{}, requests...), scrape))
	pythonMetrics := python[len(python)-1]
	if pythonMetrics.Status != http.StatusOK {
		t.Fatalf("python /metrics answered %d", pythonMetrics.Status)
	}
	receipt := venueoracle.Diff(t, base, requests, python[:len(requests)], venueoracle.DiffOptions{Normalize: normalize})
	t.Logf("receipt (%d requests):\n%s", len(requests), receipt)
	goMetrics := scrapeOperator(t, operator.URL)
	compareDiscoveryCounter(t, pythonMetrics.Body, goMetrics)
	compareMappingRejected(t, pythonMetrics.Body, goMetrics)

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

// scrapeOperator reads the Go api's operator /metrics.
func scrapeOperator(t *testing.T, base string) string {
	t.Helper()
	response, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("operator /metrics answered %d: %s", response.StatusCode, body)
	}
	return string(body)
}

// discoveryCounts reads jira_project_discovery_total out of a Prometheus text
// exposition: outcome -> value. A zero sample is no movement (prometheus_client
// exposes a series it created with inc(0)), so it is left out.
func discoveryCounts(t *testing.T, exposition string) map[string]float64 {
	t.Helper()
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(exposition))
	if err != nil {
		t.Fatalf("parse exposition: %v", err)
	}
	out := map[string]float64{}
	family := families["jira_project_discovery_total"]
	if family == nil {
		family = families["jira_project_discovery"]
	}
	if family == nil {
		return out
	}
	for _, metric := range family.GetMetric() {
		var outcome string
		for _, label := range metric.GetLabel() {
			if label.GetName() == "outcome" {
				outcome = label.GetValue()
			}
		}
		if value := metric.GetCounter().GetValue(); value != 0 {
			out[outcome] = value
		}
	}
	return out
}

// compareDiscoveryCounter requires jira_project_discovery_total to have moved
// by the same amount on both planes, outcome by outcome, over the whole run:
// every outcome the discoveries produced, and the same outcomes on both.
func compareDiscoveryCounter(t *testing.T, python, goExposition string) {
	t.Helper()
	pythonCounts, goCounts := discoveryCounts(t, python), discoveryCounts(t, goExposition)
	// Every outcome the discovery emits (rejected_at_enable_repo_limit is the
	// enable route's, compared by TestCounterParityVenueOracle) must have moved
	// on the Python plane, or SAME would be two planes agreeing on nothing.
	for _, outcome := range []string{"discovered", "created", "existing", "discovered_zero", "skipped_no_planner_parent",
		"superseded_by_scope_change", "capped_by_repo_limit", "recovered_from_repo_limit_cap"} {
		if pythonCounts[outcome] == 0 {
			t.Errorf("the Python api did not move outcome %s (%v); the run does not exercise it", outcome, pythonCounts)
		}
	}
	if !reflect.DeepEqual(pythonCounts, goCounts) {
		t.Errorf("jira_project_discovery_total: DIFF\n python %v\n go     %v", pythonCounts, goCounts)
		return
	}
	t.Logf("jira_project_discovery_total: SAME %v", goCounts)
}

// compareMappingRejected requires credential_mapping_rejected_total to have
// moved by the same amount on both planes, label set by label set: the two
// stored credentials the resolver refuses are each counted against the field
// they lack.
func compareMappingRejected(t *testing.T, python, goExposition string) {
	t.Helper()
	read := func(exposition string) map[string]float64 {
		parser := expfmt.NewTextParser(model.UTF8Validation)
		families, err := parser.TextToMetricFamilies(strings.NewReader(exposition))
		if err != nil {
			t.Fatalf("parse exposition: %v", err)
		}
		out := map[string]float64{}
		family := families["credential_mapping_rejected_total"]
		if family == nil {
			family = families["credential_mapping_rejected"]
		}
		if family == nil {
			return out
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if value := metric.GetCounter().GetValue(); value != 0 {
				out[labels["provider"]+"/"+labels["missing_field"]] = value
			}
		}
		return out
	}
	pythonCounts, goCounts := read(python), read(goExposition)
	for _, want := range []string{"jira/base_url", "jira/email"} {
		if pythonCounts[want] == 0 {
			t.Errorf("the Python api did not count %s (%v); the run does not exercise it", want, pythonCounts)
		}
	}
	if !reflect.DeepEqual(pythonCounts, goCounts) {
		t.Errorf("credential_mapping_rejected_total: DIFF\n python %v\n go     %v", pythonCounts, goCounts)
		return
	}
	t.Logf("credential_mapping_rejected_total: SAME %v", goCounts)
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
