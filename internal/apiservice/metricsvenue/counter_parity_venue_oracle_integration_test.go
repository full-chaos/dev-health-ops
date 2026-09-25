//go:build integration

package metricsvenue

import (
	"bufio"
	"context"
	_ "embed"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// encryptionKey is SETTINGS_ENCRYPTION_KEY on both planes.
const encryptionKey = "venue-metrics-settings-encryption-key"

// ingestAPIKey is INGEST_API_KEYS on both planes: the legacy ingest routes
// refuse any other key (reason invalid_api_key).
const ingestAPIKey = "venue-metrics-ingest-key"

// TestCounterParityVenueOracle sends the same requests to the real Python
// api and to the dho api (both on the venue's seeded database), then reads
// each plane's Prometheus exposition -- the Python app's /metrics and the
// dho api's operator /metrics -- and requires every counter named in
// routeCounters to have moved by the same amount, label set by label set.
// Each plane's process starts with the counter at zero, so the sample is
// the movement. A counter that moved on neither plane fails too: the case
// then proves nothing.
func TestCounterParityVenueOracle(t *testing.T) {
	ctx := context.Background()
	orgID, ownerID, otherOrgID := uuid.New(), uuid.New(), uuid.New()
	// A community-tier org at its max_repos (3 active sync configs) with a
	// disabled Jira source: enabling it is refused at the repo limit.
	cappedOrg, cappedOwner, jiraIntegration, jiraSource := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	garbled, notJSON, emptyList, oddProvider := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	jwtKey := uuid.NewString() + uuid.NewString()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:      venueRoot(),
		JWTKey:    jwtKey,
		PythonEnv: []string{"SETTINGS_ENCRYPTION_KEY=" + encryptionKey, "INGEST_API_KEYS=" + ingestAPIKey},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			// A payload the key cannot decrypt, and one it decrypts to text
			// json.loads refuses: get_decrypted_credentials_by_id_with_outcome
			// counts both as DECRYPT_FAILED.
			// And one that decrypts to valid JSON that is not an object
			// (json.loads succeeds: the OK outcome, never counted; the route
			// answers 404 for a falsy value).
			encrypted := v.CallPython(t,
				venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{"not a json object {"}},
				venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{"[]"}},
			)
			var notJSONCiphertext, emptyListCiphertext string
			if err := json.Unmarshal(encrypted[0], &notJSONCiphertext); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encrypted[1], &emptyListCiphertext); err != nil {
				t.Fatal(err)
			}
			for _, statement := range []struct {
				sql  string
				args []any
			}{
				{`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'venue-metrics', 'Venue Metrics', 'enterprise', 'stripe', true, now(), now())`, []any{orgID}},
				{`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'venue-metrics-other', 'Venue Metrics Other', 'enterprise', 'stripe', true, now(), now())`, []any{otherOrgID}},
				{`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'venue-metrics-capped', 'Venue Metrics Capped', 'community', 'stripe', true, now(), now())`, []any{cappedOrg}},
				{`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-metrics-capped@example.com', true, true, false, 0, now(), now())`, []any{cappedOwner}},
				{`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'owner', now(), now(), now())`, []any{uuid.New(), cappedOrg, cappedOwner}},
				{`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, 'jira', NULL, 'capped-jira', '{}'::json, true, now(), now())`, []any{jiraIntegration, cappedOrg.String()}},
				{`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name,
metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'jira', 'repository', 'P1', 'Proj One', 'Proj One', '{}'::json, false, now(), now())`, []any{jiraSource, cappedOrg.String(), jiraIntegration}},
				{`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'c1', 'github', '[]'::json, '{}'::json, true, false, now(), now()),
       (gen_random_uuid(), $1, 'c2', 'github', '[]'::json, '{}'::json, true, false, now(), now()),
       (gen_random_uuid(), $1, 'c3', 'github', '[]'::json, '{}'::json, true, false, now(), now())`, []any{cappedOrg.String()}},
				{`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-metrics-owner@example.com', true, true, false, 0, now(), now())`, []any{ownerID}},
				{`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'owner', now(), now(), now())`, []any{uuid.New(), orgID, ownerID}},
				{`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'github', 'garbled', true, 'gAAAAABnot-a-fernet-token', '{}'::json, now(), now())`, []any{garbled, orgID.String()}},
				{`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'gitlab', 'not-json', true, $3, '{}'::json, now(), now())`, []any{notJSON, orgID.String(), notJSONCiphertext}},
				{`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'jira', 'empty-list', true, $3, '{}'::json, now(), now())`, []any{emptyList, orgID.String(), emptyListCiphertext}},
				// A free-form provider whose label value holds "}", "," and
				// a quote: the exposition's label parsing must not stop early.
				{`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'odd},"x', 'garbled', true, 'gAAAAABnot-a-fernet-token', '{}'::json, now(), now())`, []any{oddProvider, orgID.String()}},
			} {
				if _, err := admin.Exec(ctx, statement.sql, statement.args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, statement.sql)
				}
			}
			return map[string]map[string]any{
				"owner":  {"user_id": ownerID.String(), "email": "venue-metrics-owner@example.com", "org_id": orgID.String(), "role": "owner"},
				"capped": {"user_id": cappedOwner.String(), "email": "venue-metrics-capped@example.com", "org_id": cappedOrg.String(), "role": "owner"},
			}
		},
	})

	bearer := map[string]string{"Authorization": "Bearer " + venue.Tokens["owner"], "Content-Type": "application/json"}
	testByID := func(name string, id uuid.UUID, provider string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: http.MethodPost, Path: "/api/v1/admin/credentials/test", Headers: bearer,
			Body: venueoracle.B64(`{"provider":"` + provider + `","credential_id":"` + id.String() + `"}`)}
	}
	requests := []venueoracle.Request{
		testByID("test a garbled stored credential", garbled, "github"),
		testByID("test it again", garbled, "github"),
		testByID("test a stored credential that is not JSON", notJSON, "gitlab"),
		testByID("test a stored credential that is an empty JSON list", emptyList, "jira"),
		testByID("test a garbled credential with an odd provider", oddProvider, "github"),
		// Telemetry: an org the caller is not a member of (not_a_member),
		// and the instance-wide report without a platform role
		// (report_not_platform_role).
		// The org-scope middleware refuses another org's X-Org-Id before the
		// route's own not_a_member check, so on both planes this 403 must
		// move no counter.
		{Name: "telemetry status for another org", Method: http.MethodGet, Path: "/api/v1/telemetry/status",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["owner"], "X-Org-Id": otherOrgID.String()}},
		{Name: "telemetry report without a platform role", Method: http.MethodPost, Path: "/api/v1/telemetry/report", Headers: bearer},
		// An operator's enable of a Jira source past the org's max_repos.
		{Name: "enable a jira source past the repo limit", Method: http.MethodPatch,
			Path:    "/api/v1/admin/integrations/" + jiraIntegration.String() + "/sources/" + jiraSource.String(),
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["capped"], "Content-Type": "application/json"},
			Body:    venueoracle.B64(`{"is_enabled":true}`)},
		// Legacy ingest with a key INGEST_API_KEYS does not hold.
		{Name: "legacy ingest with a wrong key", Method: http.MethodPost, Path: "/api/v1/ingest/commits",
			Headers: map[string]string{"X-API-Key": "not-" + ingestAPIKey, "Content-Type": "application/json"},
			Body:    venueoracle.B64(`{"repo":"acme/api","commits":[]}`)},
	}
	scrape := venueoracle.Request{Name: "metrics", Method: http.MethodGet, Path: "/metrics"}

	python := venue.ServePython(t, append(append([]venueoracle.Request{}, requests...), scrape))
	pythonMetrics := python[len(python)-1]
	if pythonMetrics.Status != http.StatusOK {
		t.Fatalf("python /metrics answered %d", pythonMetrics.Status)
	}

	t.Setenv("INGEST_API_KEYS", ingestAPIKey)
	goBase, operator := startGoAPI(t, ctx, venue, jwtKey)
	receipt := venueoracle.Diff(t, goBase, requests, python[:len(requests)], venueoracle.DiffOptions{})
	t.Log("\n" + receipt)
	goMetrics := operator()

	checkPythonMetricsTable(t)

	for _, counter := range routeCounters {
		pythonSamples := samples(t, pythonMetrics.Body, counter.metric)
		goSamples := samples(t, goMetrics, counter.metric)
		if len(pythonSamples) == 0 {
			t.Errorf("%s (%s): the Python api did not move it; the case proves nothing", counter.metric, counter.route)
			continue
		}
		if !equalSamples(pythonSamples, goSamples) {
			t.Errorf("%s (%s): DIFF\n python %v\n go     %v", counter.metric, counter.route, pythonSamples, goSamples)
			continue
		}
		t.Logf("%s (%s): SAME %v", counter.metric, counter.route, goSamples)
	}
	venueoracle.WriteProof(t)
}

// routeCounters are the counters this oracle drives, with the route whose
// Python handler moves them.
var routeCounters = []struct{ metric, route string }{
	{"devhealth_integration_credential_decrypt_failed_total", "POST /api/v1/admin/credentials/test"},
	{"devhealth_telemetry_org_id_rejected_total", "POST /api/v1/telemetry/report"},
	{"devhealth_ingest_legacy_auth_rejected_total", "POST /api/v1/ingest/commits"},
	{"jira_project_discovery_total", "PATCH /api/v1/admin/integrations/{integration_id}/sources/{source_id}"},
}

//go:embed testdata/python_metrics.tsv
var pythonMetricsTable string

// pythonMetricFamilies lists every metric family the Python api registers,
// by importing the app in the venue's interpreter: family -> type.
const pythonMetricFamilies = `
import json
import dev_health_ops.api.main
from prometheus_client import REGISTRY
print("RESULT " + json.dumps({m.name: m.type for m in REGISTRY.collect()}))
`

// tableStatuses are the statuses python_metrics.tsv may give a family; its
// header says what each means.
var tableStatuses = map[string]bool{
	"ported": true, "partial": true, "route-missing": true, "unreached": true, "route-not-ported": true,
	"other-go-service": true, "worker": true, "python-only": true, "retired": true,
	"unused": true, "runtime": true,
}

// checkPythonMetricsTable is the route-to-counter guard. python_metrics.tsv
// must name exactly the families the live Python api registers at import,
// each with its type, so a Python counter cannot be added or ported without
// a row. A "lazy:" row is a family registered only when its code first
// runs, so it must be absent from the import-time listing (a family that
// moves to import time needs its row corrected). Every "ported" or
// "partial" row must be a counter this oracle compares, and every counter
// the oracle compares must have such a row.
func checkPythonMetricsTable(t *testing.T) {
	t.Helper()
	command := exec.Command("python3", "-c", pythonMetricFamilies)
	command.Dir = venueRoot()
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(venueRoot(), "src"))
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list the Python api's metric families: %v", err)
	}
	var live map[string]string
	for _, line := range strings.Split(string(output), "\n") {
		if rest, ok := strings.CutPrefix(line, "RESULT "); ok {
			if err := json.Unmarshal([]byte(rest), &live); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(live) == 0 {
		t.Fatal("the Python api registered no metric families; the listing did not run")
	}
	driven := map[string]bool{}
	for _, counter := range routeCounters {
		driven[counter.metric] = false
	}
	table := map[string]bool{}
	lazy := 0
	for _, row := range parsePythonMetricsTable(t) {
		family, kind, status := row.family, row.kind, row.status
		if table[family] {
			t.Errorf("python_metrics.tsv names %s twice", family)
		}
		table[family] = true
		liveKind, registered := live[family]
		if row.lazy {
			lazy++
			if registered {
				t.Errorf("python_metrics.tsv marks %s lazy, but the Python api registers it at import", family)
			}
		} else {
			switch {
			case !registered:
				t.Errorf("python_metrics.tsv names %s, which the Python api no longer registers", family)
			case liveKind != kind:
				t.Errorf("python_metrics.tsv types %s as %s; the Python api registers a %s", family, kind, liveKind)
			}
		}
		if !tableStatuses[status] {
			t.Errorf("python_metrics.tsv: %s has status %q, which the header does not define", family, status)
		}
		exposed := family
		if kind == "counter" {
			exposed += "_total"
		}
		_, compared := driven[exposed]
		switch {
		case (status == "ported" || status == "partial") && !compared:
			t.Errorf("python_metrics.tsv marks %s %s, but the oracle does not compare %s", family, status, exposed)
		case status != "ported" && status != "partial" && compared:
			t.Errorf("the oracle compares %s, but python_metrics.tsv marks %s %s", exposed, family, status)
		case compared:
			driven[exposed] = true
		}
	}
	for family, kind := range live {
		if !table[family] {
			t.Errorf("the Python api registers %s (%s), which python_metrics.tsv does not name", family, kind)
		}
	}
	for metric, rowed := range driven {
		if !rowed {
			t.Errorf("the oracle compares %s, which python_metrics.tsv does not mark ported or partial", metric)
		}
	}
	t.Logf("python_metrics.tsv: %d families (%d lazy); every import-time family of the live Python api is named", len(table), lazy)
}

// tableRow is one python_metrics.tsv row: <family> <type> <status>
// <routes> <note>, where type may carry a "lazy:" prefix and routes is
// "-" or the ";"-separated route labels the family fires under.
type tableRow struct {
	family, kind, status, note string
	lazy                       bool
	routes                     []string
}

func parsePythonMetricsTable(t *testing.T) []tableRow {
	t.Helper()
	var rows []tableRow
	for _, line := range strings.Split(pythonMetricsTable, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 5 || fields[3] == "" || fields[4] == "" {
			t.Errorf("python_metrics.tsv: %q is not <family> <type> <status> <routes> <note>", line)
			continue
		}
		row := tableRow{family: fields[0], status: fields[2], note: fields[4]}
		row.kind, row.lazy = strings.CutPrefix(fields[1], "lazy:")
		if fields[3] != "-" {
			row.routes = strings.Split(fields[3], ";")
		}
		rows = append(rows, row)
	}
	return rows
}

// startGoAPI serves the dho api route set on the venue's Go database, and
// returns its base URL and a reader of its operator /metrics, the Go api's
// scrape surface.
func startGoAPI(t *testing.T, ctx context.Context, venue *venueoracle.Venue, key string) (string, func() string) {
	t.Helper()
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(encryptionKey), "")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatal(err)
	}
	deps := apiservice.Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, logger), Verifier: verifier, Decryptor: decryptor}
	// The operator endpoint as dho api's configure builds it, before the
	// routes (it sets the legacy-ingest counter on deps).
	registry := health.NewRegistry(0)
	if err := apiservice.RegisterOperatorMetrics(registry, &deps); err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, apiservice.Routes(deps, logger), scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(server.Handler())
	t.Cleanup(api.Close)

	operatorServer, err := health.NewServer(health.ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "api"})
	if err != nil {
		t.Fatal(err)
	}
	operator := httptest.NewServer(operatorServer.Handler())
	t.Cleanup(operator.Close)
	return api.URL, func() string {
		response, err := http.Get(operator.URL + "/metrics")
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
}

// samples reads metric's samples out of a Prometheus text exposition: label
// set (each name="value" pair as written, sorted) -> value. It reads the
// counter's own samples only, not its _created companion. Label values are
// read as the text format quotes them, so a value holding "}", "," or an
// escaped quote does not end the label set early; a line that does not
// parse fails the test rather than being skipped.
func samples(t *testing.T, exposition, metric string) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	scanner := bufio.NewScanner(strings.NewReader(exposition))
	scanner.Buffer(make([]byte, 1<<20), 1<<24)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || !strings.HasPrefix(line, metric) {
			continue
		}
		rest := line[len(metric):]
		var pairs []string
		switch {
		case strings.HasPrefix(rest, "{"):
			var ok bool
			pairs, rest, ok = labelPairs(rest[1:])
			if !ok {
				t.Fatalf("unparsable label set in %q", line)
			}
		case strings.HasPrefix(rest, " "):
		default:
			continue // another metric whose name starts with this one
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			t.Fatalf("no value in %q", line)
		}
		value, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			t.Fatalf("unparsable value in %q: %v", line, err)
		}
		// A zero sample is no movement: a Go fragment may pre-seed every
		// label value at 0 (so an alert sees the series before the first
		// increment), where prometheus_client exposes only the label values
		// it has counted. Movement is what the oracle compares.
		if value == 0 {
			continue
		}
		sort.Strings(pairs)
		out[strings.Join(pairs, ",")] = value
	}
	return out
}

// labelPairs reads `name="value",...}` (the text after "{"): the pairs as
// written and the text after the closing "}".
func labelPairs(text string) ([]string, string, bool) {
	var pairs []string
	for {
		if strings.HasPrefix(text, "}") {
			return pairs, text[1:], true
		}
		eq := strings.Index(text, "=\"")
		if eq <= 0 {
			return nil, "", false
		}
		end := -1
		for index := eq + 2; index < len(text); index++ {
			if text[index] == '\\' {
				index++
				continue
			}
			if text[index] == '"' {
				end = index
				break
			}
		}
		if end < 0 {
			return nil, "", false
		}
		pairs = append(pairs, text[:end+1])
		text = strings.TrimPrefix(text[end+1:], ",")
	}
}

func equalSamples(a, b map[string]float64) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || other != value {
			return false
		}
	}
	return true
}

func venueRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
