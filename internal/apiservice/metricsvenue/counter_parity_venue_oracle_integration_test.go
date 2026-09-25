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
	orgID, ownerID := uuid.New(), uuid.New()
	garbled, notJSON := uuid.New(), uuid.New()
	jwtKey := uuid.NewString() + uuid.NewString()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:      venueRoot(),
		JWTKey:    jwtKey,
		PythonEnv: []string{"SETTINGS_ENCRYPTION_KEY=" + encryptionKey},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			// A payload the key cannot decrypt, and one it decrypts to text
			// json.loads refuses: get_decrypted_credentials_by_id_with_outcome
			// counts both as DECRYPT_FAILED.
			encrypted := v.CallPython(t, venueoracle.PythonCall{
				Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{"not a json object {"},
			})
			var notJSONCiphertext string
			if err := json.Unmarshal(encrypted[0], &notJSONCiphertext); err != nil {
				t.Fatal(err)
			}
			for _, statement := range []struct {
				sql  string
				args []any
			}{
				{`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'venue-metrics', 'Venue Metrics', 'enterprise', 'stripe', true, now(), now())`, []any{orgID}},
				{`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-metrics-owner@example.com', true, true, false, 0, now(), now())`, []any{ownerID}},
				{`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'owner', now(), now(), now())`, []any{uuid.New(), orgID, ownerID}},
				{`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'github', 'garbled', true, 'gAAAAABnot-a-fernet-token', '{}'::json, now(), now())`, []any{garbled, orgID.String()}},
				{`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'gitlab', 'not-json', true, $3, '{}'::json, now(), now())`, []any{notJSON, orgID.String(), notJSONCiphertext}},
			} {
				if _, err := admin.Exec(ctx, statement.sql, statement.args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, statement.sql)
				}
			}
			return map[string]map[string]any{
				"owner": {"user_id": ownerID.String(), "email": "venue-metrics-owner@example.com", "org_id": orgID.String(), "role": "owner"},
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
	}
	scrape := venueoracle.Request{Name: "metrics", Method: http.MethodGet, Path: "/metrics"}

	python := venue.ServePython(t, append(append([]venueoracle.Request{}, requests...), scrape))
	pythonMetrics := python[len(python)-1]
	if pythonMetrics.Status != http.StatusOK {
		t.Fatalf("python /metrics answered %d", pythonMetrics.Status)
	}

	goBase, operator := startGoAPI(t, ctx, venue, jwtKey)
	receipt := venueoracle.Diff(t, goBase, requests, python[:len(requests)], venueoracle.DiffOptions{})
	t.Log("\n" + receipt)
	goMetrics := operator()

	checkPythonMetricsTable(t)

	for _, counter := range routeCounters {
		pythonSamples := samples(pythonMetrics.Body, counter.metric)
		goSamples := samples(goMetrics, counter.metric)
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
}

//go:embed python_metrics.tsv
var pythonMetricsTable string

// pythonMetricFamilies lists every metric family the Python api registers,
// by importing the app in the venue's interpreter: family -> type.
const pythonMetricFamilies = `
import json
import dev_health_ops.api.main
from prometheus_client import REGISTRY
print("RESULT " + json.dumps({m.name: m.type for m in REGISTRY.collect()}))
`

// checkPythonMetricsTable is the route-to-counter guard. python_metrics.tsv
// must name exactly the families the live Python api registers, each with
// its type, so a Python counter cannot be added or ported without a row;
// and every "ported" row must be a counter this oracle compares.
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
		driven[counter.metric] = true
	}
	table := map[string]bool{}
	for _, line := range strings.Split(pythonMetricsTable, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 3 {
			t.Errorf("python_metrics.tsv: %q is not <family> <type> <status>", line)
			continue
		}
		family, kind, status := fields[0], fields[1], fields[2]
		table[family] = true
		switch liveKind, ok := live[family]; {
		case !ok:
			t.Errorf("python_metrics.tsv names %s, which the Python api no longer registers", family)
		case liveKind != kind:
			t.Errorf("python_metrics.tsv types %s as %s; the Python api registers a %s", family, kind, liveKind)
		}
		switch status {
		case "ported":
			exposed := family
			if kind == "counter" {
				exposed += "_total"
			}
			if !driven[exposed] {
				t.Errorf("python_metrics.tsv marks %s ported, but the oracle does not compare %s", family, exposed)
			}
		case "runtime", "pending":
		default:
			t.Errorf("python_metrics.tsv: %s has status %q, not ported/runtime/pending", family, status)
		}
	}
	for family, kind := range live {
		if !table[family] {
			t.Errorf("the Python api registers %s (%s), which python_metrics.tsv does not name", family, kind)
		}
	}
	t.Logf("python_metrics.tsv: %d families, all registered by the live Python api", len(table))
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
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, apiservice.Routes(deps, logger), scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(server.Handler())
	t.Cleanup(api.Close)

	// The operator endpoint as dho api's configure builds it.
	registry := health.NewRegistry(0)
	if err := apiservice.RegisterAPIInstruments(registry); err != nil {
		t.Fatal(err)
	}
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
// set (as written, labels sorted) -> value. It reads the counter's own
// samples only, not its _created companion.
func samples(exposition, metric string) map[string]float64 {
	out := map[string]float64{}
	scanner := bufio.NewScanner(strings.NewReader(exposition))
	scanner.Buffer(make([]byte, 1<<20), 1<<24)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || !strings.HasPrefix(line, metric) {
			continue
		}
		rest := line[len(metric):]
		labels := ""
		if strings.HasPrefix(rest, "{") {
			end := strings.Index(rest, "}")
			if end < 0 {
				continue
			}
			labels, rest = rest[1:end], rest[end+1:]
		} else if !strings.HasPrefix(rest, " ") {
			continue // another metric whose name starts with this one
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		value, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		parts := strings.Split(labels, ",")
		sort.Strings(parts)
		out[strings.Join(parts, ",")] = value
	}
	return out
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
