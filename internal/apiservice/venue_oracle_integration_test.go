//go:build integration

package apiservice

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// The venue oracle: the REAL Python api (dev_health_ops.api.main:app, every
// middleware and handler) and the REAL dho api (configure(), as the api role
// after the River migration) answer the same requests against two copies
// of one Postgres database built by the real Alembic chain and seeded once.
// Responses must match byte for byte on status, body and the compared
// headers, and the rows a write touches must match afterwards.

const venueKey = "venue-oracle-signing-key-0123456789abcdef"

// pythonVenueProgram has three modes: "migrate" runs the Alembic heads,
// "mint" mints access tokens with the real AuthService, and "serve" answers
// a batch of requests with TestClient over the real app.
const pythonVenueProgram = `
import base64, json, sys
mode = sys.argv[1]
if mode == "migrate":
    from alembic import command
    from dev_health_ops.migrate import _make_alembic_config
    command.upgrade(_make_alembic_config(), "heads")
    print(json.dumps({"ok": True}))
elif mode == "mint":
    from dev_health_ops.api.services.auth import AuthService
    svc = AuthService()
    out = {}
    for name, spec in json.loads(sys.stdin.read()).items():
        out[name] = svc.create_access_token(**spec)
    print(json.dumps(out))
elif mode == "serve":
    from fastapi.testclient import TestClient
    from dev_health_ops.api.main import app
    client = TestClient(app, raise_server_exceptions=False)
    out = []
    for req in json.loads(sys.stdin.read()):
        body = base64.b64decode(req["body"]) if req.get("body") is not None else None
        r = client.request(req["method"], req["path"], headers=req.get("headers") or {}, content=body)
        out.append({"status": r.status_code, "headers": {k.lower(): v for k, v in r.headers.items()},
                    "body": base64.b64encode(r.content).decode()})
    print(json.dumps(out))
`

type venueRequest struct {
	Name    string            `json:"name"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    *string           `json:"body"`
}

type venueResponse struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

func venueRoot(t *testing.T) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func runPython(t *testing.T, python string, env []string, stdin any, args ...string) []byte {
	t.Helper()
	command := exec.Command(python, append([]string{"-c", pythonVenueProgram}, args...)...)
	command.Env = append(os.Environ(), env...)
	if stdin != nil {
		payload, _ := json.Marshal(stdin)
		command.Stdin = bytes.NewReader(payload)
	}
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	if path := os.Getenv("DEV_HEALTH_VENUE_PY_LOG"); path != "" {
		_ = os.WriteFile(path+"."+args[0], stderr.Bytes(), 0o600)
	}
	if err != nil {
		t.Fatalf("python %v: %v\n%s", args, err, tail(stderr.String()))
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	return []byte(lines[len(lines)-1])
}

func tail(text string) string {
	if len(text) > 4000 {
		return text[len(text)-4000:]
	}
	return text
}

func withDatabase(t *testing.T, raw, database, user, password string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + database
	if user != "" {
		parsed.User = url.UserPassword(user, password)
	}
	return parsed.String()
}

// TestVenueOracleProtectedRoutes is the write-and-read
// differential.
func TestVenueOracleProtectedRoutes(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the venue oracle needs the live Python api; run with DEV_HEALTH_LIVE_PYTHON_ORACLES=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	root := venueRoot(t)
	python := pyoracle.Resolve(t, root)

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	sourceDB, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	asyncURI := func(database string) string {
		return strings.Replace(withDatabase(t, instance.URI, database, "", ""), "postgres://", "postgresql+asyncpg://", 1)
	}
	pyEnv := func(database string) []string {
		return []string{"PYTHONPATH=" + filepath.Join(root, "src"), "POSTGRES_URI=" + strings.Replace(asyncURI(database), "postgresql://", "postgresql+asyncpg://", 1),
			"JWT_SECRET_KEY=" + venueKey, "OTEL_SDK_DISABLED=true", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1",
			// NullPool: TestClient gives each request its own event loop, and a
			// pooled asyncpg connection cannot cross loops.
			"PGBOUNCER_TRANSACTION_MODE=true", "ENVIRONMENT=test", "REDIS_URL=", "CLICKHOUSE_URI="}
	}

	// 1. The real schema, then one seed.
	runPython(t, python, pyEnv(sourceDB), nil, "migrate")
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	seed := venueSeed(t, ctx, admin)
	tokens := map[string]string{}
	if err := json.Unmarshal(runPython(t, python, pyEnv(sourceDB), seed.tokenSpecs(), "mint"), &tokens); err != nil {
		t.Fatal(err)
	}
	admin.Close()

	// 2. Two identical copies: Python serves the source, Go serves the copy.
	goDB := sourceDB + "_go"
	adminConn, err := pgxpool.New(ctx, withDatabase(t, instance.URI, "postgres", "", ""))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(adminConn.Close)
	if _, err := adminConn.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q TEMPLATE %q`, goDB, sourceDB)); err != nil {
		t.Fatal(err)
	}

	// 3. The api role, provisioned and granted exactly as a deploy does.
	roles := map[string]string{}
	for _, name := range []string{"domain", "queue", "coordinator", "api"} {
		role, err := containers.RoleName("venue_"+name, instance)
		if err != nil {
			t.Fatal(err)
		}
		roles[name] = role
	}
	goAdmin, err := pgxpool.New(ctx, withDatabase(t, instance.URI, goDB, "", ""))
	if err != nil {
		t.Fatal(err)
	}
	defer goAdmin.Close()
	t.Cleanup(func() {
		for _, role := range roles {
			containers.DropRole(adminConn, role, t.Logf)
		}
	})
	provisionVenueRoles(t, ctx, withDatabase(t, instance.URI, goDB, "", ""), roles)
	migrateVenue(t, ctx, withDatabase(t, instance.URI, goDB, "", ""), roles)

	cfg := config.Config{
		APIAddress: "127.0.0.1:0", RiverDatabaseSchema: "river", APIDatabaseRole: roles["api"],
		APIDatabaseURI: secrets.NewValue(withDatabase(t, instance.URI, goDB, roles["api"], "venue_api_password")),
		APIJWTSecret:   secrets.NewValue(venueKey), APIJWTIssuer: "dev-health-ops", APIJWTAudience: "dev-health-api",
		CORSAllowedOrigins: []string{"http://localhost:3000"},
	}
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
	defer func() {
		for index := len(components) - 1; index >= 0; index-- {
			_ = components[index].Shutdown(context.Background())
		}
	}()
	if ready := registry.CheckRequired(ctx); !ready.Ready {
		t.Fatalf("dho api not ready as the api role: %+v", ready)
	}
	var server interface{ Address() string }
	for _, component := range components {
		if candidate, ok := component.(interface{ Address() string }); ok {
			server = candidate
		}
	}

	requests := venueRequests(seed, tokens)
	pythonResponses := []venueResponse{}
	if err := json.Unmarshal(runPython(t, python, pyEnv(sourceDB), requests, "serve"), &pythonResponses); err != nil {
		t.Fatal(err)
	}
	// Every response header is compared except the per-response ones.
	volatile := map[string]bool{"date": true, "server": true, "x-request-id": true}
	var receipt strings.Builder
	for index, request := range requests {
		goResponse := doVenueRequest(t, "http://"+server.Address(), request)
		python := pythonResponses[index]
		pythonBody, _ := base64.StdEncoding.DecodeString(python.Body)
		same := goResponse.Status == python.Status && goResponse.Body == string(pythonBody)
		compared := headerUnion(python.Headers, goResponse.Headers, volatile)
		for _, header := range compared {
			if goResponse.Headers[header] != python.Headers[header] {
				same = false
			}
		}
		fmt.Fprintf(&receipt, "%-58s python=%d go=%d %s\n", request.Name, python.Status, goResponse.Status, map[bool]string{true: "SAME", false: "DIFF"}[same])
		if !same {
			t.Errorf("%s:\n python %d %s %v\n go     %d %s %v", request.Name, python.Status, pythonBody,
				pick(python.Headers, compared), goResponse.Status, goResponse.Body, pick(goResponse.Headers, compared))
		}
	}
	// The rows the writes touched are identical on both copies.
	pyRows := orgRows(t, ctx, withDatabase(t, instance.URI, sourceDB, "", ""))
	goRows := orgRows(t, ctx, withDatabase(t, instance.URI, goDB, "", ""))
	if pyRows != goRows {
		t.Errorf("organizations after the writes differ:\n python %s\n go     %s", pyRows, goRows)
	}
	fmt.Fprintf(&receipt, "organizations rows after writes: %s\n", map[bool]string{true: "SAME", false: "DIFF"}[pyRows == goRows])
	if path := os.Getenv("DEV_HEALTH_VENUE_RECEIPT"); path != "" {
		_ = os.WriteFile(path, []byte(receipt.String()), 0o600)
	}
	t.Log("\n" + receipt.String())
}

func pick(headers map[string]string, keys []string) map[string]string {
	out := map[string]string{}
	for _, key := range keys {
		if value, ok := headers[key]; ok {
			out[key] = value
		}
	}
	return out
}

func doVenueRequest(t *testing.T, base string, request venueRequest) venueResponse {
	t.Helper()
	var body io.Reader
	if request.Body != nil {
		raw, _ := base64.StdEncoding.DecodeString(*request.Body)
		body = bytes.NewReader(raw)
	}
	httpRequest, err := http.NewRequest(request.Method, base+request.Path, body)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range request.Headers {
		httpRequest.Header.Set(key, value)
	}
	response, err := http.DefaultClient.Do(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	headers := map[string]string{}
	for key, values := range response.Header {
		headers[strings.ToLower(key)] = strings.Join(values, ", ")
	}
	return venueResponse{Status: response.StatusCode, Headers: headers, Body: string(raw)}
}

func orgRows(t *testing.T, ctx context.Context, uri string) string {
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	rows, err := pool.Query(ctx, `SELECT id::text, slug, name, coalesce(description, '<null>'), tier, is_active,
		updated_at > created_at FROM organizations ORDER BY slug`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, fmt.Sprint(values...))
	}
	sort.Strings(lines)
	return strings.Join(lines, " | ")
}

func provisionVenueRoles(t *testing.T, ctx context.Context, uri string, roles map[string]string) {
	t.Helper()
	command := exec.CommandContext(ctx, "psql", uri, "--set=ON_ERROR_STOP=1",
		"--set=domain_role="+roles["domain"], "--set=queue_role="+roles["queue"],
		"--set=coordinator_role="+roles["coordinator"], "--set=domain_password=venue_domain",
		"--set=queue_password=venue_queue", "--set=coordinator_password=venue_coordinator",
		"--set=api_role="+roles["api"], "--set=api_password=venue_api_password",
		"--file="+filepath.Join(venueRoot(t), "scripts", "worker", "provision_river_roles.sql"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("provision: %v\n%s", err, output)
	}
}

func migrateVenue(t *testing.T, ctx context.Context, uri string, roles map[string]string) {
	t.Helper()
	poolConfig, err := pgxpool.ParseConfig(uri)
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	posture := postgres.APIPosture()
	var grants []riverstore.TableGrant
	for _, table := range posture.RequiredTables {
		grants = append(grants, riverstore.TableGrant{TableName: table.TableName, AllowInsert: table.AllowInsert,
			AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete})
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, pool, riverstore.MigrationOptions{
		Schema: "river", DomainRole: roles["domain"], QueueRole: roles["queue"],
		APIRole: roles["api"], APIGrants: grants, Logger: quietLogger(),
	}); err != nil {
		t.Fatalf("River migration: %v", err)
	}
}

func headerUnion(a, b map[string]string, skip map[string]bool) []string {
	seen := map[string]bool{}
	var out []string
	for _, headers := range []map[string]string{a, b} {
		for key := range headers {
			if !skip[key] && !seen[key] {
				seen[key] = true
				out = append(out, key)
			}
		}
	}
	sort.Strings(out)
	return out
}
