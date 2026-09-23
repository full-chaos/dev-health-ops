// Package venueoracle is the venue differential oracle for `dho api` routes
// ported from the Python api. The REAL Python app (dev_health_ops.api.main:app,
// every middleware and handler, driven by TestClient) and the REAL Go api
// answer the same requests against two copies of one Postgres database that
// the real Alembic chain built and one seed filled. Each plane has its own
// Valkey database. Responses must match byte for byte on status, body and
// every header except the per-response ones. After the writes, the caller
// compares the rows and stream entries they touched with TableRows and
// StreamEntries.
//
// Start builds the venue: Postgres and Valkey containers, the Alembic
// heads, the caller's seed, tokens minted by the real AuthService, a
// CREATE DATABASE ... TEMPLATE copy for Go, and the api role provisioned
// and granted on that copy as a deploy does (provision_river_roles.sql,
// then the River migration with postgres.APIPosture). The caller starts
// the Go api on Venue.GoAPIDatabaseURI and passes its base URL to Diff.
//
// The oracle needs the live Python api: Start skips unless
// DEV_HEALTH_LIVE_PYTHON_ORACLES=1, and ci/check_go.sh live-python-oracles
// sets it.
package venueoracle

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// pythonProgram has three modes: "migrate" runs the Alembic heads, "mint"
// mints access tokens with the real AuthService (stdin: name -> keyword
// arguments of create_access_token), and "serve" answers a batch of
// requests with TestClient over the real app.
const pythonProgram = `
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

// APIPassword is the api role's password on the Go copy.
const APIPassword = "venue_api_password"

// Request is one request sent to both planes. Body is base64 (nil = none).
type Request struct {
	Name    string            `json:"name"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    *string           `json:"body"`
}

// Response is one plane's answer; header names are lower case, and
// repeated values are joined with ", ".
type Response struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

// B64 encodes a request body.
func B64(text string) *string {
	encoded := base64.StdEncoding.EncodeToString([]byte(text))
	return &encoded
}

// Options configure Start.
type Options struct {
	// Root is the repository root (the directory that holds src/ and
	// scripts/).
	Root string
	// JWTKey is JWT_SECRET_KEY on the Python plane; the Go api must be given
	// the same key.
	JWTKey string
	// PythonEnv is extra environment for the Python plane, for example
	// EXPECTED_WORKER_GROUPS or TELEMETRY_ENDPOINT.
	PythonEnv []string
	// Seed fills the source database after the Alembic heads and before the
	// copy, as a superuser. It returns the tokens to mint: name ->
	// create_access_token keyword arguments (nil = none).
	Seed func(t *testing.T, ctx context.Context, admin *pgxpool.Pool) map[string]map[string]any
	// Logger receives the River migration logs (nil = discard).
	Logger *slog.Logger
}

// Venue is a built venue.
type Venue struct {
	Root   string
	Python string
	// Tokens are the minted access tokens, by seed name.
	Tokens map[string]string
	// Roles are the provisioned role names: domain, queue, coordinator, api.
	Roles map[string]string
	// SourceDB is the Python plane's database; GoDB is its copy.
	SourceDB, GoDB string
	// ValkeyURI is the Go plane's Valkey database; PythonValkeyURI is the
	// Python plane's, on the same server.
	ValkeyURI, PythonValkeyURI string

	postgresURI string
	pythonEnv   []string
}

// Start builds the venue; see the package comment. It skips unless
// DEV_HEALTH_LIVE_PYTHON_ORACLES=1. Everything it creates is removed by
// t.Cleanup.
func Start(t *testing.T, ctx context.Context, options Options) *Venue {
	t.Helper()
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the venue oracle needs the live Python api; run with DEV_HEALTH_LIVE_PYTHON_ORACLES=1")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	v := &Venue{Root: options.Root, Python: pyoracle.Resolve(t, options.Root), Tokens: map[string]string{}, Roles: map[string]string{}}

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	valkeyInstance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = valkeyInstance.Close(context.Background()) })
	v.postgresURI = instance.URI
	v.ValkeyURI = valkeyInstance.URI
	v.PythonValkeyURI = strings.TrimSuffix(valkeyInstance.URI, "/1") + "/2"
	if v.PythonValkeyURI == valkeyInstance.URI+"/2" {
		t.Fatalf("venue: Valkey URI %q does not name database 1", valkeyInstance.URI)
	}
	if v.SourceDB, err = containers.DatabaseName(instance.URI); err != nil {
		t.Fatal(err)
	}
	async := strings.Replace(v.AdminURI(t, v.SourceDB), "postgres://", "postgresql+asyncpg://", 1)
	async = strings.Replace(async, "postgresql://", "postgresql+asyncpg://", 1)
	v.pythonEnv = append([]string{"PYTHONPATH=" + filepath.Join(options.Root, "src"), "POSTGRES_URI=" + async,
		"JWT_SECRET_KEY=" + options.JWTKey, "OTEL_SDK_DISABLED=true", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1",
		// NullPool: TestClient gives each request its own event loop, and a
		// pooled asyncpg connection cannot cross loops.
		"PGBOUNCER_TRANSACTION_MODE=true", "ENVIRONMENT=test", "REDIS_URL=" + v.PythonValkeyURI, "CLICKHOUSE_URI="},
		options.PythonEnv...)

	// 1. The real schema, then one seed and its tokens.
	v.runPython(t, nil, "migrate")
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	var specs map[string]map[string]any
	if options.Seed != nil {
		specs = options.Seed(t, ctx, admin)
	}
	admin.Close()
	if len(specs) > 0 {
		if err := json.Unmarshal(v.runPython(t, specs, "mint"), &v.Tokens); err != nil {
			t.Fatal(err)
		}
	}

	// 2. Two identical copies: Python serves the source, Go serves the copy.
	v.GoDB = v.SourceDB + "_go"
	server, err := pgxpool.New(ctx, v.AdminURI(t, "postgres"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	if _, err := server.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q TEMPLATE %q`, v.GoDB, v.SourceDB)); err != nil {
		t.Fatal(err)
	}

	// 3. The api role, provisioned and granted exactly as a deploy does.
	for _, name := range []string{"domain", "queue", "coordinator", "api"} {
		role, err := containers.RoleName("venue_"+name, instance)
		if err != nil {
			t.Fatal(err)
		}
		v.Roles[name] = role
	}
	t.Cleanup(func() {
		for _, role := range v.Roles {
			containers.DropRole(server, role, t.Logf)
		}
	})
	v.provisionRoles(t, ctx)
	v.migrate(t, ctx, logger)
	return v
}

// AdminURI is a superuser DSN for database on the venue's Postgres.
func (v *Venue) AdminURI(t *testing.T, database string) string {
	return withDatabase(t, v.postgresURI, database, "", "")
}

// GoAPIDatabaseURI is API_DATABASE_URI for the Go api: the api role on the
// Go copy.
func (v *Venue) GoAPIDatabaseURI(t *testing.T) string {
	return withDatabase(t, v.postgresURI, v.GoDB, v.Roles["api"], APIPassword)
}

// DiagnoseAPIRole reports the api role's missing grants on the Go copy, for
// a readiness failure message.
func (v *Venue) DiagnoseAPIRole(t *testing.T, ctx context.Context) string {
	pool, err := pgxpool.New(ctx, v.GoAPIDatabaseURI(t))
	if err != nil {
		return err.Error()
	}
	defer pool.Close()
	gaps, err := postgres.DiagnoseRolePosture(ctx, pool, v.Roles["api"], postgres.APIPosture())
	return fmt.Sprintf("gaps=%v err=%v", gaps, err)
}

// ServePython answers requests with the Python plane, in order, in one
// process.
func (v *Venue) ServePython(t *testing.T, requests []Request) []Response {
	t.Helper()
	var out []Response
	if err := json.Unmarshal(v.runPython(t, requests, "serve"), &out); err != nil {
		t.Fatal(err)
	}
	if len(out) != len(requests) {
		t.Fatalf("python answered %d of %d requests", len(out), len(requests))
	}
	for index := range out {
		raw, err := base64.StdEncoding.DecodeString(out[index].Body)
		if err != nil {
			t.Fatal(err)
		}
		out[index].Body = string(raw)
	}
	return out
}

func (v *Venue) runPython(t *testing.T, stdin any, args ...string) []byte {
	t.Helper()
	command := exec.Command(v.Python, append([]string{"-c", pythonProgram}, args...)...)
	command.Env = append(os.Environ(), v.pythonEnv...)
	if stdin != nil {
		payload, err := json.Marshal(stdin)
		if err != nil {
			t.Fatal(err)
		}
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

func (v *Venue) provisionRoles(t *testing.T, ctx context.Context) {
	t.Helper()
	command := exec.CommandContext(ctx, "psql", v.AdminURI(t, v.GoDB), "--set=ON_ERROR_STOP=1",
		"--set=domain_role="+v.Roles["domain"], "--set=queue_role="+v.Roles["queue"],
		"--set=coordinator_role="+v.Roles["coordinator"], "--set=domain_password=venue_domain",
		"--set=queue_password=venue_queue", "--set=coordinator_password=venue_coordinator",
		"--set=api_role="+v.Roles["api"], "--set=api_password="+APIPassword,
		"--file="+filepath.Join(v.Root, "scripts", "worker", "provision_river_roles.sql"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("provision: %v\n%s", err, output)
	}
}

func (v *Venue) migrate(t *testing.T, ctx context.Context, logger *slog.Logger) {
	t.Helper()
	poolConfig, err := pgxpool.ParseConfig(v.AdminURI(t, v.GoDB))
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var grants []riverstore.TableGrant
	for _, table := range postgres.APIPosture().RequiredTables {
		grants = append(grants, riverstore.TableGrant{TableName: table.TableName, AllowInsert: table.AllowInsert,
			AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete})
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, pool, riverstore.MigrationOptions{
		Schema: "river", DomainRole: v.Roles["domain"], QueueRole: v.Roles["queue"],
		APIRole: v.Roles["api"], APIGrants: grants, Logger: logger,
	}); err != nil {
		t.Fatalf("River migration: %v", err)
	}
}

// Volatile are the per-response headers Diff never compares.
var Volatile = map[string]bool{"date": true, "server": true, "x-request-id": true}

// DiffOptions tune Diff for the ruled differences of a route set.
type DiffOptions struct {
	// Normalize blanks values that differ by ruling or by construction (a
	// random id, a build version). It runs on both bodies. When it changes
	// either body, content-length is not compared.
	Normalize func(request Request, body string) string
	// SkipContentLength reports requests whose content-length is not
	// compared for another ruled reason.
	SkipContentLength func(request Request) bool
	// Inspect sees each raw Go response before normalization, for checks of
	// a ruled value against another source.
	Inspect func(request Request, goResponse Response)
}

// Diff sends each request to the Go api at goBase, compares it with the
// matching Python response, reports each difference with t.Errorf, and
// returns the receipt: one "name python=S go=S SAME|DIFF" line per request.
func Diff(t *testing.T, goBase string, requests []Request, python []Response, options DiffOptions) string {
	t.Helper()
	if len(python) != len(requests) {
		t.Fatalf("diff: %d python responses for %d requests", len(python), len(requests))
	}
	var receipt strings.Builder
	for index, request := range requests {
		goResponse := Do(t, goBase, request)
		if options.Inspect != nil {
			options.Inspect(request, goResponse)
		}
		same, compared, pyShown, goShown := Compare(request, python[index], goResponse, options)
		fmt.Fprintf(&receipt, "%-58s python=%d go=%d %s\n", request.Name, python[index].Status, goResponse.Status, Mark(same))
		if !same {
			t.Errorf("%s:\n python %d %s %v\n go     %d %s %v", request.Name, python[index].Status, pyShown.Body,
				pick(pyShown.Headers, compared), goResponse.Status, goShown.Body, pick(goShown.Headers, compared))
		}
	}
	return receipt.String()
}

// Compare is Diff's decision for one request: statuses and normalized
// bodies equal, and every header outside Volatile (and content-length when
// a body was normalized) equal; Allow compares as a set, because a Python
// route keeps its methods in a set whose order follows the hash seed. It
// returns the compared header names and both normalized responses.
func Compare(request Request, python, goResponse Response, options DiffOptions) (bool, []string, Response, Response) {
	py, gr := clone(python), clone(goResponse)
	skip := Volatile
	if options.Normalize != nil {
		py.Body, gr.Body = options.Normalize(request, py.Body), options.Normalize(request, gr.Body)
	}
	if py.Body != python.Body || gr.Body != goResponse.Body || (options.SkipContentLength != nil && options.SkipContentLength(request)) {
		skip = map[string]bool{"content-length": true}
		for key := range Volatile {
			skip[key] = true
		}
	}
	for _, response := range []*Response{&py, &gr} {
		if allow, ok := response.Headers["allow"]; ok {
			response.Headers["allow"] = sortedAllow(allow)
		}
	}
	same := py.Status == gr.Status && py.Body == gr.Body
	compared := headerUnion(py.Headers, gr.Headers, skip)
	for _, header := range compared {
		pv, pok := py.Headers[header]
		gv, gok := gr.Headers[header]
		if pv != gv || pok != gok {
			same = false
		}
	}
	return same, compared, py, gr
}

// Mark is "SAME" or "DIFF".
func Mark(same bool) string {
	if same {
		return "SAME"
	}
	return "DIFF"
}

// Do sends one request to base and reads the whole answer.
func Do(t *testing.T, base string, request Request) Response {
	t.Helper()
	var body io.Reader
	if request.Body != nil {
		raw, err := base64.StdEncoding.DecodeString(*request.Body)
		if err != nil {
			t.Fatal(err)
		}
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
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	headers := map[string]string{}
	for key, values := range response.Header {
		headers[strings.ToLower(key)] = strings.Join(values, ", ")
	}
	return Response{Status: response.StatusCode, Headers: headers, Body: string(raw)}
}

// TableRows runs query on uri and renders every row, in query order, as
// "v1 v2 ... | ...".
func TableRows(t *testing.T, ctx context.Context, uri, query string) string {
	t.Helper()
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	rows, err := pool.Query(ctx, query)
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
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(lines, " | ")
}

// StreamEntries reads every Valkey stream whose key matches pattern as
// "key: field=value ..." (fields sorted, keys sorted, entries in stream
// order), with the fields named in blank replaced by "<blank>".
func StreamEntries(t *testing.T, ctx context.Context, uri, pattern string, blank ...string) string {
	t.Helper()
	options, err := valkeygo.ParseURL(uri)
	if err != nil {
		t.Fatal(err)
	}
	client, err := valkeygo.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	keys, err := client.Do(ctx, client.B().Keys().Pattern(pattern).Build()).AsStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(keys)
	blanked := map[string]bool{}
	for _, name := range blank {
		blanked[name] = true
	}
	var out []string
	for _, key := range keys {
		entries, err := client.Do(ctx, client.B().Xrange().Key(key).Start("-").End("+").Build()).AsXRange()
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			fields := []string{}
			for name, value := range entry.FieldValues {
				if blanked[name] {
					value = "<blank>"
				}
				fields = append(fields, name+"="+value)
			}
			sort.Strings(fields)
			out = append(out, key+": "+strings.Join(fields, " "))
		}
	}
	return strings.Join(out, " | ")
}

func clone(response Response) Response {
	headers := make(map[string]string, len(response.Headers))
	for key, value := range response.Headers {
		headers[key] = value
	}
	response.Headers = headers
	return response
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

func sortedAllow(value string) string {
	if value == "" {
		return ""
	}
	methods := strings.Split(value, ", ")
	sort.Strings(methods)
	return strings.Join(methods, ", ")
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

func tail(text string) string {
	if len(text) > 4000 {
		return text[len(text)-4000:]
	}
	return text
}
