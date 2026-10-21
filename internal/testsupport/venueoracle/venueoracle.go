// Package venueoracle is the venue differential oracle for `dho api` routes
// ported from the Python api. The Python api is gone: what the REAL Python app
// answered (dev_health_ops.api.main:app, every middleware and handler, driven
// by TestClient) was executed once on the last Python-bearing build and frozen
// in golden files (Golden). The REAL Go api answers the same requests against a
// copy of one Postgres database that one seed filled, and Diff compares its
// responses with the frozen ones byte for byte on status, body and every
// header except the per-response ones. After the writes, the caller compares
// the rows and stream entries they touched with TableRows and StreamEntries.
//
// Start builds the venue: Postgres, Valkey and ClickHouse containers, the
// schemas (pgmigrate and chmigrate), the caller's seed, tokens minted by the
// Go minter, a CREATE DATABASE ... TEMPLATE copy for Go, and the api role
// provisioned and granted on that copy as a deploy does
// (provision_river_roles.sql, then the River migration with
// postgres.APIPosture). The caller starts the Go api on
// Venue.GoAPIDatabaseURI and passes its base URL to Diff. ClickHouse has two
// isolated databases (CHAOS-6310), and a dedicated login on the Go one,
// granted exactly clickhouse.APIPosture()'s manifest via
// clickhouse.GrantStatements; Venue.GoAPIClickHouseURI is that login's DSN.
//
// Start skips unless DEV_HEALTH_VENUE_ORACLES=1, and ci/check_go.sh
// venue-oracles sets it. A venue runs no Python: it is built for a frozen
// golden or for a Go-only test (Options.Golden, Options.GoOnly).
package venueoracle

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/api/adminroutes"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pydanticecho"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

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
	// slot identifies the answer a Golden handed out (0 for any other response):
	// Diff and Consumed use it to bind each answer to the request it belongs to.
	slot int
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
	// PythonEnv is the Python plane's settings the golden was recorded under,
	// for example SETTINGS_ENCRYPTION_KEY: the Go ports of the substrate
	// (gosubstrate.go) read them.
	PythonEnv []string
	// Seed fills the source database after the schema and before the copy, as
	// a superuser. It may call the Go ports of Python functions through
	// venue.CallPython, for example to write a value the way the Python api
	// encrypted it. It returns the tokens to mint: name -> create_access_token
	// keyword arguments (nil = none).
	Seed func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *Venue) map[string]map[string]any
	// Logger receives the River migration logs (nil = discard).
	Logger *slog.Logger
	// Golden is the test's golden, when it has one: the Python plane's frozen
	// answers.
	Golden *Golden
	// GoOnly is a test that compares nothing with the Python plane (it
	// writes the Go-only proof, WriteGoOnlyProof).
	GoOnly bool
	// External, when set, is the Go side of the venue: a target that already runs, with its own stores.
	External *ExternalStores
}

// Venue is a built venue.
type Venue struct {
	Root string
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
	// pythonEnv is the Python plane's settings as the Go ports read them (planeEnv).
	pythonEnv []string

	// ClickHouse (CHAOS-6310): one server, two databases. PythonClickHouseDB
	// is what CLICKHOUSE_URI (Python's env) named; GoClickHouseDB is what
	// the dedicated api login is scoped to.
	clickHouseURI, clickHouseHTTPURI     string
	PythonClickHouseDB, GoClickHouseDB   string
	clickHouseAPIRole, clickHouseAPIPass string

	// frozen is a venue built for a frozen golden or a Go-only test. Every
	// venue is one: no Python plane can be served.
	frozen bool
}

// Start builds the venue; see the package comment. It skips unless
// DEV_HEALTH_VENUE_ORACLES=1. Everything it creates is removed by
// t.Cleanup. A venue needs a golden (Options.Golden) or is Go-only
// (Options.GoOnly): no Python plane can be served.
//
// Start itself writes no proof file: it only proves the venue built, not
// that the caller went on to compare anything against it, and a test that
// calls Start and returns (no Diff, no failure, no skip) would otherwise
// still satisfy a proof-file check while never running a real comparison.
// Diff writes the proof instead, once it has actually sent requests to
// the Go plane and compared them with the frozen answers -- see its doc comment.
func Start(t *testing.T, ctx context.Context, options Options) *Venue {
	t.Helper()
	if os.Getenv("DEV_HEALTH_VENUE_ORACLES") != "1" {
		t.Skip("the venue oracle runs only through ci/check_go.sh venue-oracles; run with DEV_HEALTH_VENUE_ORACLES=1")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if options.GoOnly && options.Golden != nil {
		t.Fatal("venue: a GoOnly venue has no golden: a golden holds the Python plane's answers, which a Go-only test never compares")
	}
	if !options.GoOnly && options.Golden == nil {
		t.Fatal("venue: a venue needs its golden (Options.Golden) or Options.GoOnly: no Python plane can be served")
	}
	v := &Venue{Root: options.Root, Tokens: map[string]string{}, Roles: map[string]string{}, frozen: true, pythonEnv: planeEnv(options)}
	if options.External != nil {
		return startExternal(t, ctx, options, v)
	}

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
	v.GoDB = v.SourceDB + "_go"

	// ClickHouse (CHAOS-6310): one server, two isolated databases, named
	// after the Postgres ones above for the same "one identical copy per
	// plane" shape (ClickHouse has no CREATE DATABASE ... TEMPLATE, so
	// this is two migration runs, not a copy).
	chInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chInstance.Close(context.Background()) })
	v.clickHouseURI = chInstance.URI
	chHTTPURI, err := containers.ClickHouseHTTPDSN(ctx, chInstance)
	if err != nil {
		t.Fatal(err)
	}
	v.clickHouseHTTPURI = chHTTPURI
	v.PythonClickHouseDB, v.GoClickHouseDB = v.SourceDB, v.GoDB
	chAdmin, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(chInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chAdmin.Close() })
	for _, database := range []string{v.PythonClickHouseDB, v.GoClickHouseDB} {
		// IF NOT EXISTS: v.SourceDB can collide with containers.ClickHouseDatabase
		// ("worker_test"), which StartClickHouse's own CLICKHOUSE_DB env var
		// already pre-creates on every fresh container -- confirmed live.
		if err := chAdmin.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+database); err != nil {
			t.Fatal(err)
		}
	}
	for _, database := range []string{v.PythonClickHouseDB, v.GoClickHouseDB} {
		v.migrateClickHouseGo(t, ctx, database)
	}
	chRole, err := containers.RoleName("venue_ch_api", instance)
	if err != nil {
		t.Fatal(err)
	}
	v.clickHouseAPIRole, v.clickHouseAPIPass = chRole, "venue_ch_api_password"
	if err := chAdmin.Exec(ctx, fmt.Sprintf(
		"CREATE USER %s IDENTIFIED WITH plaintext_password BY '%s'", v.clickHouseAPIRole, v.clickHouseAPIPass)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chAdmin.Exec(context.Background(), "DROP USER IF EXISTS "+v.clickHouseAPIRole) })
	for _, statement := range chclickhouse.GrantStatements(v.clickHouseAPIRole, chclickhouse.APIPosture(v.GoClickHouseDB)) {
		if err := chAdmin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	// 1. The schema, then one seed and its tokens.
	v.migratePostgresGo(t, ctx)
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	var specs map[string]map[string]any
	if options.Seed != nil {
		specs = options.Seed(t, ctx, admin, v)
	}
	admin.Close()
	if len(specs) > 0 {
		v.Tokens = v.mintGo(t, options.JWTKey, specs)
	}

	// 2. Two identical copies: Python serves the source, Go serves the copy.
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

// AdminClickHouseURI is the ClickHouse server's admin connection (native
// protocol), scoped to database.
func (v *Venue) AdminClickHouseURI(t *testing.T, database string) string {
	return withDatabase(t, v.clickHouseURI, database, "", "")
}

// AdminClickHouseHTTPURI is AdminClickHouseURI's HTTP-port sibling, for a
// Python-side caller (clickhouse_connect is HTTP-only).
func (v *Venue) AdminClickHouseHTTPURI(t *testing.T, database string) string {
	return withDatabase(t, v.clickHouseHTTPURI, database, "", "")
}

// GoAPIClickHouseURI is API_CLICKHOUSE_URI for the Go api: the dedicated
// login (granted exactly clickhouse.APIPosture()'s manifest) on the
// GoClickHouseDB copy.
func (v *Venue) GoAPIClickHouseURI(t *testing.T) string {
	return withDatabase(t, v.clickHouseURI, v.GoClickHouseDB, v.clickHouseAPIRole, v.clickHouseAPIPass)
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

// PythonCall is one function call for CallPython: Target is
// "module:attribute" (attribute may be dotted), and the arguments and the
// result are JSON values.
type PythonCall struct {
	Target string         `json:"target"`
	Args   []any          `json:"args,omitempty"`
	Kwargs map[string]any `json:"kwargs,omitempty"`
}

// CallPython answers calls to Python functions with their Go ports
// (goCallPorts), each result as raw JSON, in order, and fails naming a target
// that has none.
func (v *Venue) CallPython(t *testing.T, calls ...PythonCall) []json.RawMessage {
	t.Helper()
	return v.callGo(t, calls)
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
	var columnGrants []riverstore.ColumnGrant
	for _, column := range postgres.APIPosture().ColumnScoped {
		columnGrants = append(columnGrants, riverstore.ColumnGrant{TableName: column.TableName, ColumnName: column.ColumnName, Privilege: column.Privilege})
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, pool, riverstore.MigrationOptions{
		Schema: "river", DomainRole: v.Roles["domain"], QueueRole: v.Roles["queue"],
		APIRole: v.Roles["api"], APIGrants: grants, APIColumnGrants: columnGrants, Logger: logger,
	}); err != nil {
		t.Fatalf("River migration: %v", err)
	}
}

// Volatile are the per-response headers Diff never compares. They are
// transport fingerprints, not route contract: x-request-id is a random id
// per request on both planes, and date and server are added by uvicorn's
// server layer (src/dev_health_ops/api/runner.py calls uvicorn.run with its
// default date_header and server_header), which TestClient never reaches,
// so the Python plane here cannot show them. In production, Cloudflare in
// front of both planes sets its own Server header. x-dev-health-plane and
// x-dev-health-build are the Go api's own provenance stamp, which the REST
// prover reads to bind a receipt to a build; the Python api carries neither,
// so they can never agree across planes. The Go side of them is asserted by
// the api's own tests (internal/apiservice, internal/api/buildinfo).
var Volatile = map[string]bool{"date": true, "server": true, "x-request-id": true, "x-dev-health-plane": true, "x-dev-health-build": true}

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
	// Golden, when set, says the Python answers Diff compares against come
	// from a frozen golden, not a live Python plane: Diff then writes no
	// both-planes proof (Golden.Finish writes the Go-only one).
	Golden *Golden // Retire, with Golden and Pin, reports the requests whose Python
	// reference Pin's ruling retired: Diff still sends each to the Go plane in
	// its place in the sequence, checks Go's normalized answer against Pin,
	// and declares the Python answer retired (Golden.Retired), uncompared.
	Retire func(request Request) bool
	Pin    *GoPin
}

// Diff sends each request to the Go api at goBase, compares it with the
// matching Python response, reports each difference with t.Errorf, and
// returns the receipt: one "name python=S go=S SAME|DIFF" line per request.
//
// On completion (whatever the verdict -- DIFF fails the test via t.Errorf
// above, same as ever) it writes this test's proof file (see writeProof):
// Diff is where a real request actually goes to both planes and gets
// compared, so reaching the end of it is genuine evidence a comparison ran,
// unlike Start returning (which only proves the venue was built). A test
// that calls Start and never calls Diff gets no proof file and fails the
// CI job's check loudly, rather than reading as a pass built on nothing.
func Diff(t *testing.T, goBase string, requests []Request, python []Response, options DiffOptions) string {
	t.Helper()
	if len(python) != len(requests) {
		t.Fatalf("diff: %d python responses for %d requests", len(python), len(requests))
	}
	if options.Golden != nil {
		options.Golden.beforeDiff(t)
		if err := options.Golden.bindAnswers(requests, python); err != nil {
			t.Fatal(err)
		}
	}
	if (options.Retire != nil) != (options.Pin != nil) || (options.Retire != nil && options.Golden == nil) {
		t.Fatal("diff: Retire needs a Pin and a Golden")
	}
	var receipt strings.Builder
	for index, request := range requests {
		goResponse := Do(t, goBase, request)
		if options.Inspect != nil {
			options.Inspect(request, goResponse)
		}
		pythonResponse, compareOptions := python[index], options
		if options.Golden != nil {
			// A golden stores no token value: both planes are compared as
			// projected, and a projected body has another length.
			projectedPython, err := options.Golden.projectResponse(pythonResponse)
			if err != nil {
				t.Fatal(err)
			}
			projectedGo, err := options.Golden.projectResponseAt(request.Name, goResponse)
			if err != nil {
				t.Fatal(err)
			}
			if projectedPython.Body != pythonResponse.Body || projectedGo.Body != goResponse.Body {
				compareOptions.SkipContentLength = func(r Request) bool { return true }
			}
			for _, projected := range []Response{projectedPython, projectedGo} {
				if err := undecodableErr(request, projected); err != nil {
					t.Fatal(err)
				}
			}
			pythonResponse, goResponse = projectedPython, projectedGo
		}
		if options.Retire != nil && options.Retire(request) {
			pinned := goResponse
			if options.Normalize != nil {
				pinned.Body = options.Normalize(request, pinned.Body)
			}
			options.Golden.retireBound(python[index], options.Pin.spec.Ruling)
			same := options.Pin.Check(t, request.Name, PinText(pinned))
			fmt.Fprintf(&receipt, "%-58s retired     go=%d %s\n", request.Name, goResponse.Status, pinMark(same))
			continue
		}
		same, compared, pyShown, goShown := Compare(request, pythonResponse, goResponse, compareOptions)
		fmt.Fprintf(&receipt, "%-58s python=%d go=%d %s\n", request.Name, python[index].Status, goResponse.Status, Mark(same))
		if !same {
			t.Errorf("%s:\n python %d %s %v\n go     %d %s %v", request.Name, python[index].Status, pyShown.Body,
				pick(pyShown.Headers, compared), goResponse.Status, goShown.Body, pick(goShown.Headers, compared))
		}
	}
	// Every Go handler must have written each body with the writer its
	// route's ResponseModel flag allows (policy.WriteModel for a FastAPI
	// response_model success body, WriteJSON otherwise). The count is
	// process-wide, so any violation in this package's venues fails here.
	if violations := policy.WriterViolations(); violations > 0 {
		t.Errorf("%d body writes used the wrong writer for their route (see the policy ERROR logs naming each route)", violations)
	}
	if options.Golden == nil {
		writeProof(t)
	} else {
		options.Golden.afterDiff()
	}
	return receipt.String()
}

// pinMark is a retired request's receipt mark.
func pinMark(same bool) string {
	if same {
		return "PINNED"
	}
	return "PIN-DIFF"
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
	// The Go api never writes a validation error's input or ctx (FastAPI's echo
	// of the submitted value, which can be a secret): the Python body is
	// compared without them, the Go body as it is, so a Go echo is a DIFF. A
	// Python 500 whose cause is rendering that echo meets a Go 4xx that carries
	// none.
	pyBody := py.Body
	py.Body = pydanticecho.Strip(py.Body)
	if py.Body != pyBody {
		skip = map[string]bool{"content-length": true}
		for key := range Volatile {
			skip[key] = true
		}
	}
	same := py.Status == gr.Status && py.Body == gr.Body
	if py.Status == http.StatusInternalServerError && pydanticecho.GenericPython500(pyBody) && (gr.Status == http.StatusUnprocessableEntity || gr.Status == http.StatusBadRequest) &&
		strings.Contains(gr.Body, `"loc"`) && pydanticecho.Strip(gr.Body) == gr.Body {
		// Python's 500 comes from its server-error middleware, outside the
		// middleware that adds the security headers, so no header is compared.
		return true, nil, py, gr
	}
	if intendedNoStore(request, py, gr) {
		skip = withSkipped(skip, "cache-control")
	}
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

// intendedNoStore is the declared divergence "admin no-store": every Go admin
// route answers Cache-Control: no-store, which the frozen Python goldens never
// carried. It applies only to a route of the admin route table
// (adminroutes.Table), only when the golden has no cache-control, and only for
// exactly "no-store"; any other Go value, a golden that has the header, or an
// admin-prefixed route of another package still compares.
func intendedNoStore(request Request, py, gr Response) bool {
	target, err := url.ParseRequestURI(request.Path)
	if err != nil || !adminroutes.Matches(&http.Request{Method: request.Method, URL: target}) {
		return false
	}
	if _, has := py.Headers["cache-control"]; has {
		return false
	}
	return gr.Headers["cache-control"] == "no-store"
}

func withSkipped(skip map[string]bool, name string) map[string]bool {
	out := make(map[string]bool, len(skip)+1)
	for key, value := range skip {
		out[key] = value
	}
	out[name] = true
	return out
}

// Mark is "SAME" or "DIFF".
func Mark(same bool) string {
	if same {
		return "SAME"
	}
	return "DIFF"
}

// noRedirects answers with the response itself, a redirect included, as the
// Python plane's TestClient(follow_redirects=False) does.
var noRedirects = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

// Do sends one request to base and reads the whole answer; a redirect is
// returned, not followed.
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
	// TestClient sends Host: testserver unless the request names one; net/http
	// takes the Host from req.Host, never from a Host header.
	httpRequest.Host = "testserver"
	for key, value := range request.Headers {
		if strings.EqualFold(key, "Host") {
			httpRequest.Host = value
			continue
		}
		httpRequest.Header.Set(key, value)
	}
	response, err := noRedirects.Do(httpRequest)
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

// RowsReporter is the part of *testing.T TableRows reports through.
type RowsReporter interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

// jsonTypeOIDs are Postgres's json, jsonb and their array types.
var jsonTypeOIDs = map[uint32]string{114: "json", 3802: "jsonb", 199: "json[]", 3807: "jsonb[]"}

// decodedJSONColumns names each result column TableRows would decode as
// JSON. A decoded value renders through fmt.Sprint, which hides spacing,
// escaping (ensure_ascii) and key order (it sorts map keys), so two
// planes that store different JSON text would compare equal. A venue
// compares stored JSON as raw text: the query casts the column ::text.
func decodedJSONColumns(fields []pgconn.FieldDescription) []string {
	var named []string
	for _, field := range fields {
		if kind, ok := jsonTypeOIDs[field.DataTypeOID]; ok {
			named = append(named, field.Name+" ("+kind+")")
		}
	}
	return named
}

// OpenPool opens a pool on uri for a test and closes it at cleanup.
func OpenPool(t *testing.T, ctx context.Context, uri string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TableRows runs query on uri and renders every row, in query order, as
// "v1 v2 ... | ...". A json or jsonb result column fails the test: cast it
// ::text so the stored text is compared (decodedJSONColumns).
func TableRows(t RowsReporter, ctx context.Context, uri, query string) string {
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
	if named := decodedJSONColumns(rows.FieldDescriptions()); len(named) > 0 {
		t.Fatalf("venueoracle.TableRows: cast %s to text; a venue compares stored JSON as raw text, not decoded: %s",
			strings.Join(named, ", "), query)
	}
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

// CHRows is TableRows' ClickHouse sibling: opens its own connection, runs
// query, and renders every row the same "space-joined values, rows joined
// by | " shape TableRows does, so a caller's comparison code (e.g.
// compareRows in internal/apiservice's own venue oracle test) treats both
// planes identically regardless of which database engine backs them.
// Column values are read generically via each column's own ScanType
// (reflection), since driver.Rows has no pgx-style Values() -- a caller
// query must select FINAL and order deterministically, exactly as the
// Python readers this compares against do, or two otherwise-identical
// ReplacingMergeTree states can render in a different row order.
func CHRows(t *testing.T, ctx context.Context, uri, query string) string {
	t.Helper()
	conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columnTypes := rows.ColumnTypes()
	var lines []string
	for rows.Next() {
		dests := make([]any, len(columnTypes))
		for index, columnType := range columnTypes {
			dests[index] = reflect.New(columnType.ScanType()).Interface()
		}
		if err := rows.Scan(dests...); err != nil {
			t.Fatal(err)
		}
		values := make([]string, len(dests))
		for index, dest := range dests {
			values[index] = fmt.Sprint(reflect.ValueOf(dest).Elem().Interface())
		}
		lines = append(lines, strings.Join(values, " "))
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

// writeProof marks that THIS test genuinely compared a real request against
// the live Python api -- one file per test name, so a CI job naming every
// expected venue-oracle test by name can fail loudly when one is missing
// (a t.Skip before Start, a t.Fatal along the way, or a test that built a
// venue and never actually called Diff), rather than reporting a pass built
// on nothing. Called from Diff, not Start -- see Diff's doc comment for why.
// DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is optional: unset (the common
// local case) writes nothing and is not an error.
func writeProof(t *testing.T) { writeProofText(t, "executed") }

func writeProofText(t *testing.T, text string) {
	t.Helper()
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		return
	}
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	if err := os.WriteFile(filepath.Join(proofDir, name), []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

// WriteProof is writeProof, exported for a venue-oracle test whose
// comparison shape does not fit Diff (e.g. comparing stored digest VALUES
// across planes, or planting a row cross-plane and retrying) and so calls
// ServePython/Do/TableRows directly instead. Such a test must call this
// itself once its real, both-planes comparison has actually run; a test
// that compares no Python response calls WriteGoOnlyProof instead. The
// venue-oracles verb in ci/check_go.sh fails loudly when a test it
// discovered leaves no proof file.
func WriteProof(t *testing.T) { writeProof(t) }

// WriteGoOnlyProof marks that THIS test ran its measurement to the end in
// the venue but compared no Python response: it checks Go against a
// recorded Python truth or a property of the venue itself. reason says
// what it measures. The venue-oracles verb accepts this proof, counts it
// apart from the both-planes proofs and names the test and reason in its
// summary, so a Go-only check never reads as a parity comparison.
func WriteGoOnlyProof(t *testing.T, reason string) {
	t.Helper()
	if strings.TrimSpace(reason) == "" || strings.ContainsAny(reason, "\n\r") {
		t.Fatal("WriteGoOnlyProof needs a one-line reason naming what the test measures instead of a Python response")
	}
	writeProofText(t, "go-only: "+reason)
}
