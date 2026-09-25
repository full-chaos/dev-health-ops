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
// A ClickHouse container is started too (CHAOS-6310), with two isolated
// databases -- Python's own CLICKHOUSE_URI is pointed at one (unset by
// default otherwise, so a route touching ClickHouse would 500 without
// this), and a dedicated login is provisioned on the other, granted
// exactly clickhouse.APIPosture()'s manifest via
// clickhouse.GrantStatements -- the same statements a deploy's grant
// recipe runs, so the two can never drift. Venue.GoAPIClickHouseURI is
// that login's DSN, for a route area's Deps.ClickHouse.
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
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// pythonProgram has four modes: "migrate" runs the Alembic heads, "mint"
// mints access tokens with the real AuthService (stdin: name -> keyword
// arguments of create_access_token), "call" runs Python functions (stdin: a
// list of {"target": "module:attribute", "args": [...], "kwargs": {...}};
// stdout: their JSON results, in order -- CHAOS-6247's PagerDuty binding
// secret is seeded through this mode, calling core.encryption.encrypt_value,
// so the ciphertext is a genuine cross-language artifact rather than a
// Go-side round-trip), and "serve" answers a batch of requests with
// TestClient over the real app.
const pythonProgram = `
import base64, json, os, sys
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
elif mode == "call":
    import importlib
    out = []
    for call in json.loads(sys.stdin.read()):
        module, _, attribute = call["target"].partition(":")
        target = importlib.import_module(module)
        for part in attribute.split("."):
            target = getattr(target, part)
        out.append(target(*call.get("args", []), **call.get("kwargs", {})))
    print(json.dumps(out))
elif mode == "serve":
    # Test-runner-only monkeypatch (never a production src/ edit, CHAOS-6306
    # condition 2): when set, points PagerDutyOAuthConfig.from_env()'s
    # revoke_url at a fake endpoint, so a venue test can prove the Go and
    # Python planes revoke against the SAME fake server without the
    # production PagerDutyOAuthConfig ever gaining a revoke_url env knob.
    _pd_overrides = {}
    _pd_revoke_override = os.environ.get("VENUE_PAGERDUTY_REVOKE_URL_OVERRIDE")
    if _pd_revoke_override:
        _pd_overrides["revoke_url"] = _pd_revoke_override
    # Same rule, for the code exchange and the client-credentials token
    # request (PagerDutyOAuthConfig.token_url and credential_validation's
    # own _TOKEN_URL) and for the regional REST base the live validation
    # reads (credential_validation.pagerduty_base_url): a venue test points
    # both planes at one fake PagerDuty, and the Go plane's
    # PagerDutyRevokeConfig.TokenURL/APIBaseOverride are its counterparts.
    _pd_token_override = os.environ.get("VENUE_PAGERDUTY_TOKEN_URL_OVERRIDE")
    if _pd_token_override:
        _pd_overrides["token_url"] = _pd_token_override
    if _pd_overrides:
        import dataclasses
        from dev_health_ops.providers.pagerduty import oauth as _pd_oauth
        _pd_original_from_env = _pd_oauth.PagerDutyOAuthConfig.from_env.__func__

        def _pd_patched_from_env(cls):
            config = _pd_original_from_env(cls)
            if config is None:
                return config
            return dataclasses.replace(config, **_pd_overrides)

        _pd_oauth.PagerDutyOAuthConfig.from_env = classmethod(_pd_patched_from_env)
    if _pd_token_override or os.environ.get("VENUE_PAGERDUTY_API_BASE_OVERRIDE"):
        from dev_health_ops.providers.pagerduty import credential_validation as _pd_cv
        if _pd_token_override:
            _pd_cv._TOKEN_URL = _pd_token_override
        _pd_api_override = os.environ.get("VENUE_PAGERDUTY_API_BASE_OVERRIDE")
        if _pd_api_override:
            _pd_cv.pagerduty_base_url = lambda *, region: _pd_api_override + "/" + region
            # The services route builds its client through providers/
            # pagerduty/client.py, which reads its own imported copy.
            from dev_health_ops.providers.pagerduty import client as _pd_client
            _pd_client.pagerduty_base_url = lambda *, region: _pd_api_override + "/" + region
        if _pd_token_override:
            # The services route builds its client-credentials OAuth config
            # directly (not from the environment), so its token request is
            # redirected on the router's own imported function.
            from dev_health_ops.api.admin.routers import pagerduty_services as _pd_services
            _pd_services_client_credentials = _pd_services.client_credentials

            async def _pd_patched_client_credentials(config, **kwargs):
                return await _pd_services_client_credentials(
                    dataclasses.replace(config, token_url=_pd_token_override), **kwargs
                )

            _pd_services.client_credentials = _pd_patched_client_credentials
    # Test-runner-only monkeypatch, same rule as above: when set, every
    # StripeClient the app builds talks to this base instead of Stripe, so
    # a venue test can record what each plane asks of Stripe on one fake
    # server. The production get_stripe_client gains no knob.
    _stripe_api_base = os.environ.get("VENUE_STRIPE_API_BASE")
    if _stripe_api_base:
        import stripe as _stripe
        _stripe_original_init = _stripe.StripeClient.__init__

        def _stripe_patched_init(self, *args, **kwargs):
            kwargs["base_addresses"] = {"api": _stripe_api_base}
            _stripe_original_init(self, *args, **kwargs)

        _stripe.StripeClient.__init__ = _stripe_patched_init
    # VENUE_STRIPE_LIST_KWARGS=1 lets the v1 subscription, invoice and
    # refund services take list(limit=...) as keywords, the call the
    # reconciliation service makes (and stripe-python refuses with a
    # TypeError). The Python plane then runs the reconciliation it was
    # written to run, which is what the Go port implements.
    if os.environ.get("VENUE_STRIPE_LIST_KWARGS") == "1":
        from stripe._invoice_service import InvoiceService as _InvoiceService
        from stripe._refund_service import RefundService as _RefundService
        from stripe._subscription_service import SubscriptionService as _SubscriptionService

        def _stripe_list_kwargs(original):
            def patched(self, params=None, options=None, **keywords):
                if keywords:
                    params = {**(params or {}), **keywords}
                return original(self, params, options)
            return patched

        for _service in (_SubscriptionService, _InvoiceService, _RefundService):
            _service.list = _stripe_list_kwargs(_service.list)
    # Test-runner-only monkeypatch, same rule as above: when set, every
    # httpx call the social-login providers make to GitHub, GitLab or
    # Google goes to a fake provider instead (the provider URLs are
    # hard-coded in api/services/oauth.py, the GitHub emails URL inside a
    # method), so both planes read the SAME fake profile.
    _oauth_override = os.environ.get("VENUE_OAUTH_PROVIDER_BASE_URL")
    if _oauth_override:
        import httpx
        from dev_health_ops.api.services import oauth as _oauth
        _hosts = {"https://api.github.com": "/github", "https://gitlab.com": "/gitlab", "https://www.googleapis.com": "/google"}

        class _RedirectedClient(httpx.AsyncClient):
            async def get(self, url, *args, **kwargs):
                for host, prefix in _hosts.items():
                    if str(url).startswith(host):
                        url = _oauth_override + prefix + str(url)[len(host):]
                return await super().get(url, *args, **kwargs)

        _oauth.httpx = type("httpx_shim", (), {"AsyncClient": _RedirectedClient, "HTTPStatusError": httpx.HTTPStatusError,
                                               "RequestError": httpx.RequestError, "Response": httpx.Response})
    # VENUE_STRIPE_SESSION_LINE_ITEMS=1 gives the checkout session service
    # the list_line_items(session_id) the webhook's checkout handler calls
    # (stripe-python's is sessions.line_items.list), so the handler reads the
    # line items it was written to read instead of failing to TEAM.
    if os.environ.get("VENUE_STRIPE_SESSION_LINE_ITEMS") == "1":
        from stripe.checkout._session_service import SessionService as _CheckoutSessionService

        def _list_line_items(self, session, params=None, options=None):
            return self.line_items.list(session, params, options)

        _CheckoutSessionService.list_line_items = _list_line_items
    # VENUE_STRIPE_EVENT_AS_DICT names the event types (comma separated,
    # or 1 for every type) whose verified event reaches the webhook
    # handlers as plain JSON dicts with attribute access, which is what
    # those handlers were written against; stripe-python's StripeObject is
    # not a dict, so the unpatched handlers crash or skip. Other types keep
    # the deployed StripeObject. The signature is still verified by the
    # real stripe-python code first.
    _event_as_dict = os.environ.get("VENUE_STRIPE_EVENT_AS_DICT", "")
    if _event_as_dict:
        import json as _json
        import stripe as _stripe_ev

        class _EventDict(dict):
            def __getattr__(self, name):
                try:
                    return self[name]
                except KeyError:
                    raise AttributeError(name) from None

        def _as_event_dict(value):
            if isinstance(value, dict):
                return _EventDict((k, _as_event_dict(v)) for k, v in value.items())
            if isinstance(value, list):
                return [_as_event_dict(v) for v in value]
            return value

        _stripe_original_construct = _stripe_ev.StripeClient.construct_event

        def _stripe_construct_as_dict(self, payload, sig_header, secret, *args, **kwargs):
            event = _stripe_original_construct(self, payload, sig_header, secret, *args, **kwargs)
            if _event_as_dict != "1" and event.type not in _event_as_dict.split(","):
                return event
            return _as_event_dict(_json.loads(payload))

        _stripe_ev.StripeClient.construct_event = _stripe_construct_as_dict
    # Test-runner-only monkeypatch, same rule as above: when set, every
    # datetime.now()/utcnow() the listed modules call reads the pinned
    # instant instead of the wall clock (each module's own global name
    # 'datetime' is replaced by a subclass whose now() is pinned), so a
    # venue test can compare Python-written timestamps as raw values
    # against a Go plane run on the same injected clock.
    _pinned_now = os.environ.get("VENUE_PINNED_NOW")
    if _pinned_now:
        import datetime as _dt, importlib as _importlib
        _pin = _dt.datetime.fromisoformat(_pinned_now)

        class _PinnedDatetime(_dt.datetime):
            @classmethod
            def now(cls, tz=None):
                return _pin.astimezone(tz) if tz is not None else _pin.replace(tzinfo=None)

            @classmethod
            def utcnow(cls):
                return _pin.astimezone(_dt.timezone.utc).replace(tzinfo=None)

        for _module_name in os.environ.get("VENUE_PINNED_NOW_MODULES", "").split(","):
            if _module_name:
                setattr(_importlib.import_module(_module_name), "datetime", _PinnedDatetime)
        # Same rule for a module that reads the clock through its own
        # global name 'time' (croniter's "from time import time"): the
        # listed modules' time() returns the pinned instant's epoch.
        for _module_name in os.environ.get("VENUE_PINNED_TIME_MODULES", "").split(","):
            if _module_name:
                setattr(_importlib.import_module(_module_name), "time", lambda: _pin.timestamp())
    from fastapi.testclient import TestClient
    from dev_health_ops.api.main import app
    # VENUE_STRIPE_SUBSCRIPTION_HANDLERS_AS_DICT=1 hands the router's
    # subscription updated/deleted/trial_will_end handlers the subscription
    # as what they were written against: a dict (isinstance, .get) whose
    # fields are also attributes, a field winning over a dict method of the
    # same name (subscription.items is the Stripe items list, not
    # dict.items). On the deployed StripeObject, metadata is not a dict and
    # those handlers skip every event. SubscriptionService.process_event
    # keeps the deployed StripeObject, which it reads correctly.
    if os.environ.get("VENUE_STRIPE_SUBSCRIPTION_HANDLERS_AS_DICT") == "1":
        import importlib as _importlib

        # sys.modules, not attribute access: the billing package exports an
        # APIRouter named router that shadows the module.
        _importlib.import_module("dev_health_ops.api.billing.router")
        _billing_router = sys.modules["dev_health_ops.api.billing.router"]

        class _FieldDict(dict):
            # A field is an attribute; an absent field is getattr's default
            # (not a dict method of the same name: a subscription without
            # items has no items). .get stays, for metadata.get.
            def __getattribute__(self, name):
                if name.startswith("__"):
                    return dict.__getattribute__(self, name)
                if dict.__contains__(self, name):
                    return dict.__getitem__(self, name)
                if name == "get":
                    return dict.__getattribute__(self, name)
                raise AttributeError(name)

        def _as_field_dict(value):
            if isinstance(value, dict):
                return _FieldDict((k, _as_field_dict(v)) for k, v in value.items())
            if isinstance(value, list):
                return [_as_field_dict(v) for v in value]
            return value

        def _field_dict_handler(original):
            async def handler(subscription):
                return await original(_as_field_dict(subscription.to_dict()))
            return handler

        for _name in ("_handle_subscription_updated", "_handle_subscription_deleted", "_handle_trial_will_end"):
            setattr(_billing_router, _name, _field_dict_handler(getattr(_billing_router, _name)))
    # VENUE_PY_LOGGING=1 sends the app's log records (INFO and up) to stderr,
    # so a handled failure (a logged, swallowed exception) shows its cause.
    if os.environ.get("VENUE_PY_LOGGING") == "1":
        import logging as _logging
        _logging.basicConfig(stream=sys.stderr, level=_logging.INFO, force=True,
                             format="VENUE LOG %(levelname)s %(name)s: %(message)s")
    # VENUE_PY_TRACEBACKS=1 writes each unhandled exception's traceback to
    # stderr (DEV_HEALTH_VENUE_PY_LOG keeps it), so a 500 carries its cause.
    if os.environ.get("VENUE_PY_TRACEBACKS") == "1":
        import traceback as _traceback
        _inner_app = app

        async def app(scope, receive, send):
            try:
                await _inner_app(scope, receive, send)
            except Exception:
                sys.stderr.write("VENUE TRACEBACK %s %s\n" % (scope.get("method"), scope.get("path")))
                _traceback.print_exc(file=sys.stderr)
                raise
    client = TestClient(app, raise_server_exceptions=False, follow_redirects=False)
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
	// copy, as a superuser. It may call Python through venue.CallPython, for
	// example to write a value the way the Python api encrypts it. It
	// returns the tokens to mint: name -> create_access_token keyword
	// arguments (nil = none).
	Seed func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *Venue) map[string]map[string]any
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

	// ClickHouse (CHAOS-6310): one server, two databases. PythonClickHouseDB
	// is what CLICKHOUSE_URI (Python's env) names; GoClickHouseDB is what
	// the dedicated api login is scoped to.
	clickHouseURI, clickHouseHTTPURI     string
	PythonClickHouseDB, GoClickHouseDB   string
	clickHouseAPIRole, clickHouseAPIPass string
}

// Start builds the venue; see the package comment. It skips unless
// DEV_HEALTH_LIVE_PYTHON_ORACLES=1. Everything it creates is removed by
// t.Cleanup.
//
// Start itself writes no proof file: it only proves the venue built, not
// that the caller went on to compare anything against it, and a test that
// calls Start and returns (no Diff, no failure, no skip) would otherwise
// still satisfy a proof-file check while never running a real comparison.
// Diff writes the proof instead, once it has actually sent requests to
// both planes and compared them -- see its doc comment.
func Start(t *testing.T, ctx context.Context, options Options) *Venue {
	t.Helper()
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the venue oracle needs the live Python api; run with DEV_HEALTH_LIVE_PYTHON_ORACLES=1")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	python := pyoracle.Resolve(t, options.Root)
	bin, err := interpreterDir(python)
	if err != nil {
		t.Fatalf("venue: %v", err)
	}
	// Activate the interpreter's environment as `source bin/activate` does:
	// its directory goes first on PATH, and the program runs as "python3".
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if found, err := exec.LookPath("python3"); err != nil || filepath.Dir(found) != bin {
		t.Fatalf("venue: python3 on PATH is %q (%v), want the one in %s", found, err, bin)
	}
	v := &Venue{Root: options.Root, Python: filepath.Join(bin, "python3"), Tokens: map[string]string{}, Roles: map[string]string{}}

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
	// plane" shape. Real migrations, applied to each database
	// independently (ClickHouse has no CREATE DATABASE ... TEMPLATE, so
	// this is two migration runs, not a copy).
	chInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chInstance.Close(context.Background()) })
	v.clickHouseURI = chInstance.URI
	// Python's clickhouse_connect is an HTTP-only client: it needs the
	// HTTP-port DSN, never Instance.URI's native-port one (clickhouse-go,
	// this package's own admin/CHRows connections, speaks the native
	// protocol and needs the opposite -- confirmed live, both ways, with
	// the wrong port on either side).
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
		v.migrateClickHouse(t, ctx, database)
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

	async := strings.Replace(v.AdminURI(t, v.SourceDB), "postgres://", "postgresql+asyncpg://", 1)
	async = strings.Replace(async, "postgresql://", "postgresql+asyncpg://", 1)
	v.pythonEnv = append([]string{"PYTHONPATH=" + filepath.Join(options.Root, "src"), "POSTGRES_URI=" + async,
		"JWT_SECRET_KEY=" + options.JWTKey, "OTEL_SDK_DISABLED=true", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1",
		// NullPool: TestClient gives each request its own event loop, and a
		// pooled asyncpg connection cannot cross loops.
		"PGBOUNCER_TRANSACTION_MODE=true", "ENVIRONMENT=test", "REDIS_URL=" + v.PythonValkeyURI,
		"CLICKHOUSE_URI=" + v.AdminClickHouseHTTPURI(t, v.PythonClickHouseDB)},
		options.PythonEnv...)

	// 1. The real schema, then one seed and its tokens.
	v.runPython(t, nil, "migrate")
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
		if err := json.Unmarshal(v.runPython(t, specs, "mint"), &v.Tokens); err != nil {
			t.Fatal(err)
		}
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

// interpreterDir returns the directory of the interpreter pyoracle chose,
// which must also hold an executable python3 (every virtualenv and every
// Python 3 install directory does), as an absolute path. The venue runs
// that python3 by activating the directory, never by executing a path.
func interpreterDir(path string) (string, error) {
	if !filepath.IsAbs(path) {
		found, err := exec.LookPath(path)
		if err != nil {
			return "", fmt.Errorf("interpreter %q: %w", path, err)
		}
		path = found
	}
	dir, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return "", err
	}
	python3 := filepath.Join(dir, "python3")
	info, err := os.Stat(python3)
	if err != nil {
		return "", fmt.Errorf("interpreter %q: no python3 beside it: %w", path, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return "", fmt.Errorf("interpreter %q: %s is not an executable file", path, python3)
	}
	return dir, nil
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

// CallPython runs calls in one Python process with the Python plane's
// environment (the source database, Valkey and PythonEnv) and returns each
// result as raw JSON, in order.
func (v *Venue) CallPython(t *testing.T, calls ...PythonCall) []json.RawMessage {
	t.Helper()
	var out []json.RawMessage
	if err := json.Unmarshal(v.runPython(t, calls, "call"), &out); err != nil {
		t.Fatal(err)
	}
	if len(out) != len(calls) {
		t.Fatalf("python returned %d of %d call results", len(out), len(calls))
	}
	return out
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
	// Start put the chosen interpreter's directory first on PATH. The
	// program is the compiled-in pythonProgram; only its mode and the JSON
	// on stdin vary.
	command := exec.Command("python3", append([]string{"-c", pythonProgram}, args...)...)
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

// migrateClickHouse runs the real `dev-hops migrate clickhouse upgrade`
// CLI against database -- the same entrypoint a deploy's init step runs,
// never a hand-copied CREATE TABLE list (the trap class this whole family
// of test fixtures kept hitting elsewhere in this repo). CLICKHOUSE_URI is
// this call's own env, never v.pythonEnv: the venue's Python plane must
// keep pointing at PythonClickHouseDB throughout, including while this
// runs against GoClickHouseDB.
func (v *Venue) migrateClickHouse(t *testing.T, ctx context.Context, database string) {
	t.Helper()
	command := exec.CommandContext(ctx, "python3", "-m", "dev_health_ops.cli", "migrate", "clickhouse", "upgrade")
	// os.Environ() already carries the activated interpreter's PATH (Start
	// set it with t.Setenv, which changes this test process's own env, not
	// only pythonEnv); only PYTHONPATH and CLICKHOUSE_URI are this call's
	// own additions.
	command.Env = append(os.Environ(),
		"PYTHONPATH="+filepath.Join(v.Root, "src"), "CLICKHOUSE_URI="+v.AdminClickHouseHTTPURI(t, database))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("clickhouse migrate %s: %v\n%s", database, err, output)
	}
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
	// Every Go handler must have written each body with the writer its
	// route's ResponseModel flag allows (policy.WriteModel for a FastAPI
	// response_model success body, WriteJSON otherwise). The count is
	// process-wide, so any violation in this package's venues fails here.
	if violations := policy.WriterViolations(); violations > 0 {
		t.Errorf("%d body writes used the wrong writer for their route (see the policy ERROR logs naming each route)", violations)
	}
	writeProof(t)
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

func tail(text string) string {
	if len(text) > 4000 {
		return text[len(text)-4000:]
	}
	return text
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
