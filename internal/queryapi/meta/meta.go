// Package meta is the Go port of GET /api/v1/meta
// (src/dev_health_ops/api/main.py:424-466's meta handler; response shape
// from MetaResponse, src/dev_health_ops/api/models/schemas.py:214-222).
//
// # Auth: PUBLIC, verified from source, not assumed
//
// Unlike every other endpoint this codebase has ported so far, main.py's
// meta() carries no `Depends(get_current_user)` and no router-level
// dependency -- it is a bare `@app.get("/api/v1/meta", ...)` with an empty
// parameter list. OrgIdMiddleware (src/dev_health_ops/api/middleware/
// __init__.py) is the single enforcement point for tenant scoping in this
// app and its own comment says anonymous requests "pass through... routes
// that require auth will 401 via their own dependencies" -- meta has no
// such dependency, so an anonymous caller reaches it and gets a normal
// response. This is confirmed by a committed test,
// tests/api/test_analytics_auth.py::test_public_endpoints_do_not_require_auth
// (parametrized over "/health" and "/api/v1/meta", asserting status !=
// 401), and by contracts/auth/v1/endpoint-profiles.ops.json's own row for
// "GET /api/v1/meta" (classification: "public", tenant_requirement:
// "none", accepted_credential_classes: []). This port therefore adds NO
// auth check at all -- see meta_route.go's entry handler.
//
// # No tenant table read
//
// The only ClickHouse read this endpoint makes is `SELECT version()`, a
// server-level system call with no org_id column to filter on. The class
// ruling on this codebase's ported routes (org filter inside every
// tenant-table read; ReplacingMergeTree dedup via FINAL/argMax) has
// nothing to apply to here -- there is no tenant table in this handler at
// all.
//
// # Field-by-field parity notes
//
//   - backend: main.py calls detect_backend(_analytics_db_url()).value.
//     metrics/sinks/factory.py's SinkBackend enum has exactly one member,
//     CLICKHOUSE, and its module doc comment says other backends were
//     removed -- so this always evaluates to "clickhouse" whenever
//     _analytics_db_url() succeeds, which requires CLICKHOUSE_URI to already be a
//     clickhouse(+native|+http|+https):// URL. This route's own mount
//     precondition (loadMetaRouteConfig, meta_route.go) already requires
//     CLICKHOUSE_URI to be set, so "clickhouse" is hardcoded here rather
//     than re-derived from a scheme parse that can only ever agree.
//   - last_ingest_at / coverage: main.py's handler hardcodes these to
//     `None` / `{}` itself (locals never assigned to by any code path in
//     the function) -- not read from ClickHouse or anywhere else. Ported
//     as the same static values.
//   - limits / supported_endpoints: literal values from main.py:452-463,
//     ported verbatim.
//   - version: `SELECT version() AS version`, degraded to "unknown" on ANY
//     failure. See queryVersion's doc comment for the two-layer Python
//     try/except this collapses into one degrade path, and the one
//     declared divergence that follows from this port's persistent (not
//     per-request-reconnected) ClickHouse client.
//
// # Declared divergence: Python's 503 "Metadata unavailable" branch
//
// main.py's meta() wraps `async with clickhouse_client(db_url) as sink:`
// in a try/except that maps ANY exception -- including the async context
// manager's own `__aenter__` failing to obtain/construct the shared sink
// -- to HTTPException(503, "Metadata unavailable") (main.py:465-466).
// Confirmed by an uncommitted, ad hoc probe (separate from golden_test.go's
// committed capture command, which only exercises the happy path): calling
// main.meta() with clickhouse_client monkeypatched to a context manager
// that raises on entry reproduces exactly this 503.
//
// This port's ClickHouse client (readClient in meta_route.go's
// buildMetaRoute) is built ONCE at process start, the same pattern every
// sibling ported route in this binary uses -- not reconnected per request.
// There is therefore no per-request equivalent of "the context manager
// itself failed to construct a sink": the only thing that can fail on a
// given request is the `SELECT version()` query itself, and that already
// degrades to "unknown" + 200 here exactly as it does on the Python side's
// inner try/except (main.py:438-445, "Silently ignore version query
// failures - not critical for meta endpoint"). Declared, not a defect:
// Python's 503 branch is reachable only through a construction failure
// this port's client architecture does not have a per-request analog for.
//
// # Declared divergence: missing CLICKHOUSE_URI
//
// main.py calls `_analytics_db_url()` OUTSIDE its own try/except
// (main.py:429, before the `try:` at 432) -- when CLICKHOUSE_URI is unset
// this raises an uncaught RuntimeError, and FastAPI's registered
// catch-all (_errors.py's _generic_exception_handler) answers 500
// "Internal Server Error" (confirmed by the same kind of ad hoc probe
// referenced above, with CLICKHOUSE_URI popped from os.environ before
// calling main.meta()). This port follows every sibling route in this
// binary instead:
// an unset CLICKHOUSE_URI simply leaves /api/v1/meta unmounted
// (buildMetaRoute returns ok=false; main() logs and continues) rather than
// 500ing per request -- the same pre-existing, already-documented gap
// main.go's own comments describe for every other optionally-configured
// route, not a new divergence this port introduces.
package meta

import (
	"context"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// QueryClient is the read-only ClickHouse query boundary this package
// needs -- same single-method shape every sibling operation package
// (quadrant.QueryClient, filteroptions.QueryClient, etc.) declares
// independently.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// Response ports MetaResponse (api/models/schemas.py:214-222). Plain field
// names: main.py's FastAPI app carries no alias generator, so Pydantic's
// wire form is the snake_case attribute name verbatim.
type Response struct {
	Backend            string         `json:"backend"`
	Version            string         `json:"version"`
	LastIngestAt       *string        `json:"last_ingest_at"`
	Coverage           map[string]any `json:"coverage"`
	Limits             map[string]int `json:"limits"`
	SupportedEndpoints []string       `json:"supported_endpoints"`
}

// versionQuery ports the inline `SELECT version() AS version` read
// (main.py:440) verbatim -- a server-level call, not a tenant table, so it
// carries no org_id binding.
const versionQuery = "SELECT version() AS version"

// unknownVersion ports main.py:434's `version = "unknown"` initial value,
// which main.py's own code returns unmodified whenever the query fails,
// returns no rows, or the row's value is falsy.
const unknownVersion = "unknown"

// supportedEndpoints ports main.py:453-463's literal list verbatim.
func supportedEndpoints() []string {
	return []string{
		"/api/v1/home",
		"/api/v1/quadrant",
		"/api/v1/flame",
		"/api/v1/heatmap",
		"/api/v1/work-units",
		"/api/v1/sankey",
		"/api/v1/investment",
		"/api/v1/opportunities",
		"/graphql",
	}
}

// BuildResponse ports main.py:424-466's meta() handler body. It never
// returns an error: every failure this function can encounter (a
// ClickHouse query error, an empty result set, a scan error) degrades the
// version field to "unknown" and still answers a complete Response,
// matching Python's inner try/except exactly (see the package doc comment
// for the one declared divergence, the outer try/except's unreachable-here
// 503 branch).
func BuildResponse(ctx context.Context, client QueryClient) Response {
	return Response{
		Backend:            "clickhouse",
		Version:            queryVersion(ctx, client),
		LastIngestAt:       nil,
		Coverage:           map[string]any{},
		Limits:             map[string]int{"max_days": 365, "max_repos": 1000},
		SupportedEndpoints: supportedEndpoints(),
	}
}

// queryVersion ports main.py:436-445's ClickHouse version probe: run
// `SELECT version()`, use the first row's value if one comes back,
// otherwise (and on ANY error along the way) fall back to "unknown". This
// single function is where BOTH of main.py's try/except layers collapse
// (main.py:438-445's inner one, which explicitly swallows query failures
// as "not critical for meta endpoint", and the part of main.py:432-465's
// outer one that would otherwise apply to a query failure) -- see the
// package doc comment's "Declared divergence" sections for what does NOT
// collapse here (the outer try/except's client-construction-failure-only
// 503 branch, which has no per-request analog against this port's
// persistent ClickHouse client).
func queryVersion(ctx context.Context, client QueryClient) string {
	if client == nil {
		return unknownVersion
	}
	rows, err := client.Query(ctx, versionQuery, nil)
	if err != nil {
		return unknownVersion
	}
	defer rows.Close()

	if !rows.Next() {
		return unknownVersion
	}
	var version string
	if err := rows.Scan(&version); err != nil {
		return unknownVersion
	}
	if err := rows.Err(); err != nil {
		return unknownVersion
	}
	if version == "" {
		return unknownVersion
	}
	return version
}
