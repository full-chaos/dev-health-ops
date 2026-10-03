// Command query-api is the Go read-only GraphQL analytics service (CHAOS-4366
// Wave 0 / CHAOS-4352 plan §3). This binary is deliberately EMPTY of
// resolver logic: every field resolver gqlgen generated in
// internal/graph/schema.resolvers.go panics with "not implemented" (see
// that file's header comment and the plan §6 Wave-0 scope: "deploy an
// empty Go query-api and prove a route becomes reachable when, and only
// when, its individual switch is enabled").
//
// The GraphQL executable schema is wired below but is NOT mounted on any
// route in this Wave -- there is nothing behind it that should serve
// traffic yet. What IS live: /healthz, /readyz, and the routeswitch
// mechanism (internal/routeswitch), proven reachable-only-when-enabled by
// its own table-driven test. A later wave mounts /query behind
// routeswitch.Mux, keyed by the operation registry
// (src/dev_health_ops/models/go_api_registry.py's ROUTING_STATE table, once
// a Go reader exists -- CHAOS-4377/dev-health-go).
//
// CHAOS-4512: /readyz now reflects /query's actual live dependencies
// (ClickHouse, registry Postgres) once that route is mounted -- see
// readyzHandler's doc comment below for the full contract. It is no
// longer true, as an earlier version of this comment claimed, that
// readiness has nothing to check: that was Wave 0's shape, before /query
// existed.
package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"time"

	"github.com/99designs/gqlgen/graphql"
	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
)

// getenvFunc reads one setting by name. Build gets it from the caller (dho
// query-api passes the shell's declared-settings reader), so no route builder
// reads the process environment itself.
type getenvFunc func(string) string

// newExecutableSchemaHandler constructs the gqlgen HTTP handler over the
// canonical SDL's generated schema. Kept separate from main() so a future
// integration test can exercise it directly without starting a real
// listener. Unused for now (see package doc) -- built here, not mounted,
// so `go build`/`go vet` catch a schema/resolver mismatch immediately
// rather than only once a later wave tries to wire it in.
func newExecutableSchemaHandler() http.Handler {
	schema := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}})
	server := gqlhandler.NewDefaultServer(schema)
	server.AroundFields(graph.RefuseNullForNonNullArguments)
	return server
}

// newListenerServers builds the public server over publicBase and, when
// internalAddr is set, the internal one over internalBase (CHAOS-6780,
// route sets split CHAOS-7078 groundwork): the two bases are DIFFERENT
// values now -- internalBase may serve routes publicBase does not, so a
// route mounted only for the internal listener genuinely does not exist on
// the public one, rather than existing on both and relying on the identity
// middleware alone to keep it unreachable. The public server still deletes
// the X-DH-Internal-* identity headers before any handler sees them; only
// the internal server, on a port no Ingress routes to, marks requests as
// allowed to carry them. internalAddr empty = no internal server (nil): the
// headers are honoured nowhere, and internalBase is never served.
func newListenerServers(publicAddr, internalAddr string, publicBase, internalBase http.Handler) (public, internal *http.Server) {
	public = &http.Server{
		Addr:              publicAddr,
		Handler:           analytics.InvestmentMembershipScopeRequestMiddleware(internalidentity.Public(traceListener(publicBase, "public", false))),
		ReadHeaderTimeout: 5 * time.Second,
	}
	if internalAddr != "" {
		internal = &http.Server{
			Addr:              internalAddr,
			Handler:           analytics.InvestmentMembershipScopeRequestMiddleware(internalidentity.Internal(traceListener(internalBase, "internal", true))),
			ReadHeaderTimeout: 5 * time.Second,
		}
	}
	return public, internal
}

// queryProbePaths are the paths the query listeners answer for the kubelet and
// the scraper; they get no span (a span per probe is volume with no
// information).
var queryProbePaths = []string{"/healthz", "/readyz", "/metrics"}

// traceListener wraps one listener's mux with the server span the api
// listeners carry (httpapi.TraceHandler): named by the mux's registered
// pattern, with the method, status code and listener, and no query string,
// header, body, client address, user or org value. It sits INSIDE the identity
// and membership middleware, directly around the mux, so the pattern the mux
// records is the one it reads. Only the internal listener (in-cluster callers)
// honours an incoming traceparent; the others start a new root.
func traceListener(base http.Handler, listener string, trustRemoteSampling bool) http.Handler {
	return httpapi.TraceHandler(base, httpapi.TraceOptions{Listener: listener, TrustRemoteSampling: trustRemoteSampling, ProbePaths: queryProbePaths})
}

// newMCPListenerServer is the MCP caller-class listener's server
// (CHAOS-7085). internalidentity.MCP marks the request as arriving there;
// internalidentity.Internal is NOT applied, so no internal-listener route
// could honour the identity headers even if one were mounted on base.
func newMCPListenerServer(mcpAddr string, base http.Handler) *http.Server {
	return &http.Server{
		Addr:              mcpAddr,
		Handler:           analytics.InvestmentMembershipScopeRequestMiddleware(internalidentity.MCP(traceListener(base, "mcp", false))),
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// readyzTimeout bounds every dependency check readyzHandler runs. An
// unbounded readiness probe hangs whatever polls it (an orchestrator, a
// rollout gate, a load balancer health check) for as long as the
// dependency itself is wedged, which is strictly worse than a fast,
// definite "not ready" -- see readyzHandler's doc comment.
const readyzTimeout = 3 * time.Second

// ObserveProbe is one of /query's dependency probes as the required readiness check
// dho query-api registers on the operator listener (/readyz on --http-addr). What
// used to be readyzHandler on the query listener is now split by dependency class:
// the operator /readyz names the failing CHECK ("query_postgres", ...) and nothing
// else, so the class is the whole disclosure and the underlying error never reaches
// the wire (CHAOS-4724: /readyz is UNAUTHENTICATED, pgx/clickhouse-go dial errors
// render a host:port and CheckJWKS's errors name GO_API_ENVELOPE_JWKS_PATH's
// filesystem path). The full error still goes to the log (the shell's logger
// redacts credentials), so an operator can diagnose without shell access.
//
// The probe runs LIVE on every readiness request under a bounded timeout, never a
// cached result from process start: buildQueryRoute's eager ClickHouse ping only ever
// ran once, and pgxpool.New never pinged Postgres at all, so a dependency that failed
// after boot was invisible before (CHAOS-4512). The outcome is counted per check
// ("healthy" / "unhealthy") so a dashboard can tell "no probes yet" from "every check
// is failing".
func ObserveProbe(probe ReadinessProbe) func(context.Context) error {
	return func(parent context.Context) error {
		ctx, cancel := context.WithTimeout(parent, readyzTimeout)
		defer cancel()
		if err := probe.Check(ctx); err != nil {
			log.Printf("query-api: %s readiness check failed: %v", probe.Name, err)
			recordReadyzOutcome("unhealthy", probe.Name)
			return errors.New(readyzDependencyClass(err))
		}
		recordReadyzOutcome("healthy", probe.Name)
		return nil
	}
}

// NotConfiguredCheckName is the required readiness check of the "no /query configured" mode.
const notConfiguredCheckName = "query_routes"

// NotConfiguredCheck is the readiness check of a deployment where /query is not
// configured (loadQueryRouteConfig's ok=false, the "nothing mounted" shape). That
// is a DELIBERATE, documented operating mode ("an operator who has not yet configured
// this service's dependencies must not be forced to also configure
// ClickHouse/Postgres/JWKS just to build or run the binary"), so it passes; the
// shell's registry fails closed on a service with no required check at all, so the
// mode is one explicit passing check, counted as "not_configured" so it never reads
// as a verified-healthy answer (CHAOS-4512).
func NotConfiguredCheck() func(context.Context) error {
	return func(context.Context) error {
		recordReadyzOutcome("not_configured", notConfiguredCheckName)
		return nil
	}
}

// readyzDependencyClass extracts the safe-to-disclose dependency class
// from a readinessCheck error -- see readyzDependencyError's doc comment
// in query_route.go for what it deliberately does not disclose. Falls
// back to a generic, still-detail-free "dependency" label for any error
// that is not a *readyzDependencyError: readyzHandler must NEVER fall
// back to err.Error() here, because that is exactly the leak CHAOS-4724
// closes -- a future caller of readinessCheck that forgets to wrap its
// error in *readyzDependencyError fails safe (a less specific body), not
// open (a raw error string reaching an unauthenticated caller).
func readyzDependencyClass(err error) string {
	var depErr *readyzDependencyError
	if errors.As(err, &depErr) {
		return depErr.Class
	}
	return "dependency"
}

// runningBuild is the build identity this process stamps on /query and
// /query/proof responses.
//
// A function, not an inline call, because the inline form could not be
// tested: `mountQueryRoute(mux, handlers.Query, "")` compiled and left
// every suite green, since the pin called mountQueryRoute itself with its
// own constant and proved only that the HELPER stamps what it is given.
// Nothing proved that main gives it the RUNNING build -- and an empty one
// there is precisely the state this change exists to make impossible, with
// a consequence (every edge measurement `unsupported`) byte-identical to
// the documented interim state.
//
// Now the value has one source that a test can call and compare against
// version.Current directly.
func runningBuild() string {
	return version.Current("query-api").Commit
}

// mountQueryRoute registers the production /query handler WITH provenance.
//
// It takes NO build argument, deliberately. The previous signature accepted
// one so a test could assert a chosen value -- and that left
// `mountQueryRoute(mux, handlers.Query, "")` as a call-site mutation which
// compiled and survived every suite, because the test passed its own
// constant and proved only that the helper stamps what it is handed.
// Nothing proved main handed it the running build.
//
// With the value read inside, there is no argument at the call site to get
// wrong: the mutation is unwritable rather than undetected. The test drives
// this function and compares the WIRE header against runningBuild(), so the
// two cannot disagree.
func mountQueryRoute(mux *http.ServeMux, query http.HandlerFunc) {
	mux.HandleFunc("/query", withProofProvenance(query, runningBuild()))
}

// Plane is the query plane built from a settings reader: every route it mounts,
// the live readiness check of /query's dependencies, and the release of what the
// routes opened.
type Plane struct {
	// Handler is the mux of every mounted route, with the response-model marker.
	// The listeners (Listeners) add the identity middleware around it. Served
	// on BOTH listeners -- this field's route set is the public one and must
	// stay exactly what it always was; a route only the internal listener
	// should serve belongs on InternalHandler, never here.
	Handler http.Handler
	// InternalHandler serves the internal listener ONLY (CHAOS-7078
	// groundwork for CHAOS-7096's /query/proof-write and CHAOS-7085's MCP
	// caller-class routes): every route Handler serves, reached by falling
	// through to it, PLUS whatever route a future mount registers here
	// directly (which then takes precedence over the fallthrough for its own
	// pattern, ServeMux's ordinary most-specific-match rule). Build sets
	// this to a mux with nothing of its own mounted yet, so today it is
	// behaviourally identical to Handler; the public listener never sees
	// this value.
	InternalHandler http.Handler
	// MCPHandler serves the MCP caller-class listener ONLY (CHAOS-7085,
	// QUERY_API_MCP_ADDR): POST /query of that class and nothing else -- no
	// fallthrough to Handler, so no /registry, /buildinfo, /metrics or REST
	// route answers there. Neither Handler nor InternalHandler can reach it.
	// When /query is not configured it answers 404 to everything.
	MCPHandler http.Handler
	// Ready is nil when /query is not configured (nothing to check, see
	// ReadinessCheck) and otherwise the live dependency check.
	Ready func(context.Context) error
	// Probes are the same checks, one per dependency class (nil when /query is not
	// configured), for a caller that reports each on its own.
	Probes []ReadinessProbe
	// Close releases every route's dependencies, last opened first.
	Close func()
}

// mountQueryRouteSets mounts the configured /query family on the three
// route sets: the public set (mux, also reached by the internal listener
// through its fallthrough), the internal-only set (internalMux) and the MCP
// caller-class set (mcpMux, CHAOS-7085). One function, called by Build and
// by the listener tests, so the test's route sets are Build's route sets.
func mountQueryRouteSets(getenv getenvFunc, mux, internalMux, mcpMux *http.ServeMux, handlers queryRouteHandlers, edge graphQLEdgeDeps) {
	// Wrapped, not raw. The provenance headers are what let a proof
	// receipt be bound to the process that actually served the
	// request, and until CHAOS-5479 only /query/proof carried them --
	// so the Python edge's pass-through forwarded a header the normal
	// route never set, and every canary/primary measurement was
	// unbindable. A prover cannot certify what it cannot bind, so it
	// downgraded all of them: the gate was correct and useless.
	//
	// The same wrapper as the proof route, deliberately: one
	// implementation, so the two routes cannot drift into disagreeing
	// about what they claim.
	mountQueryRoute(mux, handlers.Query)
	// /graphql, the product path, on the public set only (the internal
	// listener reaches it through its fallthrough and refuses its own
	// carriers there). Never on mcpMux: the MCP caller class has one route.
	mountGraphQLRoute(mux, handlers.Query, edge)
	// GET /registry: what THIS process registers, and the schema digest
	// it computed. Mounted with /query, not beside /healthz, on purpose
	// -- it describes /query's registration set, so an unconfigured
	// environment where /query never mounted must 404 here too rather
	// than answer for a route that does not exist. `dev-hops go-api
	// routing enable` treats that 404 as a refusal, which is correct:
	// there is nothing to enable into. See registry_route.go.
	mux.HandleFunc("/registry", handlers.Registry)
	// GET /buildinfo: which BUILD this process is. Separate from
	// /registry on purpose (team-lead ruling R51, 2026-09-09):
	// /registry's own doc comment states its body is exactly the
	// schema digest and the operation map and is "not an invitation
	// to add build paths, env, or pool state later", and that
	// restriction is worth keeping. This route answers the different
	// question CHAOS-5425 needs -- a proof receipt names the build
	// that served it, and until now the only available answer was an
	// operator-typed sha nothing verified. See buildinfo_route.go.
	mux.HandleFunc("/buildinfo", handlers.BuildInfo)
	mountProofRoute(getenv, mux, handlers.Proof)
	// CHAOS-7096: mounted on internalMux ONLY -- never on mux, which the
	// public listener is built from (CHAOS-7097's Listeners split).
	// A test proves the public route
	// set has no /query/proof-write.
	mountProofWriteRoute(getenv, internalMux, handlers.ProofWrite)
	// CHAOS-7214: the proof variant of the MCP class route, internalMux ONLY.
	mountProofMCPRoute(getenv, internalMux, handlers.MCPProof)
	// CHAOS-7831: acr's run_operation route, internalMux ONLY (never on mux, which the public listener is built from): the serving pipeline of /query
	// behind the MCP class rows. /query keeps serving the web edge un-gated. A nil handler (a test that builds no run-operation route) registers nothing.
	mountRunOperationRoute(internalMux, handlers.RunOperation)
	// CHAOS-7085: on mcpMux ONLY. A test proves neither the public nor
	// the internal route set reaches the MCP class.
	mcpMux.Handle("/query", handlers.MCP)
}

// Build mounts the query plane. Every setting it, and every route builder, reads
// comes from get (the declared-settings reader of dho query-api); a route whose
// settings are absent stays unmounted, and a route that cannot be built is an error
// and nothing stays open. A setting read with get is absent when empty; use
// BuildWithLookup where an empty value must differ from an absent one.
func Build(get func(string) string) (*Plane, error) {
	return BuildWithLookup(presentWhenSet(get))
}

// presentWhenSet reads get as a lookup whose empty value is absent.
func presentWhenSet(get func(string) string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value := get(name)
		return value, value != ""
	}
}

// BuildWithLookup is Build over a reader that tells an absent setting from an
// empty one (config.Config.Setting). Only CORS_ALLOWED_ORIGINS reads the
// difference: the Python api's os.getenv default applies when the variable is
// absent, and a present empty value is an empty allow-list.
func BuildWithLookup(lookup func(string) (string, bool)) (*Plane, error) {
	getenv := getenvFunc(func(name string) string {
		value, _ := lookup(name)
		return value
	})
	var cleanups []func()
	closeAll := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	fail := func(err error) (*Plane, error) {
		closeAll()
		return nil, err
	}

	// Constructed to prove the schema/resolver pair builds and links
	// correctly (see newExecutableSchemaHandler's doc comment); not
	// mounted on any mux route in this Wave.
	_ = newExecutableSchemaHandler()

	mux := http.NewServeMux()
	// internalMux (CHAOS-7078/CHAOS-7097 groundwork) is declared here, not
	// at the end of Build, so /query/proof-write (CHAOS-7096, below) can
	// mount directly on it in the same scope handlers.ProofWrite is built
	// in. mux and internalMux diverge from this point on, never
	// re-converging except through internalMux's own fallthrough
	// ("/" -> handler, registered at the very end of Build once handler --
	// mux's final, wrapped form -- exists).
	internalMux := http.NewServeMux()
	// mcpMux (CHAOS-7085) is the MCP listener's whole route set: /query of
	// the MCP caller class, mounted below when /query is configured, and
	// nothing else. It never falls through to mux or internalMux.
	mcpMux := http.NewServeMux()

	// CHAOS-4367 Wave 1 / CHAOS-4368 Wave 2 / CHAOS-4369 Wave 3: mount the
	// real featureFlags, reviewEdges, and cognitiveLoad routes when their
	// (shared) dependencies are configured. See query_route.go's doc
	// comments for what "configured" means and why an unconfigured
	// environment falls back to Wave 0's "nothing mounted" behavior
	// instead of failing to start.
	//
	// ready is CHAOS-4512's fix: nil until (and unless) /query mounts
	// successfully, matching readyzHandler's documented "nothing
	// configured, nothing to check" contract for that state.
	var ready func(context.Context) error
	var probes []ReadinessProbe
	var registryPool *pgxpool.Pool
	if routeCfg, ok := loadQueryRouteConfig(getenv); ok {
		handlers, readyFn, cleanup, buildErr := buildQueryRoute(getenv, routeCfg)
		if buildErr != nil {
			log.Printf("query-api: build /query route: %v", buildErr)
			return fail(buildErr)
		}
		cleanups = append(cleanups, cleanup)
		// /graphql is the product path over the same handler (CHAOS-6263):
		// see graphql_edge_route.go for what it adds and why.
		edgeAuth, _, edgeErr := buildQueryEdgeAuthenticatorFromEnv(getenv, handlers.RegistryPool)
		if edgeErr != nil {
			return fail(edgeErr)
		}
		mountQueryRouteSets(getenv, mux, internalMux, mcpMux, handlers, graphQLEdgeDeps{
			auth:        edgeAuth,
			corsOrigins: corsAllowedOrigins(lookup),
			maxBytes:    graphQLMaxQueryBytes(getenv),
			logger:      slog.Default(),
		})
		ready = readyFn
		probes = handlers.Probes
		registryPool = handlers.RegistryPool
		// CHAOS-4710 deliverable 3: the mount-confirmation log line used to
		// live here as a hand-typed, six-of-twelve literal (stale since
		// Wave 3 -- the real registration is all twelve of
		// newQueryHandler's digestByOperation keys). It is now emitted
		// from inside newQueryHandler itself (query_route.go), right next
		// to the digestByOperation map it describes, via
		// mountedRouteLogMessage -- see that function's doc comment for
		// why it cannot be a package-level function called from here
		// instead (cmd/registrydump's AST parser requires
		// digestByOperation's composite literal to stay exactly where it
		// is, assigned directly, not returned from a helper).
	} else {
		log.Print("query-api: /query route not configured (CLICKHOUSE_URI/GO_API_REGISTRY_POSTGRES_URI/GO_API_ENVELOPE_* unset) -- staying Wave-0 empty")
		// Say so about the proof route too, with its own explicit zero.
		// Previously this branch logged nothing about /query/proof, so an
		// operator reading the log could not tell "the proof route is off"
		// from "nobody ever considered it" -- the same conflation the
		// registered/not-registered lines exist to prevent (codex r1 F8).
		mountProofRoute(getenv, mux, nil)
		mountProofWriteRoute(getenv, internalMux, nil)
		mountProofMCPRoute(getenv, internalMux, nil)
	}

	// The live users-row store behind every edge-verified REST route below
	// (CHAOS-6290): PGStore over the /query route's ONE registry pool, not a
	// pool of its own; an error (edge secret set, no pool) refuses to start.
	edgeUsers, err := newEdgeUserStore(getenv, registryPool)
	if err != nil {
		log.Printf("query-api: build edge users store: %v", err)
		return fail(err)
	}

	// The REST routes are the rows of restGroups (rest_routes.go): the table IS
	// the production route set, so a test (and internal/migrationmatrix) walks
	// it instead of parsing this file. Order, switches, cleanup order, log lines
	// and handler bodies are exactly what the per-route blocks here were.
	for _, group := range restGroups {
		handlers, cleanup, ok, buildErr := group.Build(getenv, edgeUsers)
		if buildErr != nil {
			group.LogBuildError(buildErr)
			return fail(buildErr)
		}
		if !ok {
			group.LogNotConfigured()
			continue
		}
		// Collected BEFORE the count check so a builder that built its resources
		// and then disagrees with its row still has them closed by fail().
		cleanups = append(cleanups, cleanup)
		if len(handlers) != len(group.Mounts) {
			return fail(fmt.Errorf("query-api: %s returned %d handler(s) for %d mount(s)", group.Builder, len(handlers), len(group.Mounts)))
		}
		for i, mount := range group.Mounts {
			// Wrapped in withProofProvenance so go-api-rest-prove can bind a
			// receipt to the process that actually served this request, the same
			// reason /query and /query/proof carry it.
			mux.HandleFunc(mount.Pattern, withProofProvenance(handlers[i], runningBuild()))
		}
	}

	// A /api/v1/* path no route above claims answers Starlette's own
	// default 404 body -- {"detail": "Not Found"}, confirmed live --
	// instead of net/http's plain-text default. This is a SUBTREE pattern
	// (trailing slash): every specific /api/v1/... registration above
	// still wins (Go's ServeMux routes to the longest matching pattern);
	// this only catches what none of them do, including an
	// individually-unmounted route (its own CLICKHOUSE_URI/envelope vars
	// unset) -- indistinguishable from "does not exist" to an outside
	// caller either way.
	mux.HandleFunc("/api/v1/", apiV1CatchAllNotFound)

	handler := markResponseModelRoutes(mux)
	// CHAOS-7096: internalMux already carries /query/proof-write (mounted
	// above, in the same scope as handlers.ProofWrite) when that route is
	// configured. This fallthrough registration makes every OTHER path
	// behave identically to the public listener -- a route only the
	// internal listener should serve is registered directly on internalMux
	// BEFORE this call (Go's ServeMux matches the longest registered
	// pattern regardless of registration order, so /query/proof-write
	// still wins over this "/" catch-all) -- and it structurally CANNOT be
	// reachable through the public listener, which is built from handler
	// and never sees internalMux at all.
	internalMux.Handle("/", handler)
	return &Plane{Handler: handler, InternalHandler: internalMux, MCPHandler: mcpMux, Ready: ready, Probes: probes, Close: closeAll}, nil
}

// recordErrorCount stamps the number of errors a GraphQL response carried on
// the request's server span (httpapi.GraphQLErrorCountAttribute): gqlgen
// answers a resolver error with HTTP 200, so the status class alone would
// read every one of them as a success. A count only: no message, no variable.
func recordErrorCount(ctx context.Context, next graphql.ResponseHandler) *graphql.Response {
	response := next(ctx)
	if response != nil {
		httpapi.RecordGraphQLErrorCount(ctx, len(response.Errors))
	}
	return response
}

// recordOperationErrorCount counts the errors of a response that an operation
// interceptor answered itself. Registered BEFORE the limits and
// graph.OperationOrgGuard it is the outermost wrapper, so their refusals
// (HTTP 200, errors body) are counted: they return before the response
// middleware (recordErrorCount) runs. It only ever stamps a non-zero count, so
// it never overwrites what recordErrorCount stamped for a resolver error.
func recordOperationErrorCount(ctx context.Context, next graphql.OperationHandler) graphql.ResponseHandler {
	handle := next(ctx)
	if handle == nil {
		return nil
	}
	// The span is read from the operation's own ctx: when an inner interceptor
	// refuses, gqlgen hands the response handler no ctx at all (innerCtx stays nil).
	operationCtx := ctx
	return func(ctx context.Context) *graphql.Response {
		response := handle(ctx)
		if response != nil && len(response.Errors) > 0 {
			httpapi.RecordGraphQLErrorCount(operationCtx, len(response.Errors))
		}
		return response
	}
}

// apiV1CatchAllNotFound answers every /api/v1/ path no registered route took:
// 404, and the span says why (a route that does not exist).
func apiV1CatchAllNotFound(w http.ResponseWriter, r *http.Request) {
	httpapi.RecordNotFoundCause(r.Context(), httpapi.NotFoundNoRoute)
	writeRESTError(w, r, "api_v1", "", http.StatusNotFound, "Not Found")
}
