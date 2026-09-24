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
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	"go.opentelemetry.io/otel"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/tracing"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
)

// otelServiceName is this binary's OTEL_SERVICE_NAME fallback (CHAOS-5408) --
// tracing.InitWithServiceName's default when the env var is unset, so
// query-api is distinguishable from the worker binaries (whose own default,
// "dev-health-ops", tracing.Init keeps) in a trace backend without every
// deployment needing to set OTEL_SERVICE_NAME by hand. The env var still
// wins whenever it is set.
const otelServiceName = "dev-health-query-api"

// tracingShutdownTimeout bounds the final flush of any buffered spans on
// process shutdown -- the same bound style readyzTimeout uses below, so a
// wedged exporter cannot hang the shutdown sequence indefinitely.
const tracingShutdownTimeout = 5 * time.Second

const defaultAddr = ":8090"

// usage is what -h/--help prints. query-api takes no arguments; every setting
// is an environment variable, read through the lookup Run is given.
const usage = `Usage: dho query-api

Serves the read-only Go query plane on QUERY_API_ADDR (default :8090):
/query, /registry, /buildinfo, the /api/v1 routes, /healthz, /readyz and
/metrics. It takes no arguments; each route is configured by its own
environment variables and stays unmounted until they are set.
`

// Exit codes Run returns, the same set every dho command uses.
const (
	exitOK      = 0
	exitFailure = 1
)

// getenvFunc reads one setting by name. Run builds it from the lookup the
// caller injects, so no route builder reads the process environment itself.
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

func addr(getenv getenvFunc) string {
	if v := getenv("QUERY_API_ADDR"); v != "" {
		return v
	}
	return defaultAddr
}

func healthzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}

// readyzTimeout bounds every dependency check readyzHandler runs. An
// unbounded readiness probe hangs whatever polls it (an orchestrator, a
// rollout gate, a load balancer health check) for as long as the
// dependency itself is wedged, which is strictly worse than a fast,
// definite "not ready" -- see readyzHandler's doc comment.
const readyzTimeout = 3 * time.Second

// readyzHandler reports whether THIS instance is fit to receive traffic
// -- distinct from healthzHandler's pure process-liveness (CHAOS-4512:
// the two must never collapse into one signal; a process that is alive
// but whose query dependencies are down is live, not ready).
//
// ready is nil when /query is not configured/mounted in this deployment
// (loadQueryRouteConfig's ok=false, main()'s Wave-0 "nothing mounted"
// shape -- see that call site's comment for what configures it out of
// this mode). That is a DELIBERATE, documented operating mode elsewhere
// in this codebase (this file's and query_route.go's own comments both
// treat an unconfigured environment as intentional, not a failure --
// "an operator who has not yet configured this service's dependencies
// must not be forced to also configure ClickHouse/Postgres/JWKS just to
// build or run the binary"), so this handler does not fail it: there is
// no /query dependency to check, so there is nothing to report as
// unreachable. It still answers distinctly (body text + the
// "not_configured" telemetry outcome below) rather than reading
// identically to a verified-healthy 200, per CHAOS-4512's explicit
// instruction not to let "no dependencies configured" silently read as
// "ready to serve" -- an operator or dashboard can tell the two apart
// even though both return 200.
//
// When ready is non-nil (/query IS mounted), this handler calls it with
// a bounded timeout on every request -- a LIVE check of ClickHouse and
// registry-Postgres reachability, not a cached result from process
// start. That is the actual CHAOS-4512 defect: buildQueryRoute's own
// eager ClickHouse ping only ever ran once, at startup, and
// pgxpool.New never pinged Postgres at all, so a dependency that failed
// or went unreachable after boot was invisible to this endpoint before
// this fix -- the process stayed up, /readyz kept answering 200
// unconditionally, and every real /query request then failed or 404'd
// against a rollout gate that believed the instance was healthy.
//
// CHAOS-4724: /readyz is UNAUTHENTICATED and this handler previously set
// no Content-Type (Go content-sniffed every response) and wrote the raw
// ready(ctx) error into the 503 body -- pgx/clickhouse-go dial errors
// render a host:port, and CheckJWKS's errors name
// GO_API_ENVELOPE_JWKS_PATH's filesystem path directly, so an
// unauthenticated caller could read either straight off the wire. Every
// response now sets an explicit text/plain Content-Type, and the
// unhealthy body carries only the failing dependency's CLASS
// ("clickhouse" / "postgres" / "jwks", see readyzDependencyError in
// query_route.go) -- fail-closed (503 stays 503) but not a detail leak.
// The full error -- everything Class deliberately leaves out -- still
// goes to the log line below, unredacted, so an operator can diagnose
// without shell access; that log line IS this fix's telemetry.
//
// The unhealthy branch's write carries a `nosemgrep` suppression for
// go.lang.security.audit.xss.no-direct-write-to-responsewriter: this is
// server-side plain-text (Content-Type set above), never HTML, and the
// concatenated value is now one of readyzDependencyClass's own closed-set
// literals -- not attacker-controlled input, and (after this CHAOS-4724
// fix) not even the underlying dependency error text anymore. Triaged and
// confirmed a false positive against the pre-fix code (Semgrep alert
// 2197) and re-confirmed against this fix's narrower body (alert 2199) --
// see the PR thread. Do not "fix" this by routing through html/template;
// that cargo-cults a scanner rule into a worse design for a plain-text
// health endpoint.
func readyzHandler(ready func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")

		if ready == nil {
			recordReadyzOutcome("not_configured")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready: /query not configured"))
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), readyzTimeout)
		defer cancel()
		if err := ready(ctx); err != nil {
			log.Printf("query-api: /readyz dependency check failed: %v", err)
			recordReadyzOutcome("unhealthy")
			w.WriteHeader(http.StatusServiceUnavailable)
			// nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter.no-direct-write-to-responsewriter
			_, _ = w.Write([]byte("not ready: " + readyzDependencyClass(err)))
			return
		}
		recordReadyzOutcome("healthy")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
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

// Run serves query-api until ctx ends or SIGINT/SIGTERM arrives, and returns
// the process exit code: 0 after a clean shutdown, 1 when a route cannot be
// built or the listener fails. query-api takes no arguments except -h/--help,
// which prints the usage; any other it is given are logged and ignored, as the
// binary always did. Logs go to stdout as JSON
// through the redacting handler, installed as the process default for the run
// and restored when Run returns.
//
// Where settings come from. Every setting query-api's own wiring reads -- the
// listener address, each route's ClickHouse/registry/envelope configuration,
// the route switches and the proof route -- comes from lookup. Three kinds of
// setting are still read from the process environment below this function,
// exactly as every other dho Service reads them: the OTEL_* tracing settings
// (internal/platform/tracing, the same path internal/platform/shell uses),
// and the per-request resolver settings IDENTITY_MAPPING_PATH, the LLM_*
// provider settings and two work-graph switches. TestDirectEnvironmentReads
// pins that set, so a new direct read cannot appear unnoticed. Every caller
// passes the process environment as lookup, so the two sources agree; moving
// the rest onto one option registry is the shell-lifecycle port.
func Run(ctx context.Context, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer) int {
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Fprint(stdout, usage)
		return exitOK
	}
	getenv := getenvFunc(func(key string) string {
		value, _ := lookup(key)
		return value
	})
	// CHAOS-5408: installs the process-wide OTel TracerProvider so the spans
	// the resolvers in internal/graph already start (org-scoping-rejection
	// spans included) actually reach a collector instead of the global no-op
	// provider every span in this binary silently fell into before this line
	// existed -- confirmed absent by grep across this whole tree prior to
	// this change. Fails soft on a bad/absent OTEL_* config (same contract
	// tracing.Init's own doc comment describes): a broken collector or
	// malformed env var never stops query-api from serving traffic, it just
	// leaves tracing disabled. Started before anything else so no early
	// resolver call can race an uninitialised global provider; shut down
	// last, after the HTTP server has stopped accepting requests, so
	// buffered spans from the final in-flight requests still flush.
	logger := logging.NewJSON(stdout, slog.LevelInfo)
	restoreDefaultLogger := logging.InstallDefault(logger)
	defer restoreDefaultLogger()
	if len(args) != 0 {
		logger.Warn("query-api takes no arguments; ignoring them", "arguments", args)
	}
	tracingComponent := tracing.InitWithServiceName(logger, otelServiceName)

	// Installs the process-wide OTel MeterProvider so the gauges/counters
	// registry_drift_telemetry.go, readyz_telemetry.go, and
	// internal/routeswitch/telemetry.go already create via otel.Meter(...)
	// actually record somewhere, and mounts the Prometheus text they
	// collect at /metrics. Fails soft, matching tracing.InitWithServiceName
	// just above: a broken exporter must not stop query-api from serving
	// traffic, only leave /metrics unavailable.
	var metricsHTTPHandler http.Handler
	if meterProvider, promRegistry, err := newPrometheusMeterProvider(); err != nil {
		log.Printf("query-api: build Prometheus meter provider: %v -- /metrics will 404", err)
	} else {
		otel.SetMeterProvider(meterProvider)
		metricsHTTPHandler = metricsHandler(promRegistry)
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), tracingShutdownTimeout)
			defer cancel()
			if shutdownErr := meterProvider.Shutdown(shutdownCtx); shutdownErr != nil {
				log.Printf("query-api: meter provider shutdown error: %v", shutdownErr)
			}
		}()
	}

	// Constructed to prove the schema/resolver pair builds and links
	// correctly (see newExecutableSchemaHandler's doc comment); not
	// mounted on any mux route in this Wave.
	_ = newExecutableSchemaHandler()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthzHandler())
	if metricsHTTPHandler != nil {
		mux.Handle("/metrics", metricsHTTPHandler)
	}

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
	if routeCfg, ok := loadQueryRouteConfig(getenv); ok {
		handlers, readyFn, cleanup, buildErr := buildQueryRoute(getenv, routeCfg)
		if buildErr != nil {
			log.Printf("query-api: build /query route: %v", buildErr)
			return exitFailure
		}
		defer cleanup()
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
		ready = readyFn
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
	}
	mux.HandleFunc("/readyz", readyzHandler(ready))

	// CHAOS-4977 step 5a: POST /api/v1/investment/explain, gated by its
	// own routeswitch entry (default OFF via GO_API_INVESTMENT_EXPLAIN_ENABLED)
	// -- see investment_explain_route.go's package doc comment for the
	// reachability story (5b: a separate Python-side REST forwarder,
	// not this file's job) and this route's documented scope gaps.
	if explainHandler, explainCleanup, explainOK, explainErr := buildInvestmentExplainRoute(getenv); explainErr != nil {
		log.Printf("query-api: build /api/v1/investment/explain route: %v", explainErr)
		return exitFailure
	} else if explainOK {
		defer explainCleanup()
		// Wrapped in withProofProvenance so go-api-rest-prove can bind a
		// receipt to the process that actually served this request, the
		// same reason /query and /query/proof carry it. Reassigned, not
		// inlined into the mux.HandleFunc call below:
		// restendpoints.go's LoadQueryAPIMuxRoutes mechanically parses
		// this file for `mux.HandleFunc("/api/v1/...", <bareIdentifier>)`
		// and cannot resolve a call expression in the handler position --
		// inlining the wrapper here would silently render this route
		// "python-only" on the migration matrix page.
		explainHandler = withProofProvenance(explainHandler, runningBuild())
		mux.HandleFunc("/api/v1/investment/explain", explainHandler)
	} else {
		log.Print("query-api: /api/v1/investment/explain route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// CHAOS-5550: GET /api/v1/quadrant, gated by its own routeswitch entry
	// (default OFF via GO_API_QUADRANT_ENABLED) -- see quadrant_route.go's
	// package doc comment for the reachability story and internal/quadrant
	// for the ported resolver and its documented developer/person scope gap.
	if quadrantHandler, quadrantCleanup, quadrantOK, quadrantErr := buildQuadrantRoute(getenv); quadrantErr != nil {
		log.Printf("query-api: build /api/v1/quadrant route: %v", quadrantErr)
		return exitFailure
	} else if quadrantOK {
		defer quadrantCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		quadrantHandler = withProofProvenance(quadrantHandler, runningBuild())
		mux.HandleFunc("/api/v1/quadrant", quadrantHandler)
	} else {
		log.Print("query-api: /api/v1/quadrant route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// GET /api/v1/heatmap, gated by its own routeswitch entry
	// (default OFF via GO_API_HEATMAP_ENABLED) -- see heatmap_route.go's
	// package doc comment for the reachability story and internal/heatmap
	// for the ported resolver.
	if heatmapHandler, heatmapCleanup, heatmapOK, heatmapErr := buildHeatmapRoute(getenv); heatmapErr != nil {
		log.Printf("query-api: build /api/v1/heatmap route: %v", heatmapErr)
		return exitFailure
	} else if heatmapOK {
		defer heatmapCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		heatmapHandler = withProofProvenance(heatmapHandler, runningBuild())
		mux.HandleFunc("/api/v1/heatmap", heatmapHandler)
	} else {
		log.Print("query-api: /api/v1/heatmap route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// GET+POST /api/v1/sankey, gated by its own routeswitch entries
	// (default OFF via GO_API_SANKEY_ENABLED) -- see sankey_route.go's
	// package doc comment for the reachability story and internal/sankey
	// for the ported resolver and its declared ReplacingMergeTree-dedup
	// notes.
	if sankeyHandler, sankeyCleanup, sankeyOK, sankeyErr := buildSankeyRoute(getenv); sankeyErr != nil {
		log.Printf("query-api: build /api/v1/sankey route: %v", sankeyErr)
		return exitFailure
	} else if sankeyOK {
		defer sankeyCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		sankeyHandler = withProofProvenance(sankeyHandler, runningBuild())
		mux.HandleFunc("/api/v1/sankey", sankeyHandler)
	} else {
		log.Print("query-api: /api/v1/sankey route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// GET+POST /api/v1/home, gated by its own routeswitch entries
	// (default OFF via GO_API_HOME_ENABLED) -- see home_route.go's
	// package doc comment for the reachability story and internal/home
	// for the ported resolver and its declared ReplacingMergeTree-dedup
	// notes.
	if homeHandler, homeCleanup, homeOK, homeErr := buildHomeRoute(getenv); homeErr != nil {
		log.Printf("query-api: build /api/v1/home route: %v", homeErr)
		return exitFailure
	} else if homeOK {
		defer homeCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		homeHandler = withProofProvenance(homeHandler, runningBuild())
		mux.HandleFunc("/api/v1/home", homeHandler)
	} else {
		log.Print("query-api: /api/v1/home route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_*/GO_API_REGISTRY_POSTGRES_URI unset) -- staying unmounted")
	}

	// GET+POST /api/v1/opportunities, gated by its own routeswitch entries
	// (default OFF via GO_API_OPPORTUNITIES_ENABLED) -- see
	// opportunities_route.go's package doc comment for the reachability
	// story and internal/opportunities for the ported card-building
	// logic, which composes internal/home's own exported builder rather
	// than reading any table of its own.
	if opportunitiesHandler, opportunitiesCleanup, opportunitiesOK, opportunitiesErr := buildOpportunitiesRoute(getenv); opportunitiesErr != nil {
		log.Printf("query-api: build /api/v1/opportunities route: %v", opportunitiesErr)
		return exitFailure
	} else if opportunitiesOK {
		defer opportunitiesCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		opportunitiesHandler = withProofProvenance(opportunitiesHandler, runningBuild())
		mux.HandleFunc("/api/v1/opportunities", opportunitiesHandler)
	} else {
		log.Print("query-api: /api/v1/opportunities route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// POST /api/v1/investment/flow and POST /api/v1/investment/flow/
	// repo-team, gated by one shared routeswitch toggle (default OFF via
	// GO_API_INVESTMENT_FLOW_ENABLED) -- see investment_flow_route.go's
	// package doc comment for the reachability story and
	// internal/investmentflow for the ported builders, their dynamic
	// coverage-driven mode decision, and the declared ReplacingMergeTree
	// dedup notes.
	if flowHandler, flowRepoTeamHandler, flowCleanup, flowOK, flowErr := buildInvestmentFlowRoute(getenv); flowErr != nil {
		log.Printf("query-api: build /api/v1/investment/flow routes: %v", flowErr)
		return exitFailure
	} else if flowOK {
		defer flowCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		flowHandler = withProofProvenance(flowHandler, runningBuild())
		flowRepoTeamHandler = withProofProvenance(flowRepoTeamHandler, runningBuild())
		mux.HandleFunc("/api/v1/investment/flow", flowHandler)
		mux.HandleFunc("/api/v1/investment/flow/repo-team", flowRepoTeamHandler)
	} else {
		log.Print("query-api: /api/v1/investment/flow routes not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// GET /api/v1/filters/options, gated by its own routeswitch
	// entry (default OFF via GO_API_FILTER_OPTIONS_ENABLED) -- see
	// filter_options_route.go's package doc comment for the reachability
	// story and internal/filteroptions for the ported reader and its
	// declared ReplacingMergeTree dedup fixes.
	if filterOptionsHandler, filterOptionsCleanup, filterOptionsOK, filterOptionsErr := buildFilterOptionsRoute(getenv); filterOptionsErr != nil {
		log.Printf("query-api: build /api/v1/filters/options route: %v", filterOptionsErr)
		return exitFailure
	} else if filterOptionsOK {
		defer filterOptionsCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		filterOptionsHandler = withProofProvenance(filterOptionsHandler, runningBuild())
		mux.HandleFunc("/api/v1/filters/options", filterOptionsHandler)
	} else {
		log.Print("query-api: /api/v1/filters/options route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// GET+POST /api/v1/investment, gated by its own routeswitch entries
	// (default OFF via GO_API_INVESTMENT_ENABLED) -- see
	// investment_route.go's package doc comment for the reachability
	// story and internal/investment for the ported builders and their
	// declared ReplacingMergeTree-dedup/membership-scope notes.
	if investmentHandler, investmentCleanup, investmentOK, investmentErr := buildInvestmentRoute(getenv); investmentErr != nil {
		log.Printf("query-api: build /api/v1/investment route: %v", investmentErr)
		return exitFailure
	} else if investmentOK {
		defer investmentCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		investmentHandler = withProofProvenance(investmentHandler, runningBuild())
		mux.HandleFunc("/api/v1/investment", investmentHandler)
	} else {
		log.Print("query-api: /api/v1/investment route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// GET /api/v1/investment/sunburst, gated by its own routeswitch entry
	// (default OFF via GO_API_INVESTMENT_SUNBURST_ENABLED) -- see
	// investment_route.go's package doc comment and internal/investment
	// for the ported resolver.
	if investmentSunburstHandler, investmentSunburstCleanup, investmentSunburstOK, investmentSunburstErr := buildInvestmentSunburstRoute(getenv); investmentSunburstErr != nil {
		log.Printf("query-api: build /api/v1/investment/sunburst route: %v", investmentSunburstErr)
		return exitFailure
	} else if investmentSunburstOK {
		defer investmentSunburstCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		investmentSunburstHandler = withProofProvenance(investmentSunburstHandler, runningBuild())
		mux.HandleFunc("/api/v1/investment/sunburst", investmentSunburstHandler)
	} else {
		log.Print("query-api: /api/v1/investment/sunburst route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// GET+POST /api/v1/drilldown/prs, gated by its own routeswitch
	// entries (default OFF via GO_API_DRILLDOWN_PRS_ENABLED)
	// -- see drilldown_prs_route.go's package doc comment for the
	// reachability story and internal/drilldown for the ported resolver
	// and its documented ReplacingMergeTree-dedup/org-scope notes.
	if drilldownPRsHandler, drilldownPRsCleanup, drilldownPRsOK, drilldownPRsErr := buildDrilldownPRsRoute(getenv); drilldownPRsErr != nil {
		log.Printf("query-api: build /api/v1/drilldown/prs route: %v", drilldownPRsErr)
		return exitFailure
	} else if drilldownPRsOK {
		defer drilldownPRsCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		drilldownPRsHandler = withProofProvenance(drilldownPRsHandler, runningBuild())
		mux.HandleFunc("/api/v1/drilldown/prs", drilldownPRsHandler)
	} else {
		log.Print("query-api: /api/v1/drilldown/prs route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// GET+POST /api/v1/work-units, gated by its own routeswitch entries
	// (default OFF via GO_API_WORK_UNITS_ENABLED) -- see
	// workunits_route.go's package doc comment for the reachability story;
	// business logic is internal/investmentexplain's own
	// BuildWorkUnitInvestments, shared with POST /api/v1/investment/explain.
	if workUnitsHandler, workUnitsCleanup, workUnitsOK, workUnitsErr := buildWorkUnitsRoute(getenv); workUnitsErr != nil {
		log.Printf("query-api: build /api/v1/work-units route: %v", workUnitsErr)
		return exitFailure
	} else if workUnitsOK {
		defer workUnitsCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		workUnitsHandler = withProofProvenance(workUnitsHandler, runningBuild())
		mux.HandleFunc("/api/v1/work-units", workUnitsHandler)
	} else {
		log.Print("query-api: /api/v1/work-units route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// POST /api/v1/work-units/{work_unit_id}/explain, gated by its own
	// routeswitch entry (default OFF via
	// GO_API_WORK_UNIT_EXPLAIN_ENABLED) -- see
	// workunit_explain_route.go's package doc comment for the
	// reachability story, why this LLM route answers one JSON body rather
	// than a keep-alive stream, and the rate-limiting gap it shares with
	// every other ported route. The path pattern carries its
	// {work_unit_id} wildcard, the same mechanism the people routes use.
	if workUnitExplainHandler, workUnitExplainCleanup, workUnitExplainOK, workUnitExplainErr := buildWorkUnitExplainRoute(getenv); workUnitExplainErr != nil {
		log.Printf("query-api: build /api/v1/work-units/{work_unit_id}/explain route: %v", workUnitExplainErr)
		return exitFailure
	} else if workUnitExplainOK {
		defer workUnitExplainCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		workUnitExplainHandler = withProofProvenance(workUnitExplainHandler, runningBuild())
		mux.HandleFunc("/api/v1/work-units/{work_unit_id}/explain", workUnitExplainHandler)
	} else {
		log.Print("query-api: /api/v1/work-units/{work_unit_id}/explain route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_*/GO_API_REGISTRY_POSTGRES_URI unset) -- staying unmounted")
	}

	// GET+POST /api/v1/drilldown/issues, gated by its own routeswitch
	// entries (default OFF via GO_API_DRILLDOWN_ISSUES_ENABLED)
	// -- see drilldown_issues_route.go's package doc comment for the
	// reachability story and internal/drilldown/issues.go for the ported
	// resolver and its documented ReplacingMergeTree-dedup/org-scope notes.
	if drilldownIssuesHandler, drilldownIssuesCleanup, drilldownIssuesOK, drilldownIssuesErr := buildDrilldownIssuesRoute(getenv); drilldownIssuesErr != nil {
		log.Printf("query-api: build /api/v1/drilldown/issues route: %v", drilldownIssuesErr)
		return exitFailure
	} else if drilldownIssuesOK {
		defer drilldownIssuesCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		drilldownIssuesHandler = withProofProvenance(drilldownIssuesHandler, runningBuild())
		mux.HandleFunc("/api/v1/drilldown/issues", drilldownIssuesHandler)
	} else {
		log.Print("query-api: /api/v1/drilldown/issues route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// GET /api/v1/people, gated by its own routeswitch entry (default OFF
	// via GO_API_PEOPLE_SEARCH_ENABLED) -- see people_route.go's package
	// doc comment for the reachability story and internal/people for the
	// ported resolver and its documented ReplacingMergeTree-dedup notes.
	if peopleSearchHandler, peopleSearchCleanup, peopleSearchOK, peopleSearchErr := buildPeopleSearchRoute(getenv); peopleSearchErr != nil {
		log.Printf("query-api: build /api/v1/people route: %v", peopleSearchErr)
		return exitFailure
	} else if peopleSearchOK {
		defer peopleSearchCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		peopleSearchHandler = withProofProvenance(peopleSearchHandler, runningBuild())
		mux.HandleFunc("/api/v1/people", peopleSearchHandler)
	} else {
		log.Print("query-api: /api/v1/people route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// GET /api/v1/people/{person_id}/summary, gated by its own routeswitch
	// entry (default OFF via GO_API_PEOPLE_SUMMARY_ENABLED) -- see
	// people_summary_route.go's package doc comment for the reachability
	// story, the path-parameter mechanism and internal/people for the
	// ported resolver.
	if peopleSummaryHandler, peopleSummaryCleanup, peopleSummaryOK, peopleSummaryErr := buildPeopleSummaryRoute(getenv); peopleSummaryErr != nil {
		log.Printf("query-api: build %s route: %v", peopleSummaryPath, peopleSummaryErr)
		return exitFailure
	} else if peopleSummaryOK {
		defer peopleSummaryCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		peopleSummaryHandler = withProofProvenance(peopleSummaryHandler, runningBuild())
		mux.HandleFunc("/api/v1/people/{person_id}/summary", peopleSummaryHandler)
	} else {
		log.Printf("query-api: %s route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted", peopleSummaryPath)
	}

	// GET /api/v1/people/{person_id}/metric, gated by its own routeswitch
	// entry (default OFF via GO_API_PEOPLE_METRIC_ENABLED) -- see
	// people_metric_route.go's package doc comment for the reachability
	// story and internal/people for the ported resolver.
	if peopleMetricHandler, peopleMetricCleanup, peopleMetricOK, peopleMetricErr := buildPeopleMetricRoute(getenv); peopleMetricErr != nil {
		log.Printf("query-api: build %s route: %v", peopleMetricPath, peopleMetricErr)
		return exitFailure
	} else if peopleMetricOK {
		defer peopleMetricCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		peopleMetricHandler = withProofProvenance(peopleMetricHandler, runningBuild())
		mux.HandleFunc("/api/v1/people/{person_id}/metric", peopleMetricHandler)
	} else {
		log.Printf("query-api: %s route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted", peopleMetricPath)
	}

	// GET /api/v1/people/{person_id}/drilldown/prs, gated by its own
	// routeswitch entry (default OFF via GO_API_PEOPLE_DRILLDOWN_PRS_ENABLED)
	// -- see people_drilldown_prs_route.go's package doc comment for the
	// reachability story and internal/people/drilldownprs.go for the ported
	// resolver.
	if peopleDrilldownPRsHandler, peopleDrilldownPRsCleanup, peopleDrilldownPRsOK, peopleDrilldownPRsErr := buildPeopleDrilldownPRsRoute(getenv); peopleDrilldownPRsErr != nil {
		log.Printf("query-api: build %s route: %v", peopleDrilldownPRsPath, peopleDrilldownPRsErr)
		return exitFailure
	} else if peopleDrilldownPRsOK {
		defer peopleDrilldownPRsCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		peopleDrilldownPRsHandler = withProofProvenance(peopleDrilldownPRsHandler, runningBuild())
		mux.HandleFunc("/api/v1/people/{person_id}/drilldown/prs", peopleDrilldownPRsHandler)
	} else {
		log.Printf("query-api: %s route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted", peopleDrilldownPRsPath)
	}

	// GET /api/v1/people/{person_id}/drilldown/issues, gated by its own
	// routeswitch entry (default OFF via
	// GO_API_PEOPLE_DRILLDOWN_ISSUES_ENABLED) -- see
	// people_drilldown_issues_route.go's package doc comment for the
	// reachability story and internal/people/drilldownissues.go for the
	// ported resolver.
	if peopleDrilldownIssuesHandler, peopleDrilldownIssuesCleanup, peopleDrilldownIssuesOK, peopleDrilldownIssuesErr := buildPeopleDrilldownIssuesRoute(getenv); peopleDrilldownIssuesErr != nil {
		log.Printf("query-api: build %s route: %v", peopleDrilldownIssuesPath, peopleDrilldownIssuesErr)
		return exitFailure
	} else if peopleDrilldownIssuesOK {
		defer peopleDrilldownIssuesCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		peopleDrilldownIssuesHandler = withProofProvenance(peopleDrilldownIssuesHandler, runningBuild())
		mux.HandleFunc("/api/v1/people/{person_id}/drilldown/issues", peopleDrilldownIssuesHandler)
	} else {
		log.Printf("query-api: %s route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted", peopleDrilldownIssuesPath)
	}

	// GET /api/v1/meta, gated by its own routeswitch entry (default OFF via
	// GO_API_META_ENABLED) -- see meta_route.go's package doc comment for
	// the reachability story and internal/meta for the ported handler.
	// Unlike every sibling route above, this one carries no envelope
	// verifier dependency: main.py's meta() route is public (see
	// meta_route.go's doc comment for the citation trail), so only
	// CLICKHOUSE_URI gates whether it mounts.
	if metaHandler, metaCleanup, metaOK, metaErr := buildMetaRoute(getenv); metaErr != nil {
		log.Printf("query-api: build /api/v1/meta route: %v", metaErr)
		return exitFailure
	} else if metaOK {
		defer metaCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		metaHandler = withProofProvenance(metaHandler, runningBuild())
		mux.HandleFunc("/api/v1/meta", metaHandler)
	} else {
		log.Print("query-api: /api/v1/meta route not configured (CLICKHOUSE_URI unset) -- staying unmounted")
	}

	// GET+POST /api/v1/explain, gated by its own routeswitch entries
	// (default OFF via GO_API_EXPLAIN_ENABLED) -- see explain_route.go's
	// package doc comment for the reachability story and
	// internal/explain for the ported resolver and its documented
	// ReplacingMergeTree-dedup/org-scope notes.
	if explainRESTHandler, explainRESTCleanup, explainRESTOK, explainRESTErr := buildExplainRoute(getenv); explainRESTErr != nil {
		log.Printf("query-api: build /api/v1/explain route: %v", explainRESTErr)
		return exitFailure
	} else if explainRESTOK {
		defer explainRESTCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		explainRESTHandler = withProofProvenance(explainRESTHandler, runningBuild())
		mux.HandleFunc("/api/v1/explain", explainRESTHandler)
	} else {
		log.Print("query-api: /api/v1/explain route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// GET /api/v1/flame, gated by its own routeswitch entry
	// (default OFF via GO_API_FLAME_ENABLED) -- see flame_route.go's
	// package doc comment for the reachability story and internal/flame
	// for the ported resolver and its documented ReplacingMergeTree-dedup
	// notes.
	if flameHandler, flameCleanup, flameOK, flameErr := buildFlameRoute(getenv); flameErr != nil {
		log.Printf("query-api: build /api/v1/flame route: %v", flameErr)
		return exitFailure
	} else if flameOK {
		defer flameCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		flameHandler = withProofProvenance(flameHandler, runningBuild())
		mux.HandleFunc("/api/v1/flame", flameHandler)
	} else {
		log.Print("query-api: /api/v1/flame route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// GET /api/v1/flame/aggregated, gated by its own routeswitch entry
	// (default OFF via GO_API_FLAME_AGGREGATED_ENABLED) -- see
	// flame_aggregated_route.go's package doc comment for the
	// reachability story and internal/aggflame for the ported resolver
	// and its documented ReplacingMergeTree-dedup notes.
	if flameAggHandler, flameAggCleanup, flameAggOK, flameAggErr := buildFlameAggregatedRoute(getenv); flameAggErr != nil {
		log.Printf("query-api: build /api/v1/flame/aggregated route: %v", flameAggErr)
		return exitFailure
	} else if flameAggOK {
		defer flameAggCleanup()
		// See the investment/explain mount above for why this is a
		// reassignment, not an inlined wrapper.
		flameAggHandler = withProofProvenance(flameAggHandler, runningBuild())
		mux.HandleFunc("/api/v1/flame/aggregated", flameAggHandler)
	} else {
		log.Print("query-api: /api/v1/flame/aggregated route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
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
	mux.HandleFunc("/api/v1/", func(w http.ResponseWriter, r *http.Request) {
		writeRESTError(w, r, "api_v1", "", http.StatusNotFound, "Not Found")
	})

	server := &http.Server{
		Addr:              addr(getenv),
		Handler:           analytics.InvestmentMembershipScopeRequestMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	listenErr := make(chan error, 1)
	go func() {
		log.Printf("query-api listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listenErr <- err
		}
	}()

	code := exitOK
	select {
	case <-ctx.Done():
	case err := <-listenErr:
		log.Printf("query-api: listen error: %v", err)
		code = exitFailure
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("query-api: graceful shutdown error: %v", err)
	}

	// Shut down tracing LAST, after the server has stopped accepting new
	// requests -- flushes any spans still buffered from the final in-flight
	// requests. A no-op on a disabled/never-installed component (see
	// tracing.Component.Shutdown's own doc comment).
	tracingShutdownCtx, tracingCancel := context.WithTimeout(context.Background(), tracingShutdownTimeout)
	defer tracingCancel()
	if err := tracingComponent.Shutdown(tracingShutdownCtx); err != nil {
		log.Printf("query-api: tracing shutdown error: %v", err)
	}
	return code
}
