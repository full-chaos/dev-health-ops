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
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	gqlhandler "github.com/99designs/gqlgen/graphql/handler"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/tracing"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
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

// newExecutableSchemaHandler constructs the gqlgen HTTP handler over the
// canonical SDL's generated schema. Kept separate from main() so a future
// integration test can exercise it directly without starting a real
// listener. Unused for now (see package doc) -- built here, not mounted,
// so `go build`/`go vet` catch a schema/resolver mismatch immediately
// rather than only once a later wave tries to wire it in.
func newExecutableSchemaHandler() http.Handler {
	schema := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}})
	return gqlhandler.NewDefaultServer(schema)
}

func addr() string {
	if v := os.Getenv("QUERY_API_ADDR"); v != "" {
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

func main() {
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
	logger := logging.NewJSON(os.Stdout, slog.LevelInfo)
	tracingComponent := tracing.InitWithServiceName(logger, otelServiceName)

	// Constructed to prove the schema/resolver pair builds and links
	// correctly (see newExecutableSchemaHandler's doc comment); not
	// mounted on any mux route in this Wave.
	_ = newExecutableSchemaHandler()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthzHandler())

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
	if routeCfg, ok := loadQueryRouteConfig(); ok {
		handlers, readyFn, cleanup, buildErr := buildQueryRoute(routeCfg)
		if buildErr != nil {
			log.Fatalf("query-api: build /query route: %v", buildErr)
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
		mountProofRoute(mux, handlers.Proof)
		ready = readyFn
		// CHAOS-4710 deliverable 3: the mount-confirmation log line used to
		// live here as a hand-typed, six-of-twelve literal (stale since
		// Wave 3 -- the real registration is all twelve of
		// newQueryHandler's digestByOperation keys). It is now emitted
		// from inside newQueryHandler itself (query_route.go), right next
		// to the digestByOperation map it describes, via
		// mountedRouteLogMessage -- see that function's doc comment for
		// why it cannot be a package-level function called from here
		// instead (cmd/query-api/tools/registrydump's AST parser requires
		// digestByOperation's composite literal to stay exactly where it
		// is, assigned directly, not returned from a helper).
	} else {
		log.Print("query-api: /query route not configured (CLICKHOUSE_URI/GO_API_REGISTRY_POSTGRES_URI/GO_API_ENVELOPE_* unset) -- staying Wave-0 empty")
		// Say so about the proof route too, with its own explicit zero.
		// Previously this branch logged nothing about /query/proof, so an
		// operator reading the log could not tell "the proof route is off"
		// from "nobody ever considered it" -- the same conflation the
		// registered/not-registered lines exist to prevent (codex r1 F8).
		mountProofRoute(mux, nil)
	}
	mux.HandleFunc("/readyz", readyzHandler(ready))

	// CHAOS-4977 step 5a: POST /api/v1/investment/explain, gated by its
	// own routeswitch entry (default OFF via GO_API_INVESTMENT_EXPLAIN_ENABLED)
	// -- see investment_explain_route.go's package doc comment for the
	// reachability story (5b: a separate Python-side REST forwarder,
	// not this file's job) and this route's documented scope gaps.
	if explainHandler, explainCleanup, explainOK, explainErr := buildInvestmentExplainRoute(); explainErr != nil {
		log.Fatalf("query-api: build /api/v1/investment/explain route: %v", explainErr)
	} else if explainOK {
		defer explainCleanup()
		mux.HandleFunc("/api/v1/investment/explain", explainHandler)
	} else {
		log.Print("query-api: /api/v1/investment/explain route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	// CHAOS-5550: GET /api/v1/quadrant, gated by its own routeswitch entry
	// (default OFF via GO_API_QUADRANT_ENABLED) -- see quadrant_route.go's
	// package doc comment for the reachability story and internal/quadrant
	// for the ported resolver and its documented developer/person scope gap.
	if quadrantHandler, quadrantCleanup, quadrantOK, quadrantErr := buildQuadrantRoute(); quadrantErr != nil {
		log.Fatalf("query-api: build /api/v1/quadrant route: %v", quadrantErr)
	} else if quadrantOK {
		defer quadrantCleanup()
		mux.HandleFunc("/api/v1/quadrant", quadrantHandler)
	} else {
		log.Print("query-api: /api/v1/quadrant route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
	}

	server := &http.Server{
		Addr:              addr(),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("query-api listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("query-api: listen error: %v", err)
		}
	}()

	<-ctx.Done()
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
}
