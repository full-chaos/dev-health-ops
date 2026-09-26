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
	"log"
	"net/http"
	"time"

	gqlhandler "github.com/99designs/gqlgen/graphql/handler"

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

// newListenerServers builds the public server and, when internalAddr is set,
// the internal one over the same handler (CHAOS-6780). The public server
// deletes the X-DH-Internal-* identity headers before any handler sees them;
// only the internal server, on a port no Ingress routes to, marks requests as
// allowed to carry them. internalAddr empty = no internal server (nil): the
// headers are honoured nowhere.
func newListenerServers(publicAddr, internalAddr string, base http.Handler) (public, internal *http.Server) {
	public = &http.Server{
		Addr:              publicAddr,
		Handler:           analytics.InvestmentMembershipScopeRequestMiddleware(internalidentity.Public(base)),
		ReadHeaderTimeout: 5 * time.Second,
	}
	if internalAddr != "" {
		internal = &http.Server{
			Addr:              internalAddr,
			Handler:           analytics.InvestmentMembershipScopeRequestMiddleware(internalidentity.Internal(base)),
			ReadHeaderTimeout: 5 * time.Second,
		}
	}
	return public, internal
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
	// The listeners (Listeners) add the identity middleware around it.
	Handler http.Handler
	// Ready is nil when /query is not configured (nothing to check, see
	// ReadinessCheck) and otherwise the live dependency check.
	Ready func(context.Context) error
	// Probes are the same checks, one per dependency class (nil when /query is not
	// configured), for a caller that reports each on its own.
	Probes []ReadinessProbe
	// Close releases every route's dependencies, last opened first.
	Close func()
}

// Build mounts the query plane. Every setting it, and every route builder, reads
// comes from get (the declared-settings reader of dho query-api); a route whose
// settings are absent stays unmounted, and a route that cannot be built is an error
// and nothing stays open.
func Build(get func(string) string) (*Plane, error) {
	getenv := getenvFunc(get)
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
	if routeCfg, ok := loadQueryRouteConfig(getenv); ok {
		handlers, readyFn, cleanup, buildErr := buildQueryRoute(getenv, routeCfg)
		if buildErr != nil {
			log.Printf("query-api: build /query route: %v", buildErr)
			return fail(buildErr)
		}
		cleanups = append(cleanups, cleanup)
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
		probes = handlers.Probes
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

	// CHAOS-4977 step 5a: POST /api/v1/investment/explain, gated by its
	// own routeswitch entry (default OFF via GO_API_INVESTMENT_EXPLAIN_ENABLED)
	// -- see investment_explain_route.go's package doc comment for the
	// reachability story (5b: a separate Python-side REST forwarder,
	// not this file's job) and this route's documented scope gaps.
	if explainHandler, explainCleanup, explainOK, explainErr := buildInvestmentExplainRoute(getenv); explainErr != nil {
		log.Printf("query-api: build /api/v1/investment/explain route: %v", explainErr)
		return fail(explainErr)
	} else if explainOK {
		cleanups = append(cleanups, explainCleanup)
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
		return fail(quadrantErr)
	} else if quadrantOK {
		cleanups = append(cleanups, quadrantCleanup)
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
		return fail(heatmapErr)
	} else if heatmapOK {
		cleanups = append(cleanups, heatmapCleanup)
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
		return fail(sankeyErr)
	} else if sankeyOK {
		cleanups = append(cleanups, sankeyCleanup)
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
		return fail(homeErr)
	} else if homeOK {
		cleanups = append(cleanups, homeCleanup)
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
		return fail(opportunitiesErr)
	} else if opportunitiesOK {
		cleanups = append(cleanups, opportunitiesCleanup)
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
		return fail(flowErr)
	} else if flowOK {
		cleanups = append(cleanups, flowCleanup)
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
		return fail(filterOptionsErr)
	} else if filterOptionsOK {
		cleanups = append(cleanups, filterOptionsCleanup)
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
		return fail(investmentErr)
	} else if investmentOK {
		cleanups = append(cleanups, investmentCleanup)
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
		return fail(investmentSunburstErr)
	} else if investmentSunburstOK {
		cleanups = append(cleanups, investmentSunburstCleanup)
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
		return fail(drilldownPRsErr)
	} else if drilldownPRsOK {
		cleanups = append(cleanups, drilldownPRsCleanup)
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
		return fail(workUnitsErr)
	} else if workUnitsOK {
		cleanups = append(cleanups, workUnitsCleanup)
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
		return fail(workUnitExplainErr)
	} else if workUnitExplainOK {
		cleanups = append(cleanups, workUnitExplainCleanup)
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
		return fail(drilldownIssuesErr)
	} else if drilldownIssuesOK {
		cleanups = append(cleanups, drilldownIssuesCleanup)
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
		return fail(peopleSearchErr)
	} else if peopleSearchOK {
		cleanups = append(cleanups, peopleSearchCleanup)
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
		return fail(peopleSummaryErr)
	} else if peopleSummaryOK {
		cleanups = append(cleanups, peopleSummaryCleanup)
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
		return fail(peopleMetricErr)
	} else if peopleMetricOK {
		cleanups = append(cleanups, peopleMetricCleanup)
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
		return fail(peopleDrilldownPRsErr)
	} else if peopleDrilldownPRsOK {
		cleanups = append(cleanups, peopleDrilldownPRsCleanup)
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
		return fail(peopleDrilldownIssuesErr)
	} else if peopleDrilldownIssuesOK {
		cleanups = append(cleanups, peopleDrilldownIssuesCleanup)
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
		return fail(metaErr)
	} else if metaOK {
		cleanups = append(cleanups, metaCleanup)
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
		return fail(explainRESTErr)
	} else if explainRESTOK {
		cleanups = append(cleanups, explainRESTCleanup)
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
		return fail(flameErr)
	} else if flameOK {
		cleanups = append(cleanups, flameCleanup)
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
		return fail(flameAggErr)
	} else if flameAggOK {
		cleanups = append(cleanups, flameAggCleanup)
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

	return &Plane{Handler: markResponseModelRoutes(mux), Ready: ready, Probes: probes, Close: closeAll}, nil
}
