package server

import (
	"log"
	"net/http"
)

// restMount is one ServeMux pattern a restGroup mounts and the HTTP methods the
// handler behind it serves (CHAOS-8307). The mux registration is path-only (net/http
// ServeMux patterns written without a method), and every handler answers any other
// method with its own refusal (writeRESTMethodNotAllowed), so the method set is
// DECLARED here, read from each handler's own method check, and pinned by
// rest_routes_test.go: a handler that serves a method this row does not declare, or
// refuses one it does, fails that test.
type restMount struct {
	Pattern string
	Methods []string
}

// restGroup is one buildXRoute of the query-api REST surface: the builder (kept
// untouched), the log lines it has always written, and the mounts its handlers get,
// in the order Build returns the handlers.
type restGroup struct {
	// Builder and File name the function and the file that build the route, the
	// citation internal/migrationmatrix prints (a test pins that the symbol exists).
	Builder string
	File    string
	Mounts  []restMount
	// LogBuildError writes the log line of a failed build (the builder's own error).
	LogBuildError func(err error)
	// LogNotConfigured writes the "staying unmounted" line of a route whose switch or
	// dependencies are absent.
	LogNotConfigured func()
	// Build runs the existing builder and returns one handler per Mount.
	Build func(getenv getenvFunc, edgeUsers edgeUserStore) (handlers []http.HandlerFunc, cleanup func(), ok bool, err error)
}

// RESTRoute is one (method, pattern) pair the query-api REST surface serves, with the
// builder that mounts it.
type RESTRoute struct {
	Method  string
	Pattern string
	Builder string
	File    string
}

// RESTRoutes is the query-api REST route set, walked from the production table
// BuildWithLookup iterates (no dependency is built). The /api/v1/ catch-all 404 is not
// a route and is not listed.
func RESTRoutes() []RESTRoute {
	var routes []RESTRoute
	for _, group := range restGroups {
		for _, mount := range group.Mounts {
			for _, method := range mount.Methods {
				routes = append(routes, RESTRoute{Method: method, Pattern: mount.Pattern, Builder: group.Builder, File: group.File})
			}
		}
	}
	return routes
}

// restGroups is the production REST route table, in the order BuildWithLookup mounts
// the routes (cleanup order follows it). Every route is gated by its own routeswitch
// entry and mounts only when its builder reports ok; otherwise it stays unmounted and
// the /api/v1/ catch-all answers its path with the REST 404.
var restGroups = []restGroup{
	// CHAOS-4977 step 5a: POST /api/v1/investment/explain, gated by its
	// own routeswitch entry (default OFF via GO_API_INVESTMENT_EXPLAIN_ENABLED)
	// -- see investment_explain_route.go's package doc comment for the
	// reachability story (5b: a separate Python-side REST forwarder,
	// not this file's job) and this route's documented scope gaps.
	{
		Builder: "buildInvestmentExplainRoute",
		File:    "investment_explain_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/investment/explain", Methods: []string{http.MethodPost}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/investment/explain route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/investment/explain route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildInvestmentExplainRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// CHAOS-5550: GET /api/v1/quadrant, gated by its own routeswitch entry
	// (default OFF via GO_API_QUADRANT_ENABLED) -- see quadrant_route.go's
	// package doc comment for the reachability story and internal/quadrant
	// for the ported resolver and its documented developer/person scope gap.
	{
		Builder: "buildQuadrantRoute",
		File:    "quadrant_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/quadrant", Methods: []string{http.MethodGet}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/quadrant route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/quadrant route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildQuadrantRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET /api/v1/heatmap, gated by its own routeswitch entry
	// (default OFF via GO_API_HEATMAP_ENABLED) -- see heatmap_route.go's
	// package doc comment for the reachability story and internal/heatmap
	// for the ported resolver.
	{
		Builder: "buildHeatmapRoute",
		File:    "heatmap_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/heatmap", Methods: []string{http.MethodGet}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/heatmap route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/heatmap route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildHeatmapRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET+POST /api/v1/sankey, gated by its own routeswitch entries
	// (default OFF via GO_API_SANKEY_ENABLED) -- see sankey_route.go's
	// package doc comment for the reachability story and internal/sankey
	// for the ported resolver and its declared ReplacingMergeTree-dedup
	// notes.
	{
		Builder: "buildSankeyRoute",
		File:    "sankey_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/sankey", Methods: []string{http.MethodGet, http.MethodPost}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/sankey route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/sankey route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildSankeyRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET+POST /api/v1/home, gated by its own routeswitch entries
	// (default OFF via GO_API_HOME_ENABLED) -- see home_route.go's
	// package doc comment for the reachability story and internal/home
	// for the ported resolver and its declared ReplacingMergeTree-dedup
	// notes.
	{
		Builder: "buildHomeRoute",
		File:    "home_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/home", Methods: []string{http.MethodGet, http.MethodPost}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/home route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/home route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_*/GO_API_REGISTRY_POSTGRES_URI unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildHomeRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET+POST /api/v1/opportunities, gated by its own routeswitch entries
	// (default OFF via GO_API_OPPORTUNITIES_ENABLED) -- see
	// opportunities_route.go's package doc comment for the reachability
	// story and internal/opportunities for the ported card-building
	// logic, which composes internal/home's own exported builder rather
	// than reading any table of its own.
	{
		Builder: "buildOpportunitiesRoute",
		File:    "opportunities_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/opportunities", Methods: []string{http.MethodGet, http.MethodPost}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/opportunities route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/opportunities route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildOpportunitiesRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// POST /api/v1/investment/flow and POST /api/v1/investment/flow/
	// repo-team, gated by one shared routeswitch toggle (default OFF via
	// GO_API_INVESTMENT_FLOW_ENABLED) -- see investment_flow_route.go's
	// package doc comment for the reachability story and
	// internal/investmentflow for the ported builders, their dynamic
	// coverage-driven mode decision, and the declared ReplacingMergeTree
	// dedup notes.
	{
		Builder: "buildInvestmentFlowRoute",
		File:    "investment_flow_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/investment/flow", Methods: []string{http.MethodPost}},
			{Pattern: "/api/v1/investment/flow/repo-team", Methods: []string{http.MethodPost}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/investment/flow routes: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/investment/flow routes not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, h1, cleanup, ok, err := buildInvestmentFlowRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0, h1}, cleanup, ok, err
		},
	},
	// GET /api/v1/filters/options, gated by its own routeswitch
	// entry (default OFF via GO_API_FILTER_OPTIONS_ENABLED) -- see
	// filter_options_route.go's package doc comment for the reachability
	// story and internal/filteroptions for the ported reader and its
	// declared ReplacingMergeTree dedup fixes.
	{
		Builder: "buildFilterOptionsRoute",
		File:    "filter_options_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/filters/options", Methods: []string{http.MethodGet}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/filters/options route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/filters/options route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildFilterOptionsRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET+POST /api/v1/investment, gated by its own routeswitch entries
	// (default OFF via GO_API_INVESTMENT_ENABLED) -- see
	// investment_route.go's package doc comment for the reachability
	// story and internal/investment for the ported builders and their
	// declared ReplacingMergeTree-dedup/membership-scope notes.
	{
		Builder: "buildInvestmentRoute",
		File:    "investment_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/investment", Methods: []string{http.MethodGet, http.MethodPost}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/investment route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/investment route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildInvestmentRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET /api/v1/investment/sunburst, gated by its own routeswitch entry
	// (default OFF via GO_API_INVESTMENT_SUNBURST_ENABLED) -- see
	// investment_route.go's package doc comment and internal/investment
	// for the ported resolver.
	{
		Builder: "buildInvestmentSunburstRoute",
		File:    "investment_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/investment/sunburst", Methods: []string{http.MethodGet}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/investment/sunburst route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/investment/sunburst route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildInvestmentSunburstRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET+POST /api/v1/drilldown/prs, gated by its own routeswitch
	// entries (default OFF via GO_API_DRILLDOWN_PRS_ENABLED)
	// -- see drilldown_prs_route.go's package doc comment for the
	// reachability story and internal/drilldown for the ported resolver
	// and its documented ReplacingMergeTree-dedup/org-scope notes.
	{
		Builder: "buildDrilldownPRsRoute",
		File:    "drilldown_prs_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/drilldown/prs", Methods: []string{http.MethodGet, http.MethodPost}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/drilldown/prs route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/drilldown/prs route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildDrilldownPRsRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET+POST /api/v1/work-units, gated by its own routeswitch entries
	// (default OFF via GO_API_WORK_UNITS_ENABLED) -- see
	// workunits_route.go's package doc comment for the reachability story;
	// business logic is internal/investmentexplain's own
	// BuildWorkUnitInvestments, shared with POST /api/v1/investment/explain.
	{
		Builder: "buildWorkUnitsRoute",
		File:    "workunits_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/work-units", Methods: []string{http.MethodGet, http.MethodPost}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/work-units route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/work-units route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildWorkUnitsRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// POST /api/v1/work-units/{work_unit_id}/explain, gated by its own
	// routeswitch entry (default OFF via
	// GO_API_WORK_UNIT_EXPLAIN_ENABLED) -- see
	// workunit_explain_route.go's package doc comment for the
	// reachability story, why this LLM route answers one JSON body rather
	// than a keep-alive stream, and the rate-limiting gap it shares with
	// every other ported route. The path pattern carries its
	// {work_unit_id} wildcard, the same mechanism the people routes use.
	{
		Builder: "buildWorkUnitExplainRoute",
		File:    "workunit_explain_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/work-units/{work_unit_id}/explain", Methods: []string{http.MethodPost}},
		},
		LogBuildError: func(err error) {
			log.Printf("query-api: build /api/v1/work-units/{work_unit_id}/explain route: %v", err)
		},
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/work-units/{work_unit_id}/explain route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_*/GO_API_REGISTRY_POSTGRES_URI unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildWorkUnitExplainRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET+POST /api/v1/drilldown/issues, gated by its own routeswitch
	// entries (default OFF via GO_API_DRILLDOWN_ISSUES_ENABLED)
	// -- see drilldown_issues_route.go's package doc comment for the
	// reachability story and internal/drilldown/issues.go for the ported
	// resolver and its documented ReplacingMergeTree-dedup/org-scope notes.
	{
		Builder: "buildDrilldownIssuesRoute",
		File:    "drilldown_issues_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/drilldown/issues", Methods: []string{http.MethodGet, http.MethodPost}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/drilldown/issues route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/drilldown/issues route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildDrilldownIssuesRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET /api/v1/people, gated by its own routeswitch entry (default OFF
	// via GO_API_PEOPLE_SEARCH_ENABLED) -- see people_route.go's package
	// doc comment for the reachability story and internal/people for the
	// ported resolver and its documented ReplacingMergeTree-dedup notes.
	{
		Builder: "buildPeopleSearchRoute",
		File:    "people_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/people", Methods: []string{http.MethodGet}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/people route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/people route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildPeopleSearchRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET /api/v1/people/{person_id}/summary, gated by its own routeswitch
	// entry (default OFF via GO_API_PEOPLE_SUMMARY_ENABLED) -- see
	// people_summary_route.go's package doc comment for the reachability
	// story, the path-parameter mechanism and internal/people for the
	// ported resolver.
	{
		Builder: "buildPeopleSummaryRoute",
		File:    "people_summary_route.go",
		Mounts: []restMount{
			{Pattern: peopleSummaryPath, Methods: []string{http.MethodGet}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build %s route: %v", peopleSummaryPath, err) },
		LogNotConfigured: func() {
			log.Printf("query-api: %s route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted", peopleSummaryPath)
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildPeopleSummaryRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET /api/v1/people/{person_id}/metric, gated by its own routeswitch
	// entry (default OFF via GO_API_PEOPLE_METRIC_ENABLED) -- see
	// people_metric_route.go's package doc comment for the reachability
	// story and internal/people for the ported resolver.
	{
		Builder: "buildPeopleMetricRoute",
		File:    "people_metric_route.go",
		Mounts: []restMount{
			{Pattern: peopleMetricPath, Methods: []string{http.MethodGet}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build %s route: %v", peopleMetricPath, err) },
		LogNotConfigured: func() {
			log.Printf("query-api: %s route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted", peopleMetricPath)
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildPeopleMetricRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET /api/v1/people/{person_id}/drilldown/prs, gated by its own
	// routeswitch entry (default OFF via GO_API_PEOPLE_DRILLDOWN_PRS_ENABLED)
	// -- see people_drilldown_prs_route.go's package doc comment for the
	// reachability story and internal/people/drilldownprs.go for the ported
	// resolver.
	{
		Builder: "buildPeopleDrilldownPRsRoute",
		File:    "people_drilldown_prs_route.go",
		Mounts: []restMount{
			{Pattern: peopleDrilldownPRsPath, Methods: []string{http.MethodGet}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build %s route: %v", peopleDrilldownPRsPath, err) },
		LogNotConfigured: func() {
			log.Printf("query-api: %s route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted", peopleDrilldownPRsPath)
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildPeopleDrilldownPRsRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET /api/v1/people/{person_id}/drilldown/issues, gated by its own
	// routeswitch entry (default OFF via
	// GO_API_PEOPLE_DRILLDOWN_ISSUES_ENABLED) -- see
	// people_drilldown_issues_route.go's package doc comment for the
	// reachability story and internal/people/drilldownissues.go for the
	// ported resolver.
	{
		Builder: "buildPeopleDrilldownIssuesRoute",
		File:    "people_drilldown_issues_route.go",
		Mounts: []restMount{
			{Pattern: peopleDrilldownIssuesPath, Methods: []string{http.MethodGet}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build %s route: %v", peopleDrilldownIssuesPath, err) },
		LogNotConfigured: func() {
			log.Printf("query-api: %s route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted", peopleDrilldownIssuesPath)
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildPeopleDrilldownIssuesRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET /api/v1/meta, gated by its own routeswitch entry (default OFF via
	// GO_API_META_ENABLED) -- see meta_route.go's package doc comment for
	// the reachability story and internal/meta for the ported handler.
	// Unlike every sibling route above, this one carries no envelope
	// verifier dependency: main.py's meta() route is public (see
	// meta_route.go's doc comment for the citation trail), so only
	// CLICKHOUSE_URI gates whether it mounts.
	{
		Builder: "buildMetaRoute",
		File:    "meta_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/meta", Methods: []string{http.MethodGet}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/meta route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/meta route not configured (CLICKHOUSE_URI unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildMetaRoute(getenv)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET+POST /api/v1/explain, gated by its own routeswitch entries
	// (default OFF via GO_API_EXPLAIN_ENABLED) -- see explain_route.go's
	// package doc comment for the reachability story and
	// internal/explain for the ported resolver and its documented
	// ReplacingMergeTree-dedup/org-scope notes.
	{
		Builder: "buildExplainRoute",
		File:    "explain_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/explain", Methods: []string{http.MethodGet, http.MethodPost}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/explain route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/explain route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildExplainRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET /api/v1/flame, gated by its own routeswitch entry
	// (default OFF via GO_API_FLAME_ENABLED) -- see flame_route.go's
	// package doc comment for the reachability story and internal/flame
	// for the ported resolver and its documented ReplacingMergeTree-dedup
	// notes.
	{
		Builder: "buildFlameRoute",
		File:    "flame_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/flame", Methods: []string{http.MethodGet}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/flame route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/flame route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildFlameRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
	// GET /api/v1/flame/aggregated, gated by its own routeswitch entry
	// (default OFF via GO_API_FLAME_AGGREGATED_ENABLED) -- see
	// flame_aggregated_route.go's package doc comment for the
	// reachability story and internal/aggflame for the ported resolver
	// and its documented ReplacingMergeTree-dedup notes.
	{
		Builder: "buildFlameAggregatedRoute",
		File:    "flame_aggregated_route.go",
		Mounts: []restMount{
			{Pattern: "/api/v1/flame/aggregated", Methods: []string{http.MethodGet}},
		},
		LogBuildError: func(err error) { log.Printf("query-api: build /api/v1/flame/aggregated route: %v", err) },
		LogNotConfigured: func() {
			log.Print("query-api: /api/v1/flame/aggregated route not configured (CLICKHOUSE_URI/GO_API_ENVELOPE_* unset) -- staying unmounted")
		},
		Build: func(getenv getenvFunc, edgeUsers edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			h0, cleanup, ok, err := buildFlameAggregatedRoute(getenv, edgeUsers)
			return []http.HandlerFunc{h0}, cleanup, ok, err
		},
	},
}
