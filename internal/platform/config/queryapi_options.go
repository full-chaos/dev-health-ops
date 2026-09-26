package config

// QueryAPIServiceName is the service identity of `dho query-api`, the read-only
// Go query plane. Its options are scoped to it, so no other binary advertises or
// parses them. The environment names are the ones the chart and the compose files
// already set (the declared-only environment contract is unchanged); the flags are
// the new discovery surface.
const QueryAPIServiceName = "dev-health-query-api"

const defaultQueryAPIAddress = ":8090"

// queryAPIOptions are the settings of `dho query-api`, in --help order. The query
// routes' builders read them through Config.Setting, which answers only for a name
// declared here (or declared for every service): a setting that is not declared has
// no value, so a new read cannot bypass this registry.
var queryAPIOptions = func() []Option {
	q := []string{QueryAPIServiceName}
	options := []Option{
		{
			Flag: "query-addr", Env: "QUERY_API_ADDR", Kind: KindString,
			Default: defaultQueryAPIAddress, Services: q, Group: GroupRuntime,
			Usage: "host:port the query routes (/query, /registry, /buildinfo, /api/v1/*) listen on; must differ from --http-addr, which serves /healthz, /readyz and /metrics",
		},
		{
			Flag: "internal-addr", Env: "QUERY_API_INTERNAL_ADDR", Kind: KindString,
			Services: q, Group: GroupRuntime,
			Usage: "host:port of the internal listener that honours the X-DH-Internal-* identity headers (off when unset; no Ingress may route to it)",
		},
		{
			Flag: "database-role", Env: "QUERY_API_DATABASE_ROLE", Kind: KindString,
			Services: q, Group: GroupDatabase,
			Usage: "PostgreSQL role the readiness write-grant check expects the registry pool to hold",
		},
		{
			Flag: "graphql-max-query-bytes", Env: "GRAPHQL_MAX_QUERY_BYTES", Kind: KindString,
			Services: q, Group: GroupRuntime,
			Usage: "largest accepted GraphQL query document, in bytes (the service default applies when unset)",
		},
		{
			Flag: "envelope-jwks-path", Env: "GO_API_ENVELOPE_JWKS_PATH", Kind: KindString,
			Services: q, Group: GroupRoutes,
			Usage: "file path of the JWKS the edge envelope is verified against; every route needs it, with the issuer and audience",
		},
		{
			Flag: "envelope-issuer", Env: "GO_API_ENVELOPE_ISSUER", Kind: KindString,
			Services: q, Group: GroupRoutes,
			Usage: "issuer the edge envelope must carry",
		},
		{
			Flag: "envelope-audience", Env: "GO_API_ENVELOPE_AUDIENCE", Kind: KindString,
			Services: q, Group: GroupRoutes,
			Usage: "audience the edge envelope must carry",
		},
		{
			Flag: "edge-jwt-issuer", Env: "GO_API_EDGE_JWT_ISSUER", Kind: KindString,
			Services: q, Group: GroupRoutes,
			Usage: "issuer of the edge JWT the proof route verifies",
		},
		{
			Flag: "edge-jwt-audience", Env: "GO_API_EDGE_JWT_AUDIENCE", Kind: KindString,
			Services: q, Group: GroupRoutes,
			Usage: "audience of the edge JWT the proof route verifies",
		},
		{
			Flag: "drilldown-issues-enabled", Env: "GO_API_DRILLDOWN_ISSUES_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the drilldown issues route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "drilldown-prs-enabled", Env: "GO_API_DRILLDOWN_PRS_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the drilldown prs route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "explain-enabled", Env: "GO_API_EXPLAIN_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the explain route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "filter-options-enabled", Env: "GO_API_FILTER_OPTIONS_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the filter options route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "flame-aggregated-enabled", Env: "GO_API_FLAME_AGGREGATED_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the flame aggregated route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "flame-enabled", Env: "GO_API_FLAME_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the flame route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "heatmap-enabled", Env: "GO_API_HEATMAP_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the heatmap route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "home-enabled", Env: "GO_API_HOME_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the home route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "investment-enabled", Env: "GO_API_INVESTMENT_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the investment route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "investment-explain-enabled", Env: "GO_API_INVESTMENT_EXPLAIN_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the investment explain route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "investment-flow-enabled", Env: "GO_API_INVESTMENT_FLOW_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the investment flow route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "investment-sunburst-enabled", Env: "GO_API_INVESTMENT_SUNBURST_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the investment sunburst route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "meta-enabled", Env: "GO_API_META_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the meta route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "opportunities-enabled", Env: "GO_API_OPPORTUNITIES_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the opportunities route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "people-drilldown-issues-enabled", Env: "GO_API_PEOPLE_DRILLDOWN_ISSUES_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the people drilldown issues route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "people-drilldown-prs-enabled", Env: "GO_API_PEOPLE_DRILLDOWN_PRS_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the people drilldown prs route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "people-metric-enabled", Env: "GO_API_PEOPLE_METRIC_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the people metric route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "people-search-enabled", Env: "GO_API_PEOPLE_SEARCH_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the people search route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "people-summary-enabled", Env: "GO_API_PEOPLE_SUMMARY_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the people summary route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "proof-route-enabled", Env: "GO_API_PROOF_ROUTE_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the proof route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "quadrant-enabled", Env: "GO_API_QUADRANT_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the quadrant route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "sankey-enabled", Env: "GO_API_SANKEY_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the sankey route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "work-unit-explain-enabled", Env: "GO_API_WORK_UNIT_EXPLAIN_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the work unit explain route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{
			Flag: "work-units-enabled", Env: "GO_API_WORK_UNITS_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the work units route (default off; it also needs its dependencies configured, or it stays unmounted)",
		},
		{Env: "GO_API_REGISTRY_POSTGRES_URI", Secret: true, Services: q, Group: GroupCredentials, Usage: "registry PostgreSQL DSN the /query route reads its operation registry from (the query-api role's own login)"},
		{Env: "GO_API_EDGE_JWT_SECRET", Secret: true, Services: q, Group: GroupCredentials, Usage: "shared secret the proof route verifies the edge JWT with"},
	}
	// Read from the process environment by the code below the routes (identity
	// mapping, the LLM model choice, two work-graph switches): declared so the service's whole contract is here, environment
	// only because a flag would be accepted and then ignored. TestDirectEnvironmentReads
	// requires every such read to be declared here.
	for _, env := range []struct{ name, usage string }{
		{"IDENTITY_MAPPING_PATH", "identity mapping file the heatmap, people and quadrant routes read"},
		{"LLM_MODEL", "model the investment explain route asks its provider for, when no provider-specific one is set"},
		{"LLM_MODEL_OPENAI", "model the investment explain route asks the OpenAI provider for"},
		{"LLM_MODEL_LOCAL", "model the investment explain route asks a local provider for"},
		{"LOCAL_LLM_MODEL", "alias of LLM_MODEL_LOCAL"},
		{"OPERATIONAL_ORDERING_CONTRACT", "operational ordering contract version the work-graph display names follow (2 selects the new one)"},
		{"WORKGRAPH_INVESTMENT_MATERIALIZE_NATIVE_ENABLED", "1 selects the native investment materialization the work-graph issue/PR reader assumes"},
	} {
		options = append(options, Option{Env: env.name, EnvOnly: true, Services: q, Group: GroupRoutes, Usage: env.usage})
	}
	return options
}()
