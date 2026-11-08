package config

import "github.com/full-chaos/dev-health-ops/internal/authzenforce"

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
			Flag: "internal-allowed-cidrs", Env: "QUERY_API_INTERNAL_ALLOWED_CIDRS", Kind: KindString,
			Services: q, Group: GroupRuntime,
			Usage: "comma-separated CIDR list (IPv4/IPv6) the internal listener accepts a peer connection from; unset accepts every peer (D2953) -- checked at accept time, never from a header",
		},
		{
			Flag: "mcp-addr", Env: "QUERY_API_MCP_ADDR", Kind: KindString,
			Services: q, Group: GroupRuntime,
			Usage: "host:port of the MCP caller-class listener (CHAOS-7085): POST /query only, queries only, internal identity headers only, allowlisted root fields (off when unset; no Ingress may route to it)",
		},
		{
			Flag: "mcp-boundary", Env: "QUERY_API_MCP_BOUNDARY", Kind: KindString,
			Services: q, Group: GroupRuntime,
			Usage: "boundary marker the MCP listener requires with --mcp-addr: networkpolicy (set by the chart beside the MCP NetworkPolicy) or allowed-cidrs (requires --mcp-allowed-cidrs); anything else refuses boot",
		},
		{
			Flag: "mcp-allowed-cidrs", Env: "QUERY_API_MCP_ALLOWED_CIDRS", Kind: KindString,
			Services: q, Group: GroupRuntime,
			Usage: "comma-separated CIDR list (IPv4/IPv6) the MCP listener accepts a peer connection from; unset accepts every peer -- checked at accept time, never from a header",
		},
		{
			Flag: "database-role", Env: "QUERY_API_DATABASE_ROLE", Kind: KindString,
			Services: q, Group: GroupDatabase,
			Usage: "PostgreSQL role the readiness write-grant check expects the registry pool to hold",
		},
		{
			Flag: "auth-proxy-sessions", Env: "AUTH_PROXY_SESSIONS", Kind: KindString,
			Services: q, Group: GroupDatabase,
			Usage: "true while auth-service owns sessions (go-api's flag): the role posture then also requires the read of the auth schema's sessions; unset or false = off, any other value refuses readiness",
		},
		{
			Flag: "auth-platform-roles-owned", Env: "AUTH_PLATFORM_ROLES_OWNED", Kind: KindString,
			Services: q, Group: GroupDatabase,
			Usage: "true while auth-service owns the platform roles: is_superuser is read from the auth platform role assignments and the role posture also requires that read (needs AUTH_PROXY_SESSIONS); unset or false = off, any other value refuses startup",
		},
		{
			Flag: "auth-database-schema", Env: "AUTH_DATABASE_SCHEMA", Kind: KindString,
			Services: q, Group: GroupDatabase,
			Usage: "schema of the auth tables (default auth) that session checks and the role posture read",
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
			Flag: "access-token-issuer", Env: "AUTH_ACCESS_TOKEN_ISSUER", Kind: KindString,
			Services: q, Group: GroupRoutes,
			Usage: "https issuer of EdDSA user access tokens (go-api's value): set, the REST, /query and /graphql routes validate the edge and user access tokens by their endpoint-profile row against its JWKS; unset, they do not",
		},
		{
			Flag: "pdp-mode", Env: "QUERY_API_PDP_MODE", Kind: KindString,
			Services: q, Group: GroupRoutes,
			Usage: "the REST routes' PDP gate: off (unset, default: no call), shadow (ask auth-service's PDP, count and compare, never deny) or enforce (a PDP deny is 403, no verdict is 503); a bad value, or shadow/enforce with a missing input, never fails the start: one ERROR names the variable and every PDP-gated request answers 503",
		},
		{
			// The name is authzenforce's: one reader of the variable (internal/authzenforce census_test.go).
			Flag: "authz-enforce", Env: authzenforce.QueryAPI.Env(), Kind: KindString,
			Services: q, Group: GroupRoutes,
			Usage: "comma list of catalog action namespaces (analytics, metrics, git, work_items, ...) whose REST routes and GraphQL root fields of a user access token caller the PDP decides (the 5.17 cells; unset: none). A listed namespace needs QUERY_API_PDP_MODE shadow or enforce for the client, else its routes answer 503; with no namespace listed the PDP never refuses. An item that is not a namespace is ignored and stays off (one ERROR), never a start failure",
		},
		{
			// The name is authzenforce's: one reader of the variable (internal/authzenforce census_test.go).
			Flag: "headers-authz-enforce", Env: authzenforce.QueryAPIHeaders.Env(), Kind: KindString,
			Services: q, Group: GroupRoutes,
			Usage: "comma list of catalog action namespaces whose GraphQL root fields are decided for the X-DH-Internal-* header carrier (the MCP listener, /query on the internal listener; D6721, unset: none). That carrier has no principal the PDP can judge, so a listed namespace refuses its operations with 503 authz_enforce_no_principal; unset, they stay on today's path. An item that is not a namespace is ignored and stays off (one ERROR), never a start failure",
		},
		{
			Flag: "auth-proxy-url", Env: "AUTH_PROXY_URL", Kind: KindString,
			Services: q, Group: GroupRoutes,
			Usage: "auth-service's base URL (go-api's setting), an http(s) origin: the PDP gate's workload exchange and authorize calls go there; read only while QUERY_API_PDP_MODE is on",
		},
		{
			Flag: "auth-workload-subject-file", Env: "AUTH_WORKLOAD_SUBJECT_FILE", Kind: KindString,
			Services: q, Group: GroupRoutes,
			Usage: "projected ServiceAccount token file, the PDP gate's workload exchange subject on Kubernetes (exactly one of this and the bootstrap file); read only while QUERY_API_PDP_MODE is on",
		},
		{
			Flag: "auth-workload-bootstrap-file", Env: "AUTH_WORKLOAD_BOOTSTRAP_FILE", Kind: KindString,
			Services: q, Group: GroupRoutes,
			Usage: "workload bootstrap credential file, the PDP gate's workload exchange subject on Compose; read only while QUERY_API_PDP_MODE is on",
		},
		{
			Flag: "auth-workload-issuer", Env: "AUTH_WORKLOAD_ISSUER", Kind: KindString,
			Services: q, Group: GroupRoutes,
			Usage: "https issuer (auth-service's token issuer URL) every workload token of the PDP gate must carry; read only while QUERY_API_PDP_MODE is on",
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
			Flag: "proof-write-route-enabled", Env: "GO_API_PROOF_WRITE_ROUTE_ENABLED", Kind: KindBool,
			Default: "false", Services: q, Group: GroupRoutes,
			Usage: "enable the internal listener's operator proof route (default off): /query/proof-write (CHAOS-7096, mutation-only), gated per request by the go_api_proof_orgs allowlist (empty by default)",
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
		{Env: "GO_API_EDGE_JWT_SECRET", Secret: true, Services: q, Group: GroupCredentials, Usage: "retired HS256 edge secret: nothing verifies with it; a set value only logs one WARN that it is unused"},
	}
	// Read from the process environment by the code below the routes (identity
	// mapping, the LLM model choice, two work-graph switches): declared so the service's whole contract is here, environment
	// only because a flag would be accepted and then ignored. TestDirectEnvironmentReads
	// requires every such read to be declared here.
	for _, env := range []struct{ name, usage string }{
		{"IDENTITY_MAPPING_PATH", "identity mapping file the heatmap, people and quadrant routes read"},
		{"LLM_MODEL", "model the investment explain route asks its provider for, when no provider-specific one is set"},
		{"LLM_MODEL_EXPLAIN", "model the investment and work unit explain routes ask the platform provider for; wins over LLM_MODEL and the provider-specific names, never over an organization's own model"},
		{"LLM_MODEL_OPENAI", "model the investment explain route asks the OpenAI provider for"},
		{"LLM_MODEL_ANTHROPIC", "model the investment explain route asks the Anthropic provider for"},
		{"LLM_MODEL_GEMINI", "model the investment explain route asks the Gemini provider for"},
		{"GEMINI_MODEL", "alias of LLM_MODEL_GEMINI"},
		{"LLM_MODEL_QWEN", "model the investment explain route asks the Qwen provider for"},
		{"QWEN_MODEL", "alias of LLM_MODEL_QWEN"},
		{"LLM_MODEL_LOCAL", "model the investment explain route asks a local provider for"},
		{"LOCAL_LLM_MODEL", "alias of LLM_MODEL_LOCAL"},
		{"OPERATIONAL_ORDERING_CONTRACT", "operational ordering contract version the work-graph display names follow (2 selects the new one)"},
		{"WORKGRAPH_INVESTMENT_MATERIALIZE_NATIVE_ENABLED", "1 selects the native investment materialization the work-graph issue/PR reader assumes"},
	} {
		options = append(options, Option{Env: env.name, EnvOnly: true, Services: q, Group: GroupRoutes, Usage: env.usage})
	}
	return options
}()
