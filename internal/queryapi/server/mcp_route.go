package server

// CHAOS-7085 (with CHAOS-7091 built inside it): the MCP caller class. A
// third, separate query-api listener (QUERY_API_MCP_ADDR) serves exactly
// one route, POST /query, to exactly one caller class: acr-api building a
// validated free-form query for the hosted MCP (design CHAOS-7036 r5 D.8
// form (iii), ruled K12; this listener is also the K17 option (b) narrow
// carrier).
//
// What this route refuses, every refusal BEFORE a resolver runs, so a
// refused request makes zero ClickHouse calls:
//
//   - any carrier except the four X-DH-Internal-* headers (no envelope, no
//     edge token, no Authorization header at all);
//   - a header set that claims superuser, impersonation or an operator role
//     (admin/owner/operator, datahealth.RequireOperator's set): refused, loud
//     (lead D3468 option R, GWC-2 Q4 "refuse, do not override"). Every other
//     role is forced to "" and both flags are always false, so
//     graph.OperationOrgGuard can never rebind the org;
//   - anything but a single query operation (mutation and subscription are
//     refused by operation TYPE, not by role -- the saved-report mutations
//     read no role);
//   - introspection (__schema/__type), APQ or any other body extension;
//   - a root field outside mcpRootFieldAllowlist;
//   - depth, alias count or complexity over this class's own caps;
//   - an orgId/org_id argument, at any depth and INSIDE input objects and
//     variables (hotspots and cognitiveLoad carry the org in `input`, which
//     graph.OperationOrgGuard does not read), that is not the header org;
//   - a root field whose class routing row (go_api_routing_state, see
//     mcpRoutingDigests) is not canary/primary.
//
// What passes runs on its OWN gqlgen server (same explicit options as
// CHAOS-7078, re-checking the operation type and allowlist) over its OWN
// ClickHouse client (CHAOS-7091: a read-bytes ceiling instead of the shared
// path's unrestricted setting). A ClickHouse budget refusal anywhere in the
// request -- including one a resolver swallows into an empty result, such
// as flowMatrix inside analytics -- replaces the whole response with a typed
// refusal, never a silently truncated answer.
//
// Accepted residual (K17 option b, stated by the MCP team and accepted): the
// headers are assertions this listener trusts, so a taken-over acr-api
// still reads any org through the allowlisted root fields. What this
// removes: it cannot write, and cannot reach operator or superuser views.
// The network (NetworkPolicy admitting acr-api pods only, or
// QUERY_API_MCP_ALLOWED_CIDRS on a venue with no NetworkPolicy) is the
// authentication boundary -- there is no service-to-service credential by
// rule.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/99designs/gqlgen/complexity"
	"github.com/99designs/gqlgen/graphql"
	gqlhandler "github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"github.com/vektah/gqlparser/v2/parser"
	"github.com/vektah/gqlparser/v2/validator"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/datahealth"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/featureflags"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// The MCP class's caps. Deliberately their own literals, not aliases of
// graphQLDepthLimit/graphQLComplexityLimit: a change to the shared server's
// limits must not silently move this class's contract, which the MCP team
// builds against (TestMCPCapsArePinned pins each value).
//
//   - depth 10, complexity 150: CHAOS-7078's values, measured over the 55
//     registered documents (max depth 5, max complexity 76) with ~2x headroom.
//   - aliases 15: the Python plane's alias limit (design r5 D.8, R).
//   - MaxBytesToRead 4 GiB (CHAOS-7091, lead D3468 = max(2 GiB, 2 x measured)):
//     the largest read of an allowlisted root field on bigboy
//     system.query_log over 30 days was hotspots/file_hotspot_daily at
//     2,014,173,780 bytes (1.88 GiB); 2x = 3.75 GiB, rounded up to 4 GiB.
//   - max_execution_time 10 s: today's library default, stated explicitly so
//     it cannot drift; below acr-api's 30 s deadline, so the query is refused
//     before acr gives up. dev-health-go enforces it as a 10 s context
//     deadline; clickhouse-go then sends the server a slightly longer
//     max_execution_time (measured: 14), so it fires client-side first --
//     mcpBudgetReason classifies both as time_ceiling.
const (
	mcpDepthLimit              = 10
	mcpComplexityLimit         = 150
	mcpAliasLimit              = 15
	mcpMaxBytesToRead          = uint64(4) << 30
	mcpMaxExecutionTimeSeconds = uint(10)
)

// The expanded-document bounds of mcpFragmentGraphCheck, as multiples of the
// complexity and depth caps: every node kind counts (a spread and an inline
// fragment are nodes, not only fields), and a legitimate field sits under at
// most a spread or an inline fragment.
const (
	mcpExpandedNodeFactor  = 4
	mcpExpandedDepthFactor = 4
)

// mcpCallerClass is the caller_class telemetry value. The class comes from
// WHERE the request arrived (internalidentity.MCP), never from a header.
const mcpCallerClass = "mcp"

// mcpRootFieldAllowlist is the ceiling of what the MCP class may ever call:
// the SDL Query root fields of design r5 D.3's first-slice operations for an
// unrestricted caller. Reviewed by PR, never by configuration -- an env
// list would let an operator widen it without review. Enablement below this
// ceiling is per root field, by class routing row (mcpRoutingDigests).
//
// Out on purpose (TestMCPAllowlistExcludesPersonOperatorAndWriteFields):
// person-level fields (pr, busFactor, reviewEdges, ai*), operator and
// superuser views (dataHealth, productTelemetry*, experiments), user-owned
// product objects (savedReport(s), reportRuns), the legacy-table
// operatingReview (K14), and home/recommendations/workItemTeamAttributions
// (D.8's "3 resolvers that panic" at design time). Sub-field policy (person
// output paths, the basis-free analytics shape) is acr-api's policy artifact
// (D.5), not this list.
var mcpRootFieldAllowlist = map[string]bool{
	"analytics":            true,
	"capacityForecast":     true,
	"capacityForecasts":    true,
	"catalog":              true,
	"cognitiveLoad":        true,
	"complexityTimeseries": true,
	"compoundingRisk":      true,
	"hotspots":             true,
	"securityAlerts":       true,
	"securityOverview":     true,
	"throughputForecast":   true,
	"workGraphArtifacts":   true,
	"workGraphEdges":       true,
	"workGraphFlow":        true,
}

// mcpClassDocumentKey names the free-form class in go_api_routing_state. A
// free-form query has no registered document, so no per-document row can
// exist for it; the class row stands in, one per root field, keyed
// (schema_digest, sha256(mcpClassDocumentKey), "mcp:<rootField>"). Bump the
// version only with the routing tooling that writes these rows.
const mcpClassDocumentKey = "dev-health-ops/mcp-freeform-class/v1"

// mcpRoutingOperationPrefix prefixes a root field to form its class row's
// selected_operation. No registered operation name contains ':', so a class
// row can never collide with a per-document row.
const mcpRoutingOperationPrefix = "mcp:"

// mcpRoutingDigests is the operation -> document digest map the class's
// routeswitch.PostgresSwitch looks rows up by.
func mcpRoutingDigests() map[string]string {
	classDigest := digestHex(mcpClassDocumentKey)
	digests := make(map[string]string, len(mcpRootFieldAllowlist))
	for root := range mcpRootFieldAllowlist {
		digests[mcpRoutingOperationPrefix+root] = classDigest
	}
	return digests
}

// mcpOperatorRoles is datahealth.RequireOperator's role set (compared
// lowercased there too). A header claiming one of them is refused.
var mcpOperatorRoles = map[string]bool{"admin": true, "owner": true, "operator": true}

// Refusal reasons: a closed vocabulary, used in the response, the log line
// and the counter. Never a value from the request.
const (
	mcpReasonOffListener         = "off_mcp_listener"
	mcpReasonMethod              = "method_not_allowed"
	mcpReasonContentType         = "content_type"
	mcpReasonBodyTooLarge        = "body_too_large"
	mcpReasonBadBody             = "bad_body"
	mcpReasonBodyField           = "unknown_body_field"
	mcpReasonAuthorization       = "authorization_header"
	mcpReasonNoCarrier           = "no_carrier"
	mcpReasonInvalidOrg          = "invalid_org"
	mcpReasonElevatedClaim       = "elevated_claim"
	mcpReasonInvalidDocument     = "invalid_document"
	mcpReasonOperationCount      = "operation_count"
	mcpReasonOperationName       = "operation_name_mismatch"
	mcpReasonNotAQuery           = "not_a_query"
	mcpReasonIntrospection       = "introspection"
	mcpReasonRootField           = "root_field_not_allowed"
	mcpReasonDepth               = "depth_limit"
	mcpReasonAliases             = "alias_limit"
	mcpReasonInvalidVariables    = "invalid_variables"
	mcpReasonComplexity          = "complexity_limit"
	mcpReasonInvalidOrgArgument  = "invalid_org_argument"
	mcpReasonOrgMismatch         = "org_mismatch"
	mcpReasonPersonScope         = "person_scope"
	mcpReasonInputLimit          = "input_limit"
	mcpReasonRootFieldNotEnabled = "root_field_not_enabled"
	mcpReasonBytesCeiling        = "bytes_ceiling"
	mcpReasonRowsCeiling         = "rows_ceiling"
	mcpReasonTimeCeiling         = "time_ceiling"
	mcpReasonFieldErrors         = "field_errors"
)

const (
	mcpOutcomeServed  = "served"
	mcpOutcomeRefused = "refused"
)

// ClickHouse server exception codes for the limits this class's client sets
// (newMCPClickHouseOptions), one per setting, each mapped to a refusal
// reason by mcpBudgetReason:
//
//	307 TOO_MANY_BYTES          max_bytes_to_read      -> bytes_ceiling
//	396 TOO_MANY_ROWS_OR_BYTES  max_result_rows        -> rows_ceiling
//	158 TOO_MANY_ROWS           max_rows_to_read (library default path) -> rows_ceiling
//	159 TIMEOUT_EXCEEDED        max_execution_time     -> time_ceiling
//
// 396 is what a real server returns for max_result_rows (r1 on #3425,
// executed; see also workgraph/membership.go's CHAOS-4655 note), not 158.
const (
	clickHouseTooManyBytesCode       = 307
	clickHouseTooManyRowsOrBytesCode = 396
	clickHouseTooManyRowsCode        = 158
	clickHouseTimeoutExceededCode    = 159
)

// newMCPClickHouseClient is the MCP class's own ClickHouse client
// (CHAOS-7091): the shared read options, then this class's ceilings. It
// starts from newUnrestrictedReadClickHouseOptions so every other setting
// matches /query's, and overwrites MaxBytesToRead on purpose -- the one
// client in this service that must NOT run unrestricted.
func newMCPClickHouseClient(dsn string) (*dhclickhouse.Client, error) {
	opts := newUnrestrictedReadClickHouseOptions(dsn)
	applyMCPClickHouseCeilings(&opts)
	return dhclickhouse.NewClickHouseQueryClientWithOptions(opts)
}

// newMCPClickHouseOptions is exactly what newMCPClickHouseClient sends, for
// the test that pins the ceilings.
func newMCPClickHouseOptions(dsn string) dhclickhouse.Options {
	opts := newUnrestrictedReadClickHouseOptions(dsn)
	applyMCPClickHouseCeilings(&opts)
	return opts
}

// applyMCPClickHouseCeilings is the ONE sanctioned overwrite of
// newUnrestrictedReadClickHouseOptions's MaxBytesToRead (CHAOS-7091).
func applyMCPClickHouseCeilings(opts *dhclickhouse.Options) {
	maxBytes := mcpMaxBytesToRead
	opts.MaxBytesToRead = &maxBytes
	opts.MaxExecutionTime = mcpMaxExecutionTimeSeconds
	maxResultRows := queryRouteMaxResultRows
	opts.MaxResultRows = &maxResultRows
}

// mcpLimits is the class's three query caps. Production uses
// mcpDefaultLimits; a test lowers one to drive a real refusal over HTTP
// (no allowlisted root field nests past depth 10, so the production depth
// cap cannot be exceeded by a valid document).
type mcpLimits struct {
	depth      int
	aliases    int
	complexity int
}

func mcpDefaultLimits() mcpLimits {
	return mcpLimits{depth: mcpDepthLimit, aliases: mcpAliasLimit, complexity: mcpComplexityLimit}
}

// mcpHandler is POST /query on the MCP listener.
type mcpHandler struct {
	gql    http.Handler
	es     graphql.ExecutableSchema
	sw     routeswitch.Switch
	getenv getenvFunc
	limits mcpLimits
}

// newMCPHandler builds the MCP class's route over ch (the class's own
// ClickHouse client), pg (read-only Postgres) and sw (the class routing
// switch). The saved-report writer is deliberately absent: mutations are
// refused before dispatch, and a resolver that somehow ran one would fail,
// never write.
func newMCPHandler(ch featureflags.QueryClient, pg datahealth.PGQuerier, sw routeswitch.Switch, getenv getenvFunc) http.Handler {
	return newMCPHandlerWithLimits(ch, pg, sw, getenv, mcpDefaultLimits())
}

func newMCPHandlerWithLimits(ch featureflags.QueryClient, pg datahealth.PGQuerier, sw routeswitch.Switch, getenv getenvFunc, limits mcpLimits) http.Handler {
	resolver := &graph.Resolver{
		ClickHouse: analytics.PinInvestmentMembershipScope(mcpObservedClient{next: ch}),
		Postgres:   pg,
	}
	es := graph.NewExecutableSchema(graph.Config{Resolvers: resolver})
	return &mcpHandler{gql: newMCPGraphQLServer(es, limits), es: es, sw: sw, getenv: getenv, limits: limits}
}

// newMCPGraphQLServer is the class's own gqlgen server: POST only, no
// introspection, no APQ (both simply absent), this class's caps, and the
// operation guard again as defence in depth behind the handler's gate.
func newMCPGraphQLServer(es graphql.ExecutableSchema, limits mcpLimits) *gqlhandler.Server {
	gql := gqlhandler.New(es)
	gql.AddTransport(transport.POST{})
	gql.Use(extension.FixedComplexityLimit(limits.complexity))
	gql.Use(depthLimit{Max: limits.depth})
	gql.Use(mcpOperationGuard{})
	gql.AroundFields(graph.RefuseNullForNonNullArguments)
	gql.Use(graph.OperationOrgGuard{})
	gql.SetErrorPresenter(func(ctx context.Context, err error) *gqlerror.Error {
		if obs := mcpObservationFrom(ctx); obs != nil {
			obs.fieldError()
		}
		chain := []string{err.Error()}
		for unwrapped := errors.Unwrap(err); unwrapped != nil; unwrapped = errors.Unwrap(unwrapped) {
			chain = append(chain, unwrapped.Error())
		}
		log.Printf("query-api: mcp resolver error unwrap chain: %s", truncateForLog(strings.Join(chain, " <- "), maxUnwrapChainLogBytes))
		return graphql.DefaultErrorPresenter(ctx, err)
	})
	return gql
}

// mcpOperationGuard re-checks, inside the executor, the two properties the
// class exists for: a query operation, and only allowlisted root fields.
// The handler already refused both before dispatch; this is the second
// wall, so a future change to the handler cannot open them alone.
type mcpOperationGuard struct{}

var _ interface {
	graphql.HandlerExtension
	graphql.OperationContextMutator
} = mcpOperationGuard{}

func (mcpOperationGuard) ExtensionName() string                   { return "MCPOperationGuard" }
func (mcpOperationGuard) Validate(graphql.ExecutableSchema) error { return nil }

func (mcpOperationGuard) MutateOperationContext(_ context.Context, opCtx *graphql.OperationContext) *gqlerror.Error {
	if opCtx.Operation == nil || opCtx.Operation.Operation != ast.Query {
		return gqlerror.Errorf("the MCP caller class serves query operations only")
	}
	roots, introspection := mcpRootFields(opCtx.Operation.SelectionSet, opCtx.Doc.Fragments)
	if introspection {
		return gqlerror.Errorf("introspection is not served to the MCP caller class")
	}
	for _, root := range roots {
		if root != "__typename" && !mcpRootFieldAllowlist[root] {
			return gqlerror.Errorf("root field %q is not served to the MCP caller class", root)
		}
	}
	return nil
}

// mcpRequest is what the gate learned about one request, for the log line
// and the counter. It never holds a variable value or the query text.
type mcpRequest struct {
	start          time.Time
	outcome        string
	reason         string
	status         int
	documentDigest string
	rootFields     []string
	aliases        int
	depth          int
	complexity     int
	clickHouse     int
}

func (h *mcpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req := &mcpRequest{start: time.Now()}
	defer req.emit(r)

	refuse := func(status int, reason, message string) {
		req.outcome, req.reason, req.status = mcpOutcomeRefused, reason, status
		writeMCPRefusal(w, status, reason, message)
	}

	if !internalidentity.OnMCPListener(r.Context()) {
		// Wired without the listener middleware: the class is where the
		// request arrived, and this one did not arrive on the MCP listener.
		refuse(http.StatusNotFound, mcpReasonOffListener, "not found")
		return
	}
	if r.Method != http.MethodPost {
		refuse(http.StatusMethodNotAllowed, mcpReasonMethod, "the MCP caller class accepts POST only")
		return
	}
	if mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mediaType != "application/json" {
		refuse(http.StatusUnsupportedMediaType, mcpReasonContentType, "the MCP caller class accepts application/json only")
		return
	}
	limit := graphQLMaxQueryBytes(h.getenv)
	body, readErr := io.ReadAll(io.LimitReader(r.Body, int64(limit)+1))
	_ = r.Body.Close()
	if readErr != nil {
		refuse(http.StatusBadRequest, mcpReasonBadBody, "the request body could not be read")
		return
	}
	if len(body) > limit {
		refuse(http.StatusRequestEntityTooLarge, mcpReasonBodyTooLarge, "the request body exceeds the size limit")
		return
	}

	// Carrier before the document: every identity outcome is decided before
	// anything about the query is.
	claims, status, reason := mcpAuthenticate(r)
	if reason != "" {
		refuse(status, reason, "the MCP caller class accepts the internal identity headers only, naming one org with no elevated claim")
		return
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		// A JSON array (a batch) lands here too: one operation per request.
		// A JSON null decodes to a nil map and is refused below (no query).
		refuse(http.StatusBadRequest, mcpReasonBadBody, "the request body must be one JSON object")
		return
	}
	for key := range raw {
		if key != "query" && key != "variables" && key != "operationName" {
			// "extensions" is how APQ travels; nothing but the three
			// standard members is accepted.
			refuse(http.StatusBadRequest, mcpReasonBodyField, "the request body may carry only query, variables and operationName")
			return
		}
	}
	var payload struct {
		Query         string         `json:"query"`
		Variables     map[string]any `json:"variables"`
		OperationName string         `json:"operationName"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.Query == "" {
		refuse(http.StatusBadRequest, mcpReasonBadBody, "the request body must carry a query string")
		return
	}
	req.documentDigest = digestHex(payload.Query)

	// Before validation: gqlparser's validator is super-linear in the
	// document's raw size (measured on this schema: ~0.2-0.6 s for 17-31 KB
	// of repeated fields, fragment spreads or a fragment chain). A linear
	// parse, then a linear count of the UNEXPANDED nodes: in a valid document
	// every fragment is used and contains a field, so each of these counts
	// is at most the expanded field count -- and gqlgen charges every field
	// at least 1. A document over the complexity cap on any of them cannot
	// be under it once valid.
	if raw, parseErr := parser.ParseQuery(&ast.Source{Input: payload.Query}); parseErr != nil {
		refuse(http.StatusBadRequest, mcpReasonInvalidDocument, "the document does not parse")
		return
	} else if !mcpDocumentNodesWithin(raw, h.limits.complexity) {
		refuse(http.StatusBadRequest, mcpReasonComplexity, fmt.Sprintf("the document holds more than %d fields, fragment spreads, inline fragments or fragment definitions, which exceeds the complexity limit of %d", h.limits.complexity, h.limits.complexity))
		return
	} else if reason, message := mcpFragmentGraphCheck(raw, h.limits); reason != "" {
		// Before ANY stage that walks spreads, the validator included: its
		// cost is super-linear in the EXPANDED document (r3 on #3425: 1.7 KB
		// of self-doubling fragments took 15 s to validate; 0.9 KB of
		// field-less spread-only doubling took 4.5 s). The check is exact and
		// O(document), so no doubling shape passes it.
		refuse(http.StatusBadRequest, reason, message)
		return
	}
	schema := h.es.Schema()
	doc, parseErrs := gqlparser.LoadQuery(schema, payload.Query)
	if len(parseErrs) > 0 {
		refuse(http.StatusBadRequest, mcpReasonInvalidDocument, "the document does not validate against the schema")
		return
	}
	if len(doc.Operations) != 1 {
		refuse(http.StatusBadRequest, mcpReasonOperationCount, "the document must hold exactly one operation")
		return
	}
	op := doc.Operations[0]
	if payload.OperationName != "" && payload.OperationName != op.Name {
		refuse(http.StatusBadRequest, mcpReasonOperationName, "operationName does not name the document's operation")
		return
	}
	if op.Operation != ast.Query {
		refuse(http.StatusMethodNotAllowed, mcpReasonNotAQuery, "the MCP caller class serves query operations only")
		return
	}
	// FIRST, before any walk that expands fragments per use (roots,
	// introspection, depth, aliases, org arguments, complexity): a bounded
	// count of the expanded field occurrences. A few hundred bytes of
	// self-doubling fragments expand to millions of fields (r2 on #3425:
	// 969 bytes, 4,194,304 alias visits, 4.5 s of CPU before the refusal),
	// so every later walk is safe only once this has passed. gqlgen charges
	// every field at least 1 (complexity.Calculate), so a document that
	// expands past the complexity cap can never be under it: refused as
	// complexity_limit, having visited at most cap+1 fields.
	if !mcpExpandedFieldsWithin(op.SelectionSet, doc.Fragments, h.limits.complexity) {
		refuse(http.StatusBadRequest, mcpReasonComplexity, fmt.Sprintf("operation expands to more than %d fields, which exceeds the complexity limit of %d", h.limits.complexity, h.limits.complexity))
		return
	}
	roots, introspection := mcpRootFields(op.SelectionSet, doc.Fragments)
	req.rootFields = roots
	if introspection {
		refuse(http.StatusBadRequest, mcpReasonIntrospection, "introspection is not served to the MCP caller class")
		return
	}
	for _, root := range roots {
		if root != "__typename" && !mcpRootFieldAllowlist[root] {
			refuse(http.StatusForbidden, mcpReasonRootField, "a root field is not served to the MCP caller class")
			return
		}
	}
	req.depth = selectionSetDepth(op.SelectionSet, 1)
	if req.depth > h.limits.depth {
		refuse(http.StatusBadRequest, mcpReasonDepth, fmt.Sprintf("operation has depth %d, which exceeds the limit of %d", req.depth, h.limits.depth))
		return
	}
	req.aliases = mcpAliasCount(op.SelectionSet, doc.Fragments)
	if req.aliases > h.limits.aliases {
		refuse(http.StatusBadRequest, mcpReasonAliases, fmt.Sprintf("operation has %d aliases, which exceeds the limit of %d", req.aliases, h.limits.aliases))
		return
	}
	variables, varErr := validator.VariableValues(schema, op, payload.Variables)
	if varErr != nil {
		refuse(http.StatusBadRequest, mcpReasonInvalidVariables, "the variables do not validate against the operation")
		return
	}
	req.complexity = complexity.Calculate(h.es, op, variables)
	if req.complexity > h.limits.complexity {
		refuse(http.StatusBadRequest, mcpReasonComplexity, fmt.Sprintf("operation has complexity %d, which exceeds the limit of %d", req.complexity, h.limits.complexity))
		return
	}
	if orgReason := mcpCheckOrgArguments(op.SelectionSet, doc.Fragments, variables, claims.OrgID); orgReason != "" {
		refuse(http.StatusForbidden, orgReason, "every orgId argument must name the caller's org")
		return
	}
	if status, reason := mcpCheckRequestInputs(schema, op, doc.Fragments, variables, payload.Variables, claims.OrgID); reason != "" {
		refuse(status, reason, "the request names another org, selects a person, or exceeds an input limit of the MCP caller class")
		return
	}
	for _, root := range roots {
		if root == "__typename" {
			continue
		}
		if !h.sw.Enabled(mcpRoutingOperationPrefix + root) {
			refuse(http.StatusNotFound, mcpReasonRootFieldNotEnabled, "a root field is not enabled for the MCP caller class")
			return
		}
	}

	obs := &mcpObservation{}
	ctx := authctx.WithClaims(r.Context(), claims)
	ctx = graph.WithRequestBody(ctx, body)
	ctx = context.WithValue(ctx, mcpObservationKey{}, obs)
	dispatched := r.WithContext(ctx)
	dispatched.Body = io.NopCloser(bytes.NewReader(body))

	buffered := &mcpBufferedResponse{header: http.Header{}, status: http.StatusOK}
	h.gql.ServeHTTP(buffered, dispatched)
	req.clickHouse = obs.callCount()

	if budget := obs.budgetReason(); budget != "" {
		// A budget refusal wins over whatever the resolvers answered: a
		// resolver that swallowed it would otherwise hand back an empty
		// result that reads as "no data".
		refuse(http.StatusUnprocessableEntity, budget, "the query exceeded the MCP caller class's ClickHouse read budget; narrow the window or the scope")
		return
	}
	req.outcome, req.status = mcpOutcomeServed, buffered.status
	if obs.fieldErrorCount() > 0 {
		req.reason = mcpReasonFieldErrors
	}
	for key, values := range buffered.header {
		w.Header()[key] = values
	}
	// gqlgen's body is already JSON: re-emit it through the one response path.
	writeMCPJSON(w, buffered.status, json.RawMessage(buffered.body.Bytes()))
}

// mcpAuthenticate takes the identity from the internal identity headers
// ONLY, and refuses any elevated claim. The returned claims always carry
// Role "", IsSuperuser false and ImpersonationActive false.
func mcpAuthenticate(r *http.Request) (authctx.Claims, int, string) {
	if len(r.Header.Values("Authorization")) > 0 {
		return authctx.Claims{}, http.StatusUnauthorized, mcpReasonAuthorization
	}
	if !internalidentity.Present(r.Header) {
		return authctx.Claims{}, http.StatusUnauthorized, mcpReasonNoCarrier
	}
	claims, err := internalidentity.FromHeader(r.Header)
	if err != nil {
		return authctx.Claims{}, http.StatusUnauthorized, internalidentity.ReasonOf(err)
	}
	if claims.OrgID == "" || claims.OrgID != strings.TrimSpace(claims.OrgID) {
		return authctx.Claims{}, http.StatusUnauthorized, mcpReasonInvalidOrg
	}
	var elevated []string
	if claims.IsSuperuser {
		elevated = append(elevated, "superuser")
	}
	if claims.ImpersonationActive {
		elevated = append(elevated, "impersonation")
	}
	if role := strings.ToLower(strings.TrimSpace(claims.Role)); mcpOperatorRoles[role] {
		elevated = append(elevated, "role_"+role)
	}
	if len(elevated) > 0 {
		// Loud on purpose: acr-api sends superuser=false and impersonation=false
		// as constants and a least role, so any of these means a misconfigured
		// or taken-over caller. Names only (a fixed vocabulary), never a value.
		for _, claim := range elevated {
			mcpElevatedClaimCounter.Add(r.Context(), 1, metric.WithAttributes(attribute.String("claim", claim)))
		}
		slog.ErrorContext(r.Context(), "query-api: MCP caller class REFUSED a request claiming elevated privileges",
			"caller_class", mcpCallerClass, "claims", strings.Join(elevated, ","), "remote_addr", r.RemoteAddr)
		return authctx.Claims{}, http.StatusForbidden, mcpReasonElevatedClaim
	}
	return authctx.Claims{OrgID: claims.OrgID}, 0, ""
}

// mcpRootFields lists the operation's root field names in order, following
// root-level fragment spreads and inline fragments, and reports whether any
// field at any depth is __schema or __type.
func mcpRootFields(set ast.SelectionSet, fragments ast.FragmentDefinitionList) (roots []string, introspection bool) {
	seen := map[string]bool{}
	var collect func(set ast.SelectionSet, visiting map[string]bool)
	collect = func(set ast.SelectionSet, visiting map[string]bool) {
		for _, selection := range set {
			switch node := selection.(type) {
			case *ast.Field:
				if !seen[node.Name] {
					seen[node.Name] = true
					roots = append(roots, node.Name)
				}
			case *ast.InlineFragment:
				collect(node.SelectionSet, visiting)
			case *ast.FragmentSpread:
				if visiting[node.Name] {
					continue
				}
				if fragment := fragments.ForName(node.Name); fragment != nil {
					visiting[node.Name] = true
					collect(fragment.SelectionSet, visiting)
					delete(visiting, node.Name)
				}
			}
		}
	}
	collect(set, map[string]bool{})
	mcpWalkFields(set, fragments, func(field *ast.Field) {
		if field.Name == "__schema" || field.Name == "__type" {
			introspection = true
		}
	})
	return roots, introspection
}

// mcpDocumentNodesWithin counts, without expanding anything, the field
// nodes, fragment-spread nodes and inline-fragment nodes of every operation
// and fragment definition, and the fragment definitions themselves. It
// reports false as soon as any one count exceeds limit.
func mcpDocumentNodesWithin(doc *ast.QueryDocument, limit int) bool {
	if len(doc.Fragments) > limit {
		return false
	}
	fields, spreads, inlines := 0, 0, 0
	var count func(set ast.SelectionSet) bool
	count = func(set ast.SelectionSet) bool {
		for _, selection := range set {
			switch node := selection.(type) {
			case *ast.Field:
				fields++
				if fields > limit || !count(node.SelectionSet) {
					return false
				}
			case *ast.FragmentSpread:
				spreads++
				if spreads > limit {
					return false
				}
			case *ast.InlineFragment:
				inlines++
				if inlines > limit || !count(node.SelectionSet) {
					return false
				}
			}
		}
		return true
	}
	for _, op := range doc.Operations {
		if !count(op.SelectionSet) {
			return false
		}
	}
	for _, fragment := range doc.Fragments {
		if !count(fragment.SelectionSet) {
			return false
		}
	}
	return true
}

// mcpFragmentGraphCheck computes, exactly and in time linear in the
// document, the fully expanded size and depth of every operation and every
// fragment definition of a parsed, not yet validated document, by memoised
// dynamic programming over the fragment graph:
//
//	size(set)  = sum over nodes of 1 + size(children)        (field, inline fragment)
//	           + 1 + size(target fragment)                   (fragment spread)
//	depth(set) = 1 + max over nodes of depth(children / target fragment)
//
// Every node kind counts, so a fragment graph that expands without reaching a
// field is bounded too. Sizes saturate at the cap + 1, so nothing overflows
// and nothing is computed past the cap. A cycle or an unknown spread target is
// refused as an invalid document (the validator would refuse both, but only
// after walking them). It returns the refusal reason and message, or "".
func mcpFragmentGraphCheck(doc *ast.QueryDocument, limits mcpLimits) (reason, message string) {
	maxNodes, maxDepth := limits.complexity*mcpExpandedNodeFactor, limits.depth*mcpExpandedDepthFactor
	type result struct{ size, depth int }
	const inProgress = -1
	memo := map[string]result{}
	state := map[string]int{} // fragment name -> inProgress / done
	var bad string
	var walk func(set ast.SelectionSet) result
	walkFragment := func(name string) result {
		if state[name] == inProgress {
			bad = fmt.Sprintf("fragment %q spreads itself, directly or through other fragments", name)
			return result{}
		}
		if r, ok := memo[name]; ok {
			return r
		}
		fragment := doc.Fragments.ForName(name)
		if fragment == nil {
			bad = fmt.Sprintf("fragment %q is spread but not defined", name)
			return result{}
		}
		state[name] = inProgress
		r := walk(fragment.SelectionSet)
		delete(state, name)
		memo[name] = r
		return r
	}
	walk = func(set ast.SelectionSet) result {
		total, deepest := 0, 0
		for _, selection := range set {
			var child result
			switch node := selection.(type) {
			case *ast.Field:
				child = walk(node.SelectionSet)
			case *ast.InlineFragment:
				child = walk(node.SelectionSet)
			case *ast.FragmentSpread:
				child = walkFragment(node.Name)
			}
			if bad != "" {
				return result{}
			}
			total += 1 + child.size
			if total > maxNodes {
				total = maxNodes + 1
			}
			if child.depth > deepest {
				deepest = child.depth
			}
		}
		return result{size: total, depth: deepest + 1}
	}
	over := false
	check := func(set ast.SelectionSet) {
		if r := walk(set); bad == "" && (r.size > maxNodes || r.depth > maxDepth) {
			over = true
		}
	}
	for _, op := range doc.Operations {
		if check(op.SelectionSet); bad != "" || over {
			break
		}
	}
	for _, fragment := range doc.Fragments {
		if bad != "" || over {
			break
		}
		check(fragment.SelectionSet)
	}
	if bad != "" {
		return mcpReasonInvalidDocument, bad
	}
	if over {
		return mcpReasonComplexity, fmt.Sprintf("the document expands to more than %d nodes or %d levels, which exceeds the limit", maxNodes, maxDepth)
	}
	return "", ""
}

// mcpExpandedFieldsWithin reports whether the selection set, expanded
// through every fragment use, holds at most budget field occurrences. It
// stops at the first field over budget, so its own cost is bounded by the
// budget (times the fragment chain length), never by the expanded size.
func mcpExpandedFieldsWithin(set ast.SelectionSet, fragments ast.FragmentDefinitionList, budget int) bool {
	count := 0
	var walk func(set ast.SelectionSet, visiting map[string]bool) bool
	walk = func(set ast.SelectionSet, visiting map[string]bool) bool {
		for _, selection := range set {
			switch node := selection.(type) {
			case *ast.Field:
				count++
				if count > budget {
					return false
				}
				if !walk(node.SelectionSet, visiting) {
					return false
				}
			case *ast.InlineFragment:
				if !walk(node.SelectionSet, visiting) {
					return false
				}
			case *ast.FragmentSpread:
				if visiting[node.Name] {
					continue
				}
				if fragment := fragments.ForName(node.Name); fragment != nil {
					visiting[node.Name] = true
					within := walk(fragment.SelectionSet, visiting)
					delete(visiting, node.Name)
					if !within {
						return false
					}
				}
			}
		}
		return true
	}
	return walk(set, map[string]bool{})
}

// mcpAliasCount counts every field, at every depth and through every
// fragment use, whose alias differs from its name. A fragment spread twice
// is counted twice: it executes twice.
func mcpAliasCount(set ast.SelectionSet, fragments ast.FragmentDefinitionList) int {
	count := 0
	mcpWalkFields(set, fragments, func(field *ast.Field) {
		// gqlparser sets Alias to Name when the document gives none.
		if field.Alias != field.Name {
			count++
		}
	})
	return count
}

// mcpWalkFields visits every field of a selection set, following fragment
// spreads (each use) and inline fragments. A spread already on the current
// path is not re-entered, so even an unvalidated cyclic document ends.
func mcpWalkFields(set ast.SelectionSet, fragments ast.FragmentDefinitionList, visit func(*ast.Field)) {
	var walk func(set ast.SelectionSet, visiting map[string]bool)
	walk = func(set ast.SelectionSet, visiting map[string]bool) {
		for _, selection := range set {
			switch node := selection.(type) {
			case *ast.Field:
				visit(node)
				walk(node.SelectionSet, visiting)
			case *ast.InlineFragment:
				walk(node.SelectionSet, visiting)
			case *ast.FragmentSpread:
				if visiting[node.Name] {
					continue
				}
				if fragment := fragments.ForName(node.Name); fragment != nil {
					visiting[node.Name] = true
					walk(fragment.SelectionSet, visiting)
					delete(visiting, node.Name)
				}
			}
		}
	}
	walk(set, map[string]bool{})
}

// mcpCheckOrgArguments requires every orgId/org_id the operation carries --
// a field argument, a field of an input object at any depth, or either of
// those supplied through a variable -- to be exactly the header org. It
// returns "" when the operation passes, else the refusal reason.
func mcpCheckOrgArguments(set ast.SelectionSet, fragments ast.FragmentDefinitionList, variables map[string]any, orgID string) string {
	reason := ""
	check := func(value any) {
		if reason != "" {
			return
		}
		// A non-string value reads as "" and is refused with the empty one.
		s, _ := value.(string)
		if s == "" || s != strings.TrimSpace(s) {
			reason = mcpReasonInvalidOrgArgument
			return
		}
		if s != orgID {
			reason = mcpReasonOrgMismatch
		}
	}
	mcpWalkFields(set, fragments, func(field *ast.Field) {
		for _, argument := range field.Arguments {
			if argument.Name == "orgId" || argument.Name == "org_id" {
				value, err := argument.Value.Value(variables)
				if err != nil {
					reason = mcpReasonInvalidOrgArgument
					return
				}
				check(value)
				continue
			}
			value, err := argument.Value.Value(variables)
			if err != nil {
				reason = mcpReasonInvalidOrgArgument
				return
			}
			mcpCheckNestedOrg(value, check)
		}
	})
	return reason
}

// mcpCheckNestedOrg walks an argument's resolved value (input objects and
// lists) and checks every orgId/org_id member it holds.
func mcpCheckNestedOrg(value any, check func(any)) {
	switch v := value.(type) {
	case map[string]any:
		for key, member := range v {
			if key == "orgId" || key == "org_id" {
				check(member)
				continue
			}
			mcpCheckNestedOrg(member, check)
		}
	case []any:
		for _, member := range v {
			mcpCheckNestedOrg(member, check)
		}
	}
}

// mcpMaxSimulations is the largest Monte Carlo draw count an MCP caller may
// ask of capacityForecast: the SDL default. The resolver accepts up to
// 1,000,000 (6.8 s of uncancellable CPU for one call, measured; times the
// alias cap), and ClickHouse's budget does not bound resolver-side CPU.
const mcpMaxSimulations = 10000

// mcpCheckRequestInputs fails closed over EVERY value the request carries,
// not only the arguments the selected fields read: the field arguments
// (literals and variables resolved), the coerced variables (declared, used or
// not, with their defaults), and the raw variables (including undeclared
// ones). It refuses another org's id anywhere (orgId/org_id members and the
// ids of an ORG-level scope), any person selector (decided from the schema at
// the value's typed position: see mcp_person_policy.go) and a `simulations` count over
// mcpMaxSimulations. It returns the HTTP status and reason, or 0 and "".
func mcpCheckRequestInputs(schema *ast.Schema, op *ast.OperationDefinition, fragments ast.FragmentDefinitionList, variables, rawVariables map[string]any, orgID string) (int, string) {
	status, reason := 0, ""
	fail := func(s int, r string) {
		if reason == "" {
			status, reason = s, r
		}
	}
	var scan func(key string, value any)
	scan = func(key string, value any) {
		switch v := value.(type) {
		case string:
			if key == "orgId" || key == "org_id" {
				if v != orgID {
					fail(http.StatusForbidden, mcpReasonOrgMismatch)
				}
			}
		case map[string]any:
			if level, _ := v["level"].(string); level == "ORG" {
				if ids, ok := v["ids"].([]any); ok {
					for _, id := range ids {
						if s, _ := id.(string); s != orgID {
							fail(http.StatusForbidden, mcpReasonOrgMismatch)
						}
					}
				}
			}
			for k, member := range v {
				switch k {
				case "simulations":
					if n, ok := mcpInt(member); !ok || n > mcpMaxSimulations {
						fail(http.StatusBadRequest, mcpReasonInputLimit)
					}
				case "orgId", "org_id":
					if _, isString := member.(string); !isString {
						fail(http.StatusForbidden, mcpReasonInvalidOrgArgument)
					}
				}
				scan(k, member)
			}
		case []any:
			for _, member := range v {
				scan(key, member)
			}
		}
	}
	scan("", map[string]any(variables))
	scan("", map[string]any(rawVariables))
	for _, definition := range op.VariableDefinitions {
		if definition.DefaultValue != nil {
			if value, err := definition.DefaultValue.Value(nil); err == nil {
				scan(definition.Variable, value)
			}
		}
	}
	mcpWalkFields(op.SelectionSet, fragments, func(field *ast.Field) {
		for _, argument := range field.Arguments {
			value, err := argument.Value.Value(variables)
			if err != nil {
				fail(http.StatusForbidden, mcpReasonInvalidOrgArgument)
				return
			}
			if field.Definition != nil && mcpArgumentSelectsPerson(schema, field.Definition.Arguments.ForName(argument.Name), value) {
				fail(http.StatusForbidden, mcpReasonPersonScope)
			}
			if argument.Name == "simulations" {
				if n, ok := mcpInt(value); !ok || n > mcpMaxSimulations {
					fail(http.StatusBadRequest, mcpReasonInputLimit)
				}
			}
			scan(argument.Name, value)
		}
	})
	return status, reason
}

// mcpInt reads a JSON or coerced GraphQL number as an int.
func mcpInt(value any) (int, bool) {
	switch n := value.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), n == float64(int(n))
	case json.Number:
		i, err := n.Int64()
		return int(i), err == nil
	}
	return 0, false
}

// writeMCPRefusal answers a refusal as a GraphQL error body with a typed
// code and the closed-vocabulary reason.
func writeMCPRefusal(w http.ResponseWriter, status int, reason, message string) {
	code := "MCP_REFUSED"
	switch reason {
	case mcpReasonBytesCeiling, mcpReasonRowsCeiling, mcpReasonTimeCeiling:
		code = "MCP_READ_BUDGET_EXCEEDED"
	}
	writeMCPJSON(w, status, map[string]any{
		"errors": []map[string]any{{
			"message":    message,
			"extensions": map[string]any{"code": code, "reason": reason},
		}},
	})
}

// writeMCPJSON is this listener's one JSON response path: an explicit
// Content-Type and json.NewEncoder(w).Encode, never a raw w.Write of bytes
// (the repo's JSON-response rule; semgrep no-direct-write-to-responsewriter).
func writeMCPJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("query-api: mcp request: encode response failed: err=%v", err)
	}
}

// emit writes the request's one log line and its counter and duration. No
// variable value and no query text: the document is named by its digest.
func (req *mcpRequest) emit(r *http.Request) {
	duration := time.Since(req.start)
	roots := append([]string(nil), req.rootFields...)
	sort.Strings(roots)
	attrs := metric.WithAttributes(
		attribute.String("caller_class", mcpCallerClass),
		attribute.String("outcome", req.outcome),
		attribute.String("reason", req.reason),
	)
	mcpRequestCounter.Add(r.Context(), 1, attrs)
	mcpRequestDuration.Record(r.Context(), duration.Seconds(), attrs)
	log.Printf("query-api: mcp request: caller_class=%s outcome=%s reason=%s status=%d document_digest=%s root_fields=%s aliases=%d depth=%d complexity=%d clickhouse_queries=%d duration_ms=%d request_id=%s",
		mcpCallerClass, req.outcome, req.reason, req.status, req.documentDigest, strings.Join(roots, ","),
		req.aliases, req.depth, req.complexity, req.clickHouse, duration.Milliseconds(), envelopeRequestID(r))
}

var (
	mcpRequestCounter       = mustMCPCounter("devhealth_query_api_mcp_requests_total", "MCP caller-class requests by outcome and reason")
	mcpElevatedClaimCounter = mustMCPCounter("devhealth_query_api_mcp_elevated_claims_total", "MCP caller-class requests refused for claiming superuser, impersonation or an operator role, by claim")
	mcpRequestDuration      = mustMCPHistogram("devhealth_query_api_mcp_request_duration_seconds", "MCP caller-class request duration by outcome and reason")
)

func mcpMeter() metric.Meter {
	return otel.Meter("github.com/full-chaos/dev-health-ops/internal/queryapi/server/mcp")
}

func mustMCPCounter(name, description string) metric.Int64Counter {
	counter, err := mcpMeter().Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(name)
	}
	return counter
}

func mustMCPHistogram(name, description string) metric.Float64Histogram {
	histogram, err := mcpMeter().Float64Histogram(name, metric.WithDescription(description), metric.WithUnit("s"))
	if err != nil {
		histogram, _ = otel.GetMeterProvider().Meter("noop").Float64Histogram(name)
	}
	return histogram
}

// mcpObservation is one request's view of its ClickHouse calls: how many,
// and whether any hit a read budget.
type mcpObservation struct {
	mu          sync.Mutex
	calls       int
	budget      string
	fieldErrors int
}

type mcpObservationKey struct{}

func mcpObservationFrom(ctx context.Context) *mcpObservation {
	obs, _ := ctx.Value(mcpObservationKey{}).(*mcpObservation)
	return obs
}

func (o *mcpObservation) call() {
	o.mu.Lock()
	o.calls++
	o.mu.Unlock()
}

func (o *mcpObservation) callCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls
}

func (o *mcpObservation) fieldError() {
	o.mu.Lock()
	o.fieldErrors++
	o.mu.Unlock()
}

func (o *mcpObservation) fieldErrorCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.fieldErrors
}

func (o *mcpObservation) observe(err error) {
	if err == nil {
		return
	}
	reason := mcpBudgetReason(err)
	if reason == "" {
		return
	}
	o.mu.Lock()
	if o.budget == "" {
		o.budget = reason
	}
	o.mu.Unlock()
}

func (o *mcpObservation) budgetReason() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.budget
}

// mcpBudgetReason classifies a ClickHouse error as one of the class's
// budget refusals, or "".
func mcpBudgetReason(err error) string {
	var exception *clickhousedriver.Exception
	if errors.As(err, &exception) {
		switch exception.Code {
		case clickHouseTooManyBytesCode:
			return mcpReasonBytesCeiling
		case clickHouseTooManyRowsOrBytesCode, clickHouseTooManyRowsCode:
			return mcpReasonRowsCeiling
		case clickHouseTimeoutExceededCode:
			return mcpReasonTimeCeiling
		}
	}
	// dev-health-go bounds every query with a context deadline of
	// MaxExecutionTime, and clickhouse-go then sends the server a
	// max_execution_time a few seconds LONGER than that deadline (measured:
	// 10 s configured reads back as 14 on the server). So the 10 s ceiling
	// normally fires client-side, as context.DeadlineExceeded, not as the
	// server's TIMEOUT_EXCEEDED -- both are the class's time ceiling.
	if errors.Is(err, context.DeadlineExceeded) {
		return mcpReasonTimeCeiling
	}
	// The same deadline reaches the connection as a socket read deadline, and
	// whichever of the two fires first decides the error shape: a read that is
	// in flight at the deadline fails as a net timeout (os.ErrDeadlineExceeded
	// under a *net.OpError, wrapped by the driver), not as the context's
	// error. CI saw exactly that at the 1 s ceiling: "ClickHouse row
	// iteration failed: *fmt.wrapError", served as field_errors instead of
	// the typed 422. This client's only read deadline is the ceiling's.
	// A DIAL timeout (the client's own 5 s DialTimeout) is a connectivity
	// failure, not the read ceiling: it stays a store error.
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return ""
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return mcpReasonTimeCeiling
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return mcpReasonTimeCeiling
	}
	return ""
}

// mcpObservedClient counts every ClickHouse call a request makes and
// records a budget error wherever it surfaces: at Query, while iterating,
// or at Close.
type mcpObservedClient struct {
	next featureflags.QueryClient
}

func (c mcpObservedClient) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	obs := mcpObservationFrom(ctx)
	if obs != nil {
		obs.call()
	}
	rows, err := c.next.Query(ctx, statement, bindings)
	if err != nil {
		if obs != nil {
			obs.observe(err)
		}
		return nil, err
	}
	if obs == nil {
		return rows, nil
	}
	return mcpObservedRows{RowScanner: rows, obs: obs}, nil
}

type mcpObservedRows struct {
	dhclickhouse.RowScanner
	obs *mcpObservation
}

func (r mcpObservedRows) Scan(dest ...any) error {
	err := r.RowScanner.Scan(dest...)
	r.obs.observe(err)
	return err
}

func (r mcpObservedRows) Err() error {
	err := r.RowScanner.Err()
	r.obs.observe(err)
	return err
}

func (r mcpObservedRows) Close() error {
	err := r.RowScanner.Close()
	r.obs.observe(err)
	return err
}

// mcpBufferedResponse holds the executor's answer until the budget check
// has run, so a budget refusal can replace it whole.
type mcpBufferedResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
	wrote  bool
}

func (b *mcpBufferedResponse) Header() http.Header { return b.header }

func (b *mcpBufferedResponse) WriteHeader(status int) {
	if b.wrote {
		return
	}
	b.status, b.wrote = status, true
}

func (b *mcpBufferedResponse) Write(p []byte) (int, error) {
	b.wrote = true
	return b.body.Write(p)
}
