package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"io"
	"log"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyheaders"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// graphQLEdgePath is the product GraphQL path: the one the web app (browser
// urql client and server components alike) sends every document to, and the
// one the Ingress names. Until CHAOS-6263 the Python api answered it and
// forwarded each registered document to /query here; query-api now answers it
// itself, through the SAME document pipeline /query runs
// (newDocumentDispatchHandler), so the registered-document gate, the routing
// switch, the carriers and the executor cannot differ between the two paths.
//
// What /graphql adds over /query is only what the Python edge did before it
// forwarded, so that a request answers the same whichever plane is in front:
//
//   - GET. urql sends a query as GET when its URL fits in 2047 bytes
//     (@urql/core's preferGetMethod default, "within-url-limit"), and the
//     Python edge turned such a request into the POST body /query reads
//     (go_api_dispatcher.py _build_outbound_body). A mutation over GET is
//     refused and never runs, as GraphQL requires and Strawberry did.
//   - the identity is checked before the body is parsed: the Python app
//     resolved its GraphQL context (and answered 401) before Strawberry
//     parsed anything, so an unauthenticated malformed request is a 401,
//     not a 400.
//   - refusals the Python edge answered itself carry its bodies (see
//     graphQLEdgeRefuse).
//
// /query stays exactly what it was: internal-listener callers and the proof
// routes never see any of this, because the behaviour is keyed on a context
// value only newGraphQLEdgeHandler sets.
const graphQLEdgePath = "/graphql"

// graphQLEdgeKey marks a request that arrived on /graphql. Unexported and set
// only by newGraphQLEdgeHandler, so no client input can switch the edge
// behaviour on or off.
type graphQLEdgeKey struct{}

func withGraphQLEdge(ctx context.Context) context.Context {
	return context.WithValue(ctx, graphQLEdgeKey{}, true)
}

func isGraphQLEdge(ctx context.Context) bool {
	edge, _ := ctx.Value(graphQLEdgeKey{}).(bool)
	return edge
}

// newGraphQLEdgeHandler is /graphql over the /query pipeline. The answer is
// sent whole, with its Content-Length: the Python edge held query-api's
// entire body before it replied (httpx's resp.content), so no answer of it
// was ever chunked, where net/http streams a body past its buffer.
func newGraphQLEdgeHandler(query http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		whole := &wholeResponse{ResponseWriter: w}
		query(whole, r.WithContext(withGraphQLEdge(r.Context())))
		whole.send()
	}
}

// wholeResponse holds a handler's answer until the handler returns.
type wholeResponse struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (w *wholeResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *wholeResponse) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(p)
}

func (w *wholeResponse) send() {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.ResponseWriter.Header().Set("Content-Length", strconv.Itoa(w.body.Len()))
	w.ResponseWriter.WriteHeader(w.status)
	_, _ = w.ResponseWriter.Write(w.body.Bytes())
}

// graphQLEdgeDeps is what /graphql's request middleware needs beyond the
// /query pipeline: the edge authenticator the org scope and impersonation
// middlewares read the caller with, the CORS allow-list, the body limit.
type graphQLEdgeDeps struct {
	auth        *policy.Authenticator
	corsOrigins []string
	maxBytes    int
	logger      *slog.Logger
}

// graphQLEdgeChain is /graphql with the Python api's request middleware, in
// its order (api/_middleware.py registers in reverse, so the last one added
// runs first) inside Starlette's ServerErrorMiddleware (an unhandled error's
// 500 carries only its content headers): the correlation id, OrgIdMiddleware,
// ImpersonationMiddleware,
// GraphQLQuerySizeLimitMiddleware, SecurityHeadersMiddleware,
// CORSMiddleware, then the route. The order is the contract, not a detail:
// a refusal from a middleware outside SecurityHeadersMiddleware (the org
// scope's 403, the size limit's 413 and browser 404) carries no security
// header, as it did not on the Python api; everything inside does. The org
// scope and impersonation middlewares are go-api's own ports
// (internal/api/policy), the headers and CORS the shared ones
// (internal/api/pyheaders), so query-api and go-api cannot drift apart on
// them. The provenance stamp is outermost, so every answer is bound to the
// build that gave it.
func graphQLEdgeChain(query http.HandlerFunc, deps graphQLEdgeDeps) http.Handler {
	scope := policy.NewScope(deps.auth, deps.logger)
	var handler http.Handler = newGraphQLEdgeHandler(query)
	handler = pyheaders.NewCORS(deps.corsOrigins).Wrap(handler)
	handler = pyheaders.SecurityHeaders(handler)
	handler = graphQLEdgeLimits(deps.maxBytes, handler)
	handler = scope.Impersonation(handler)
	handler = scope.OrgScope(handler)
	// Outside every consumer of the caller's identity, so the org scope, the
	// impersonation middleware and the pipeline read each fact of it once and
	// see one answer.
	handler = deps.auth.ReadOnce(handler)
	handler = correlationID(handler)
	handler = pyheaders.UnhandledErrorShape(handler)
	return withProofProvenance(handler.ServeHTTP, runningBuild())
}

// mountGraphQLRoute registers /graphql. It stays unmounted -- a 404, and a
// log line saying why -- when the pod has no edge secret: the user's edge
// access token is the path's only credential, so without it the path could
// only ever refuse.
func mountGraphQLRoute(mux *http.ServeMux, query http.HandlerFunc, deps graphQLEdgeDeps) {
	if deps.auth == nil {
		log.Printf("query-api: %s NOT mounted: %s is unset, so the product path would have no credential to accept", graphQLEdgePath, edgeJWTSecretEnvVar)
		return
	}
	mux.Handle(graphQLEdgePath, graphQLEdgeChain(query, deps))
	log.Printf("query-api: %s mounted (the /query pipeline behind the Python api's request middleware; %d CORS origin(s))", graphQLEdgePath, len(deps.corsOrigins))
}

// defaultCORSAllowedOrigins is the Python api's own default
// (api/_middleware.py _parse_cors_origins), the same as go-api's.
const defaultCORSAllowedOrigins = "http://localhost:3000"

// corsAllowedOrigins is _parse_cors_origins: CORS_ALLOWED_ORIGINS split on
// commas, each entry trimmed, empty entries dropped; the default only when
// the variable is absent (a present empty value is an empty allow-list).
func corsAllowedOrigins(lookup func(string) (string, bool)) []string {
	raw, present := lookup("CORS_ALLOWED_ORIGINS")
	if !present {
		raw = defaultCORSAllowedOrigins
	}
	origins := []string{}
	for _, entry := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(entry); trimmed != "" {
			origins = append(origins, trimmed)
		}
	}
	return origins
}

// correlationID is CorrelationIdMiddleware: the first X-Request-ID the
// request carries, read as written, else a new UUID, echoed on the response.
func correlationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := ""
		if values := r.Header.Values("X-Request-ID"); len(values) > 0 {
			id = values[0]
		}
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Add("X-Request-ID", id)
		next.ServeHTTP(w, r)
	})
}

// graphQLEdgeLimits is GraphQLQuerySizeLimitMiddleware
// (graphql/security.py), which runs before the route and before any
// credential is read: a browser's GET is answered 404 (the GraphQL IDE is
// off in every deployment), and a POST, PUT or PATCH body past the limit
// 413. A body within the limit is handed on whole.
func graphQLEdgeLimits(limit int, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && acceptsHTML(r) {
			httpapi.RecordNotFoundCause(r.Context(), httpapi.NotFoundIDEOff)
			refuseGraphQLEdgeBrowse(w)
			return
		}
		switch r.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch:
		default:
			next.ServeHTTP(w, r)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, int64(limit)+1))
		_ = r.Body.Close()
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if len(body) > limit {
			refuseGraphQLEdgeOversize(w, limit)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		next.ServeHTTP(w, r)
	})
}

// authenticateGraphQLEdge is authenticateEdgeTokenOnly with the Python
// app's answers: a missing, malformed or rejected credential is FastAPI's 401
// {"detail": "Authentication required"} (graphql/app.py get_context); a
// store the check could not read is the unhandled 500 its database error
// raised there -- never a 401, which would tell the client its credential is
// bad when nothing was decided. The authenticator is the one the chain bound
// to the request (graphQLEdgeChain), the same instance the middleware read
// the caller with, so the pipeline gets their answers, not new reads.
func authenticateGraphQLEdge(w http.ResponseWriter, r *http.Request) (authctx.Claims, bool) {
	claims, outcome := authenticateEdgeTokenOnly(r, policy.BoundAuthenticator(r.Context()))
	switch outcome {
	case edgeAccepted:
		// The identity the resolvers run as, beside the response headers
		// the middleware set, so a served request can be traced to both.
		slog.DebugContext(r.Context(), "query-api: /graphql identity",
			slog.String("org_id", claims.OrgID), slog.String("role", claims.Role),
			slog.Bool("is_superuser", claims.IsSuperuser), slog.Bool("impersonation_active", claims.ImpersonationActive),
			slog.String("request_id", envelopeRequestID(r)))
		return claims, true
	case edgeUnavailable:
		policy.WriteInternal(w)
	default:
		policy.WriteDetail(w, http.StatusUnauthorized, "Authentication required", nil)
	}
	return authctx.Claims{}, false
}

// acceptsHTML is the Python edge's browser check (graphql/security.py
// _accepts_html): the LAST Accept header (Starlette builds a dict from the
// header list) contains "text/html", compared as written.
func acceptsHTML(r *http.Request) bool {
	values := r.Header.Values("Accept")
	if len(values) == 0 {
		return false
	}
	return strings.Contains(values[len(values)-1], "text/html")
}

// refuseGraphQLEdgeBrowse is GraphQLQuerySizeLimitMiddleware's answer to a
// browser GET while the GraphQL IDE is off -- always, in every deployment of
// the Python api that set no ENVIRONMENT (graphql/security.py
// _send_not_found): 404, before any credential is read.
func refuseGraphQLEdgeBrowse(w http.ResponseWriter) {
	body := pyjson.NewObject()
	body.Set("detail", "Not Found")
	writeDumped(w, http.StatusNotFound, body)
}

// refuseGraphQLEdgeMethod is FastAPI's answer for a method the /graphql
// router does not declare. The router declares GET and POST as two routes,
// and Starlette's 405 names the methods of the first partial match: GET.
func refuseGraphQLEdgeMethod(w http.ResponseWriter) {
	policy.WriteDetail(w, http.StatusMethodNotAllowed, "Method Not Allowed", http.Header{"Allow": {"GET"}})
}

// refuseGraphQLEdgeOversize is GraphQLQuerySizeLimitMiddleware's 413
// (graphql/security.py _send_too_large): json.dumps of the detail, spaced.
func refuseGraphQLEdgeOversize(w http.ResponseWriter, limit int) {
	detail := pyjson.NewObject()
	detail.Set("message", "GraphQL request body exceeds size limit")
	detail.Set("limit_bytes", limit)
	body := pyjson.NewObject()
	body.Set("detail", detail)
	writeDumped(w, http.StatusRequestEntityTooLarge, body)
}

func writeDumped(w http.ResponseWriter, status int, body pyjson.Value) {
	encoded, err := pyjson.Dumps(body)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	writeBody(w, status, jsonBody, encoded)
}

// bodyType is the media type of a body this file writes: JSON or plain text,
// never a type a browser renders as a page.
type bodyType string

const (
	jsonBody  bodyType = "application/json"
	plainBody bodyType = "text/plain; charset=utf-8"
)

// writeBody is the one writer of the bodies this file builds (a fixed refusal
// text, or the JSON encoding of one). It states the body's non-HTML type and
// forbids a browser to sniff another one itself, whatever middleware is or is
// not in front of it, so no answer of this route can be rendered as a page.
// The two size-middleware refusals run outside the security headers, as in
// the Python app, so on those this header is the only defence.
func writeBody(w http.ResponseWriter, status int, kind bodyType, body string) {
	header := w.Header()
	header.Set("Content-Type", string(kind))
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = io.Copy(w, bytes.NewReader([]byte(body)))
}

func writePlain(w http.ResponseWriter, status int, text string) {
	writeBody(w, status, plainBody, text)
}

// queryParam is one name=value pair of a query string, decoded.
type queryParam struct{ name, value string }

// parseQueryString is Starlette's QueryParams over a raw query string:
// parse_qsl(raw.decode("latin-1"), keep_blank_values=True) -- pairs split on
// "&" only, a pair without "=" kept with an empty value, "+" read as a space,
// a well-formed %XX decoded and a malformed one kept as written, and the
// decoded bytes read as UTF-8 with each invalid byte replaced.
func parseQueryString(raw string) []queryParam {
	var params []queryParam
	for _, pair := range strings.Split(raw, "&") {
		if pair == "" {
			continue
		}
		name, value, _ := strings.Cut(pair, "=")
		params = append(params, queryParam{name: unquotePlus(name), value: unquotePlus(value)})
	}
	return params
}

// lastParam is QueryParams.get: the LAST value of a repeated name (Go's
// url.Values.Get returns the first). "" when the name is absent.
func lastParam(params []queryParam, name string) string {
	value := ""
	for _, param := range params {
		if param.name == name {
			value = param.value
		}
	}
	return value
}

// unquotePlus is urllib.parse.unquote_plus(s, encoding="utf-8",
// errors="replace") over a latin-1 decoded string.
func unquotePlus(s string) string {
	s = strings.ReplaceAll(s, "+", " ")
	var decoded []byte
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) && isHex(s[i+1]) && isHex(s[i+2]) {
			decoded = append(decoded, unhex(s[i+1])<<4|unhex(s[i+2]))
			i += 2
			continue
		}
		decoded = append(decoded, s[i])
	}
	var out strings.Builder
	for len(decoded) > 0 {
		r, size := utf8.DecodeRune(decoded)
		out.WriteRune(r)
		decoded = decoded[size:]
	}
	return out.String()
}

func isHex(c byte) bool {
	return ('0' <= c && c <= '9') || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F')
}

func unhex(c byte) byte {
	switch {
	case '0' <= c && c <= '9':
		return c - '0'
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10
	default:
		return c - 'A' + 10
	}
}

// graphQLErrorBody is a GraphQL response carrying one error, in gqlgen's own
// key order, so every /graphql answer query-api decides itself reads like the
// answers its executor gives.
func graphQLErrorBody(message, code string) string {
	encoded, err := json.Marshal(struct {
		Errors []graphQLError `json:"errors"`
		Data   any            `json:"data"`
	}{Errors: []graphQLError{{Message: message, Extensions: map[string]string{"code": code}}}})
	if err != nil {
		return `{"errors":[{"message":"internal error"}],"data":null}`
	}
	return string(encoded)
}

type graphQLError struct {
	Message    string            `json:"message"`
	Extensions map[string]string `json:"extensions"`
}

func writeGraphQLError(w http.ResponseWriter, status int, message, code string) {
	writeBody(w, status, jsonBody, graphQLErrorBody(message, code))
}

// refuseGraphQLEdgeUnregistered answers a document query-api does not
// register: 404, as a GraphQL response. query-api serves registered
// documents only; the Python edge let Strawberry answer these.
func refuseGraphQLEdgeUnregistered(w http.ResponseWriter) {
	writeGraphQLError(w, http.StatusNotFound, "This GraphQL document is not registered.", "UNREGISTERED_DOCUMENT")
}

// graphQLEdgeNotEnabled answers a registered operation whose routing row is
// off, or unreadable (the switch fails closed): a GraphQL error with status
// 200, as the Python edge's Strawberry fallback answered a query in that
// state -- a store failure is an error in the response, never a bare 404.
// Nothing runs.
func graphQLEdgeNotEnabled(operation string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("query-api: /graphql refused an operation that is not enabled: operation=%s", operation)
		writeGraphQLError(w, http.StatusOK, "This operation is not enabled on this deployment.", "OPERATION_NOT_ENABLED")
	}
}

// edgeRefusal answers a /graphql request the Python edge did not forward to
// query-api: Strawberry answered those itself, and these are its answers.
type edgeRefusal func(http.ResponseWriter)

func plainRefusal(status int, text string) edgeRefusal {
	return func(w http.ResponseWriter) { writePlain(w, status, text) }
}

// Strawberry's refusals (strawberry/http/async_base_view.py, base.py), and
// the unhandled error its json.loads raises for anything but a syntax error
// (a lone invalid UTF-8 byte, an integer past 4300 digits) or its
// data.get on a JSON body that is not an object: the main app's generic 500,
// answered outside every other middleware (pyheaders.UnhandledErrorShape).
var (
	refuseUnsupportedContentType = plainRefusal(http.StatusBadRequest, "Unsupported content type")
	refuseUnparseable            = plainRefusal(http.StatusBadRequest, "Unable to parse request body as JSON")
	refuseBatch                  = plainRefusal(http.StatusBadRequest, "Batching is not enabled")
	refuseQueryType              = plainRefusal(http.StatusBadRequest, "The GraphQL operation's `query` must be a string or null, if provided.")
	refuseVariablesType          = plainRefusal(http.StatusBadRequest, "The GraphQL operation's `variables` must be an object or null, if provided.")
	refuseExtensionsType         = plainRefusal(http.StatusBadRequest, "The GraphQL operation's `extensions` must be an object or null, if provided.")
	refuseNoQuery                = plainRefusal(http.StatusBadRequest, "No GraphQL query found in the request")
	refuseMutationOverGET        = plainRefusal(http.StatusBadRequest, "mutations are not allowed when using GET")
	refuseIDE                    = plainRefusal(http.StatusNotFound, "Not Found")
	refuseUnhandled              = edgeRefusal(policy.WriteInternal)
)

// firstHeader is Starlette's Headers.get: the FIRST value, "" when absent.
func firstHeader(r *http.Request, name string) string {
	if values := r.Header.Values(name); len(values) > 0 {
		return values[0]
	}
	return ""
}

// graphQLEdgePOSTDocument decides a POST the way the Python edge did. The
// dispatcher forwarded a body json.loads read as an object whose "query" is a
// non-empty string, whatever else it held (query-api then judged the rest):
// that returns the query text. Everything else went to Strawberry, whose
// refusal this returns, in its order.
func graphQLEdgePOSTDocument(r *http.Request, body []byte) (string, edgeRefusal) {
	value, err := pyjson.Decode(body)
	if err == nil {
		if object, ok := value.(*pyjson.Object); ok {
			if query, ok := object.Get("query"); ok {
				if text, ok := query.(string); ok && text != "" {
					return text, nil
				}
			}
		}
	}
	mediaType, _, _ := strings.Cut(firstHeader(r, "Content-Type"), ";")
	if !strings.Contains(strings.TrimSpace(mediaType), "application/json") {
		return "", refuseUnsupportedContentType
	}
	var syntax *pyjson.SyntaxError
	switch {
	case errors.As(err, &syntax):
		return "", refuseUnparseable
	case err != nil:
		return "", refuseUnhandled
	}
	switch typed := value.(type) {
	case []pyjson.Value:
		return "", refuseBatch
	case *pyjson.Object:
		return "", strawberryRequestDataRefusal(typed)
	}
	return "", refuseUnhandled
}

// strawberryRequestDataRefusal is parse_http_body's checks of a request
// object with no usable query, then execute's missing-query refusal.
func strawberryRequestDataRefusal(data *pyjson.Object) edgeRefusal {
	if query, ok := data.Get("query"); ok && query != nil {
		if _, isString := query.(string); !isString {
			return refuseQueryType
		}
	}
	if variables, ok := data.Get("variables"); ok && variables != nil {
		if _, isObject := variables.(*pyjson.Object); !isObject {
			return refuseVariablesType
		}
	}
	if extensions, ok := data.Get("extensions"); ok && extensions != nil {
		if _, isObject := extensions.(*pyjson.Object); !isObject {
			return refuseExtensionsType
		}
	}
	return refuseNoQuery
}

// graphQLEdgeGETDocument decides a GET the way the Python edge did
// (go_api_dispatcher.py _extract_operation + _build_outbound_body). It
// forwarded a GET with a non-empty query whose variables, when present and
// non-empty, json.loads read: as the POST body of the query text, the
// variables re-encoded by Python's json rules, and the operation name, in
// that key order, each only when the request carried it. Every other
// parameter (the browser client's org_id, extensions) was dropped. The rest
// went to Strawberry, whose refusal this returns, in its order.
func graphQLEdgeGETDocument(r *http.Request) (query string, body []byte, refusal edgeRefusal) {
	params := parseQueryString(r.URL.RawQuery)
	query = lastParam(params, "query")
	rawVariables := lastParam(params, "variables")
	if query != "" {
		payload := pyjson.NewObject()
		payload.Set("query", query)
		forward := true
		if rawVariables != "" {
			variables, err := pyjson.DecodeString(rawVariables)
			if err != nil {
				forward = false
			} else if variables != nil {
				payload.Set("variables", variables)
			}
		}
		if forward {
			if name := lastParam(params, "operationName"); name != "" {
				payload.Set("operationName", name)
			}
			encoded, err := pyjson.Dumps(payload)
			if err != nil {
				return "", nil, refuseUnhandled
			}
			return query, []byte(encoded), nil
		}
	}
	if !hasParam(params, "query") {
		accept := firstHeader(r, "Accept")
		if strings.Contains(accept, "text/html") || strings.Contains(accept, "*/*") {
			httpapi.RecordNotFoundCause(r.Context(), httpapi.NotFoundIDEOff)
			return "", nil, refuseIDE
		}
	}
	data := pyjson.NewObject()
	for _, name := range []string{"variables", "extensions"} {
		raw := lastParam(params, name)
		if raw == "" {
			continue
		}
		decoded, err := pyjson.DecodeString(raw)
		var syntax *pyjson.SyntaxError
		switch {
		case errors.As(err, &syntax):
			return "", nil, refuseUnparseable
		case err != nil:
			return "", nil, refuseUnhandled
		}
		data.Set(name, decoded)
	}
	return "", nil, strawberryRequestDataRefusal(data)
}

func hasParam(params []queryParam, name string) bool {
	for _, param := range params {
		if param.name == name {
			return true
		}
	}
	return false
}
