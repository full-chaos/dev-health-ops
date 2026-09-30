package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// The /graphql decision, enumerated. The invariant: a document runs on
// /graphql only when the method is POST, or GET for a query; the request is
// not a browser navigation; the ONE carrier is a valid edge access token of a
// member; and the document is registered. Every other request is refused
// with the status the Python edge answered, and nothing runs.
//
// The expected outcome below is written from that rule, not from the code:
// the table is generated over every combination of the alphabets, so a new
// branch that changes any cell turns this red.

const edgeWriteDocument = "mutation InternalAuthWrite { probe }"

type edgeCell struct {
	method, accept, carrier, document string
}

var (
	edgeMethods   = []string{http.MethodPost, http.MethodGet, http.MethodPut, http.MethodHead}
	edgeAccepts   = []string{"", "text/html"}
	edgeCarriers  = []string{"none", "member", "nonmember", "envelope", "internal-headers", "internal-headers+member", "two-bearers", "basic"}
	edgeDocuments = []string{"query", "mutation", "unregistered", "empty"}
)

// edgeExpect is the rule, cell by cell: the status, and whether the document
// runs.
func edgeExpect(c edgeCell) (int, bool) {
	switch {
	case c.method != http.MethodPost && c.method != http.MethodGet:
		return http.StatusMethodNotAllowed, false
	case c.method == http.MethodGet && c.accept == "text/html":
		return http.StatusNotFound, false
	case c.carrier != "member":
		return http.StatusUnauthorized, false
	case c.document == "empty":
		return http.StatusBadRequest, false
	case c.document == "unregistered":
		return http.StatusNotFound, false
	case c.method == http.MethodGet && c.document == "mutation":
		return http.StatusBadRequest, false
	}
	return http.StatusOK, true
}

// edgeHarness is /graphql over the real pipeline, with a store where ecUser is
// a member of ecOrg and nothing else, and an envelope verifier so the
// envelope is a VALID credential for /query -- which /graphql must still
// refuse.
func edgeHarness(t *testing.T) (http.HandlerFunc, *[]string, string, string, string) {
	t.Helper()
	return edgeHarnessWith(t, &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
	}, routeswitch.StaticSwitch{"probe": true, "probeWrite": true})
}

// edgeHarnessWith is edgeHarness over a given store and routing switch.
func edgeHarnessWith(t *testing.T, store *fakeEdgeStore, sw routeswitch.Switch) (http.HandlerFunc, *[]string, string, string, string) {
	t.Helper()
	verifier, priv := iaVerifier(t)
	ran := &[]string{}
	mux := routeswitch.NewMux(sw)
	for _, op := range []string{"probe", "probeWrite"} {
		op := op
		mux.Register(op, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// What the executor is handed is what the Python edge forwarded:
			// a POST of a JSON body, whatever the client sent.
			*ran = append(*ran, op+" "+r.Method+" "+r.Header.Get("Content-Type"))
			w.WriteHeader(http.StatusOK)
		}))
	}
	byDigest := map[string]string{digestHex(iaDocument): "probe", digestHex(edgeWriteDocument): "probeWrite"}
	auth := ecEdgeAuth(t, store)
	chain := graphQLEdgeChain(newDocumentDispatchHandler(os.Getenv, mux, byDigest, verifier, auth, store, "", nil),
		graphQLEdgeDeps{auth: auth, corsOrigins: []string{"https://app.example"}, maxBytes: defaultGraphQLMaxQueryBytes})
	handler := chain.ServeHTTP
	member := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5})
	nonmember := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: "99999999-9999-9999-9999-999999999999", role: "admin", tokenVersion: 5})
	envelope := iaEnvelope(t, priv, principal.Claims{OrgID: ecOrg.String(), Role: "admin"})
	return handler, ran, member, nonmember, envelope
}

func edgeRequest(t *testing.T, c edgeCell, member, nonmember, envelope string) *http.Request {
	t.Helper()
	text := map[string]string{"query": iaDocument, "mutation": edgeWriteDocument, "unregistered": "query Other { probe }", "empty": ""}[c.document]
	var request *http.Request
	if c.method == http.MethodGet {
		target := "/graphql?org_id=" + ecOrg.String()
		if text != "" {
			target += "&query=" + url.QueryEscape(text)
		}
		request = httptest.NewRequest(c.method, target, nil)
	} else {
		request = httptest.NewRequest(c.method, "/graphql", strings.NewReader(`{"query": `+jsonQuote(t, text)+`}`))
		request.Header.Set("Content-Type", "application/json")
	}
	if c.accept != "" {
		request.Header.Set("Accept", c.accept)
	}
	switch c.carrier {
	case "member":
		request.Header.Set("Authorization", "Bearer "+member)
	case "nonmember":
		request.Header.Set("Authorization", "Bearer "+nonmember)
	case "envelope":
		request.Header.Set("Authorization", "Bearer "+envelope)
	case "basic":
		request.Header.Set("Authorization", "Basic "+member)
	case "two-bearers":
		request.Header.Add("Authorization", "Bearer "+member)
		request.Header.Add("Authorization", "Bearer "+member)
	case "internal-headers", "internal-headers+member":
		// Valid headers, on the internal listener: the one place they can
		// arrive at all (the public listener deletes them). With a valid
		// member token beside them they are still refused: the headers are
		// no carrier of the product path.
		if c.carrier == "internal-headers+member" {
			request.Header.Set("Authorization", "Bearer "+member)
		}
		request.Header.Set(internalidentity.HeaderOrgID, ecOrg.String())
		request.Header.Set(internalidentity.HeaderRole, "admin")
		request.Header.Set(internalidentity.HeaderSuperuser, "false")
		request.Header.Set(internalidentity.HeaderImpersonationActive, "false")
	}
	return request
}

// serveEdge answers request through handler on the listener the carrier
// implies: the internal headers arrive only on the internal listener.
func serveEdge(handler http.HandlerFunc, c edgeCell, request *http.Request) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	if strings.HasPrefix(c.carrier, "internal-headers") {
		internalidentity.Internal(handler).ServeHTTP(recorder, request)
		return recorder
	}
	internalidentity.Public(handler).ServeHTTP(recorder, request)
	return recorder
}

func jsonQuote(t *testing.T, text string) string {
	t.Helper()
	if text == "" {
		return `""`
	}
	encoded, err := json.Marshal(text)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func TestGraphQLEdgeDecisionIsTheRuleOverEveryCombination(t *testing.T) {
	handler, ran, member, nonmember, envelope := edgeHarness(t)
	cells := 0
	for _, method := range edgeMethods {
		for _, accept := range edgeAccepts {
			for _, carrier := range edgeCarriers {
				for _, document := range edgeDocuments {
					c := edgeCell{method: method, accept: accept, carrier: carrier, document: document}
					cells++
					*ran = (*ran)[:0]
					recorder := serveEdge(handler, c, edgeRequest(t, c, member, nonmember, envelope))
					wantStatus, wantRuns := edgeExpect(c)
					if recorder.Code != wantStatus || (len(*ran) == 1) != wantRuns || len(*ran) > 1 {
						t.Errorf("%+v: status %d ran %v, want %d runs=%v (body %q)", c, recorder.Code, *ran, wantStatus, wantRuns, recorder.Body.String())
					}
					if len(*ran) == 1 && !strings.HasSuffix((*ran)[0], " POST application/json") {
						t.Errorf("%+v: the executor was handed %q, want a POST of application/json", c, (*ran)[0])
					}
				}
			}
		}
	}
	if want := len(edgeMethods) * len(edgeAccepts) * len(edgeCarriers) * len(edgeDocuments); cells != want {
		t.Fatalf("enumerated %d cells, want %d", cells, want)
	}
	t.Logf("%d cells", cells)
}

// TestGraphQLEdgeRefusalsCarryThePythonBodies pins each refusal's status,
// content type and body to what the Python edge sent for it.
func TestGraphQLEdgeRefusalsCarryThePythonBodies(t *testing.T) {
	handler, _, member, _, _ := edgeHarness(t)
	for _, tc := range []struct {
		cell        edgeCell
		contentType string
		body        string
		allow       string
	}{
		{edgeCell{method: http.MethodPut, carrier: "member", document: "query"}, "application/json", `{"detail":"Method Not Allowed"}`, "GET"},
		{edgeCell{method: http.MethodGet, accept: "text/html", carrier: "none", document: "query"}, "application/json", `{"detail": "Not Found"}`, ""},
		{edgeCell{method: http.MethodPost, carrier: "none", document: "query"}, "application/json", `{"detail":"Authentication required"}`, ""},
		{edgeCell{method: http.MethodPost, carrier: "member", document: "empty"}, "text/plain; charset=utf-8", "No GraphQL query found in the request", ""},
		{edgeCell{method: http.MethodGet, carrier: "member", document: "mutation"}, "text/plain; charset=utf-8", "mutations are not allowed when using GET", ""},
		{edgeCell{method: http.MethodGet, carrier: "member", document: "empty"}, "text/plain; charset=utf-8", "No GraphQL query found in the request", ""},
		{edgeCell{method: http.MethodPost, carrier: "member", document: "unregistered"}, "application/json", `{"errors":[{"message":"This GraphQL document is not registered.","extensions":{"code":"UNREGISTERED_DOCUMENT"}}],"data":null}`, ""},
	} {
		recorder := serveEdge(handler, tc.cell, edgeRequest(t, tc.cell, member, "", ""))
		if got := recorder.Header().Get("Content-Type"); got != tc.contentType {
			t.Errorf("%+v: content-type %q, want %q", tc.cell, got, tc.contentType)
		}
		if got := recorder.Body.String(); got != tc.body {
			t.Errorf("%+v: body %q, want %q", tc.cell, got, tc.body)
		}
		if got := recorder.Header().Get("Allow"); got != tc.allow {
			t.Errorf("%+v: Allow %q, want %q", tc.cell, got, tc.allow)
		}
	}
}

// TestGraphQLEdgeOversizeIsThePythonMiddlewares413 pins the size refusal: it
// comes before the credential is read, with the limit named, in json.dumps's
// spaced form.
func TestGraphQLEdgeOversizeIsThePythonMiddlewares413(t *testing.T) {
	handler, ran, _, _, _ := edgeHarness(t)
	body := `{"query": "` + strings.Repeat(" ", defaultGraphQLMaxQueryBytes) + `"}`
	request := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	want := fmt.Sprintf(`{"detail": {"message": "GraphQL request body exceeds size limit", "limit_bytes": %d}}`, defaultGraphQLMaxQueryBytes)
	if recorder.Code != http.StatusRequestEntityTooLarge || recorder.Body.String() != want || len(*ran) != 0 {
		t.Fatalf("got %d %q ran=%v, want 413 %q", recorder.Code, recorder.Body.String(), *ran, want)
	}
}

// TestGraphQLEdgeGETDocumentIsWhatThePythonEdgeDid pins the GET decision:
// what the Python edge forwarded (the last of a repeated parameter, "+" as
// a space, a malformed escape kept as written, the variables re-encoded by
// Python's json rules, every other parameter dropped) and, for the rest,
// Strawberry's refusal in its order.
func TestGraphQLEdgeGETDocumentIsWhatThePythonEdgeDid(t *testing.T) {
	for _, tc := range []struct {
		raw, accept string
		want        string // the forwarded body, or "" for a refusal
		status      int    // the refusal's status
		text        string // the refusal's body
	}{
		{raw: "query=q", want: `{"query": "q"}`},
		{raw: "org_id=o&query=a+b&operationName=N&extensions=%7Bbad", want: `{"query": "a b", "operationName": "N"}`},
		{raw: "query=first&query=last", want: `{"query": "last"}`},
		{raw: "query=%ZZ%41", want: `{"query": "%ZZA"}`},
		{raw: "query=q&variables=%7B%22b%22%3A1%2C%22a%22%3A1.0%7D", want: `{"query": "q", "variables": {"b": 1, "a": 1.0}}`},
		{raw: "query=q&variables=5", want: `{"query": "q", "variables": 5}`},
		{raw: "query=q&variables=null", want: `{"query": "q"}`},
		{raw: "query=q&variables=", want: `{"query": "q"}`},
		{raw: "query=q&operationName=", want: `{"query": "q"}`},
		{raw: "query=%C3%A9", want: "{\"query\": \"\\u00e9\"}"},
		{raw: "query=q&variables=%7Bnot", status: 400, text: "Unable to parse request body as JSON"},
		{raw: "query=q&variables=" + strings.Repeat("9", 4301), status: 500, text: `{"detail":"Internal Server Error"}`},
		{raw: "query=", accept: "*/*", status: 400, text: "No GraphQL query found in the request"},
		{raw: "org_id=o", accept: "*/*", status: 404, text: "Not Found"},
		{raw: "org_id=o", accept: "application/json", status: 400, text: "No GraphQL query found in the request"},
		{raw: "variables=5", status: 400, text: "The GraphQL operation's `variables` must be an object or null, if provided."},
		{raw: "extensions=%5B%5D", status: 400, text: "The GraphQL operation's `extensions` must be an object or null, if provided."},
		{raw: "extensions=%7Bbad", status: 400, text: "Unable to parse request body as JSON"},
		{raw: "", status: 400, text: "No GraphQL query found in the request"},
	} {
		request := httptest.NewRequest(http.MethodGet, "/graphql?"+tc.raw, nil)
		if tc.accept != "" {
			request.Header.Set("Accept", tc.accept)
		}
		_, body, refusal := graphQLEdgeGETDocument(request)
		if tc.want != "" {
			if refusal != nil || string(body) != tc.want {
				t.Errorf("%q: body %q refused=%v, want %q", tc.raw, body, refusal != nil, tc.want)
			}
			continue
		}
		if refusal == nil {
			t.Errorf("%q: forwarded %q, want refusal %d", tc.raw, body, tc.status)
			continue
		}
		recorder := httptest.NewRecorder()
		refusal(recorder)
		if recorder.Code != tc.status || recorder.Body.String() != tc.text {
			t.Errorf("%q: refusal %d %q, want %d %q", tc.raw, recorder.Code, recorder.Body.String(), tc.status, tc.text)
		}
	}
}

// TestGraphQLEdgePOSTDocumentIsWhatThePythonEdgeDid pins the POST decision:
// an object with a non-empty string query is forwarded whatever else it
// holds; everything else is Strawberry's refusal, in its order (content
// type, JSON, batch, shape, missing query).
func TestGraphQLEdgePOSTDocumentIsWhatThePythonEdgeDid(t *testing.T) {
	for _, tc := range []struct {
		body, contentType string
		forwarded         string
		status            int
		text              string
	}{
		{body: `{"query": "q", "variables": 5, "extensions": []}`, contentType: "text/plain", forwarded: "q"},
		{body: `{"query": "a", "query": "b"}`, contentType: "application/json", forwarded: "b"},
		{body: `{"query": "q", "x": NaN}`, contentType: "application/json", forwarded: "q"},
		{body: `{}`, contentType: "text/plain", status: 400, text: "Unsupported content type"},
		{body: `{}`, contentType: "", status: 400, text: "Unsupported content type"},
		{body: `{}`, contentType: "Application/JSON", status: 400, text: "Unsupported content type"},
		{body: `{}`, contentType: "application/json; charset=utf-8", status: 400, text: "No GraphQL query found in the request"},
		{body: ``, contentType: "application/json", status: 400, text: "Unable to parse request body as JSON"},
		{body: `query { x }`, contentType: "application/json", status: 400, text: "Unable to parse request body as JSON"},
		{body: "{\"query\": \"\xff\"}", contentType: "application/json", status: 500, text: `{"detail":"Internal Server Error"}`},
		{body: `{"query": "q", "v": ` + strings.Repeat("9", 4301) + `}`, contentType: "application/json", status: 500, text: `{"detail":"Internal Server Error"}`},
		{body: `[]`, contentType: "application/json", status: 400, text: "Batching is not enabled"},
		{body: `5`, contentType: "application/json", status: 500, text: `{"detail":"Internal Server Error"}`},
		{body: `null`, contentType: "application/json", status: 500, text: `{"detail":"Internal Server Error"}`},
		{body: `{"query": 5}`, contentType: "application/json", status: 400, text: "The GraphQL operation's `query` must be a string or null, if provided."},
		{body: `{"query": "", "variables": []}`, contentType: "application/json", status: 400, text: "The GraphQL operation's `variables` must be an object or null, if provided."},
		{body: `{"query": null, "extensions": 1}`, contentType: "application/json", status: 400, text: "The GraphQL operation's `extensions` must be an object or null, if provided."},
		{body: `{"query": null}`, contentType: "application/json", status: 400, text: "No GraphQL query found in the request"},
	} {
		request := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(tc.body))
		if tc.contentType != "" {
			request.Header.Set("Content-Type", tc.contentType)
		}
		query, refusal := graphQLEdgePOSTDocument(request, []byte(tc.body))
		if tc.forwarded != "" {
			if refusal != nil || query != tc.forwarded {
				t.Errorf("%q: query %q refused=%v, want %q", tc.body, query, refusal != nil, tc.forwarded)
			}
			continue
		}
		if refusal == nil {
			t.Errorf("%q: forwarded %q, want refusal %d", tc.body, query, tc.status)
			continue
		}
		recorder := httptest.NewRecorder()
		refusal(recorder)
		if recorder.Code != tc.status || recorder.Body.String() != tc.text {
			t.Errorf("%q: refusal %d %q, want %d %q", tc.body, recorder.Code, recorder.Body.String(), tc.status, tc.text)
		}
	}
}

// TestGraphQLEdgeChainKeepsThePythonMiddlewareOrder pins which answers carry
// the security headers: a refusal from a middleware the Python api ran
// outside SecurityHeadersMiddleware (the org scope's 403, the size limit's
// 413 and browser 404, an unhandled error's 500) carries none; everything
// inside does, with CORS's Vary. Every answer carries X-Request-ID except
// the unhandled 500, and the provenance stamp is on all of them.
func TestGraphQLEdgeChainKeepsThePythonMiddlewareOrder(t *testing.T) {
	handler, _, member, _, _ := edgeHarness(t)
	oversize := `{"query": "` + strings.Repeat(" ", defaultGraphQLMaxQueryBytes) + `"}`
	for _, tc := range []struct {
		name                string
		request             func() *http.Request
		status              int
		security, requestID bool
	}{
		{"served", func() *http.Request {
			return edgeRequest(t, edgeCell{method: http.MethodPost, carrier: "member", document: "query"}, member, "", "")
		}, 200, true, true},
		{"401", func() *http.Request {
			return edgeRequest(t, edgeCell{method: http.MethodPost, carrier: "none", document: "query"}, member, "", "")
		}, 401, true, true},
		{"405", func() *http.Request {
			return edgeRequest(t, edgeCell{method: http.MethodPut, carrier: "member", document: "query"}, member, "", "")
		}, 405, true, true},
		{"403 foreign X-Org-Id", func() *http.Request {
			r := edgeRequest(t, edgeCell{method: http.MethodPost, carrier: "member", document: "query"}, member, "", "")
			r.Header.Set("X-Org-Id", "99999999-9999-9999-9999-999999999999")
			return r
		}, 403, false, true},
		{"413", func() *http.Request {
			r := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(oversize))
			r.Header.Set("Authorization", "Bearer "+member)
			return r
		}, 413, false, true},
		{"413 PUT", func() *http.Request {
			return httptest.NewRequest(http.MethodPut, "/graphql", strings.NewReader(oversize))
		}, 413, false, true},
		{"404 browser", func() *http.Request {
			return edgeRequest(t, edgeCell{method: http.MethodGet, accept: "text/html", carrier: "none", document: "query"}, member, "", "")
		}, 404, false, true},
		{"500 unhandled", func() *http.Request {
			r := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`5`))
			r.Header.Set("Authorization", "Bearer "+member)
			r.Header.Set("Content-Type", "application/json")
			return r
		}, 500, false, false},
	} {
		recorder := httptest.NewRecorder()
		handler(recorder, tc.request())
		header := recorder.Header()
		if recorder.Code != tc.status {
			t.Errorf("%s: status %d, want %d (body %q)", tc.name, recorder.Code, tc.status, recorder.Body.String())
		}
		if got := header.Get("X-Frame-Options") != ""; got != tc.security {
			t.Errorf("%s: security headers present=%v, want %v", tc.name, got, tc.security)
		}
		if got := header.Get("Vary") == "Origin"; got != tc.security {
			t.Errorf("%s: Vary %q, want Origin=%v", tc.name, header.Get("Vary"), tc.security)
		}
		if got := header.Get("X-Request-ID") != ""; got != tc.requestID {
			t.Errorf("%s: X-Request-ID present=%v, want %v", tc.name, got, tc.requestID)
		}
		if header.Get("x-dev-health-plane") == "" {
			t.Errorf("%s: no provenance stamp", tc.name)
		}
		if header.Get("X-Dho-Unhandled-Error") != "" {
			t.Errorf("%s: the unhandled-error marker leaked to the client", tc.name)
		}
	}
}

// TestGraphQLEdgeCorrelationIDEchoesTheFirstOrMintsOne is
// CorrelationIdMiddleware: the first X-Request-ID as sent, else a new one.
func TestGraphQLEdgeCorrelationIDEchoesTheFirstOrMintsOne(t *testing.T) {
	handler := correlationID(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	request := httptest.NewRequest(http.MethodGet, "/graphql", nil)
	request.Header.Add("X-Request-ID", "first")
	request.Header.Add("X-Request-ID", "second")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if got := recorder.Header().Values("X-Request-ID"); len(got) != 1 || got[0] != "first" {
		t.Fatalf("X-Request-ID %v, want [first]", got)
	}
	for _, sent := range []string{"", "absent"} {
		request := httptest.NewRequest(http.MethodGet, "/graphql", nil)
		if sent == "" {
			request.Header.Set("X-Request-ID", "")
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if _, err := uuid.Parse(recorder.Header().Get("X-Request-ID")); err != nil {
			t.Fatalf("%q: X-Request-ID %q is not a minted UUID", sent, recorder.Header().Get("X-Request-ID"))
		}
	}
}

// TestGraphQLEdgeCORSOriginsAreThePythonParse pins _parse_cors_origins: the
// default only when the variable is absent, entries trimmed, empties dropped.
func TestGraphQLEdgeCORSOriginsAreThePythonParse(t *testing.T) {
	for _, tc := range []struct {
		value   string
		present bool
		want    string
	}{
		{"", false, "http://localhost:3000"},
		{"", true, ""},
		{" , ", true, ""},
		{" https://a.example , ,https://b.example", true, "https://a.example|https://b.example"},
	} {
		got := corsAllowedOrigins(func(name string) (string, bool) {
			if name != "CORS_ALLOWED_ORIGINS" {
				t.Fatalf("read %q", name)
			}
			return tc.value, tc.present
		})
		if strings.Join(got, "|") != tc.want {
			t.Errorf("%q present=%v: %v, want %q", tc.value, tc.present, got, tc.want)
		}
	}
}

// TestGraphQLEdgeIsNotMountedWithoutAnEdgeSecret: the product path's only
// credential is the edge token, so without the secret it is not served.
func TestGraphQLEdgeIsNotMountedWithoutAnEdgeSecret(t *testing.T) {
	mux := http.NewServeMux()
	mountGraphQLRoute(mux, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }, graphQLEdgeDeps{})
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/graphql", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("POST /graphql with no edge secret: %d, want 404 (unmounted)", recorder.Code)
	}
}

// TestGraphQLEdgeRefusesWhenTheEdgeCarrierIsNotConfigured: the pipeline's own
// guard for a handler built without the authenticator.
func TestGraphQLEdgeRefusesWhenTheEdgeCarrierIsNotConfigured(t *testing.T) {
	handler, seen := iaDispatchWithEdge(t, nil, nil, nil)
	member := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5})
	request := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query": `+jsonQuote(t, iaDocument)+`}`))
	request.Header.Set("Authorization", "Bearer "+member)
	recorder := httptest.NewRecorder()
	newGraphQLEdgeHandler(handler)(recorder, request)
	if recorder.Code != http.StatusUnauthorized || len(*seen) != 0 {
		t.Fatalf("got %d ran=%d, want 401 and nothing run", recorder.Code, len(*seen))
	}
}

// TestQueryStaysWhatItWas: /query, which the internal callers and the proof
// routes use, gains none of /graphql's behaviour -- no GET, the bare 401.
func TestQueryStaysWhatItWas(t *testing.T) {
	handler, seen := iaDispatch(t, nil)
	get := httptest.NewRecorder()
	handler(get, httptest.NewRequest(http.MethodGet, "/query?query="+url.QueryEscape(iaDocument), nil))
	if get.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /query: %d, want 405", get.Code)
	}
	post := httptest.NewRecorder()
	handler(post, httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{"query": `+jsonQuote(t, iaDocument)+`}`)))
	if post.Code != http.StatusUnauthorized || strings.Contains(post.Body.String(), "Authentication required") || len(*seen) != 0 {
		t.Errorf("POST /query without a carrier: %d %q, want the bare 401", post.Code, post.Body.String())
	}
}

// TestGraphQLEdgeSendsTheWholeBodyWithItsLength: a body past net/http's
// buffer still carries Content-Length, as every Python edge answer did.
func TestGraphQLEdgeSendsTheWholeBodyWithItsLength(t *testing.T) {
	large := strings.Repeat("x", 64*1024)
	handler := newGraphQLEdgeHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		for i := 0; i < 4; i++ {
			_, _ = w.Write([]byte(large))
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	response, err := http.Post(server.URL, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.ContentLength != int64(4*len(large)) || len(response.TransferEncoding) != 0 {
		t.Fatalf("Content-Length %d transfer-encoding %v, want %d and none", response.ContentLength, response.TransferEncoding, 4*len(large))
	}
}

// TestGraphQLEdgeBrowserCheckReadsTheLastAcceptHeader: the size-limit
// middleware reads the header list as a dict, so the LAST Accept decides.
func TestGraphQLEdgeBrowserCheckReadsTheLastAcceptHeader(t *testing.T) {
	for _, tc := range []struct {
		accepts []string
		want    bool
	}{
		{[]string{"text/html"}, true},
		{[]string{"application/json", "text/html"}, true},
		{[]string{"text/html", "application/json"}, false},
		{[]string{"TEXT/HTML"}, false},
		{nil, false},
	} {
		request := httptest.NewRequest(http.MethodGet, "/graphql", nil)
		for _, accept := range tc.accepts {
			request.Header.Add("Accept", accept)
		}
		if got := acceptsHTML(request); got != tc.want {
			t.Errorf("%v: %v, want %v", tc.accepts, got, tc.want)
		}
	}
	// Strawberry's own browser check (no query parameter at all) reads the
	// FIRST Accept, and "text/html" counts as well as "*/*".
	request := httptest.NewRequest(http.MethodGet, "/graphql?org_id=o", nil)
	request.Header.Add("Accept", "text/html")
	request.Header.Add("Accept", "application/json")
	if _, _, refusal := graphQLEdgeGETDocument(request); refusal == nil {
		t.Fatal("GET with no query and a first Accept of text/html was forwarded")
	} else {
		recorder := httptest.NewRecorder()
		refusal(recorder)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("got %d, want Strawberry's 404", recorder.Code)
		}
	}
}

// TestGraphQLEdgePatchOversizeIs413: PATCH is sized like POST and PUT.
func TestGraphQLEdgePatchOversizeIs413(t *testing.T) {
	handler, _, _, _, _ := edgeHarness(t)
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodPatch, "/graphql", strings.NewReader(strings.Repeat("x", defaultGraphQLMaxQueryBytes+1))))
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("got %d, want 413", recorder.Code)
	}
	small := httptest.NewRecorder()
	handler(small, httptest.NewRequest(http.MethodPatch, "/graphql", strings.NewReader("x")))
	if small.Code != http.StatusMethodNotAllowed {
		t.Fatalf("a small PATCH: %d, want 405", small.Code)
	}
}

// TestBuildReadsAnEmptySettingAsAbsent: Build's plain reader cannot tell an
// empty setting from an absent one, so CORS_ALLOWED_ORIGINS falls back to the
// default there; BuildWithLookup is the reader that can.
func TestBuildReadsAnEmptySettingAsAbsent(t *testing.T) {
	lookup := presentWhenSet(func(name string) string { return map[string]string{"SET": "v"}[name] })
	if value, present := lookup("SET"); value != "v" || !present {
		t.Fatalf("SET: %q %v", value, present)
	}
	if value, present := lookup("EMPTY"); value != "" || present {
		t.Fatalf("EMPTY: %q %v, want absent", value, present)
	}
}

// TestGraphQLEdgeLogsWhyItRefused pins the reason an operator reads for each
// refused carrier: every refusal answers the same 401 body, so the log line
// (and the carrier outcome counter beside it) is the only place the cause is
// told apart.
func TestGraphQLEdgeLogsWhyItRefused(t *testing.T) {
	handler, _, member, nonmember, envelope := edgeHarness(t)
	var logged strings.Builder
	previous := log.Writer()
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(previous) })
	for carrier, reason := range map[string]string{
		"none":                    "reason=no_carrier carrier=none",
		"basic":                   "reason=no_carrier carrier=none",
		"envelope":                "reason=not_an_edge_token carrier=edge",
		"two-bearers":             "reason=ambiguous_carrier carrier=authorization",
		"internal-headers+member": "reason=internal_headers_on_edge_path carrier=headers",
		"nonmember":               "reason=edge_not_a_member carrier=edge",
	} {
		logged.Reset()
		c := edgeCell{method: http.MethodPost, carrier: carrier, document: "query"}
		recorder := serveEdge(handler, c, edgeRequest(t, c, member, nonmember, envelope))
		if recorder.Code != http.StatusUnauthorized || !strings.Contains(logged.String(), reason) {
			t.Errorf("%s: %d, logged %q, want 401 and %q", carrier, recorder.Code, logged.String(), reason)
		}
	}
}

// TestGraphQLEdgeAnswersAnOperationThatIsNotEnabledAsAGraphQLError: a
// registered operation whose routing row is off (or unreadable: the switch
// fails closed) is a GraphQL error with status 200 on /graphql, and nothing
// runs; /query keeps its 404.
func TestGraphQLEdgeAnswersAnOperationThatIsNotEnabledAsAGraphQLError(t *testing.T) {
	store := &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
	}
	handler, ran, member, _, _ := edgeHarnessWith(t, store, routeswitch.StaticSwitch{"probe": false, "probeWrite": false})
	for _, document := range []string{"query", "mutation"} {
		c := edgeCell{method: http.MethodPost, carrier: "member", document: document}
		recorder := serveEdge(handler, c, edgeRequest(t, c, member, "", ""))
		want := `{"errors":[{"message":"This operation is not enabled on this deployment.","extensions":{"code":"OPERATION_NOT_ENABLED"}}],"data":null}`
		if recorder.Code != http.StatusOK || recorder.Body.String() != want || len(*ran) != 0 {
			t.Errorf("%s: %d %q ran=%v, want 200 %q and nothing run", document, recorder.Code, recorder.Body.String(), *ran, want)
		}
	}
	queryMux := routeswitch.NewMux(routeswitch.StaticSwitch{"probe": false})
	queryMux.Register("probe", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	queryHandler := newDocumentDispatchHandler(os.Getenv, queryMux, map[string]string{digestHex(iaDocument): "probe"}, nil, ecEdgeAuth(t, store), store, "", nil)
	request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{"query": `+jsonQuote(t, iaDocument)+`}`))
	request.Header.Set("Authorization", "Bearer "+member)
	recorder := httptest.NewRecorder()
	queryHandler(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Errorf("/query with the row off: %d, want the unchanged 404", recorder.Code)
	}
}

// TestGraphQLEdgeAnswersAStoreItCannotReadAsTheUnhandled500: a live check
// that could not be read decided nothing, so /graphql answers the Python
// app's unhandled 500 (content headers only), never the 401 that would tell
// the client its credential is bad; /query keeps its bare 401.
func TestGraphQLEdgeAnswersAStoreItCannotReadAsTheUnhandled500(t *testing.T) {
	for name, store := range map[string]*fakeEdgeStore{
		"membership lookup fails": {
			states: map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
			found:  map[uuid.UUID]bool{ecUser: true}, errIsMember: errors.New("membership read failed"),
		},
	} {
		handler, ran, member, _, _ := edgeHarnessWith(t, store, routeswitch.StaticSwitch{"probe": true})
		c := edgeCell{method: http.MethodPost, carrier: "member", document: "query"}
		recorder := serveEdge(handler, c, edgeRequest(t, c, member, "", ""))
		if recorder.Code != http.StatusInternalServerError || recorder.Body.String() != `{"detail":"Internal Server Error"}` ||
			recorder.Header().Get("X-Frame-Options") != "" || len(*ran) != 0 {
			t.Errorf("%s: %d %q headers=%v ran=%v, want the bare unhandled 500", name, recorder.Code, recorder.Body.String(), recorder.Header(), *ran)
		}
		query, seen := iaDispatchWithEdge(t, nil, ecEdgeAuth(t, store), store)
		request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{"query": `+jsonQuote(t, iaDocument)+`}`))
		request.Header.Set("Authorization", "Bearer "+member)
		queryRecorder := httptest.NewRecorder()
		query(queryRecorder, request)
		if queryRecorder.Code != http.StatusUnauthorized || len(*seen) != 0 {
			t.Errorf("%s on /query: %d, want the unchanged 401", name, queryRecorder.Code)
		}
	}
	// A user-state read that fails: the org-scope middleware reads the caller
	// first and refuses with the same 500, before the pipeline is reached.
	store := &fakeEdgeStore{errUserState: policy.ErrUnavailable}
	handler, ran, member, _, _ := edgeHarnessWith(t, store, routeswitch.StaticSwitch{"probe": true})
	c := edgeCell{method: http.MethodPost, carrier: "member", document: "query"}
	recorder := serveEdge(handler, c, edgeRequest(t, c, member, "", ""))
	if recorder.Code != http.StatusInternalServerError || len(*ran) != 0 {
		t.Errorf("user state unavailable: %d ran=%v, want 500", recorder.Code, *ran)
	}
}

// TestGraphQLEdgeAuthenticatorReportsAStoreItCannotReadAsUnavailable pins the
// pipeline's own decision, beneath the org-scope middleware: every live read
// the edge carrier makes that fails is "unavailable", never "refused".
func TestGraphQLEdgeAuthenticatorReportsAStoreItCannotReadAsUnavailable(t *testing.T) {
	member := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5})
	super := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "owner", isSuperuser: true, tokenVersion: 5})
	for name, tc := range map[string]struct {
		store *fakeEdgeStore
		token string
	}{
		"user state":    {&fakeEdgeStore{errUserState: policy.ErrUnavailable}, member},
		"membership":    {&fakeEdgeStore{states: map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}}, found: map[uuid.UUID]bool{ecUser: true}, errIsMember: errors.New("down")}, member},
		"impersonation": {&fakeEdgeStore{states: map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, IsSuperuser: true, TokenVersion: 5}}, found: map[uuid.UUID]bool{ecUser: true}, errSession: errors.New("down")}, super},
	} {
		request := httptest.NewRequest(http.MethodPost, "/graphql", nil)
		request.Header.Set("Authorization", "Bearer "+tc.token)
		if _, outcome := authenticateEdgeTokenOnly(request, ecEdgeAuth(t, tc.store), tc.store); outcome != edgeUnavailable {
			t.Errorf("%s: outcome %d, want unavailable", name, outcome)
		}
		recorder := httptest.NewRecorder()
		if _, ok := authenticateGraphQLEdge(recorder, request, ecEdgeAuth(t, tc.store), tc.store); ok || recorder.Code != http.StatusInternalServerError {
			t.Errorf("%s: answered %d, want 500", name, recorder.Code)
		}
	}
}
