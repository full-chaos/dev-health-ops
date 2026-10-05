package server

// CHAOS-7831: a root's class row ("mcp:<root>") is the single serving decision for that root. The named-operation route acr's run_operation calls
// (the internal listener, /query) refuses an operation whose root is dark, with the MCP listener's own refusal. The expected sets below are DERIVED
// from mcpclass.AllowedRoots() x the registered documents' roots (goapiproof.SpecFor(op).ResponseRoot), never listed by hand; an empty set fails.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/vektah/gqlparser/v2"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// classOperationDocuments is the registered document of each operation whose response root is a class root. TestClassGateTableCoversEveryClassOperation
// fails when the derived set holds an operation this table lacks, so a new class operation cannot be added without its row here.
var classOperationDocuments = map[string]string{
	"acrRepositoryScopes":            registeredAcrRepositoryScopesDocument,
	"capacityCompletionDistribution": registeredCapacityCompletionDistributionDocument,
	"capacityForecast":               registeredCapacityForecastDocument,
	"capacityForecasts":              registeredCapacityForecastsDocument,
	"catalogValues":                  registeredCatalogValuesDocument,
	"cognitiveLoad":                  registeredCognitiveLoadDocument,
	"complexityTimeseries":           registeredComplexityTimeseriesDocument,
	"compoundingRisk":                registeredCompoundingRiskDocument,
	"featureFlagTimeseries":          registeredFeatureFlagTimeseriesDocument,
	"flowMatrix":                     registeredFlowMatrixDocument,
	"hotspots":                       registeredHotspotsDocument,
	"investmentBreakdown":            registeredInvestmentBreakdownDocument,
	"investmentFull":                 registeredInvestmentFullDocument,
	"releaseImpact":                  registeredReleaseImpactDocument,
	"securityAlerts":                 registeredSecurityAlertsDocument,
	"securityOverview":               registeredSecurityOverviewDocument,
	"testOpsCoverage":                registeredTestOpsCoverageDocument,
	"testOpsPipeline":                registeredTestOpsPipelineDocument,
	"testOpsTest":                    registeredTestOpsTestDocument,
	"throughputForecast":             registeredThroughputForecastDocument,
	"workGraphArtifacts":             registeredWorkGraphArtifactsDocument,
	"workGraphEdges":                 registeredWorkGraphEdgesDocument,
	"workGraphFlow":                  registeredWorkGraphFlowDocument,
}

// mixedRootsDocument has TWO class roots: hotspots and catalog.
const mixedRootsDocument = `query Mixed($input: HotspotsInput!, $orgId: String!, $dimension: DimensionInput!) {
  hotspots(input: $input) { rows { filePath } }
  catalog(orgId: $orgId, dimension: $dimension) { values { value } }
}`

// derivedClassOperations is every operation whose response root is an allowlisted class root, with that root.
func derivedClassOperations(t *testing.T) map[string]string {
	t.Helper()
	allowed := mcpclass.AllowedRoots()
	out := map[string]string{}
	for _, operation := range goapiproof.KnownOperations() {
		spec, err := goapiproof.SpecFor(operation)
		if err == nil && allowed[spec.ResponseRoot] {
			out[operation] = spec.ResponseRoot
		}
	}
	if len(out) == 0 {
		t.Fatal("the derived set of class operations is empty: the test would measure nothing")
	}
	return out
}

func TestClassGateTableCoversEveryClassOperation(t *testing.T) {
	derived := derivedClassOperations(t)
	for operation, root := range derived {
		document, ok := classOperationDocuments[operation]
		if !ok {
			t.Errorf("class operation %s (root %s) has no document in classOperationDocuments: add it", operation, root)
			continue
		}
		schema := graph.NewExecutableSchema(graph.Config{}).Schema()
		doc, errs := gqlparser.LoadQuery(schema, document)
		if len(errs) > 0 || len(doc.Operations) != 1 {
			t.Fatalf("%s: the registered document does not load: %v", operation, errs)
		}
		roots, _ := mcpRootFields(doc.Operations[0].SelectionSet, doc.Fragments)
		if !contains(roots, root) {
			t.Errorf("%s: the document's roots %v do not include the operation's response root %s", operation, roots, root)
		}
	}
	for operation := range classOperationDocuments {
		if _, ok := derived[operation]; !ok {
			t.Errorf("classOperationDocuments holds %s, which is not a class operation of the derived set", operation)
		}
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// gateServer is the real document dispatch handler (class gate included) over every class operation, the mixed document and a non-class one.
type gateServer struct {
	handler      http.HandlerFunc         // /query: the web edge's route, NOT class-gated
	runOperation http.HandlerFunc         // /query/run-operation: the same pipeline behind markClassGated
	classes      routeswitch.StaticSwitch // the class rows: "mcp:<root>" -> lit
	served       map[string]int           // operation -> times its handler ran
}

func newGateServer(t *testing.T) *gateServer {
	t.Helper()
	server := &gateServer{classes: routeswitch.StaticSwitch{}, served: map[string]int{}}
	documents := map[string]string{"mixed": mixedRootsDocument, "home": registeredHomeDocument}
	for operation, document := range classOperationDocuments {
		documents[operation] = document
	}
	byDigest := map[string]string{}
	documentRows := routeswitch.StaticSwitch{}
	mux := routeswitch.NewMux(documentRows)
	for operation, document := range documents {
		operation := operation
		byDigest[digestHex(document)] = operation
		documentRows[operation] = true // the DOCUMENT rows are all lit: only the class rows decide in these tests
		mux.Register(operation, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			server.served[operation]++
			w.WriteHeader(http.StatusOK)
		}))
	}
	server.handler = newDocumentDispatchHandler(os.Getenv, mux, byDigest, nil, nil, nil, "", nil, newClassRowGate(server.classes))
	server.runOperation = markClassGated(server.handler)
	return server
}

func (server *gateServer) setClassRows(lit ...string) {
	for key := range server.classes {
		delete(server.classes, key)
	}
	for _, root := range lit {
		server.classes[mcpclass.Operation(root)] = true
	}
}

func allClassRoots() []string { return mcpclass.SortedRoots() }

func without(roots []string, drop ...string) []string {
	var out []string
	for _, root := range roots {
		if !contains(drop, root) {
			out = append(out, root)
		}
	}
	return out
}

// post sends the document as acr's run_operation does: the internal listener, the four identity headers.
func (server *gateServer) post(t *testing.T, document string, internal, identity bool) *httptest.ResponseRecorder {
	t.Helper()
	return server.postVia(t, server.runOperation, document, internal, identity)
}

// postVia sends the request through handler: server.runOperation (acr's route) or server.handler (/query, the web edge's).
func (server *gateServer) postVia(t *testing.T, handler http.HandlerFunc, document string, internal, identity bool) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]any{"query": document})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if internal {
		*request = *request.WithContext(iaInternalCtx(request.Context()))
	}
	if identity {
		request.Header.Set(internalidentity.HeaderOrgID, "org-1")
		request.Header.Set(internalidentity.HeaderRole, "member")
		request.Header.Set(internalidentity.HeaderSuperuser, "false")
		request.Header.Set(internalidentity.HeaderImpersonationActive, "false")
	}
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	return recorder
}

func assertClassRefusal(t *testing.T, label string, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("%s: status %d, want 404 (body %s)", label, recorder.Code, recorder.Body.String())
	}
	if reason, _ := mcpReason(t, recorder); reason != mcpReasonRootFieldNotEnabled {
		t.Fatalf("%s: reason %q, want %s (body %s)", label, reason, mcpReasonRootFieldNotEnabled, recorder.Body.String())
	}
}

// A dark class root is refused for EVERY operation of that root, a lit root is served, and a dark OTHER root does not refuse it.
func TestRunOperationPathRefusesEveryOperationOfADarkClassRoot(t *testing.T) {
	derived := derivedClassOperations(t)
	operations := make([]string, 0, len(derived))
	for operation := range derived {
		operations = append(operations, operation)
	}
	sort.Strings(operations)
	for _, operation := range operations {
		root := derived[operation]
		t.Run(operation, func(t *testing.T) {
			server := newGateServer(t)
			document := classOperationDocuments[operation]

			server.setClassRows(without(allClassRoots(), root)...) // this root dark, every other root lit
			assertClassRefusal(t, "dark root", server.post(t, document, true, true))
			if server.served[operation] != 0 {
				t.Fatalf("the dark root %s was served for %s", root, operation)
			}

			server.setClassRows(allClassRoots()...) // every root lit
			if recorder := server.post(t, document, true, true); recorder.Code != http.StatusOK || server.served[operation] != 1 {
				t.Fatalf("the lit root %s was not served for %s: status %d, served %d", root, operation, recorder.Code, server.served[operation])
			}

			server.setClassRows(root) // only this root lit: every OTHER root dark must not refuse it
			if recorder := server.post(t, document, true, true); recorder.Code != http.StatusOK {
				t.Fatalf("a dark OTHER root refused %s: status %d, body %s", operation, recorder.Code, recorder.Body.String())
			}
		})
	}
}

// A document whose roots include one dark and one lit class root is refused (any dark root refuses); both lit is served.
func TestRunOperationPathRefusesADocumentWithOneDarkRootAndOneLitRoot(t *testing.T) {
	for _, test := range []struct {
		name        string
		dark        []string
		wantRefused bool
	}{
		{"hotspots dark, catalog lit", []string{"hotspots"}, true},
		{"catalog dark, hotspots lit", []string{"catalog"}, true},
		{"both dark", []string{"hotspots", "catalog"}, true},
		{"both lit", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := newGateServer(t)
			server.setClassRows(without(allClassRoots(), test.dark...)...)
			recorder := server.post(t, mixedRootsDocument, true, true)
			if test.wantRefused {
				assertClassRefusal(t, test.name, recorder)
				if server.served["mixed"] != 0 {
					t.Fatal("the mixed document was served with a dark root")
				}
				return
			}
			if recorder.Code != http.StatusOK || server.served["mixed"] != 1 {
				t.Fatalf("both roots lit: status %d, served %d", recorder.Code, server.served["mixed"])
			}
		})
	}
}

// A non-class operation (its root is not allowlisted) is never gated, whatever the class rows say.
func TestRunOperationPathNeverGatesANonClassOperation(t *testing.T) {
	server := newGateServer(t)
	server.setClassRows() // every class root dark
	if recorder := server.post(t, registeredHomeDocument, true, true); recorder.Code != http.StatusOK || server.served["home"] != 1 {
		t.Fatalf("a non-class operation was refused: status %d (%s)", recorder.Code, recorder.Body.String())
	}
}

// Scope: the gate acts only on a request that is on the internal listener AND carries the identity headers (run_operation's carrier). A request
// with the headers off the internal listener, or on it without the headers, is not class-gated (stated in the PR's RISK-NOTES).
func TestClassGateActsOnlyOnInternalListenerRequestsWithIdentityHeaders(t *testing.T) {
	gate := newClassRowGate(routeswitch.StaticSwitch{}) // every class root dark
	document := classOperationDocuments["hotspots"]
	for _, test := range []struct {
		name                      string
		marked, internal, headers bool
		wantRefused               bool
	}{
		{"run-operation route + internal listener + identity headers", true, true, true, true},
		{"run-operation route, internal listener, no identity headers", true, true, false, false},
		{"run-operation route, not the internal listener, identity headers", true, false, true, false},
		{"/query (not the run-operation route), internal listener + identity headers", false, true, true, false},
		{"nothing", false, false, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/query", nil)
			if test.marked {
				*request = *request.WithContext(context.WithValue(request.Context(), classGateKey{}, true))
			}
			if test.internal {
				*request = *request.WithContext(iaInternalCtx(request.Context()))
			}
			if test.headers {
				request.Header.Set(internalidentity.HeaderOrgID, "org-1")
			}
			recorder := httptest.NewRecorder()
			if refused := gate(recorder, request, "hotspots", document); refused != test.wantRefused {
				t.Fatalf("refused = %t, want %t", refused, test.wantRefused)
			}
		})
	}
}

// A registered document the gate cannot read is refused (fail closed), never waved through.
func TestClassGateRefusesADocumentItCannotRead(t *testing.T) {
	gate := newClassRowGate(routeswitch.StaticSwitch{"mcp:hotspots": true})
	request := httptest.NewRequest(http.MethodPost, "/query", nil)
	*request = *request.WithContext(context.WithValue(iaInternalCtx(request.Context()), classGateKey{}, true))
	request.Header.Set(internalidentity.HeaderOrgID, "org-1")
	recorder := httptest.NewRecorder()
	if !gate(recorder, request, "unreadable", "this is not a graphql document") {
		t.Fatal("a document the gate cannot parse was not refused")
	}
	assertClassRefusal(t, "unreadable document", recorder)
}

// The class state is read live on every request: the same handler answers differently as the rows change, with no cache between requests.
func TestRunOperationPathReadsTheClassRowLivePerRequest(t *testing.T) {
	server := newGateServer(t)
	document := classOperationDocuments["hotspots"]
	for step, lit := range []bool{false, true, false, true} {
		if lit {
			server.setClassRows(allClassRoots()...)
		} else {
			server.setClassRows(without(allClassRoots(), "hotspots")...)
		}
		recorder := server.post(t, document, true, true)
		if lit != (recorder.Code == http.StatusOK) {
			t.Fatalf("step %d: class row lit=%t but status %d: the class state was cached", step, lit, recorder.Code)
		}
	}
}

// The refusal on this route is BYTE-IDENTICAL to the MCP listener's refusal for the same dark root (status, content type and body): both are
// the real writeMCPRefusal.
func TestRunOperationRefusalIsByteIdenticalToTheMCPListenerRefusal(t *testing.T) {
	server := newGateServer(t)
	server.setClassRows(without(allClassRoots(), "hotspots")...)
	onRunOperation := server.post(t, classOperationDocuments["hotspots"], true, true)

	listeners := newMCPTestListeners(t, &countingMCPClient{}, server.classes, mcpDefaultLimits())
	onMCP := mcpDo(listeners.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))

	assertClassRefusal(t, "run_operation path", onRunOperation)
	assertClassRefusal(t, "MCP listener", onMCP)
	if onRunOperation.Body.String() != onMCP.Body.String() {
		t.Fatalf("the refusal bodies differ:\n run_operation: %s\n MCP:          %s", onRunOperation.Body.String(), onMCP.Body.String())
	}
	if onRunOperation.Header().Get("Content-Type") != onMCP.Header().Get("Content-Type") {
		t.Fatalf("content types differ: %q vs %q", onRunOperation.Header().Get("Content-Type"), onMCP.Header().Get("Content-Type"))
	}
	if !strings.Contains(onRunOperation.Body.String(), mcpReasonRootFieldNotEnabled) {
		t.Fatal("the refusal does not carry its reason")
	}
}

// An operation may be registered under MORE THAN ONE document (CHAOS-8000: the old and the new document of an operation): the check holds for EVERY
// document, because each document's own roots decide. Two documents under one operation name, one root lit and one dark: only the dark one is refused,
// whichever is asked first.
func TestClassGateDecidesPerDocumentWhenAnOperationHasTwoDocuments(t *testing.T) {
	lit, dark := classOperationDocuments["hotspots"], classOperationDocuments["catalogValues"]
	for _, order := range [][2]string{{lit, dark}, {dark, lit}} {
		gate := newClassRowGate(routeswitch.StaticSwitch{"mcp:hotspots": true}) // hotspots lit, catalog dark
		for _, document := range order {
			request := httptest.NewRequest(http.MethodPost, "/query", nil)
			*request = *request.WithContext(context.WithValue(iaInternalCtx(request.Context()), classGateKey{}, true))
			request.Header.Set(internalidentity.HeaderOrgID, "org-1")
			recorder := httptest.NewRecorder()
			refused := gate(recorder, request, "oneOperationTwoDocuments", document)
			if want := document == dark; refused != want {
				t.Fatalf("document with roots lit=%t: refused = %t, want %t (the answer of the first document was reused for the second)", document == lit, refused, want)
			}
		}
	}
}

// Source guard: ONE constructor builds the class-row switch (newClassRowSwitch: canary and primary rows only), and buildQueryRoute and
// newQueryHandler both use it. NewProofSwitch admits SHADOW rows: it must stay on the measurement-only proof handler and nowhere near the switch the
// MCP listener and this route serve through (a shadow root would be served on both ports).
func TestTheClassRowSwitchIsBuiltByOneConstructorThatDoesNotAdmitShadowRows(t *testing.T) {
	raw, err := os.ReadFile("query_route.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if got := strings.Count(text, "classSwitch := newClassRowSwitch(pgPool)"); got != 2 {
		t.Fatalf("query_route.go builds the class-row switch through newClassRowSwitch %d times, want 2 (buildQueryRoute and newQueryHandler's default)", got)
	}
	if !strings.Contains(text, "newMCPHandler(mcpClient, pgPool, classSwitch, getenv)") {
		t.Fatal("the MCP handler is not built over the shared class-row switch")
	}
	if !strings.Contains(text, "handlers.RunOperation = markClassGated(handler)") {
		t.Fatal("buildQueryRoute does not build RunOperation as the class-gated alias of the serving handler")
	}
	if strings.Contains(text, "NewClassDecisionSwitch(pgPool, mcpRoutingDigests())") {
		t.Fatal("query_route.go builds a class-row switch itself instead of through newClassRowSwitch")
	}
	if got := strings.Count(text, "NewClassDecisionProofSwitch(pgPool, mcpRoutingDigests())"); got != 1 {
		t.Fatalf("NewClassDecisionProofSwitch appears %d times in query_route.go, want 1 (the MCP proof handler only)", got)
	}
	gate, err := os.ReadFile("class_row_gate.go")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(gate), "func newClassRowSwitch(")
	if start < 0 {
		t.Fatal("newClassRowSwitch is gone")
	}
	body := string(gate)[start:]
	body = body[:strings.Index(body, "\n}\n")]
	if !strings.Contains(body, "return routeswitch.NewClassDecisionSwitch(pool, mcpRoutingDigests())") || strings.Contains(body, "NewClassDecisionProofSwitch") {
		t.Fatal("newClassRowSwitch is not the canary/primary-only ClassDecisionSwitch")
	}
}

// The web edge's route is NOT gated: /query with the internal identity headers (what the Python /graphql edge sends) and a DARK class root is SERVED for
// every class operation. This is the path TestGraphQLEdgeVenueOracle exercises; gating it would 404 the web.
func TestQueryRouteWithIdentityHeadersIsNotClassGated(t *testing.T) {
	derived := derivedClassOperations(t)
	for operation := range derived {
		server := newGateServer(t)
		server.setClassRows() // every class root dark
		recorder := server.postVia(t, server.handler, classOperationDocuments[operation], true, true)
		if recorder.Code != http.StatusOK || server.served[operation] != 1 {
			t.Fatalf("%s: the web edge's /query was class-gated: status %d (%s)", operation, recorder.Code, recorder.Body.String())
		}
	}
}

// /query/run-operation is mounted on the INTERNAL mux only: the public mux (what the public listener serves) answers 404 for it, and /query stays on both.
func TestRunOperationRouteIsMountedOnTheInternalMuxOnly(t *testing.T) {
	getenv := getenvFunc(func(string) string { return "" })
	stub := func(code int) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
	}
	mux, internalMux, mcpMux := http.NewServeMux(), http.NewServeMux(), http.NewServeMux()
	mountQueryRouteSets(getenv, mux, internalMux, mcpMux, queryRouteHandlers{
		Query: stub(http.StatusOK), RunOperation: stub(http.StatusAccepted), Registry: stub(http.StatusOK), BuildInfo: stub(http.StatusOK), MCP: stub(http.StatusTeapot),
	}, graphQLEdgeDeps{auth: ecEdgeAuth(t, &fakeEdgeStore{}), maxBytes: defaultGraphQLMaxQueryBytes})
	do := func(handler http.Handler, path string) int {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, nil))
		return recorder.Code
	}
	if got := do(internalMux, runOperationPath); got != http.StatusAccepted {
		t.Fatalf("the internal mux answers %d for %s, want the run-operation handler (202)", got, runOperationPath)
	}
	if got := do(mux, runOperationPath); got != http.StatusNotFound {
		t.Fatalf("the PUBLIC mux answers %d for %s, want 404: the route must not be reachable on the public listener", got, runOperationPath)
	}
	if got := do(mcpMux, runOperationPath); got != http.StatusNotFound {
		t.Fatalf("the MCP mux answers %d for %s, want 404", got, runOperationPath)
	}
	if got := do(mux, "/query"); got != http.StatusOK {
		t.Fatalf("/query is no longer served on the public mux: %d", got)
	}
}
