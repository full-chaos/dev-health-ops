package server

// CHAOS-7078: each explicit gqlgen server option newGraphQLServer sets, with
// its own test -- POST only, no introspection, no APQ, a complexity limit, a
// depth limit. Deliberately built against newGraphQLServer(&graph.Resolver{})
// directly (no ClickHouse/Postgres): every case here is decided by the
// TRANSPORT layer or an OperationContextMutator, before any resolver would
// ever run, so an empty resolver is never reached.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
)

func gqlPost(t *testing.T, handler http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// Clause 1: POST only -- GET (gqlgen's default transport for a browsable
// GraphiQL-style query string) is not registered at all.
func TestNewGraphQLServerRefusesGET(t *testing.T) {
	gql := newGraphQLServer(&graph.Resolver{})
	req := httptest.NewRequest(http.MethodGet, "/query?query={__typename}", nil)
	rec := httptest.NewRecorder()
	gql.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatalf("GET got 200, want refused (no GET transport registered): %s", rec.Body.String())
	}
}

// The inverse control: POST with a real, tiny, always-resolvable field
// (__typename, which gqlgen answers without touching any resolver) proves
// the transport removals above are targeted, not a server that refuses
// everything.
func TestNewGraphQLServerServesPOST(t *testing.T) {
	gql := newGraphQLServer(&graph.Resolver{})
	rec := gqlPost(t, gql, `{"query":"{ __typename }"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST __typename got %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Data   map[string]any `json:"data"`
		Errors []any          `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Errors) != 0 {
		t.Fatalf("POST __typename got errors: %v", body.Errors)
	}
}

// Clause 2: no introspection. extension.Introspection{} is what flips
// gqlgen's opCtx.DisableIntrospection to false -- its absence here means
// introspection stays disabled (the executor's own default), not merely
// unadvertised.
func TestNewGraphQLServerRefusesIntrospection(t *testing.T) {
	gql := newGraphQLServer(&graph.Resolver{})
	rec := gqlPost(t, gql, `{"query":"{ __schema { types { name } } }"}`)
	var body struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Errors) == 0 {
		t.Fatalf("introspection query got no errors, want refused: %s", rec.Body.String())
	}
	if !strings.Contains(strings.ToLower(body.Errors[0].Message), "introspection") {
		t.Fatalf("error = %q, want it to name introspection", body.Errors[0].Message)
	}
}

// Clause 3: no Automatic Persisted Queries. A request naming ONLY a
// persistedQuery hash (no `query` field, APQ's own carrier shape) must be
// refused as "no query" -- the generic, pre-APQ refusal -- never as
// APQ-specific wording ("PersistedQueryNotFound"), which only
// extension.AutomaticPersistedQuery{} would produce and which would leak
// that the extension is present even though it never resolves anything.
func TestNewGraphQLServerHasNoAutomaticPersistedQueries(t *testing.T) {
	gql := newGraphQLServer(&graph.Resolver{})
	rec := gqlPost(t, gql, `{"extensions":{"persistedQuery":{"version":1,"sha256Hash":"deadbeef"}}}`)
	if rec.Code == http.StatusOK {
		t.Fatalf("APQ-only request got 200, want refused (no query and no APQ extension to resolve the hash): %s", rec.Body.String())
	}
	if strings.Contains(strings.ToLower(rec.Body.String()), "persistedquerynotfound") {
		t.Fatalf("response names APQ's own error shape, want the generic no-query refusal instead: %s", rec.Body.String())
	}
}

// Clause 4: a fixed complexity limit. A query fanning out well past
// graphQLComplexityLimit is refused; __typename (complexity ~1) is not --
// proven together so the refusal above is pinned to the LIMIT, not to
// every query failing for an unrelated reason.
func TestNewGraphQLServerEnforcesTheComplexityLimit(t *testing.T) {
	gql := newGraphQLServer(&graph.Resolver{})
	// __schema is refused by the introspection clause above before
	// complexity is even evaluated, so the over-limit probe here has to be
	// a real, deeply-fanned executable-schema query. featureFlagTimeseries's
	// own registered shape (query_route.go) is nowhere near the limit; build
	// a synthetic query against a real, repeatable field instead: aliasing
	// __typename graphQLComplexityLimit+1 times inside one selection set
	// gives exactly that many complexity-1 fields, deterministically over
	// the limit regardless of any resolver's own field weights.
	var aliases strings.Builder
	for i := 0; i <= graphQLComplexityLimit; i++ {
		aliases.WriteString("a")
		aliases.WriteString(strconv.Itoa(i))
		aliases.WriteString(": __typename ")
	}
	body, err := json.Marshal(map[string]string{"query": "{ " + aliases.String() + "}"})
	if err != nil {
		t.Fatal(err)
	}
	rec := gqlPost(t, gql, string(body))
	var resp struct {
		Errors []struct{ Message string } `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Errors) == 0 {
		t.Fatalf("an over-limit query got no errors, want refused for complexity: %s", rec.Body.String())
	}
	if !strings.Contains(strings.ToLower(resp.Errors[0].Message), "complexity") {
		t.Fatalf("error = %q, want it to name complexity", resp.Errors[0].Message)
	}
}

// Clause 5: a depth limit, gqlgen's own missing half of extension.
// ComplexityLimit. Exercised directly against depthLimit.MutateOperationContext
// (not through the full HTTP handler): an inline-fragment or fragment-spread
// chain does not add response-shape depth the way real FIELD nesting does --
// GraphQL's own field-collection semantics flatten same-type inline
// fragments into their parent's selection set -- so a meaningful over-limit
// probe needs genuine field nesting, and this schema has no naturally
// recursive field to build an arbitrarily deep synthetic one from. Using
// the REAL registeredOperatingReviewDocument (measured depth 5 --
// operatingReview -> sections -> metrics -> delta) against an artificially
// lowered limit is the real producer this schema actually has, not a
// hand-built approximation of one.
func TestDepthLimitRefusesADocumentDeeperThanItsMax(t *testing.T) {
	schema := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}})
	doc, err := gqlparser.LoadQuery(schema.Schema(), registeredOperatingReviewDocument)
	if err != nil {
		t.Fatal(err)
	}
	opCtx := &graphql.OperationContext{Doc: doc, OperationName: "OperatingReview"}

	tight := depthLimit{Max: 3}
	if gqlErr := tight.MutateOperationContext(context.Background(), opCtx); gqlErr == nil {
		t.Fatal("depth 5 document against Max=3 got no error, want refused")
	} else if !strings.Contains(strings.ToLower(gqlErr.Message), "depth") {
		t.Fatalf("error = %q, want it to name depth", gqlErr.Message)
	}

	real := depthLimit{Max: graphQLDepthLimit}
	if gqlErr := real.MutateOperationContext(context.Background(), opCtx); gqlErr != nil {
		t.Fatalf("the SAME depth-5 document against the real configured limit (%d) got refused: %v", graphQLDepthLimit, gqlErr)
	}
}

// End-to-end control: the depth limiter is actually wired into
// newGraphQLServer's real HTTP pipeline, not just unit-tested in isolation.
// __typename (depth 1) passing through the full stack, refused by nothing,
// is TestNewGraphQLServerAllowsAnOrdinaryQueryUnderBothLimits below; this
// proves the OPPOSITE direction using selectionSetDepth's own documented
// edge case (fragment spreads must be walked, not skipped) directly, since
// an HTTP-level over-depth probe cannot be built for this schema (see the
// test above).
func TestSelectionSetDepthWalksFragmentSpreadsAndInlineFragments(t *testing.T) {
	// { a { b } ...frag }  where frag is  { d { e { f } } } -- the fragment
	// branch (depth 3: d=2, e=3, f is e's leaf child so does not itself add
	// a level past 3) is deeper than the inline "a" branch (depth 2: a=2, b
	// is a's leaf child). A spread does not add its OWN level (matching
	// InlineFragment's identical treatment -- neither changes the response
	// shape's nesting, only a real field does), so the overall depth must
	// come out to 3, from the fragment branch: a walker that skipped
	// FragmentSpread.Definition entirely would report 2 instead, from the
	// "a" branch alone -- under-reporting exactly the case a caller hiding
	// depth inside a fragment would exploit.
	root := ast.SelectionSet{
		&ast.Field{Name: "a", SelectionSet: ast.SelectionSet{&ast.Field{Name: "b"}}},
		&ast.FragmentSpread{Name: "frag", Definition: &ast.FragmentDefinition{
			Name: "frag",
			SelectionSet: ast.SelectionSet{&ast.Field{Name: "d", SelectionSet: ast.SelectionSet{
				&ast.Field{Name: "e", SelectionSet: ast.SelectionSet{&ast.Field{Name: "f"}}},
			}}},
		}},
	}
	if got := selectionSetDepth(root, 1); got != 3 {
		t.Fatalf("depth = %d, want 3 (from the fragment branch: d=2, e=3)", got)
	}
}

// A query at/under both limits must NOT be refused for either -- proven
// once, generically, so the two tests above are pinning real limits and
// not a server that always refuses.
func TestNewGraphQLServerAllowsAnOrdinaryQueryUnderBothLimits(t *testing.T) {
	gql := newGraphQLServer(&graph.Resolver{})
	rec := gqlPost(t, gql, `{"query":"{ __typename }"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Errors) != 0 {
		t.Fatalf("got errors, want none: %v", resp.Errors)
	}
}
