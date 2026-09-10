package goapiproof

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/validator"
)

// investmentFullDocumentPattern extracts registeredInvestmentFullDocument's
// literal text straight from cmd/query-api/query_route.go, rather than
// retyping it here -- a second, hand-copied statement of the document is
// exactly the kind of artefact that silently drifts from the one actually
// registered (this package cannot import cmd/query-api's `internal/graph`
// types directly: Go's internal-package rule puts them out of reach from
// here, which is also why this test validates shape via gqlparser against
// the published SDL rather than unmarshalling into the generated
// gqlgen input structs).
var investmentFullDocumentPattern = regexp.MustCompile(
	"(?s)const registeredInvestmentFullDocument = `(.*?)`\n",
)

// TestInvestmentFullVariablesAskForSankey is the shape test for the
// investmentFull fix: the operation's registered document
// (cmd/query-api/query_route.go's registeredInvestmentFullDocument)
// selects `analytics.sankey.nodes.value` / `.edges.value`, but the batch
// this table built never asked the resolver for a sankey sub-request --
// `resolveSankey` only runs when `batch.Sankey != nil`
// (cmd/query-api/internal/analytics/resolve.go:194) -- so both planes
// answered null and the first deployed-executed run correctly REFUSED the
// two declared FloatTierB paths as vacuous (JOB 5, 12:56Z 2026-09-10).
//
// This test fails on the pre-fix investmentVariables (shared with
// investmentBreakdown, which sends no `sankey` key at all) and passes once
// investmentFull gets its own Variables func that adds one. It validates
// the built batch against the PUBLISHED SCHEMA via gqlparser (unknown
// fields, required fields, enum membership all get checked by
// validator.VariableValues) and then asserts the business rule the SDL
// itself cannot express: a Sankey path needs >= 2 dimensions
// (validateSankeyPath, cmd/query-api/internal/analytics/sankey.go).
func TestInvestmentFullVariablesAskForSankey(t *testing.T) {
	root := repoRootFromTest(t)

	sdl, err := os.ReadFile(filepath.Join(root, "contracts", "graphql", "v1", "schema.graphql"))
	if err != nil {
		t.Fatalf("read SDL: %v", err)
	}
	schema, schemaErr := gqlparser.LoadSchema(&ast.Source{Name: "schema.graphql", Input: string(sdl)})
	if schemaErr != nil {
		t.Fatalf("parse SDL: %v", schemaErr)
	}

	routeSource, err := os.ReadFile(filepath.Join(root, "cmd", "query-api", "query_route.go"))
	if err != nil {
		t.Fatalf("read query_route.go: %v", err)
	}
	match := investmentFullDocumentPattern.FindSubmatch(routeSource)
	if match == nil {
		t.Fatal("could not find registeredInvestmentFullDocument in query_route.go -- this test's extraction regex has stopped matching")
	}
	docText := string(match[1])
	if !regexp.MustCompile(`sankey\s*\{[^}]*value`).MatchString(docText) {
		t.Fatalf("registeredInvestmentFullDocument no longer selects sankey.*.value -- the declared FloatTierB paths in operations.go would match nothing: %s", docText)
	}

	doc, gqlErr := gqlparser.LoadQuery(schema, docText)
	if gqlErr != nil {
		t.Fatalf("registered document does not validate against the published SDL: %v", gqlErr)
	}
	if len(doc.Operations) != 1 {
		t.Fatalf("expected exactly one operation in the registered document, got %d", len(doc.Operations))
	}

	spec, err := SpecFor("investmentFull")
	if err != nil {
		t.Fatalf("SpecFor: %v", err)
	}
	variables := spec.Variables("70d529e0", DefaultWindow())

	coerced, verr := validator.VariableValues(schema, doc.Operations[0], variables)
	if verr != nil {
		t.Fatalf("investmentFull's built variables do not conform to the SDL's AnalyticsRequestInput: %v", verr)
	}

	batch, ok := coerced["batch"].(map[string]any)
	if !ok {
		t.Fatalf("coerced $batch is not an object: %#v", coerced["batch"])
	}
	sankey, ok := batch["sankey"].(map[string]any)
	if !ok || sankey == nil {
		t.Fatalf("investmentFull's batch carries no sankey sub-request, so data.analytics.sankey will be null on both planes and the declared FloatTierB paths under it match nothing (got batch=%#v)", batch)
	}
	path, ok := sankey["path"].([]any)
	if !ok || len(path) < 2 {
		t.Fatalf("sankey.path has %d dimension(s), want >= 2 (validateSankeyPath, sankey.go): %#v", len(path), sankey["path"])
	}
	if sankey["dateRange"] == nil {
		t.Fatal("sankey.dateRange is required (resolve.go: \"sankey.dateRange is required\") but was not set")
	}
	if useInvestment, _ := batch["useInvestment"].(bool); !useInvestment {
		t.Fatalf("batch.useInvestment must be true -- the declared FloatTierB reasons are all investment-path-specific (CHAOS-5451), got %#v", batch["useInvestment"])
	}
}
