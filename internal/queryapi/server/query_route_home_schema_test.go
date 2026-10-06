package server

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/vektah/gqlparser/v2"
)

// TestRegisteredHomeDocumentLoadsAgainstExecutableSchema proves that the exact
// catalog-served Home wire text remains valid for the executable schema.
func TestRegisteredHomeDocumentLoadsAgainstExecutableSchema(t *testing.T) {
	schema := graph.NewExecutableSchema(graph.Config{}).Schema()
	doc, errs := gqlparser.LoadQuery(schema, registeredHomeDocument)
	if len(errs) > 0 || len(doc.Operations) != 1 {
		t.Fatalf("registered Home document does not load against executable schema: %v", errs)
	}
}
