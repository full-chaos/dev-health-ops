package server

// TestRegisteredHomeDocumentSelectsEveryHomeResultField is the direct fix
// for the CHAOS-7070 r1 P1 finding: registeredHomeDocument's selection set
// fell behind HomeResult's own SDL growth -- the schema and the resolver
// both grew to the full home payload, but the registered document (the
// ONLY thing operationForDocument matches a request against) still
// selected just the original 3 fields, so every new field was
// unreachable through /query even though queryResolver.Home mapped it
// correctly. That defect class -- "the schema/resolver grew, the
// registered document did not" -- is exactly what this test prevents
// from recurring, and it does so WITHOUT executing home.BuildResponse's
// real ClickHouse/Postgres-backed pipeline (which TestExperiments_
// ThroughTheGeneratedSchema-style tests can do for a simple resolver but
// which home's is not): it is a pure schema-vs-document static
// comparison, using the same generated executable schema's own AST
// (graph.NewExecutableSchema(...).Schema()) as the source of truth for
// "every field HomeResult declares", recursively through every nested
// object type reachable from it, and gqlparser to parse
// registeredHomeDocument's actual text into a selection tree to compare
// against. A future field added to HomeResult (or any type nested under
// it) fails THIS test the moment it lands, before anyone needs to
// remember to update the registered document by hand.
import (
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
)

func TestRegisteredHomeDocumentSelectsEveryHomeResultField(t *testing.T) {
	schema := graph.NewExecutableSchema(graph.Config{}).Schema()

	doc, err := parser.ParseQuery(&ast.Source{Input: registeredHomeDocument, Name: "registeredHomeDocument"})
	if err != nil {
		t.Fatalf("parse registeredHomeDocument: %v", err)
	}
	if len(doc.Operations) != 1 {
		t.Fatalf("registeredHomeDocument has %d operations, want 1", len(doc.Operations))
	}

	var homeSelection *ast.Field
	for _, sel := range doc.Operations[0].SelectionSet {
		if field, ok := sel.(*ast.Field); ok && field.Name == "home" {
			homeSelection = field
		}
	}
	if homeSelection == nil {
		t.Fatal("registeredHomeDocument does not select the home root field at all")
	}

	var missing []string
	assertSelectsEveryField(t, schema, "HomeResult", homeSelection.SelectionSet, "home", &missing)
	if len(missing) > 0 {
		t.Fatalf(
			"registeredHomeDocument is missing %d field(s) the schema declares "+
				"(regenerate it, then re-run scripts/go_api/generate_operation_catalog.py "+
				"to refresh the digest): %v",
			len(missing), missing,
		)
	}
}

// assertSelectsEveryField walks every field the schema's typeName
// declares and confirms selectionSet selects it (by name; a field with
// its own nested selection is recursed into using ITS field's own
// selection set, found by name in selectionSet). __typename is a
// meta-field every real client tool adds automatically and is not part
// of the type's own declared fields, so it is not required here.
func assertSelectsEveryField(t *testing.T, schema *ast.Schema, typeName string, selectionSet ast.SelectionSet, path string, missing *[]string) {
	t.Helper()
	def := schema.Types[typeName]
	if def == nil {
		t.Fatalf("schema has no type %q (path %s)", typeName, path)
	}

	selected := map[string]*ast.Field{}
	for _, sel := range selectionSet {
		if field, ok := sel.(*ast.Field); ok {
			selected[field.Name] = field
		}
	}

	for _, fieldDef := range def.Fields {
		if fieldDef.Name == "__typename" || fieldDef.Name == "__schema" || fieldDef.Name == "__type" {
			continue
		}
		fieldPath := path + "." + fieldDef.Name
		got, ok := selected[fieldDef.Name]
		if !ok {
			*missing = append(*missing, fieldPath)
			continue
		}

		namedType := underlyingNamedType(fieldDef.Type)
		underlyingDef := schema.Types[namedType]
		if underlyingDef == nil || underlyingDef.Kind != ast.Object {
			// Scalar, enum, or anything else with no fields of its own --
			// selecting the field by name (already confirmed above) is
			// the whole obligation.
			continue
		}
		assertSelectsEveryField(t, schema, namedType, got.SelectionSet, fieldPath, missing)
	}
}

// underlyingNamedType unwraps NonNull/List wrappers (e.g. [HomeSignal!]!)
// down to the bare named type (HomeSignal).
func underlyingNamedType(t *ast.Type) string {
	for t.NamedType == "" && t.Elem != nil {
		t = t.Elem
	}
	return t.NamedType
}
