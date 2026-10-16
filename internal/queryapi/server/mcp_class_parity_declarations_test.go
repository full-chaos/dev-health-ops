package server

import (
	"strings"
	"testing"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"

	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// mcpParityPathExists walks a declared "data.<root>.<field>..." path through the SDL and reports the first segment
// that names no field of the type reached so far.
func mcpParityPathExists(schema *ast.Schema, path string) (missing string, ok bool) {
	current := schema.Query
	for _, segment := range strings.Split(strings.TrimPrefix(path, "data."), ".") {
		if current == nil {
			return segment, false
		}
		field := current.Fields.ForName(segment)
		if field == nil {
			return segment, false
		}
		current = schema.Types[field.Type.Name()]
	}
	return "", true
}

// mcpParityTolerance is one tolerance of the MCP class parity step: the operation and variant it applies to, the
// float paths compared at the Tier B tolerance (1e-9, max of absolute and relative) and the lists compared by key.
// Every entry names its reason and its ticket; TestMCPParityDeclarationsStillExist fails when its operation, its
// variant or its path no longer exists.
type mcpParityTolerance struct {
	operation string
	variants  []string
	floats    []string
	lists     []goapiproof.OrderInsensitiveList
	reason    string
	ticket    string
}

func mcpParityTolerances() []mcpParityTolerance {
	return []mcpParityTolerance{{
		operation: "flowMatrix",
		variants:  []string{"TEAM", "REPO", "THEME"},
		floats:    []string{"data.analytics.flowMatrix.nodes.value", "data.analytics.flowMatrix.edges.value"},
		lists: []goapiproof.OrderInsensitiveList{
			{Path: "data.analytics.flowMatrix.nodes", KeyFields: []string{"id"}},
			{Path: "data.analytics.flowMatrix.edges", KeyFields: []string{"source", "target"}},
		},
		reason: "CHAOS-5451 class: the sums are a parallel Float64 merge in no fixed order, so values differ in the last bits and near ties flip the row order; WORK_TYPE counts are integers and stay exact",
		ticket: "CHAOS-9803",
	}}
}

// mcpParityExtraVariants adds the served shapes the proof corpus does not hold yet: flowMatrix over THEME.
func mcpParityExtraVariants(operation string, variants []goapiproof.Variant) []goapiproof.Variant {
	out := append([]goapiproof.Variant(nil), variants...)
	if operation == "flowMatrix" {
		out = append(out, goapiproof.Variant{
			Name: "THEME",
			Variables: func(orgID string, w goapiproof.Window) map[string]any {
				return map[string]any{"orgId": orgID, "batch": map[string]any{
					"flowMatrix": map[string]any{
						"dimension": "THEME", "measure": "COUNT",
						"dateRange": map[string]any{"startDate": w.SinceDate, "endDate": w.UntilDate},
						"maxNodes":  50, "maxEdges": 200,
					},
				}}
			},
		})
	}
	return out
}

// mcpParityOptions is the shape's own comparator configuration for Go against Go: the declared Python defects
// describe a difference from Python, so none applies; the volatile fields stay excused; a declared tolerance applies
// to the variants it names.
func mcpParityOptions(operation, variant string, declared goapiproof.Options) goapiproof.Options {
	declared.BaselineDefects = nil
	for _, tolerance := range mcpParityTolerances() {
		if tolerance.operation != operation || !mcpParityContains(tolerance.variants, variant) {
			continue
		}
		declared.FloatTierB = map[string]string{}
		for _, path := range tolerance.floats {
			declared.FloatTierB[path] = tolerance.reason + " (" + tolerance.ticket + ")"
		}
		for _, list := range tolerance.lists {
			list.Reason, list.Ticket = tolerance.reason, tolerance.ticket
			declared.OrderInsensitiveLists = append(declared.OrderInsensitiveLists, list)
		}
	}
	return declared
}

func mcpParityContains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

func TestMCPParityDeclarationsStillExist(t *testing.T) {
	for _, tolerance := range mcpParityTolerances() {
		if tolerance.reason == "" || !strings.HasPrefix(tolerance.ticket, "CHAOS-") {
			t.Errorf("%s: a tolerance needs a reason and a ticket id", tolerance.operation)
		}
		spec, err := goapiproof.SpecFor(tolerance.operation)
		if err != nil {
			t.Errorf("tolerance for %s: the operation is gone: %v", tolerance.operation, err)
			continue
		}
		variants := map[string]bool{}
		for _, variant := range mcpParityExtraVariants(tolerance.operation, spec.Variants) {
			variants[variant.Name] = true
		}
		for _, name := range tolerance.variants {
			if !variants[name] {
				t.Errorf("tolerance for %s: variant %s is gone", tolerance.operation, name)
			}
		}
		paths := append([]string(nil), tolerance.floats...)
		for _, list := range tolerance.lists {
			paths = append(paths, list.Path)
		}
		schema, schemaErr := gqlparser.LoadSchema(&ast.Source{Name: "schema.graphql", Input: string(schemav1.SDL)})
		if schemaErr != nil {
			t.Fatalf("load the schema: %v", schemaErr)
		}
		for _, path := range paths {
			if !strings.HasPrefix(path, "data."+spec.ResponseRoot+".") {
				t.Errorf("tolerance for %s: path %s is not under the response root %s", tolerance.operation, path, spec.ResponseRoot)
			}
			if missing, ok := mcpParityPathExists(schema, path); !ok {
				t.Errorf("tolerance for %s: path %s names %q, which the schema does not have at that place", tolerance.operation, path, missing)
			}
		}
	}
	for _, operation := range goapiproof.KnownOperations() {
		spec, _ := goapiproof.SpecFor(operation)
		for path, reason := range spec.Parity.VolatileFields {
			if reason == "" || !strings.HasPrefix(path, "data.") {
				t.Errorf("%s: volatile field %q needs a reason and a data. path", operation, path)
			}
		}
	}
}

func TestMCPParityPathCheckSeesAMissingField(t *testing.T) {
	schema, err := gqlparser.LoadSchema(&ast.Source{Name: "schema.graphql", Input: string(schemav1.SDL)})
	if err != nil {
		t.Fatal(err)
	}
	if missing, ok := mcpParityPathExists(schema, "data.analytics.flowMatrix.nodes.value"); !ok {
		t.Fatalf("a real path was refused at %q", missing)
	}
	if missing, ok := mcpParityPathExists(schema, "data.analytics.flowMatrix.nodes.missingValue"); ok || missing != "missingValue" {
		t.Fatalf("a path with a field the schema lacks was accepted (missing=%q ok=%v)", missing, ok)
	}
}
