package atlassianteams

import (
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	gqlast "github.com/vektah/gqlparser/v2/ast"
	gqlparser "github.com/vektah/gqlparser/v2/parser"

	"atlassian/atlassian/graph"
	"atlassian/atlassian/graph/gen"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// CHAOS-7493: the Atlassian gateway refused the vendored members documents with
//
//	Validation error (FieldsConflict) : 'teamworkGraph_teamUsers/edges/node/columns/value/value' :
//	returns different types 'String' and 'Int'
//
// because one response key (`value`) was selected, unaliased, under several typed-scalar fragments
// (`... on GraphStoreCypherQueryV2StringObject { value }`, `... on GraphStoreCypherQueryV2IntObject { value }`).
// The class: ANY document of the vendored graph package that selects one response key with different scalar
// types across inline fragments. These tests read every document of the vendored package from its source (a
// new document or a re-vendor that restores the old text is read the same way) and check each one.

// typedScalarFragments are the fragment types whose scalar return type is known from the type's own name
// (the vendored package carries no schema): two of them selecting the same response key conflict.
var typedScalarFragments = map[string]string{
	"GraphStoreCypherQueryV2StringObject":    "String",
	"GraphStoreCypherQueryV2IntObject":       "Int",
	"GraphStoreCypherQueryV2FloatObject":     "Float",
	"GraphStoreCypherQueryV2BooleanObject":   "Boolean",
	"GraphStoreCypherQueryV2TimestampObject": "Timestamp",
}

// vendoredDocuments is every GraphQL document (a Go string constant that parses as a GraphQL operation) of
// the vendored package's generated sources, by constant name.
func vendoredDocuments(t *testing.T) map[string]string {
	t.Helper()
	_, file, _, _ := moduleroot.Caller(0)
	dir := filepath.Join(filepath.Dir(file), "..", "..", "third_party", "vendor", "atlassian", "atlassian", "graph")
	var paths []string
	for _, pattern := range []string{filepath.Join(dir, "gen", "*.go"), filepath.Join(dir, "*.go")} {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, matches...)
	}
	documents := map[string]string{}
	fset := token.NewFileSet()
	for _, path := range paths {
		parsed, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			spec, ok := node.(*ast.ValueSpec)
			if !ok {
				return true
			}
			for index, value := range spec.Values {
				literal, ok := value.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING || index >= len(spec.Names) {
					continue
				}
				text, err := strconv.Unquote(literal.Value)
				if err != nil {
					continue
				}
				trimmed := strings.TrimSpace(text)
				if !strings.HasPrefix(trimmed, "query") && !strings.HasPrefix(trimmed, "mutation") {
					continue
				}
				if _, err := gqlparser.ParseQuery(&gqlast.Source{Input: text}); err != nil {
					continue
				}
				documents[filepath.Base(path)+":"+spec.Names[index].Name] = text
			}
			return true
		})
	}
	if len(documents) < 10 {
		t.Fatalf("found %d vendored documents: the discovery measures too little", len(documents))
	}
	return documents
}

// fieldOverlap is one response key selected as a leaf under more than one inline-fragment type.
type fieldOverlap struct {
	path, key string
	types     []string
}

// overlaps walks every selection set of a document and returns, per response key selected as a LEAF under
// more than one inline-fragment type of one selection set: the key, the path and the fragment types.
func overlaps(document *gqlast.QueryDocument) []fieldOverlap {
	var out []fieldOverlap
	var walk func(path string, set gqlast.SelectionSet)
	walk = func(path string, set gqlast.SelectionSet) {
		byKey := map[string][]string{}
		for _, selection := range set {
			switch node := selection.(type) {
			case *gqlast.Field:
				if len(node.SelectionSet) > 0 {
					walk(path+"/"+responseKey(node), node.SelectionSet)
				}
			case *gqlast.InlineFragment:
				for _, inner := range node.SelectionSet {
					if field, ok := inner.(*gqlast.Field); ok && len(field.SelectionSet) == 0 && field.Name != "__typename" {
						byKey[responseKey(field)] = append(byKey[responseKey(field)], node.TypeCondition)
					}
					if field, ok := inner.(*gqlast.Field); ok && len(field.SelectionSet) > 0 {
						walk(path+"/"+node.TypeCondition+"/"+responseKey(field), field.SelectionSet)
					}
				}
			}
		}
		for key, types := range byKey {
			if len(types) > 1 {
				out = append(out, fieldOverlap{path: path, key: key, types: types})
			}
		}
	}
	for _, operation := range document.Operations {
		walk(operation.Name, operation.SelectionSet)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path+out[i].key < out[j].path+out[j].key })
	return out
}

func responseKey(field *gqlast.Field) string {
	if field.Alias != "" {
		return field.Alias
	}
	return field.Name
}

// conflicts are the overlaps the gateway's validation (OverlappingFieldsCanBeMerged) refuses and that can be
// decided without a schema: the key is selected under two typed-scalar fragments of different scalar types.
func conflicts(found []fieldOverlap) []string {
	var out []string
	for _, overlap := range found {
		scalars := map[string]bool{}
		known := 0
		for _, fragmentType := range overlap.types {
			if scalar, ok := typedScalarFragments[fragmentType]; ok {
				scalars[scalar] = true
				known++
			}
		}
		if known >= 2 && len(scalars) >= 2 {
			out = append(out, fmt.Sprintf("%s/%s returns different types under %s", overlap.path, overlap.key, strings.Join(overlap.types, ", ")))
		}
	}
	return out
}

func TestVendoredDocumentsHaveNoConflictingTypedValueFields(t *testing.T) {
	for name, text := range vendoredDocuments(t) {
		document, err := gqlparser.ParseQuery(&gqlast.Source{Input: text})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, conflict := range conflicts(overlaps(document)) {
			t.Errorf("%s: %s: the gateway refuses it (FieldsConflict); alias each typed selection", name, conflict)
		}
	}
}

// unverifiableOverlaps are the (key, fragment types) overlaps this test CANNOT decide: the fragment types are
// not typed-scalar wrappers, so their field types are only in the Atlassian schema, which the vendored package
// does not carry. They are listed so a new one shows in review; the live gateway accepted them (the rerun that
// found the `value` conflict reported no other).
var unverifiableOverlaps = map[string]bool{
	"id: AtlassianAccountUser, JiraProject, TeamV2, TownsquareProject": true,
	"name: AtlassianAccountUser, JiraProject, TownsquareProject":       true,
	"key: JiraProject, TownsquareProject":                              true,
}

func TestVendoredDocumentsUnverifiedOverlapsAreTheDocumentedOnes(t *testing.T) {
	seen := map[string]bool{}
	for name, text := range vendoredDocuments(t) {
		document, err := gqlparser.ParseQuery(&gqlast.Source{Input: text})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, overlap := range overlaps(document) {
			if len(conflicts([]fieldOverlap{overlap})) > 0 {
				continue
			}
			types := append([]string(nil), overlap.types...)
			sort.Strings(types)
			entry := overlap.key + ": " + strings.Join(types, ", ")
			seen[entry] = true
			if !unverifiableOverlaps[entry] {
				t.Errorf("%s: %s selects %q under %v: not decidable without the schema and not in the documented list", name, overlap.path, overlap.key, overlap.types)
			}
		}
	}
	if len(seen) == 0 {
		t.Fatal("no unverifiable overlap found: the list above is stale")
	}
}

// gatewayFake is a gateway that validates the document it receives (it refuses what the live one refused) and
// answers in the shape that document asks for: the response is BUILT FROM THE DOCUMENT (its response keys,
// aliases included), not written field by field.
type gatewayFake struct {
	t        *testing.T
	server   *httptest.Server
	requests int
}

func newGatewayFake(t *testing.T) *gatewayFake {
	fake := &gatewayFake{t: t}
	fake.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.requests++
		raw, _ := io.ReadAll(r.Body)
		var body struct {
			Query string `json:"query"`
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Errorf("request body: %v", err)
		}
		document, err := gqlparser.ParseQuery(&gqlast.Source{Input: body.Query})
		if err != nil {
			t.Errorf("the client sent a document that does not parse: %v", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if found := conflicts(overlaps(document)); len(found) > 0 {
			_ = json.NewEncoder(w).Encode(map[string]any{"errors": []map[string]any{{"message": "Validation error (FieldsConflict) : " + found[0]}}})
			return
		}
		data := map[string]any{}
		for _, selection := range document.Operations[0].SelectionSet {
			if field, ok := selection.(*gqlast.Field); ok {
				data[responseKey(field)] = synthesize(field.SelectionSet)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	t.Cleanup(fake.server.Close)
	return fake
}

// listFields are the response keys of the cypher connection that are lists.
var listFields = map[string]bool{"edges": true, "columns": true, "nodes": true, "elements": true}

// leafValue is the value of a leaf the way the gateway sends it: a typed scalar by its response key's
// alias (the fix) or by the type of the fragment it sits in (the unaliased shape).
func leafValue(key, fragmentType string) any {
	switch {
	case key == "stringValue" || fragmentType == "GraphStoreCypherQueryV2StringObject":
		return "a-string"
	case key == "intValue" || fragmentType == "GraphStoreCypherQueryV2IntObject":
		return 7
	case key == "floatValue" || fragmentType == "GraphStoreCypherQueryV2FloatObject":
		return 1.5
	case key == "booleanValue" || fragmentType == "GraphStoreCypherQueryV2BooleanObject":
		return true
	case key == "timestampValue" || fragmentType == "GraphStoreCypherQueryV2TimestampObject":
		return 1700000000
	case key == "hasNextPage" || key == "hasPreviousPage":
		return false
	case key == "endCursor" || key == "startCursor" || key == "cursor":
		return nil
	case key == "accountId":
		return "account-1"
	case key == "elements":
		return []string{"e-1"}
	}
	return key + "-1"
}

// synthesize builds a value for a selection set: an object with every plain field, expanded once per inline
// fragment variant where a fragment is selected (a list field gets one element per variant).
func synthesize(set gqlast.SelectionSet) any {
	variants := expand(set, "")
	if len(variants) == 1 {
		return variants[0]
	}
	return variants
}

func expand(set gqlast.SelectionSet, fragmentType string) []map[string]any {
	base := map[string]any{}
	var fragments []*gqlast.InlineFragment
	nestedVariants := map[string][]map[string]any{}
	order := []string{}
	for _, selection := range set {
		switch node := selection.(type) {
		case *gqlast.Field:
			key := responseKey(node)
			switch {
			case node.Name == "__typename":
				base[key] = fragmentType
			case len(node.SelectionSet) > 0:
				nestedVariants[key] = expand(node.SelectionSet, "")
				order = append(order, key)
			default:
				base[key] = leafValue(key, fragmentType)
			}
		case *gqlast.InlineFragment:
			fragments = append(fragments, node)
		}
	}
	results := []map[string]any{base}
	if len(fragments) > 0 {
		results = nil
		for _, fragment := range fragments {
			for _, variant := range expand(fragment.SelectionSet, fragment.TypeCondition) {
				merged := map[string]any{"__typename": fragment.TypeCondition}
				for key, value := range base {
					if key != "__typename" {
						merged[key] = value
					}
				}
				for key, value := range variant {
					merged[key] = value
				}
				results = append(results, merged)
			}
		}
	}
	for _, key := range order {
		nested := nestedVariants[key]
		for index := range results {
			if listFields[key] {
				list := make([]any, len(nested))
				for i, item := range nested {
					list[i] = item
				}
				results[index][key] = list
			}
		}
		if !listFields[key] && len(nested) > 0 {
			// A union-typed field (the ARI node's data): one result per member, so every member is built.
			var multiplied []map[string]any
			for _, result := range results {
				for _, member := range nested {
					merged := map[string]any{}
					for k, v := range result {
						merged[k] = v
					}
					merged[key] = member
					multiplied = append(multiplied, merged)
				}
			}
			results = multiplied
		}
	}
	return results
}

// TestMembersQueriesAreAcceptedAndDecodeTheAliasedShape runs the vendored client's members reads against a
// gateway that validates the document and answers in the shape the document asks for. On the unpatched
// document the gateway refuses it with the live FieldsConflict; on the aliased one it answers, and the members
// decode.
func TestMembersQueriesAreAcceptedAndDecodeTheAliasedShape(t *testing.T) {
	fake := newGatewayFake(t)
	client := &graph.Client{BaseURL: fake.server.URL, Strict: true, HTTPClient: fake.server.Client()}
	relations, err := client.IterTeamUsers(context.Background(), "team-1", 10)
	if err != nil {
		t.Fatalf("members of a team: %v", err)
	}
	if len(relations) == 0 || relations[0].SubjectUserID == "" || fake.requests == 0 {
		t.Fatalf("no member decoded from the document-shaped response (%d requests): %+v", fake.requests, relations)
	}
	if _, err := client.IterUserTeams(context.Background(), "user-1", 10); err != nil {
		t.Fatalf("teams of a user: %v", err)
	}
}

// TestTypedValueObjectsDecodeFromTheirAliases builds the response from the members document (every typed
// scalar variant it selects, under the response keys the document names) and decodes it with the vendored
// decoder: each typed value lands in its own object.
func TestTypedValueObjectsDecodeFromTheirAliases(t *testing.T) {
	for name, document := range map[string]string{"teamUsers": gen.TEAMWORKGRAPH_TEAMUSERS, "userTeams": gen.TEAMWORKGRAPH_USERTEAMS} {
		parsed, err := gqlparser.ParseQuery(&gqlast.Source{Input: document})
		if err != nil {
			t.Fatal(err)
		}
		data := map[string]any{}
		for _, selection := range parsed.Operations[0].SelectionSet {
			if field, ok := selection.(*gqlast.Field); ok {
				data[responseKey(field)] = synthesize(field.SelectionSet)
			}
		}
		var connection *gen.GraphStoreCypherQueryV2Connection
		if name == "teamUsers" {
			connection, err = gen.DecodeTeamUsers(data)
		} else {
			connection, err = gen.DecodeUserTeams(data)
		}
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var got struct {
			str, integer, float, boolean, timestamp bool
		}
		for _, edge := range connection.Edges {
			for _, column := range edge.Node.Columns {
				value := column.Value
				if value == nil {
					continue
				}
				got.str = got.str || (value.StringObject != nil && value.StringObject.Value == "a-string")
				got.integer = got.integer || (value.IntObject != nil && value.IntObject.Value == 7)
				got.float = got.float || (value.FloatObject != nil && value.FloatObject.Value == 1.5)
				got.boolean = got.boolean || (value.BooleanObject != nil && value.BooleanObject.Value)
				got.timestamp = got.timestamp || (value.TimestampObject != nil && value.TimestampObject.Value == 1700000000)
			}
		}
		if !(got.str && got.integer && got.float && got.boolean && got.timestamp) {
			t.Errorf("%s: a typed value did not decode from its alias: %+v", name, got)
		}
	}
}

// TestPatchRecordsMatchTheVendoredFiles keeps the patch records (third_party/vendor/atlassian/patches) and the
// vendored files in step: every line a patch adds is present in the file it patches, so a re-vendor that drops
// a local change, or a change made without its record, shows here.
func TestPatchRecordsMatchTheVendoredFiles(t *testing.T) {
	root := filepath.Join("..", "..")
	patches, err := filepath.Glob(filepath.Join(root, "third_party", "vendor", "atlassian", "patches", "*.patch"))
	if err != nil || len(patches) == 0 {
		t.Fatalf("no patch record found (%v)", err)
	}
	for _, patch := range patches {
		raw, err := os.ReadFile(patch)
		if err != nil {
			t.Fatal(err)
		}
		var target string
		var contents string
		for _, line := range strings.Split(string(raw), "\n") {
			switch {
			case strings.HasPrefix(line, "+++ b/"):
				target = strings.TrimPrefix(line, "+++ b/")
				body, err := os.ReadFile(filepath.Join(root, target))
				if err != nil {
					t.Fatalf("%s patches %s: %v", filepath.Base(patch), target, err)
				}
				contents = string(body)
			case strings.HasPrefix(line, "+") && !strings.HasPrefix(line, "+++"):
				if !strings.Contains(contents, strings.TrimPrefix(line, "+")) {
					t.Errorf("%s: the line %q is not in %s: the local change is gone or unrecorded", filepath.Base(patch), strings.TrimPrefix(line, "+"), target)
				}
			}
		}
		if target == "" {
			t.Errorf("%s names no file", filepath.Base(patch))
		}
	}
}

// executeCall is one c.Execute(ctx, <document constant>, vars, "<operationName>", ...) call of the vendored
// graph package.
type executeCall struct{ where, document, operationName string }

// vendoredExecuteCalls reads every Execute call of the vendored graph package from its source.
func vendoredExecuteCalls(t *testing.T) []executeCall {
	t.Helper()
	_, file, _, _ := moduleroot.Caller(0)
	dir := filepath.Join(filepath.Dir(file), "..", "..", "third_party", "vendor", "atlassian", "atlassian", "graph")
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	var calls []executeCall
	fset := token.NewFileSet()
	for _, path := range paths {
		parsed, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) < 4 {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Execute" && selector.Sel.Name != "ExecuteWithExtraHeaders" {
				return true
			}
			var document string
			switch arg := call.Args[1].(type) {
			case *ast.SelectorExpr:
				document = arg.Sel.Name
			case *ast.Ident:
				document = arg.Name
			default:
				return true
			}
			literal, ok := call.Args[3].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			name, err := strconv.Unquote(literal.Value)
			if err != nil {
				return true
			}
			calls = append(calls, executeCall{fmt.Sprintf("%s:%d", filepath.Base(path), fset.Position(call.Pos()).Line), document, name})
			return true
		})
	}
	if len(calls) < 8 {
		t.Fatalf("found %d Execute calls: the discovery measures too little", len(calls))
	}
	return calls
}

// CHAOS-7132: the live gateway answered `Unknown operation named 'TeamworkGraph_teamUsers'` because the client
// sent operationName "TeamworkGraph_teamUsers" with a document that declares `query TeamworkGraphTeamUsers`.
// The class: ANY Execute call whose operationName is not a query name declared in the document it sends.
func TestEveryVendoredExecuteCallNamesAnOperationItsDocumentDeclares(t *testing.T) {
	documents := map[string]string{}
	for key, text := range vendoredDocuments(t) {
		documents[key[strings.Index(key, ":")+1:]] = text
	}
	for _, call := range vendoredExecuteCalls(t) {
		text, ok := documents[call.document]
		if !ok {
			// jira_projects.go sends a local variable built from a template constant
			// (gen.BuildJiraProjectsPageQuery / gen.JiraProjectOpsgenieTeamsPageQuery): the weaker check is that SOME
			// vendored document declares the operation, and only for that file and variable.
			if !strings.HasPrefix(call.where, "jira_projects.go:") || call.document != "query" {
				t.Errorf("%s: Execute sends %s, which is not a document constant the test can read", call.where, call.document)
				continue
			}
			declaredAnywhere := false
			for _, other := range documents {
				if strings.Contains(other, "query "+call.operationName+"(") {
					declaredAnywhere = true
				}
			}
			if !declaredAnywhere {
				t.Errorf("%s: operationName %q is declared by no vendored document", call.where, call.operationName)
			}
			continue
		}
		parsed, err := gqlparser.ParseQuery(&gqlast.Source{Input: text})
		if err != nil {
			t.Errorf("%s: %s does not parse: %v", call.where, call.document, err)
			continue
		}
		var declared []string
		for _, operation := range parsed.Operations {
			declared = append(declared, operation.Name)
		}
		found := false
		for _, name := range declared {
			found = found || name == call.operationName
		}
		if !found {
			t.Errorf("%s: operationName %q is not declared by %s (declares %v): the gateway answers Unknown operation", call.where, call.operationName, call.document, declared)
		}
	}
}
