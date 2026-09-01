// CHAOS-4723 CLASS-CLOSING GATE: for every registered document, diff the
// fields its selection set REQUESTS against the fields the Go resolver can
// actually POPULATE. Any field a document selects that this port hardcodes
// to nil (or never assigns at all) FAILS the gate.
//
// # Why this exists
//
// CHAOS-4723's root cause: resolve.go hardcoded
// AnalyticsResult.EvidenceQualityDistribution/EvidenceQualityStats to nil
// UNCONDITIONALLY, while the registered `investmentBreakdown` document
// selects both fields. This is the FOURTH harness-blindness instance of
// this epic (CHAOS-4650, 4657, 4701's two sites) -- the exit run's claim-1
// parity passed 29/29 across these documents and could not have compared
// these two fields, or it would have been a guaranteed red. Certification
// compared the fields the harness KNEW ABOUT, not the fields the
// registered document actually selects.
//
// This gate is the mechanical fix: it derives its answer from the
// registered document TEXT, the published SDL, and the Go resolver/model
// source on every run -- never a hand-maintained list of instances --
// exactly the discipline internal/graph/model/sdl_nullability_gate_test.go
// already established for a sibling defect class (Go-model-vs-SDL
// nullability, not resolver-vs-document field coverage). Two different
// questions, same method: derive both sides from the actual artifacts,
// diff them, ticket what's ticketed, fail on anything new.
//
// # Design
//
// Three things are parsed fresh from source on every run, never
// hand-copied:
//
//  1. The registered documents themselves -- query_route.go's
//     `const registeredXDocument = ...` declarations, discovered by
//     walking that file's AST for every `registered*Document` constant,
//     and the digestByOperation composite literal (also in query_route.go)
//     that maps an operation name to the constant identifier it digests.
//     Reading both from source, rather than hand-typing a
//     operation->constant map here, means a 13th registered document
//     appears in this gate's coverage with no separate edit -- exactly
//     mountedRouteLogMessage's own "the printed set is always exactly the
//     registered set, by construction" property (query_route.go's doc
//     comment), reused for a different purpose.
//  2. Each document's selection set, walked via gqlparser.LoadQuery against
//     the published SDL (contracts/graphql/v1/schema.graphql) -- gqlgen's
//     own parser/validator, the same one sdl_nullability_gate_test.go
//     already depends on. Validation resolves every *ast.Field's
//     ObjectDefinition (the parent GraphQL type) and Definition (the field
//     definition, including its return type), so the walk needs no
//     hand-rolled type-tracking.
//  3. Whether the Go side can populate a given (ParentType, FieldName) pair:
//     - Root Query fields resolve through a `func (r *queryResolver)
//     <Name>(...)` method (schema.resolvers.go) -- gqlgen's Wave-0
//     default resolver body is a loud `panic("not implemented: ...")`;
//     any OTHER body is treated as implemented. This is a POPULATABILITY
//     check, not a correctness check -- it does not attempt to prove the
//     method returns the right data, only that it does not immediately
//     panic.
//     - Every other selected type is a plain, gqlgen-auto-bound Go struct
//     (models_gen.go) -- there are zero non-root custom field resolvers
//     in this codebase today (confirmed: schema.resolvers.go declares
//     queryResolver/mutationResolver/subscriptionResolver methods only,
//     nothing else). For these, populatability is decided by scanning
//     EVERY non-test .go file under cmd/query-api for a composite
//     literal of that struct type and recording, per keyed field,
//     whether any occurrence assigns something other than the bare
//     literal `nil` -- this is the exact investigative method CHAOS-4723
//     itself used ("No site anywhere in cmd/query-api assigns either
//     field a non-nil value -- every assignment site was checked"),
//     mechanized. A field that never appears in ANY composite literal
//     for its type at all is exactly as unpopulated as one hardcoded to
//     nil (nothing ever sets it) and is scored identically.
//
// # Scope limitation (documented, not silent)
//
// The composite-literal oracle only carries a signal for GO-NILABLE field
// types (pointer, slice, map, interface, or this codebase's one nilable
// non-pointer custom scalar, graphqljson.JSON) -- a non-nilable field
// (int, string, bool, a non-pointer nested struct) cannot structurally
// represent "hardcoded nil" the same way, and every hand-found instance of
// this defect class in this epic's history is a nilable-typed field. This
// mirrors sdl_nullability_gate_test.go's own precedent of only checking
// what can actually carry the signal (its object-idiom/JSON-scalar/list
// exclusions) rather than drowning the real findings in structural
// no-ops. checkedFields (logged at the end) must be > 0 or the gate fails
// outright, the same non-negotiable sdl_nullability_gate_test.go enforces.
//
// This gate also only walks plain field selections (no fragment spreads
// or inline fragments) -- none of the twelve registered documents use
// either construct today (confirmed by inspection); encountering one is
// treated as a structural error (fail loudly), not silently skipped, so a
// future document using them cannot slip past unchecked.
package main

import (
	"fmt"
	goast "go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/vektah/gqlparser/v2"
	gqlast "github.com/vektah/gqlparser/v2/ast"
)

// --- source-derived registered-document inventory --------------------------

// documentFieldGateRepoRoot walks up from the current package directory
// looking for contracts/graphql/v1/schema.graphql, the same
// directory-independent search sdl_nullability_gate_test.go's
// repoRootForSDLGate uses (duplicated here, not imported: that helper is
// unexported in a different package, and importing a sibling `main`
// package is not possible in Go regardless).
func documentFieldGateRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		candidate := filepath.Join(dir, "contracts", "graphql", "v1", "schema.graphql")
		if _, statErr := os.Stat(candidate); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not locate repo root (walked up looking for contracts/graphql/v1/schema.graphql "+
				"starting from %s)", dir)
		}
		dir = parent
	}
}

// parseQueryRouteDocuments parses query_route.go's AST and returns:
//   - documentConstants: identifier name -> the raw (backtick) string
//     value of every `const registered<X>Document = ...` declaration.
//   - operationToIdentifier: operation name -> the identifier it digests,
//     read from the digestByOperation composite literal
//     (`"operationName": digestHex(registeredXDocument)`).
//
// Both are derived from source text, never hand-copied -- see this file's
// package doc comment for why that matters.
func parseQueryRouteDocuments(t *testing.T, repoRoot string) (documentConstants map[string]string, operationToIdentifier map[string]string) {
	t.Helper()
	path := filepath.Join(repoRoot, "cmd", "query-api", "query_route.go")
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	documentConstants = map[string]string{}
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*goast.GenDecl)
		if !ok || genDecl.Tok != token.CONST {
			continue
		}
		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*goast.ValueSpec)
			if !ok || len(valueSpec.Names) != 1 || len(valueSpec.Values) != 1 {
				continue
			}
			name := valueSpec.Names[0].Name
			if !strings.HasPrefix(name, "registered") || !strings.HasSuffix(name, "Document") {
				continue
			}
			lit, ok := valueSpec.Values[0].(*goast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				t.Fatalf("%s: %s is not a plain string literal constant -- parseQueryRouteDocuments' extraction assumption broke", path, name)
			}
			text, err := unquoteGoStringLiteral(lit.Value)
			if err != nil {
				t.Fatalf("%s: %s: %v", path, name, err)
			}
			documentConstants[name] = text
		}
	}
	if len(documentConstants) == 0 {
		t.Fatalf("%s: found zero `const registered*Document` declarations -- extraction is broken, not the registry", path)
	}

	operationToIdentifier = map[string]string{}
	goast.Inspect(file, func(n goast.Node) bool {
		assign, ok := n.(*goast.AssignStmt)
		if !ok {
			return true
		}
		for _, rhs := range assign.Rhs {
			composite, ok := rhs.(*goast.CompositeLit)
			if !ok {
				continue
			}
			mapType, ok := composite.Type.(*goast.MapType)
			if !ok {
				continue
			}
			if !isStringIdent(mapType.Key) || !isStringIdent(mapType.Value) {
				continue
			}
			// This IS digestByOperation's `map[string]string{...}`
			// literal (query_route.go has exactly one -- registrydump's
			// own doc comment asserts the same uniqueness).
			for _, elt := range composite.Elts {
				kv, ok := elt.(*goast.KeyValueExpr)
				if !ok {
					t.Fatalf("%s: digestByOperation entry is not a key:value pair: %#v", path, elt)
				}
				keyLit, ok := kv.Key.(*goast.BasicLit)
				if !ok || keyLit.Kind != token.STRING {
					t.Fatalf("%s: digestByOperation key is not a string literal: %#v", path, kv.Key)
				}
				opName, err := unquoteGoStringLiteral(keyLit.Value)
				if err != nil {
					t.Fatalf("%s: digestByOperation key: %v", path, err)
				}
				call, ok := kv.Value.(*goast.CallExpr)
				if !ok {
					t.Fatalf("%s: digestByOperation[%q] value is not a call expression: %#v", path, opName, kv.Value)
				}
				ident, ok := call.Args[0].(*goast.Ident)
				if !ok || len(call.Args) != 1 {
					t.Fatalf("%s: digestByOperation[%q] call does not take exactly one identifier argument: %#v", path, opName, call.Args)
				}
				operationToIdentifier[opName] = ident.Name
			}
		}
		return true
	})
	if len(operationToIdentifier) == 0 {
		t.Fatalf("%s: found zero digestByOperation entries -- extraction is broken, not the registry", path)
	}
	return documentConstants, operationToIdentifier
}

func isStringIdent(e goast.Expr) bool {
	ident, ok := e.(*goast.Ident)
	return ok && ident.Name == "string"
}

// unquoteGoStringLiteral handles both backtick raw-string literals (every
// registered*Document constant) and plain double-quoted ones (every
// digestByOperation key), via strconv.Unquote -- which supports both
// forms directly.
func unquoteGoStringLiteral(lit string) (string, error) {
	return strconv.Unquote(lit)
}

// --- registered-document field diff -----------------------------------

// selectedField is one (ParentType, FieldName) pair a registered document
// asks the Go side to populate, plus which document(s) selected it (for
// reporting).
type selectedField struct {
	typeName  string
	fieldName string
}

func (f selectedField) key() string { return f.typeName + "." + f.fieldName }

// collectSelectedFields walks a validated query document's single
// operation and returns every (ParentType, FieldName) pair it selects,
// at any depth. __typename is skipped (introspection, not a resolver
// question). A fragment spread or inline fragment is a structural error
// (see this file's package doc comment) -- unhandled by design, not by
// oversight.
func collectSelectedFields(t *testing.T, docName string, doc *gqlast.QueryDocument) []selectedField {
	t.Helper()
	if len(doc.Operations) != 1 {
		t.Fatalf("%s: expected exactly one operation, got %d", docName, len(doc.Operations))
	}
	var out []selectedField
	var walk func(set gqlast.SelectionSet)
	walk = func(set gqlast.SelectionSet) {
		for _, sel := range set {
			field, ok := sel.(*gqlast.Field)
			if !ok {
				t.Fatalf("%s: selection is not a plain field (%T) -- this gate does not yet handle fragments; extend it deliberately, don't ignore this", docName, sel)
			}
			if field.Name == "__typename" {
				continue
			}
			if field.ObjectDefinition == nil || field.Definition == nil {
				t.Fatalf("%s: field %q has no resolved definition -- LoadQuery should have validated this document against the schema", docName, field.Name)
			}
			out = append(out, selectedField{typeName: field.ObjectDefinition.Name, fieldName: field.Name})
			if len(field.SelectionSet) > 0 {
				walk(field.SelectionSet)
			}
		}
	}
	walk(doc.Operations[0].SelectionSet)
	return out
}

// --- Go-side: root resolver implementedness ---------------------------

// parseQueryResolverImplemented walks schema.resolvers.go and returns,
// for every `func (r *queryResolver) <Name>(...) (...)`, whether its body
// is anything other than gqlgen's Wave-0 default stub (a bare call to
// panic(...)). Mutation/Subscription resolvers are parsed too (harmless,
// unused today -- every registered document's root field is a Query
// field) so a future document selecting one is covered with no edit here.
func parseQueryResolverImplemented(t *testing.T, repoRoot string) map[string]bool {
	t.Helper()
	path := filepath.Join(repoRoot, "cmd", "query-api", "internal", "graph", "schema.resolvers.go")
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	implemented := map[string]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*goast.FuncDecl)
		if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
			continue
		}
		recvType := exprTypeName(fn.Recv.List[0].Type)
		if recvType != "queryResolver" && recvType != "mutationResolver" && recvType != "subscriptionResolver" {
			continue
		}
		implemented[fn.Name.Name] = !bodyIsUnimplementedStub(fn.Body)
	}
	if len(implemented) == 0 {
		t.Fatalf("%s: found zero resolver methods -- extraction is broken", path)
	}
	return implemented
}

// bodyIsUnimplementedStub reports whether a function body is (or starts
// with) gqlgen's default `panic(fmt.Errorf("not implemented: ..."))`
// stub -- a bare call to panic(...) as the body's own statement, not
// merely referenced somewhere nested (a real resolver that panics on a
// specific internal error path deep inside a helper it calls is NOT this
// stub; only checking the resolver's OWN top-level statement list avoids
// that false positive).
func bodyIsUnimplementedStub(body *goast.BlockStmt) bool {
	if body == nil || len(body.List) == 0 {
		return true
	}
	for _, stmt := range body.List {
		exprStmt, ok := stmt.(*goast.ExprStmt)
		if !ok {
			continue
		}
		call, ok := exprStmt.X.(*goast.CallExpr)
		if !ok {
			continue
		}
		if ident, ok := call.Fun.(*goast.Ident); ok && ident.Name == "panic" {
			return true
		}
	}
	return false
}

func exprTypeName(e goast.Expr) string {
	switch t := e.(type) {
	case *goast.StarExpr:
		return exprTypeName(t.X)
	case *goast.Ident:
		return t.Name
	default:
		return ""
	}
}

// --- Go-side: models_gen.go struct field existence + nilability --------

// modelFieldInfo is what this gate needs to know about one Go struct
// field: its own identifier name (for the composite-literal oracle) and
// whether its declared type is capable of representing "unset" the way a
// hardcoded nil would (see this file's package doc comment's scope
// limitation).
type modelFieldInfo struct {
	goName  string
	nilable bool
}

// nilableCustomScalarTypes names the qualified (package.Type) Go types,
// beyond the structural pointer/slice/map/interface cases, that this
// codebase uses to represent a nullable GraphQL value WITHOUT a pointer
// wrapper -- graphqljson.JSON is the only one
// (internal/graphqljson/json.go's own doc comment: "the zero value...
// marshals as the JSON literal null", the exact property
// sdl_nullability_gate_test.go's JSON-scalar exclusion already documents
// for a different gate).
var nilableCustomScalarTypes = map[string]bool{
	"graphqljson.JSON": true,
}

func isNilableFieldType(expr goast.Expr) bool {
	switch t := expr.(type) {
	case *goast.StarExpr, *goast.ArrayType, *goast.MapType, *goast.InterfaceType:
		return true
	case *goast.SelectorExpr:
		if pkg, ok := t.X.(*goast.Ident); ok {
			return nilableCustomScalarTypes[pkg.Name+"."+t.Sel.Name]
		}
		return false
	default:
		return false
	}
}

// parseModelStructFields walks models_gen.go and returns, for every
// top-level `type X struct { ... }`, its fields keyed by GraphQL field
// name (the json tag -- gqlgen's own convention, the same one
// sdl_nullability_gate_test.go's parseGoModelFields relies on).
func parseModelStructFields(t *testing.T, repoRoot string) map[string]map[string]modelFieldInfo {
	t.Helper()
	path := filepath.Join(repoRoot, "cmd", "query-api", "internal", "graph", "model", "models_gen.go")
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	structs := map[string]map[string]modelFieldInfo{}
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*goast.GenDecl)
		if !ok || genDecl.Tok != token.TYPE {
			continue
		}
		for _, spec := range genDecl.Specs {
			typeSpec, ok := spec.(*goast.TypeSpec)
			if !ok {
				continue
			}
			structType, ok := typeSpec.Type.(*goast.StructType)
			if !ok {
				continue
			}
			fields := map[string]modelFieldInfo{}
			for _, f := range structType.Fields.List {
				if f.Tag == nil || len(f.Names) != 1 {
					continue
				}
				jsonName := jsonTagFieldName(f.Tag.Value)
				if jsonName == "" {
					continue
				}
				fields[jsonName] = modelFieldInfo{goName: f.Names[0].Name, nilable: isNilableFieldType(f.Type)}
			}
			structs[typeSpec.Name.Name] = fields
		}
	}
	if len(structs) == 0 {
		t.Fatalf("%s: found zero struct declarations -- extraction is broken", path)
	}
	return structs
}

// jsonTagFieldName extracts the bare field name from a `json:"name,omitempty"`
// struct tag literal (the raw, still-quoted Go source token).
func jsonTagFieldName(rawTag string) string {
	unquoted, err := strconv.Unquote(rawTag)
	if err != nil {
		return ""
	}
	const prefix = `json:"`
	idx := strings.Index(unquoted, prefix)
	if idx < 0 {
		return ""
	}
	rest := unquoted[idx+len(prefix):]
	end := strings.IndexAny(rest, `",`)
	if end < 0 {
		return ""
	}
	name := rest[:end]
	if name == "-" {
		return ""
	}
	return name
}

// --- Go-side: composite-literal populatability oracle -------------------

// populatabilityOracle records, per (TypeName, GoFieldName), whether AT
// LEast ONE composite-literal site outside a _test.go file assigns
// something other than the bare literal `nil` -- the mechanized version
// of CHAOS-4723's own investigative method. instances counts every site
// found (nil or not), so a type with zero recorded instances at all can
// be told apart from one that was checked and found only-nil.
type populatabilityOracle struct {
	populated map[string]bool // "TypeName.FieldName" -> ever non-nil
	instances map[string]int  // "TypeName.FieldName" -> total sites seen
}

func (o *populatabilityOracle) isPopulatable(typeName, fieldName string) bool {
	return o.populated[typeName+"."+fieldName]
}

func (o *populatabilityOracle) instanceCount(typeName, fieldName string) int {
	return o.instances[typeName+"."+fieldName]
}

// buildPopulatabilityOracle walks every non-test .go file under
// root/cmd/query-api (excluding models_gen.go/generated.go, which only
// DECLARE these types, never construct them) looking for composite
// literals `model.<TypeName>{...}` -- gqlgen response types are always
// referenced through the imported `model` package alias in this
// codebase (confirmed: every Compile*/Execute*/Resolve* construction
// site in cmd/query-api/internal/analytics does this) -- and records,
// per keyed field, whether its value expression is the bare identifier
// `nil`.
func buildPopulatabilityOracle(t *testing.T, repoRoot string, typeNames map[string]bool) *populatabilityOracle {
	t.Helper()
	oracle := &populatabilityOracle{populated: map[string]bool{}, instances: map[string]int{}}
	root := filepath.Join(repoRoot, "cmd", "query-api")
	fset := token.NewFileSet()
	filesWalked := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		base := filepath.Base(path)
		if !strings.HasSuffix(base, ".go") || strings.HasSuffix(base, "_test.go") {
			return nil
		}
		if base == "models_gen.go" || base == "generated.go" {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", path, parseErr)
		}
		filesWalked++
		// Every top-level func AND every func literal gets its OWN
		// scanFunctionBody call with a FRESH varTypes scope -- see that
		// function's doc comment for why a composite-literal-only scan
		// undercounts real population sites in this codebase.
		goast.Inspect(file, func(n goast.Node) bool {
			switch fn := n.(type) {
			case *goast.FuncDecl:
				if fn.Body != nil {
					scanFunctionBody(fn.Body, oracle, typeNames)
				}
				return false
			case *goast.FuncLit:
				scanFunctionBody(fn.Body, oracle, typeNames)
				return false
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if filesWalked == 0 {
		t.Fatalf("walked zero .go files under %s -- extraction is broken", root)
	}
	return oracle
}

// scanFunctionBody is buildPopulatabilityOracle's per-function pass. A
// composite-literal-only scan (recording only `model.T{Field: value}`
// keyed elements) undercounts real population sites in THIS codebase:
// eight fields across five different packages (ReviewEdgeRow.RepoID,
// WorkGraphEdgeResult.RepoID/Provider, WorkGraphArtifactRow.Evidence,
// PageInfo.StartCursor/EndCursor, HotspotRow.EvidenceURL,
// FeatureFlagItem.ArchivedAt) are all constructed with the OPTIONAL
// pointer field left out of the literal and set immediately after, in
// the same function, e.g. (workgraph/membership.go):
//
//	result := model.WorkGraphEdgeResult{EdgeID: ..., SourceType: ..., ...}
//	if row.repoID != "" {
//	    id := row.repoID
//	    result.RepoID = &id
//	}
//	return result, nil
//
// A composite-literal-only oracle scores RepoID as "never assigned" --
// exactly indistinguishable from resolve.go's actual CHAOS-4723 bug
// (`EvidenceQualityDistribution: nil` inside the literal) without this
// pass, which is precisely the false-positive class a mechanical gate
// must not produce (a gate people learn to ignore is worse than no
// gate). This pass tracks, per function scope, which local identifiers
// were just assigned a tracked `model.T{...}` (or `&model.T{...}`)
// composite literal via `:=`, then treats a later `ident.Field = value`
// assignment on that SAME identifier, within the SAME function, as an
// equally-valid population site. A nested func literal gets its OWN
// fresh scope (closures capturing an outer variable by this exact
// pattern do not occur in this codebase today; treated as a documented
// scope limitation, not silently assumed away -- a real instance would
// surface as a normal "never assigned" gate finding, loud, not silent).
func scanFunctionBody(body *goast.BlockStmt, oracle *populatabilityOracle, typeNames map[string]bool) {
	varTypes := map[string]string{}
	goast.Inspect(body, func(n goast.Node) bool {
		switch node := n.(type) {
		case *goast.FuncLit:
			scanFunctionBody(node.Body, oracle, typeNames)
			return false
		case *goast.CompositeLit:
			if typeName, ok := compositeLitModelType(node); ok && typeNames[typeName] {
				for _, elt := range node.Elts {
					kv, ok := elt.(*goast.KeyValueExpr)
					if !ok {
						continue // positional literal -- none of these types are constructed positionally in this codebase.
					}
					fieldIdent, ok := kv.Key.(*goast.Ident)
					if !ok {
						continue
					}
					recordFieldSite(oracle, typeName, fieldIdent.Name, kv.Value)
				}
			}
		case *goast.AssignStmt:
			recordVarTypeFromDefine(node, varTypes, typeNames)
			recordPostConstructionFieldAssignments(node, varTypes, oracle)
		}
		return true
	})
}

// compositeLitModelType returns the bare type name of a `model.<Name>`
// composite literal's type expression (never a pointer -- `&model.Foo{}`
// wraps the *ast.CompositeLit in a *ast.UnaryExpr one level up; the
// CompositeLit node itself always has Type == the plain `model.Foo`
// selector regardless).
func compositeLitModelType(node *goast.CompositeLit) (string, bool) {
	sel, ok := node.Type.(*goast.SelectorExpr)
	if !ok {
		return "", false
	}
	return sel.Sel.Name, true
}

// recordVarTypeFromDefine records `v := model.T{...}` / `v := &model.T{...}`
// (single-value `:=` only -- this codebase's construction sites are
// never part of a multi-value `:=`) into varTypes, so a later
// `v.Field = ...` in the same function can be attributed to type T.
func recordVarTypeFromDefine(assign *goast.AssignStmt, varTypes map[string]string, typeNames map[string]bool) {
	if assign.Tok != token.DEFINE || len(assign.Lhs) != len(assign.Rhs) {
		return
	}
	for i, lhs := range assign.Lhs {
		ident, ok := lhs.(*goast.Ident)
		if !ok || ident.Name == "_" {
			continue
		}
		rhs := assign.Rhs[i]
		if unary, ok := rhs.(*goast.UnaryExpr); ok && unary.Op == token.AND {
			rhs = unary.X
		}
		composite, ok := rhs.(*goast.CompositeLit)
		if !ok {
			continue
		}
		typeName, ok := compositeLitModelType(composite)
		if ok && typeNames[typeName] {
			varTypes[ident.Name] = typeName
		}
	}
}

// recordPostConstructionFieldAssignments handles `v.Field = value` for
// every `v` already known (this function scope, this run) to hold a
// tracked model type.
func recordPostConstructionFieldAssignments(assign *goast.AssignStmt, varTypes map[string]string, oracle *populatabilityOracle) {
	if len(assign.Lhs) != len(assign.Rhs) {
		return
	}
	for i, lhs := range assign.Lhs {
		sel, ok := lhs.(*goast.SelectorExpr)
		if !ok {
			continue
		}
		ident, ok := sel.X.(*goast.Ident)
		if !ok {
			continue
		}
		typeName, known := varTypes[ident.Name]
		if !known {
			continue
		}
		recordFieldSite(oracle, typeName, sel.Sel.Name, assign.Rhs[i])
	}
}

// recordFieldSite is the one place instance/populated bookkeeping
// happens, shared by the composite-literal path and the
// post-construction-assignment path so both are scored identically.
func recordFieldSite(oracle *populatabilityOracle, typeName, fieldName string, value goast.Expr) {
	key := typeName + "." + fieldName
	oracle.instances[key]++
	valueIdent, isIdent := value.(*goast.Ident)
	isNilLiteral := isIdent && valueIdent.Name == "nil"
	if !isNilLiteral {
		oracle.populated[key] = true
	}
}

// --- the gate itself -----------------------------------------------------

// expectedUnpopulatedFields is this gate's ticketed-exclusion ledger,
// same discipline as sdl_nullability_gate_test.go's expectedDivergences:
// an entry needs a written reason and must actually match a real
// violation found this run (checked below), or it is stale and fails the
// gate itself.
//
// EMPTY, and that is the point. Its one entry was
// "SankeyResult.coverage", the pre-existing gap resolve.go documented as
// "NOT YET PORTED -- always nil". Coverage is now computed
// (internal/analytics/sankeycoverage.go), so the exception was removed
// with the fix rather than left behind -- a stale entry matching nothing
// fails this gate by design, which is what forces the two to move
// together. Adding a new entry here means shipping a registered document
// with a field the resolver cannot populate; that needs a ticket and a
// written reason, not a quiet append.
var expectedUnpopulatedFields = map[string]string{}

// TestRegisteredDocumentFieldsArePopulatable is CHAOS-4723's
// class-closing gate. See this file's package doc comment for the full
// design and why a naive per-instance fix is not enough.
func TestRegisteredDocumentFieldsArePopulatable(t *testing.T) {
	repoRoot := documentFieldGateRepoRoot(t)

	sdlPath := filepath.Join(repoRoot, "contracts", "graphql", "v1", "schema.graphql")
	sdlBytes, err := os.ReadFile(sdlPath)
	if err != nil {
		t.Fatalf("read SDL pin %s: %v", sdlPath, err)
	}
	schema, gqlErr := gqlparser.LoadSchema(&gqlast.Source{Name: "schema.graphql", Input: string(sdlBytes)})
	if gqlErr != nil {
		t.Fatalf("parse SDL pin %s: %v", sdlPath, gqlErr)
	}

	documentConstants, operationToIdentifier := parseQueryRouteDocuments(t, repoRoot)
	resolverImplemented := parseQueryResolverImplemented(t, repoRoot)
	structFields := parseModelStructFields(t, repoRoot)

	// Collect every selected field across every registered document
	// FIRST, so the populatability oracle only needs to walk the source
	// tree once for exactly the type names actually in play.
	type documentSelection struct {
		operation string
		fields    []selectedField
	}
	var selections []documentSelection
	typeNamesInPlay := map[string]bool{}
	operations := sortedKeys(operationToIdentifier)
	for _, operation := range operations {
		identifier := operationToIdentifier[operation]
		text, ok := documentConstants[identifier]
		if !ok {
			t.Fatalf("digestByOperation[%q] references identifier %q, but no `const %s = ...` document was found", operation, identifier, identifier)
		}
		doc, gqlErr := gqlparser.LoadQuery(schema, text)
		if gqlErr != nil {
			t.Fatalf("operation %q (document %s): failed to validate against the published SDL: %v", operation, identifier, gqlErr)
		}
		fields := collectSelectedFields(t, operation, doc)
		selections = append(selections, documentSelection{operation: operation, fields: fields})
		for _, f := range fields {
			typeNamesInPlay[f.typeName] = true
		}
	}

	oracle := buildPopulatabilityOracle(t, repoRoot, typeNamesInPlay)

	type violation struct {
		operation string
		field     selectedField
		reason    string
	}
	var violations []violation
	checkedFields := 0
	skippedNonNilable := 0
	skippedQueryRoot := 0

	for _, sel := range selections {
		for _, f := range sel.fields {
			if f.typeName == "Query" || f.typeName == "Mutation" || f.typeName == "Subscription" {
				skippedQueryRoot++
				resolverName := strings.ToUpper(f.fieldName[:1]) + f.fieldName[1:]
				implemented, known := resolverImplemented[resolverName]
				if !known {
					t.Fatalf("operation %q selects root field %q, but no queryResolver/mutationResolver/subscriptionResolver method %q was found in schema.resolvers.go", sel.operation, f.fieldName, resolverName)
				}
				checkedFields++
				if !implemented {
					violations = append(violations, violation{
						operation: sel.operation, field: f,
						reason: fmt.Sprintf("resolver method %q is still gqlgen's unimplemented-stub panic", resolverName),
					})
				}
				continue
			}

			fields, knownType := structFields[f.typeName]
			if !knownType {
				t.Fatalf("operation %q selects a field on type %q, but models_gen.go declares no such struct -- SDL/Go drift, not this gate's normal finding", sel.operation, f.typeName)
			}
			info, knownField := fields[f.fieldName]
			if !knownField {
				t.Fatalf("operation %q selects %s.%s, but models_gen.go's %s struct has no field with that json tag -- SDL/Go drift", sel.operation, f.typeName, f.fieldName, f.typeName)
			}
			if !info.nilable {
				skippedNonNilable++
				continue
			}
			checkedFields++
			if oracle.instanceCount(f.typeName, info.goName) == 0 {
				violations = append(violations, violation{
					operation: sel.operation, field: f,
					reason: fmt.Sprintf("model.%s.%s (json %q) is never assigned in any composite literal anywhere under cmd/query-api -- nothing populates it", f.typeName, info.goName, f.fieldName),
				})
				continue
			}
			if !oracle.isPopulatable(f.typeName, info.goName) {
				violations = append(violations, violation{
					operation: sel.operation, field: f,
					reason: fmt.Sprintf("model.%s.%s (json %q) is hardcoded to the literal nil at every composite-literal site found (%d site(s))", f.typeName, info.goName, f.fieldName, oracle.instanceCount(f.typeName, info.goName)),
				})
			}
		}
	}

	if checkedFields == 0 {
		t.Fatal("0 fields were actually checked this run -- stopping before the ledger logic below, which would otherwise report a false all-clear")
	}

	usedLedgerEntries := map[string]bool{}
	var unexpected []violation
	for _, v := range violations {
		if reason, known := expectedUnpopulatedFields[v.field.key()]; known {
			usedLedgerEntries[v.field.key()] = true
			t.Logf("EXPECTED (ticketed) unpopulated field -- gate is working, not silencing: operation=%s field=%s (%s)\n  reason: %s",
				v.operation, v.field.key(), v.reason, reason)
			continue
		}
		unexpected = append(unexpected, v)
	}
	for key, reason := range expectedUnpopulatedFields {
		if !usedLedgerEntries[key] {
			t.Errorf("expected-unpopulated ledger entry %q never matched an actual violation this run -- stale entry (the field was probably fixed); delete it. reason on file: %s", key, reason)
		}
	}

	t.Logf("CHAOS-4723 gate: %d registered document(s), %d field-selection(s) checked, %d root-field selection(s), %d non-nilable field(s) skipped (cannot carry the hardcoded-nil signal), %d ticketed/expected violation(s), %d unexpected violation(s)",
		len(selections), checkedFields, skippedQueryRoot, skippedNonNilable, len(usedLedgerEntries), len(unexpected))

	if len(unexpected) > 0 {
		var b strings.Builder
		fmt.Fprintf(&b, "%d unticketed registered-document field(s) selected but not populatable:\n", len(unexpected))
		for _, v := range unexpected {
			fmt.Fprintf(&b, "  operation=%s field=%s: %s\n", v.operation, v.field.key(), v.reason)
		}
		b.WriteString("Each is either a real bug (port the field, same commit) or a new, deliberately-deferred instance " +
			"that needs its own ticket and an expectedUnpopulatedFields entry in this file naming that ticket.")
		t.Fatal(b.String())
	}
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
