// CHAOS-4702: a schema-diff gate that detects Go-model-type-vs-published-SDL
// nullability divergence.
//
// # Why this exists
//
// tests/api/graphql/test_schema_sdl_pinned.py (Python) pins Strawberry's live
// schema export against contracts/graphql/v1/schema.graphql byte-for-byte.
// It NEVER reads Go code -- it is structurally incapable of noticing that a
// Go model type disagrees with the published SDL about a field's
// nullability. Proven, not argued: lane-4657 ran that test in a state where
// BOTH BreakdownItem.value and TimeseriesBucket.value were Go-nullable
// (*float64) while the SDL still said `Float!` for both -- two live
// contract violations -- and it PASSED 2/2.
//
// That class of bug is dangerous specifically because it is invisible to
// code review (each side is locally correct in isolation) and arms via a
// DATA change with no code change: the moment a go_api_routing_state row
// enables the operation, a client reading the published SDL believes a
// field is non-null and receives JSON null instead.
//
// Four hand-found instances of this class exist in this epic's history
// (CHAOS-4650, 4657, 4701's two sites, and 4658's confirmed one). This test
// is the detection strategy those four are not: it derives its answer from
// the SDL and the generated Go type on every run, not from a lane happening
// to have the file open.
//
// # Design: why a naive field-by-field diff does not work
//
// A naive diff of every SDL field's nullability against its Go struct
// field's pointer-ness returns 9 mismatches at the time this gate was
// written -- 8 are NOT this defect class:
//
//   - 8 are the `JSON` custom scalar (parameterOverrides, reportPlan x3,
//     parameters x3, provenanceRecords). It is nullable in the SDL and its
//     bound Go type (internal/graphqljson.JSON, `type JSON json.RawMessage`)
//     is never pointer-wrapped, because that type's own zero value (nil)
//     already marshals as GraphQL null -- see that package's doc comment.
//     gqlgen does not need a pointer to represent "no value" for a type
//     that natively has one.
//   - 1 is TimeseriesBucket.value: Go `*float64`, SDL `Float!`. This is
//     CHAOS-4703, still open, the standing proof this gate detects the
//     class (see expectedDivergences below).
//
// Separately, object/input-object-typed fields (e.g. `aiSide:
// AIComparisonSide!` -> Go `*AIComparisonSide`) are gqlgen's own
// nested-object codegen idiom: EVERY such field is a Go pointer regardless
// of SDL nullability, 100% uniformly across this schema (verified by this
// gate itself at runtime -- see the objectIdiomExcluded assertion). That is
// not a nullability signal at all and reporting it would drown the one real
// finding in ~230 look-alikes.
//
// List-typed fields (`[Foo!]!`) are excluded at every level (outer list AND
// element) because gqlgen.yml sets `omit_slice_element_pointers: true` for
// this repo -- a deliberate, repo-wide config choice, not something this
// defect class can manifest through: Go slices are natively nil-able, so
// "the list is null" is carried by a nil slice, never a missing pointer
// wrapper on a scalar leaf.
//
// # Vocabulary reuse
//
// internal/testsupport/oraclecompare is this repo's one row-comparison
// vocabulary (Python<->Go), and its two disciplines are reused here even
// though this gate compares SDL<->Go, not Python<->Go:
//   - An exclusion needs a written reason AND must actually match something
//     in the data, or it is dead weight that could silently swallow a real
//     finding (oraclecompare.CheckExclusionIntegrity). Each exclusion
//     category below asserts it matched >0 fields.
//   - A comparison where nothing was actually compared is a configuration
//     error, not a pass (oraclecompare.DiffRows). This gate asserts
//     checkedFields > 0.
//
// # Field completeness
//
// Both sides are derived from the actual generated artifacts on every run,
// never a hand-maintained list of type/field names:
//   - The SDL side is parsed with the real GraphQL parser this repo already
//     depends on (github.com/vektah/gqlparser/v2 -- gqlgen's own parser),
//     not a hand-rolled regex.
//   - The Go side is parsed from models_gen.go's own AST (go/parser over the
//     DO-NOT-EDIT generated file), exhaustively over every struct
//     declaration and field -- adding a field to the schema regenerates
//     models_gen.go, which this gate then re-parses on the next run. There
//     is no separate list to fall behind.
package model

import (
	"fmt"
	goast "go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/vektah/gqlparser/v2"
	gqlast "github.com/vektah/gqlparser/v2/ast"
)

// --- Go-side parsing -------------------------------------------------------

// goFieldInfo is what this gate needs to know about one Go struct field:
// whether its declared type is a pointer at the top level, and whether it
// is a list (slice/array) -- both derived from the field's AST type
// expression, never asserted by name.
type goFieldInfo struct {
	IsPointer bool
	IsList    bool
	GoType    string // rendered source text, for diagnostics only
}

// parseGoModelFields walks EVERY top-level `type X struct { ... }`
// declaration in the given Go source file and returns, for each struct,
// its fields keyed by their `json:"..."` tag name (the same name gqlgen
// uses as the GraphQL field name, and the same convention
// oraclecompare.jsonFieldName uses). Fields without a json tag (there are
// none in a gqlgen-generated model file, but fail loudly rather than
// silently skip if one ever appears) are reported as a structural error by
// the caller, not silently dropped.
func parseGoModelFields(t *testing.T, path string) (map[string]map[string]goFieldInfo, []string) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	structs := map[string]map[string]goFieldInfo{}
	var structuralErrors []string

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
				continue // enum types (`type X string`) and others: no fields to compare here.
			}
			fields := map[string]goFieldInfo{}
			for _, f := range structType.Fields.List {
				if f.Tag == nil {
					structuralErrors = append(structuralErrors, fmt.Sprintf(
						"%s: struct field with no tag at all (names=%v) -- gqlgen always emits a json "+
							"tag; this gate's assumption about the generated file's shape is stale",
						typeSpec.Name.Name, f.Names))
					continue
				}
				tagVal, unquoteErr := strconv.Unquote(f.Tag.Value)
				if unquoteErr != nil {
					structuralErrors = append(structuralErrors, fmt.Sprintf(
						"%s: could not unquote struct tag %q: %v", typeSpec.Name.Name, f.Tag.Value, unquoteErr))
					continue
				}
				jsonTag := reflect.StructTag(tagVal).Get("json")
				if jsonTag == "" || jsonTag == "-" {
					continue // not part of the wire shape.
				}
				jsonName, _, _ := strings.Cut(jsonTag, ",")
				if jsonName == "" {
					continue
				}

				expr := f.Type
				isPointer := false
				if star, ok := expr.(*goast.StarExpr); ok {
					isPointer = true
					expr = star.X
				}
				isList := false
				if _, ok := expr.(*goast.ArrayType); ok {
					isList = true
				}

				fields[jsonName] = goFieldInfo{
					IsPointer: isPointer,
					IsList:    isList,
					GoType:    exprString(f.Type),
				}
			}
			structs[typeSpec.Name.Name] = fields
		}
	}
	return structs, structuralErrors
}

// exprString renders a go/ast type expression back to source text for
// diagnostics (e.g. "*float64", "[]BreakdownItem"). Only the handful of
// shapes this generated file actually uses need to render cleanly; anything
// else falls back to a %T so a gap here is visible instead of silently
// wrong.
func exprString(expr goast.Expr) string {
	switch e := expr.(type) {
	case *goast.StarExpr:
		return "*" + exprString(e.X)
	case *goast.ArrayType:
		return "[]" + exprString(e.Elt)
	case *goast.Ident:
		return e.Name
	case *goast.SelectorExpr:
		return exprString(e.X) + "." + e.Sel.Name
	case *goast.MapType:
		return "map[" + exprString(e.Key) + "]" + exprString(e.Value)
	default:
		return fmt.Sprintf("<%T>", expr)
	}
}

// --- comparison --------------------------------------------------------

// Divergence is one field where the Go model's pointer-ness disagrees with
// the published SDL's nullability, for a field this gate actually compares
// (i.e. survived every exclusion).
type Divergence struct {
	TypeName  string
	FieldName string
	SDLType   string // e.g. "Float!"
	GoType    string // e.g. "*float64"
	Direction string // "sdl-non-null-but-go-pointer" | "sdl-nullable-but-go-non-pointer"
}

// Key identifies a divergence for matching against the expected-divergence
// ledger below.
func (d Divergence) Key() string { return d.TypeName + "." + d.FieldName }

func (d Divergence) String() string {
	return fmt.Sprintf("%s: SDL says %s, Go is %s [%s]", d.Key(), d.SDLType, d.GoType, d.Direction)
}

// exclusion reasons -- each MUST match at least one field in the real
// schema (asserted in TestGoModelNullabilityMatchesPublishedSDL) or it is
// dead weight per oraclecompare's CheckExclusionIntegrity discipline.
const (
	objectIdiomReason = "gqlgen represents every object/input-object/interface/union-typed field as a Go " +
		"pointer regardless of the field's SDL nullability (verified 100% uniform across this schema by " +
		"this gate itself) -- that is gqlgen's nested-object codegen idiom, not a nullability signal, " +
		"CHAOS-4702"
	jsonScalarReason = "the `JSON` custom scalar is bound (gqlgen.yml) to internal/graphqljson.JSON, " +
		"`type JSON json.RawMessage` -- a slice-backed type whose own zero value (nil) already marshals " +
		"as GraphQL null (see that package's doc comment), so gqlgen never pointer-wraps it regardless of " +
		"SDL nullability, CHAOS-4702"
	listReason = "gqlgen.yml sets `omit_slice_element_pointers: true`, and Go slices are natively " +
		"nil-able, so list-typed fields (outer list AND element) never signal SDL nullability through a Go " +
		"pointer at any level -- excluded entirely, not just at the outer wrapper, CHAOS-4702"
)

// compareStats counts how many fields fell into each disposition, so the
// exclusion-integrity assertions (and the human-readable report) can prove
// every declared exclusion rule actually fired instead of being vacuous.
type compareStats struct {
	CheckedFields       int
	ObjectIdiomExcluded int
	JSONScalarExcluded  int
	ListExcluded        int
}

// compareNullability is the pure comparison core: given a parsed GraphQL
// schema and the Go model's fields, return every scalar/enum-field
// nullability divergence, the exclusion-rule hit counts, and any
// structural problems (e.g. an SDL type with no matching Go struct) that
// mean the comparison itself could not run cleanly for some type/field.
//
// It is pure and takes no files, deliberately, so it can be exercised with
// small synthetic schemas in TestCompareNullabilityCatchesBothDirections
// without touching the real 2500-line SDL or 3700-line generated file --
// the same reason oraclecompare.DiffRows is pure and unit-tested with
// synthetic data via TestDiffRowsClauseCoverage.
func compareNullability(
	schema *gqlast.Schema,
	goStructs map[string]map[string]goFieldInfo,
) (violations []Divergence, stats compareStats, structuralErrors []string) {
	rootNames := map[string]bool{}
	for _, root := range []*gqlast.Definition{schema.Query, schema.Mutation, schema.Subscription} {
		if root != nil {
			rootNames[root.Name] = true
		}
	}

	typeNames := make([]string, 0, len(schema.Types))
	for name := range schema.Types {
		typeNames = append(typeNames, name)
	}
	sort.Strings(typeNames)

	for _, typeName := range typeNames {
		def := schema.Types[typeName]
		if def == nil || def.BuiltIn || rootNames[typeName] {
			continue
		}
		// Only OBJECT and INPUT_OBJECT definitions carry the struct-shaped
		// FieldList a Go struct can be compared against; SCALAR/ENUM
		// declarations here are base-type definitions referenced BY other
		// fields, not struct types to walk fields of themselves.
		if def.Kind != gqlast.Object && def.Kind != gqlast.InputObject {
			continue
		}

		goFields, ok := goStructs[typeName]
		if !ok {
			structuralErrors = append(structuralErrors, fmt.Sprintf(
				"SDL type %q has no corresponding Go struct in models_gen.go -- either the "+
					"SDL-type-name-equals-Go-struct-name convention broke, or this gate's Go-side "+
					"parser is out of date", typeName))
			continue
		}

		for _, field := range def.Fields {
			if len(field.Arguments) > 0 {
				continue // a resolver-style field (only Query/Mutation have these; excluded above anyway).
			}

			fieldType := field.Type
			isListField := fieldType.NamedType == "" // outer type has no NamedType, only Elem.

			goField, ok := goFields[field.Name]
			if !ok {
				structuralErrors = append(structuralErrors, fmt.Sprintf(
					"SDL field %s.%s has no corresponding Go struct field (matched by json tag) in "+
						"models_gen.go", typeName, field.Name))
				continue
			}

			// Shape agreement MUST be checked in BOTH directions before either
			// side trusts the other -- codex review round 1 (2026-08-31) found
			// that checking this lookup only after excluding SDL-list fields
			// meant a field that turned into a list in the SDL while the Go
			// model stayed a bare scalar (e.g. `*float64`) would silently be
			// counted as ListExcluded and never compared or flagged, the
			// mirror image of the check that already existed for "SDL says
			// not-a-list but Go is a slice".
			if isListField != goField.IsList {
				var got string
				if goField.IsList {
					got = fmt.Sprintf("Go field IS a slice/array (%s)", goField.GoType)
				} else {
					got = fmt.Sprintf("Go field is NOT a slice/array (%s)", goField.GoType)
				}
				var want string
				if isListField {
					want = fmt.Sprintf("SDL says it IS a list type (%s)", fieldType.String())
				} else {
					want = fmt.Sprintf("SDL says it is NOT a list type (%s)", fieldType.String())
				}
				structuralErrors = append(structuralErrors, fmt.Sprintf(
					"SDL field %s.%s: %s but its %s -- shape mismatch this gate cannot safely compare "+
						"pointer-ness for", typeName, field.Name, want, got))
				continue
			}

			if isListField {
				stats.ListExcluded++
				continue
			}

			baseDef, ok := schema.Types[fieldType.NamedType]
			if !ok || baseDef == nil {
				structuralErrors = append(structuralErrors, fmt.Sprintf(
					"SDL field %s.%s references unknown type %q", typeName, field.Name, fieldType.NamedType))
				continue
			}

			switch baseDef.Kind {
			case gqlast.Object, gqlast.InputObject, gqlast.Interface, gqlast.Union:
				stats.ObjectIdiomExcluded++
				continue
			}
			if fieldType.NamedType == "JSON" {
				stats.JSONScalarExcluded++
				continue
			}

			stats.CheckedFields++
			expectPointer := !fieldType.NonNull
			if goField.IsPointer != expectPointer {
				direction := "sdl-non-null-but-go-pointer"
				if expectPointer {
					direction = "sdl-nullable-but-go-non-pointer"
				}
				violations = append(violations, Divergence{
					TypeName:  typeName,
					FieldName: field.Name,
					SDLType:   fieldType.String(),
					GoType:    goField.GoType,
					Direction: direction,
				})
			}
		}
	}

	sort.Slice(violations, func(i, j int) bool { return violations[i].Key() < violations[j].Key() })
	sort.Strings(structuralErrors)
	return violations, stats, structuralErrors
}

// --- expected-divergence ledger -----------------------------------------

// expectedDivergences is this gate's exemption list for KNOWN, TICKETED,
// deliberately-deferred divergences -- the same discipline this epic
// already uses elsewhere (e.g. CHAOS-4657's "add an expected-divergence
// ledger entry naming this path"). An entry here does not silence the
// finding: it is still detected and logged every run (see
// TestGoModelNullabilityMatchesPublishedSDL), it just does not fail CI for
// an issue that already has its own ticket, owner, and reasoning.
//
// Every entry here is checked for staleness the same way an oraclecompare
// exclusion is: if it stops matching an actual violation (because the
// field was fixed), the gate FAILS, loudly, telling you to remove the
// entry -- so this ledger cannot quietly outlive the bug it documents.
var expectedDivergences = map[string]string{
	"TimeseriesBucket.value": "CHAOS-4703 (open, intentionally not fixed by CHAOS-4702's lane): Go is " +
		"*float64, SDL still says Float!. Safe today because TimeseriesBucket is not reachable from any " +
		"registered document (verified against origin/main:cmd/query-api/query_route.go's " +
		"digestByOperation -- neither registeredInvestmentBreakdownDocument nor " +
		"registeredInvestmentFullDocument selects timeseries.buckets.value). This is CHAOS-4702's " +
		"acceptance evidence: this gate finding it IS the proof the gate detects the class. Remove this " +
		"entry in the same PR that closes CHAOS-4703 (SDL widened to `value: Float`, Go regenerated + " +
		"generated.go's stale Invalids++ check corrected) -- once fixed, this entry will stop matching " +
		"anything and the gate will fail until it is deleted.",
}

// exclusionIntegrityFailures applies oraclecompare's
// CheckExclusionIntegrity discipline to this gate's OWN exclusion rules:
// each declared rule must have matched at least one field this run, and at
// least one field must have survived every exclusion to actually be
// compared -- otherwise the run proved nothing. Pure and separately unit
// tested (TestExclusionIntegrityFailures) for the same reason
// oraclecompare's own version is: it must be checkable with synthetic
// counts, not only by re-deriving a real 1000-field run.
func exclusionIntegrityFailures(stats compareStats) []string {
	var failures []string
	if stats.ObjectIdiomExcluded == 0 {
		failures = append(failures, "exclusion rule never matched a field: "+objectIdiomReason)
	}
	if stats.JSONScalarExcluded == 0 {
		failures = append(failures, "exclusion rule never matched a field: "+jsonScalarReason)
	}
	if stats.ListExcluded == 0 {
		failures = append(failures, "exclusion rule never matched a field: "+listReason)
	}
	if stats.CheckedFields == 0 {
		failures = append(failures, "0 scalar/enum fields were actually compared -- every field fell "+
			"into an exclusion, which is a gate configuration error (an over-broad exclusion), not a pass")
	}
	return failures
}

// --- the gate itself ------------------------------------------------------

func repoRootForSDLGate(t *testing.T) string {
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
				"starting from %s) -- this gate's directory-independent root search is broken", dir)
		}
		dir = parent
	}
}

// TestGoModelNullabilityMatchesPublishedSDL is CHAOS-4702's gate: it is the
// thing test_schema_sdl_pinned.py cannot be, because that test never reads
// Go code. See the package doc comment at the top of this file for the
// full design.
func TestGoModelNullabilityMatchesPublishedSDL(t *testing.T) {
	repoRoot := repoRootForSDLGate(t)

	sdlPath := filepath.Join(repoRoot, "contracts", "graphql", "v1", "schema.graphql")
	sdlBytes, err := os.ReadFile(sdlPath)
	if err != nil {
		t.Fatalf("read SDL pin %s: %v", sdlPath, err)
	}
	schema, gqlErr := gqlparser.LoadSchema(&gqlast.Source{Name: "schema.graphql", Input: string(sdlBytes)})
	if gqlErr != nil {
		t.Fatalf("parse SDL pin %s: %v", sdlPath, gqlErr)
	}

	modelPath := filepath.Join(repoRoot, "cmd", "query-api", "internal", "graph", "model", "models_gen.go")
	goStructs, parseErrs := parseGoModelFields(t, modelPath)
	for _, e := range parseErrs {
		t.Errorf("models_gen.go parse issue: %s", e)
	}

	violations, stats, structuralErrors := compareNullability(schema, goStructs)
	for _, e := range structuralErrors {
		t.Errorf("structural mismatch between SDL and Go model: %s", e)
	}

	// Exclusion-integrity: a rule that never matched anything is stale and
	// could be hiding a real finding (oraclecompare.CheckExclusionIntegrity
	// convention). If any of these ever drop to 0, something about the
	// schema or gqlgen.yml changed underneath this gate's assumptions.
	for _, failure := range exclusionIntegrityFailures(stats) {
		t.Error(failure)
	}
	if stats.CheckedFields == 0 {
		t.Fatalf("0 scalar/enum fields were actually compared this run -- stopping before the ledger " +
			"logic below, which would otherwise report a false all-clear")
	}

	// Split violations into ticketed/expected vs unexpected. Track which
	// ledger entries were actually used so a fixed-and-forgotten entry
	// fails the gate instead of silently living forever.
	usedLedgerEntries := map[string]bool{}
	var unexpected []Divergence
	for _, v := range violations {
		if reason, known := expectedDivergences[v.Key()]; known {
			usedLedgerEntries[v.Key()] = true
			t.Logf("EXPECTED (ticketed) divergence detected -- gate is working, not silencing: %s\n  reason: %s",
				v.String(), reason)
			continue
		}
		unexpected = append(unexpected, v)
	}
	for key, reason := range expectedDivergences {
		if !usedLedgerEntries[key] {
			t.Errorf("expected-divergence ledger entry %q never matched an actual violation this run -- "+
				"stale entry (the field was probably fixed elsewhere); delete it. reason on file: %s",
				key, reason)
		}
	}

	t.Logf("CHAOS-4702 gate: checked %d scalar/enum fields; excluded %d object-typed (idiom), "+
		"%d JSON-scalar (idiom), %d list-typed (idiom); %d ticketed/expected divergence(s); "+
		"%d unexpected divergence(s)",
		stats.CheckedFields, stats.ObjectIdiomExcluded, stats.JSONScalarExcluded, stats.ListExcluded,
		len(usedLedgerEntries), len(unexpected))

	if len(unexpected) > 0 {
		var b strings.Builder
		fmt.Fprintf(&b, "%d unticketed Go-model-vs-published-SDL nullability divergence(s):\n", len(unexpected))
		for _, v := range unexpected {
			fmt.Fprintf(&b, "  %s\n", v.String())
		}
		b.WriteString("Each is either a real bug (widen the SDL or fix the Go type, same commit, per " +
			"CHAOS-4658's lesson: gqlgen's generated.go embeds its own SDL copy AND an Invalids++ check, " +
			"both need correcting) or a new, deliberately-deferred instance that needs its own ticket and " +
			"an expectedDivergences entry in this file naming that ticket.")
		t.Fatal(b.String())
	}
}

// TestCHAOS4703IsDetectedByTheRawComparator is the literal acceptance
// evidence CHAOS-4702 was scoped around: with CHAOS-4703 still open, the
// comparator -- BEFORE the expected-divergence ledger filters anything --
// must flag TimeseriesBucket.value and nothing else. This subtest does not
// consult expectedDivergences at all, so it keeps proving raw detection
// works even though TestGoModelNullabilityMatchesPublishedSDL itself stays
// green (via the ledger) so CI is not blocked on an already-ticketed,
// already-deferred issue.
//
// When CHAOS-4703 is fixed, this test starts failing (0 violations where 1
// is asserted) -- which is the correct, loud signal to delete this test
// and the matching expectedDivergences entry in the same PR.
func TestCHAOS4703IsDetectedByTheRawComparator(t *testing.T) {
	repoRoot := repoRootForSDLGate(t)

	sdlBytes, err := os.ReadFile(filepath.Join(repoRoot, "contracts", "graphql", "v1", "schema.graphql"))
	if err != nil {
		t.Fatalf("read SDL pin: %v", err)
	}
	schema, gqlErr := gqlparser.LoadSchema(&gqlast.Source{Name: "schema.graphql", Input: string(sdlBytes)})
	if gqlErr != nil {
		t.Fatalf("parse SDL pin: %v", gqlErr)
	}

	goStructs, parseErrs := parseGoModelFields(t, filepath.Join(
		repoRoot, "cmd", "query-api", "internal", "graph", "model", "models_gen.go"))
	if len(parseErrs) > 0 {
		t.Fatalf("models_gen.go parse issues: %v", parseErrs)
	}

	violations, _, structuralErrors := compareNullability(schema, goStructs)
	if len(structuralErrors) > 0 {
		t.Fatalf("structural errors before this subtest can assert anything: %v", structuralErrors)
	}

	if len(violations) != 1 {
		t.Fatalf("expected exactly 1 raw divergence (CHAOS-4703, TimeseriesBucket.value) with CHAOS-4703 "+
			"still open; got %d: %v\n(if this is 0, CHAOS-4703 was fixed -- delete this test and its "+
			"expectedDivergences entry in the same PR; if this is >1, a NEW divergence exists and needs "+
			"its own ticket)", len(violations), violations)
	}
	got := violations[0]
	want := Divergence{
		TypeName: "TimeseriesBucket", FieldName: "value",
		SDLType: "Float!", GoType: "*float64",
		Direction: "sdl-non-null-but-go-pointer",
	}
	if got != want {
		t.Fatalf("the one raw divergence found does not match CHAOS-4703's known shape.\n got:  %+v\n want: %+v",
			got, want)
	}
	t.Logf("CONFIRMED RED: raw comparator (no ledger applied) detects exactly CHAOS-4703: %s", got.String())
}

// --- synthetic mutation tests --------------------------------------------
//
// compareNullability is pure precisely so it can be exercised on small,
// hand-built schemas (the same reason oraclecompare.DiffRows is pure and
// gets TestDiffRowsClauseCoverage instead of only being proven by a real
// 1000-field run). These tests take one known-good field shape and mutate
// ONLY the Go side to each of the two divergence directions -- proving the
// gate catches both, per the brief: "Nobody has ever looked for the second
// direction... expect findings there and treat them as real until shown
// otherwise."

func mustLoadSyntheticSchema(t *testing.T, sdl string) *gqlast.Schema {
	t.Helper()
	schema, err := gqlparser.LoadSchema(&gqlast.Source{Name: "synthetic.graphql", Input: sdl})
	if err != nil {
		t.Fatalf("parse synthetic SDL: %v", err)
	}
	return schema
}

const syntheticGoodSchema = `
scalar JSON

type Widget {
  id: ID!
  count: Int!
  label: String
  price: Float!
  tags: [String!]!
  detail: WidgetDetail!
  meta: WidgetDetail
  blob: JSON
}

type WidgetDetail {
  note: String!
}

type Query {
  widget: Widget!
}
`

// syntheticGoodGoStructs is models_gen.go's shape for syntheticGoodSchema
// IF every field matched the SDL's nullability correctly (the "known-good"
// baseline the two mutation tests below each perturb by exactly one
// field).
func syntheticGoodGoStructs() map[string]map[string]goFieldInfo {
	return map[string]map[string]goFieldInfo{
		"Widget": {
			"id":     {IsPointer: false, GoType: "string"},
			"count":  {IsPointer: false, GoType: "int"},
			"label":  {IsPointer: true, GoType: "*string"},  // nullable scalar -> pointer, correct
			"price":  {IsPointer: false, GoType: "float64"}, // non-null scalar -> value, correct
			"tags":   {IsList: true, GoType: "[]string"},
			"detail": {IsPointer: true, GoType: "*WidgetDetail"}, // object idiom: pointer regardless of `!`
			"meta":   {IsPointer: true, GoType: "*WidgetDetail"},
			"blob":   {IsPointer: false, GoType: "JSON"}, // JSON idiom: never pointer-wrapped
		},
		"WidgetDetail": {
			"note": {IsPointer: false, GoType: "string"},
		},
		"Query": {},
	}
}

// TestCompareNullabilityGreenOnKnownGoodBaseline is the "a gate that only
// ever fires is not a gate" proof: the untouched baseline must report ZERO
// violations, while still exercising every exclusion category (object
// idiom, JSON idiom, list idiom) so the mutation tests below are known to
// be perturbing a gate that was actually checking something.
func TestCompareNullabilityGreenOnKnownGoodBaseline(t *testing.T) {
	schema := mustLoadSyntheticSchema(t, syntheticGoodSchema)
	violations, stats, structuralErrors := compareNullability(schema, syntheticGoodGoStructs())

	if len(structuralErrors) > 0 {
		t.Fatalf("unexpected structural errors on a hand-built, self-consistent fixture: %v", structuralErrors)
	}
	if len(violations) != 0 {
		t.Fatalf("known-good baseline must report 0 divergences, got %d: %v", len(violations), violations)
	}
	if stats.CheckedFields == 0 {
		t.Fatal("baseline checked 0 fields -- the fixture is not exercising the scalar/enum path at all")
	}
	if stats.ObjectIdiomExcluded == 0 {
		t.Fatal("baseline never hit the object-idiom exclusion -- detail/meta fields not wired correctly")
	}
	if stats.JSONScalarExcluded == 0 {
		t.Fatal("baseline never hit the JSON-scalar exclusion -- blob field not wired correctly")
	}
	if stats.ListExcluded == 0 {
		t.Fatal("baseline never hit the list exclusion -- tags field not wired correctly")
	}
}

// TestCompareNullabilityCatchesDirectionOne: SDL non-null scalar, Go
// mutated to a pointer. This is CHAOS-4703's actual shape (Float! vs
// *float64) and the one direction the epic had already found by hand
// (CHAOS-4650, 4657, 4701, 4658) before this gate existed.
func TestCompareNullabilityCatchesDirectionOne(t *testing.T) {
	schema := mustLoadSyntheticSchema(t, syntheticGoodSchema)
	goStructs := syntheticGoodGoStructs()

	mutated := goStructs["Widget"]["price"]
	mutated.IsPointer = true // price: Float! is non-null; a pointer here is the bug.
	mutated.GoType = "*float64"
	goStructs["Widget"]["price"] = mutated

	violations, _, structuralErrors := compareNullability(schema, goStructs)
	if len(structuralErrors) > 0 {
		t.Fatalf("unexpected structural errors: %v", structuralErrors)
	}
	if len(violations) != 1 {
		t.Fatalf("expected exactly 1 violation from the single mutated field, got %d: %v", len(violations), violations)
	}
	got := violations[0]
	if got.Key() != "Widget.price" || got.Direction != "sdl-non-null-but-go-pointer" {
		t.Fatalf("wrong violation reported for direction 1: %+v", got)
	}
}

// TestCompareNullabilityCatchesDirectionTwo: SDL nullable scalar, Go
// mutated to NOT be a pointer. Per the brief, "nobody has ever looked for
// the second direction" -- this proves the gate would catch it if it
// occurred, even though CHAOS-4702's sweep of the real schema found zero
// live instances of this direction outside the documented JSON idiom.
func TestCompareNullabilityCatchesDirectionTwo(t *testing.T) {
	schema := mustLoadSyntheticSchema(t, syntheticGoodSchema)
	goStructs := syntheticGoodGoStructs()

	mutated := goStructs["Widget"]["label"]
	mutated.IsPointer = false // label: String is nullable; a non-pointer here silently collapses null to "".
	mutated.GoType = "string"
	goStructs["Widget"]["label"] = mutated

	violations, _, structuralErrors := compareNullability(schema, goStructs)
	if len(structuralErrors) > 0 {
		t.Fatalf("unexpected structural errors: %v", structuralErrors)
	}
	if len(violations) != 1 {
		t.Fatalf("expected exactly 1 violation from the single mutated field, got %d: %v", len(violations), violations)
	}
	got := violations[0]
	if got.Key() != "Widget.label" || got.Direction != "sdl-nullable-but-go-non-pointer" {
		t.Fatalf("wrong violation reported for direction 2: %+v", got)
	}
}

// TestCompareNullabilityExcludesObjectAndListAndJSONIdiomsEvenWhenMutated
// proves the exclusion categories are not accidentally also catching
// pointer-ness changes that AREN'T this defect class -- mutating the
// excluded fields' pointer-ness must NOT produce a violation, or the
// exclusions are too narrow and would start reporting the 66+10+189
// look-alikes CHAOS-4702 exists to keep silent.
func TestCompareNullabilityExcludesObjectAndListAndJSONIdiomsEvenWhenMutated(t *testing.T) {
	schema := mustLoadSyntheticSchema(t, syntheticGoodSchema)
	goStructs := syntheticGoodGoStructs()

	// "detail: WidgetDetail!" -- flip its Go representation to non-pointer.
	// Never observed in the real codebase, but if it happened, it is still
	// not THIS defect class (no scalar collapses to a zero value), so it
	// must not be reported by this gate.
	detail := goStructs["Widget"]["detail"]
	detail.IsPointer = false
	goStructs["Widget"]["detail"] = detail

	// "blob: JSON" -- flip to a hypothetical pointer-wrapped representation.
	blob := goStructs["Widget"]["blob"]
	blob.IsPointer = true
	blob.GoType = "*JSON"
	goStructs["Widget"]["blob"] = blob

	violations, stats, structuralErrors := compareNullability(schema, goStructs)
	if len(structuralErrors) > 0 {
		t.Fatalf("unexpected structural errors: %v", structuralErrors)
	}
	if len(violations) != 0 {
		t.Fatalf("object-typed and JSON-scalar fields must be excluded regardless of their Go "+
			"pointer-ness -- got %d violation(s): %v", len(violations), violations)
	}
	if stats.ObjectIdiomExcluded == 0 || stats.JSONScalarExcluded == 0 {
		t.Fatalf("mutated fields did not even reach the exclusion counters: stats=%+v", stats)
	}
}

// TestCompareNullabilityCatchesListShapeMismatch: codex review round 1
// (chaos-4702-20260831T211310.md, P2) found that the list exclusion was
// checked BEFORE the Go field was even looked up, so a field that is a list
// in the SDL but NOT a slice on the Go side (a codegen break more severe
// than a nullability divergence -- an outright shape mismatch) would be
// silently counted as ListExcluded and never flagged. Fixed by moving the
// Go-field lookup and a two-way shape-agreement check ahead of the list
// exclusion. This test mutates the one genuinely list-typed field the
// earlier fixture never touched (`tags`, per that same review's second
// finding that the "list mutation" claim wasn't backed by an actual test),
// in both directions.
func TestCompareNullabilityCatchesListShapeMismatch(t *testing.T) {
	t.Run("SDL list, Go not a list", func(t *testing.T) {
		schema := mustLoadSyntheticSchema(t, syntheticGoodSchema)
		goStructs := syntheticGoodGoStructs()

		tags := goStructs["Widget"]["tags"]
		tags.IsList = false // `tags: [String!]!` in the SDL; Go regressed to a bare scalar.
		tags.GoType = "string"
		goStructs["Widget"]["tags"] = tags

		violations, stats, structuralErrors := compareNullability(schema, goStructs)
		if len(violations) != 0 {
			t.Fatalf("a shape mismatch must never be reported as a nullability Divergence: %v", violations)
		}
		if len(structuralErrors) != 1 {
			t.Fatalf("expected exactly 1 structural error for the list/non-list shape mismatch, got %d: %v",
				len(structuralErrors), structuralErrors)
		}
		if !strings.Contains(structuralErrors[0], "Widget.tags") {
			t.Fatalf("structural error does not name the mutated field: %s", structuralErrors[0])
		}
		if stats.ListExcluded != 0 {
			t.Fatalf("a shape-mismatched field must NOT be silently counted as a clean list exclusion, "+
				"got ListExcluded=%d", stats.ListExcluded)
		}
	})

	t.Run("SDL not a list, Go is a list", func(t *testing.T) {
		schema := mustLoadSyntheticSchema(t, syntheticGoodSchema)
		goStructs := syntheticGoodGoStructs()

		price := goStructs["Widget"]["price"]
		price.IsList = true // `price: Float!` in the SDL; Go somehow became a slice.
		price.GoType = "[]float64"
		goStructs["Widget"]["price"] = price

		violations, _, structuralErrors := compareNullability(schema, goStructs)
		if len(violations) != 0 {
			t.Fatalf("a shape mismatch must never be reported as a nullability Divergence: %v", violations)
		}
		if len(structuralErrors) != 1 || !strings.Contains(structuralErrors[0], "Widget.price") {
			t.Fatalf("expected exactly 1 structural error naming Widget.price, got: %v", structuralErrors)
		}
	})
}

// TestExclusionIntegrityFailures unit-tests the CheckExclusionIntegrity-style
// staleness check directly against synthetic counts, matching
// oraclecompare's own TestCheckExclusionIntegrityClauseCoverage precedent:
// each clause needs its own case, not just an aggregate "some rule failed".
func TestExclusionIntegrityFailures(t *testing.T) {
	allHit := compareStats{CheckedFields: 5, ObjectIdiomExcluded: 1, JSONScalarExcluded: 1, ListExcluded: 1}
	if got := exclusionIntegrityFailures(allHit); len(got) != 0 {
		t.Fatalf("all-rules-hit stats must report 0 failures, got %v", got)
	}

	cases := []struct {
		name  string
		stats compareStats
	}{
		{"object idiom never matched", compareStats{CheckedFields: 5, JSONScalarExcluded: 1, ListExcluded: 1}},
		{"json scalar never matched", compareStats{CheckedFields: 5, ObjectIdiomExcluded: 1, ListExcluded: 1}},
		{"list never matched", compareStats{CheckedFields: 5, ObjectIdiomExcluded: 1, JSONScalarExcluded: 1}},
		{"nothing checked", compareStats{ObjectIdiomExcluded: 1, JSONScalarExcluded: 1, ListExcluded: 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := exclusionIntegrityFailures(c.stats)
			if len(got) != 1 {
				t.Fatalf("expected exactly 1 failure for %+v, got %d: %v", c.stats, len(got), got)
			}
		})
	}
}
