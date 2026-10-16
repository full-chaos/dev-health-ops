package providersync

import (
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// workItemRowCensusAllowed are the only non-test functions ("file:function")
// that may build a work item row field by field, each with its reason
// (CHAOS-9852). The rest of their file is scanned like any other.
var workItemRowCensusAllowed = map[string]string{
	"work_item_row_base.go:buildWorkItemRow":                  "the base constructor",
	"work_item_row_base.go:linearWorkItemRowFromBase":         "the one conversion of a base row into the Linear effect row",
	"linear_work_items_derived.go:linearWorkItemRowsAsGitHub": "copies a Linear row that buildWorkItemRow built, field for field, into the derivation row; it reads no payload",
}

// workItemRowCensusFieldWrites are the row fields that code outside the base may
// set after a row exists, each with its reason.
var workItemRowCensusFieldWrites = map[string]string{
	"DueAtAbsent": "a marker set when an effect is decoded, not a provider field",
}

type workItemRowCensusFinding struct {
	position string
	what     string
}

// scanWorkItemRowWrites returns every row literal with fields and every row
// field write outside the allowed functions, and the allowed functions that
// hold at least one row literal (an allowance that holds none is stale).
func scanWorkItemRowWrites(pkg *packages.Package, rowTypes []types.Type) ([]workItemRowCensusFinding, map[string]bool) {
	isRow := func(typ types.Type) bool {
		if typ == nil {
			return false
		}
		if pointer, ok := typ.(*types.Pointer); ok {
			typ = pointer.Elem()
		}
		for _, row := range rowTypes {
			if types.Identical(typ, row) {
				return true
			}
		}
		return false
	}
	var findings []workItemRowCensusFinding
	allowedWithLiteral := map[string]bool{}
	for _, file := range pkg.Syntax {
		name := filepath.Base(pkg.Fset.Position(file.Pos()).Filename)
		position := func(node ast.Node) string {
			at := pkg.Fset.Position(node.Pos())
			return filepath.Base(at.Filename) + ":" + itoaCensus(at.Line)
		}
		// report adds a finding for a row field selector, unless the field is
		// one code outside the base may set.
		report := func(selector *ast.SelectorExpr, what string) {
			if selector == nil {
				return
			}
			selection := pkg.TypesInfo.Selections[selector]
			if selection == nil || selection.Kind() != types.FieldVal || !isRow(selection.Recv()) {
				return
			}
			if _, ok := workItemRowCensusFieldWrites[selector.Sel.Name]; ok {
				return
			}
			findings = append(findings, workItemRowCensusFinding{position(selector), what + selector.Sel.Name})
		}
		for _, declaration := range file.Decls {
			allowedKey := ""
			if function, ok := declaration.(*ast.FuncDecl); ok {
				key := name + ":" + function.Name.Name
				if _, ok := workItemRowCensusAllowed[key]; ok {
					allowedKey = key
				}
			}
			ast.Inspect(declaration, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.CompositeLit:
					if len(typed.Elts) == 0 || !isRow(pkg.TypesInfo.TypeOf(typed)) {
						return true
					}
					if allowedKey != "" {
						allowedWithLiteral[allowedKey] = true
						return true
					}
					findings = append(findings, workItemRowCensusFinding{position(typed), "row literal with fields"})
				case *ast.AssignStmt:
					if typed.Tok == token.DEFINE || allowedKey != "" {
						return true
					}
					for _, left := range typed.Lhs {
						report(rowFieldTarget(left), "write of row field ")
					}
				case *ast.IncDecStmt:
					if allowedKey == "" {
						report(rowFieldTarget(typed.X), "write of row field ")
					}
				case *ast.UnaryExpr:
					if typed.Op == token.AND && allowedKey == "" {
						report(rowFieldTarget(typed.X), "address of row field ")
					}
				case *ast.RangeStmt:
					if typed.Tok == token.ASSIGN && allowedKey == "" {
						report(rowFieldTarget(typed.Key), "write of row field ")
						report(rowFieldTarget(typed.Value), "write of row field ")
					}
				}
				return true
			})
		}
	}
	return findings, allowedWithLiteral
}

// rowFieldTarget is the field selector an expression writes through: the
// selector itself, or one reached through a dereference, an index, a slice
// or parentheses (*row.ParentID, row.Labels[0], (row.Title)).
func rowFieldTarget(expr ast.Expr) *ast.SelectorExpr {
	for expr != nil {
		switch typed := expr.(type) {
		case *ast.SelectorExpr:
			return typed
		case *ast.StarExpr:
			expr = typed.X
		case *ast.IndexExpr:
			expr = typed.X
		case *ast.SliceExpr:
			expr = typed.X
		case *ast.ParenExpr:
			expr = typed.X
		default:
			return nil
		}
	}
	return nil
}

func itoaCensus(value int) string {
	digits := []byte{}
	for value > 0 || len(digits) == 0 {
		digits = append([]byte{byte('0' + value%10)}, digits...)
		value /= 10
	}
	return string(digits)
}

func loadWorkItemRowCensusPackage(t *testing.T, dir string, pattern string) (*packages.Package, []types.Type) {
	t.Helper()
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  dir,
	}, pattern)
	if err != nil || packages.PrintErrors(loaded) > 0 || len(loaded) != 1 {
		t.Fatalf("load %s: %v", pattern, err)
	}
	pkg := loaded[0]
	var rowTypes []types.Type
	for _, name := range []string{"githubWorkItemRow", "linearWorkItemRow"} {
		object := pkg.Types.Scope().Lookup(name)
		if object == nil {
			t.Fatalf("%s: type %s not found: the census would scan for nothing", pattern, name)
		}
		rowTypes = append(rowTypes, object.Type())
	}
	return pkg, rowTypes
}

// TestWorkItemRowBaseCensus fails when a non-test file outside the base builds
// a work item row literal with fields or writes a row field after the base
// built it (CHAOS-9852): a provider's rule for a field lives in its
// workItemSource method, never in a second copy.
func TestWorkItemRowBaseCensus(t *testing.T) {
	pkg, rowTypes := loadWorkItemRowCensusPackage(t, "../..", "./internal/providersync")
	if len(pkg.Syntax) < 100 {
		t.Fatalf("scanned %d files: the census lost the package", len(pkg.Syntax))
	}
	for _, file := range pkg.Syntax {
		if name := filepath.Base(pkg.Fset.Position(file.Pos()).Filename); strings.HasSuffix(name, "_test.go") {
			t.Fatalf("census scanned a test file %s", name)
		}
	}
	findings, allowedWithLiteral := scanWorkItemRowWrites(pkg, rowTypes)
	for key := range workItemRowCensusAllowed {
		if !allowedWithLiteral[key] {
			t.Errorf("allowed function %s builds no row literal (gone or renamed): remove its census entry", key)
		}
	}
	for _, finding := range findings {
		t.Errorf("%s: %s outside the work item base; read the field in the provider's workItemSource instead", finding.position, finding.what)
	}
}

// TestWorkItemRowBaseCensusCatchesABypass plants a builder that bypasses the
// base and proves the scan reports both shapes.
func TestWorkItemRowBaseCensusCatchesABypass(t *testing.T) {
	pkg, rowTypes := loadWorkItemRowCensusPackage(t, "testdata/workitem_row_census_plant", ".")
	findings, allowedWithLiteral := scanWorkItemRowWrites(pkg, rowTypes)
	if !reflect.DeepEqual(allowedWithLiteral, map[string]bool{"work_item_row_base.go:buildWorkItemRow": true}) {
		t.Fatalf("allowed functions with a literal = %v", allowedWithLiteral)
	}
	got := make([]string, 0, len(findings))
	for _, finding := range findings {
		got = append(got, finding.position+" "+finding.what)
	}
	sort.Strings(got)
	want := []string{
		"plant.go:19 row literal with fields",
		"plant.go:24 write of row field ParentID",
		"plant.go:25 write of row field EpicID",
		"plant.go:30 row literal with fields",
		"plant.go:35 write of row field ParentID",
		"plant.go:36 write of row field Labels",
		"plant.go:37 address of row field Title",
		"work_item_row_base.go:12 row literal with fields",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("plant findings = %q, want %q", got, want)
	}
}

// TestWorkItemSourceOwnsEveryRowField fails when a row field has no
// workItemSource method of the same name and is not owned by the base, so a
// new column cannot be left unread by a provider.
func TestWorkItemSourceOwnsEveryRowField(t *testing.T) {
	methods := map[string]bool{}
	source := reflect.TypeOf((*workItemSource)(nil)).Elem()
	for i := 0; i < source.NumMethod(); i++ {
		if method := source.Method(i); method.IsExported() {
			methods[method.Name] = true
		}
	}
	if len(methods) < 20 {
		t.Fatalf("workItemSource has %d field methods: the reflection lost them", len(methods))
	}
	row := reflect.TypeOf(workItemRow{})
	fields := map[string]bool{}
	for i := 0; i < row.NumField(); i++ {
		name := row.Field(i).Name
		fields[name] = true
		_, baseOwned := workItemBaseOwnedFields[name]
		if methods[name] == baseOwned {
			t.Errorf("row field %s: source method=%v base-owned=%v; exactly one must own it", name, methods[name], baseOwned)
		}
	}
	for name := range methods {
		if !fields[name] {
			t.Errorf("workItemSource.%s reads no row field", name)
		}
	}
	for name := range workItemBaseOwnedFields {
		if !fields[name] {
			t.Errorf("base-owned field %s is not a row field", name)
		}
	}
	linear := reflect.TypeOf(linearWorkItemRow{})
	for i := 0; i < linear.NumField(); i++ {
		if _, ok := row.FieldByName(linear.Field(i).Name); !ok {
			t.Errorf("linear row field %s has no base row field", linear.Field(i).Name)
		}
	}
}

// TestBuildWorkItemRowSetsEveryField proves buildWorkItemRow copies every
// source method into its field: a source that returns a non-zero value for
// every field yields a row with no zero field (DueAtAbsent excepted).
func TestBuildWorkItemRowSetsEveryField(t *testing.T) {
	claim := nativeTestClaim("jira", "work-items")
	source := fullWorkItemSource{}
	row, err := buildWorkItemRow(claim, source, fullWorkItemSourceNow)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]any{
		"ParentID": stringPointer("jira:P-1"), "EpicID": stringPointer("jira:E-1"),
		"OrgID": claim.OrgID, "LastSynced": fullWorkItemSourceNow, "DueAtAbsent": false,
	}
	sourceValue := reflect.ValueOf(source)
	value := reflect.ValueOf(row)
	for i := 0; i < value.NumField(); i++ {
		name := value.Type().Field(i).Name
		want, ok := expected[name]
		if !ok {
			method := sourceValue.MethodByName(name)
			if !method.IsValid() {
				t.Fatalf("row field %s has no source method", name)
			}
			want = method.Call(nil)[0].Interface()
		}
		if name != "DueAtAbsent" && value.Field(i).IsZero() {
			t.Errorf("buildWorkItemRow left %s zero", name)
		}
		if got := value.Field(i).Interface(); !reflect.DeepEqual(got, want) {
			t.Errorf("row.%s = %#v, want %#v from the source", name, got, want)
		}
	}
	linear := reflect.ValueOf(linearWorkItemRowFromBase(row))
	for i := 0; i < linear.NumField(); i++ {
		name := linear.Type().Field(i).Name
		got := linear.Field(i).Interface()
		if name == "RepoID" {
			if linear.Field(i).IsNil() == false {
				t.Errorf("linear RepoID = %v, want nil", got)
			}
			continue
		}
		if want := value.FieldByName(name).Interface(); !reflect.DeepEqual(got, want) {
			t.Errorf("linear.%s = %#v, want the base row's %#v", name, got, want)
		}
	}
}
