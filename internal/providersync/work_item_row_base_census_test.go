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

// workItemRowCensusAllowed are the only non-test files that may build a work
// item row field by field, each with its reason (CHAOS-9852).
var workItemRowCensusAllowed = map[string]string{
	"work_item_row_base.go":        "the base: buildWorkItemRow and linearWorkItemRowFromBase",
	"linear_work_items_derived.go": "copies a Linear row that buildWorkItemRow built, field for field, into the derivation row; it reads no payload",
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
// field write outside the allowed files.
func scanWorkItemRowWrites(pkg *packages.Package, rowTypes []types.Type) []workItemRowCensusFinding {
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
	for _, file := range pkg.Syntax {
		name := filepath.Base(pkg.Fset.Position(file.Pos()).Filename)
		if _, ok := workItemRowCensusAllowed[name]; ok {
			continue
		}
		position := func(node ast.Node) string {
			at := pkg.Fset.Position(node.Pos())
			return filepath.Base(at.Filename) + ":" + itoaCensus(at.Line)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.CompositeLit:
				if len(typed.Elts) > 0 && isRow(pkg.TypesInfo.TypeOf(typed)) {
					findings = append(findings, workItemRowCensusFinding{position(typed), "row literal with fields"})
				}
			case *ast.AssignStmt:
				if typed.Tok == token.DEFINE {
					return true
				}
				for _, left := range typed.Lhs {
					selector, ok := left.(*ast.SelectorExpr)
					if !ok {
						continue
					}
					selection := pkg.TypesInfo.Selections[selector]
					if selection == nil || selection.Kind() != types.FieldVal || !isRow(selection.Recv()) {
						continue
					}
					if _, ok := workItemRowCensusFieldWrites[selector.Sel.Name]; ok {
						continue
					}
					findings = append(findings, workItemRowCensusFinding{position(selector), "write of row field " + selector.Sel.Name})
				}
			}
			return true
		})
	}
	return findings
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
	allowedSeen := map[string]bool{}
	for _, file := range pkg.Syntax {
		name := filepath.Base(pkg.Fset.Position(file.Pos()).Filename)
		if strings.HasSuffix(name, "_test.go") {
			t.Fatalf("census scanned a test file %s", name)
		}
		if _, ok := workItemRowCensusAllowed[name]; ok {
			allowedSeen[name] = true
		}
	}
	for name := range workItemRowCensusAllowed {
		if !allowedSeen[name] {
			t.Errorf("allowed file %s no longer exists: remove its census entry", name)
		}
	}
	for _, finding := range scanWorkItemRowWrites(pkg, rowTypes) {
		t.Errorf("%s: %s outside the work item base; read the field in the provider's workItemSource instead", finding.position, finding.what)
	}
}

// TestWorkItemRowBaseCensusCatchesABypass plants a builder that bypasses the
// base and proves the scan reports both shapes.
func TestWorkItemRowBaseCensusCatchesABypass(t *testing.T) {
	pkg, rowTypes := loadWorkItemRowCensusPackage(t, "testdata/workitem_row_census_plant", ".")
	findings := scanWorkItemRowWrites(pkg, rowTypes)
	got := make([]string, 0, len(findings))
	for _, finding := range findings {
		got = append(got, finding.position+" "+finding.what)
	}
	sort.Strings(got)
	want := []string{
		"plant.go:17 row literal with fields",
		"plant.go:22 write of row field ParentID",
		"plant.go:23 write of row field EpicID",
		"plant.go:28 row literal with fields",
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
	row, err := buildWorkItemRow(claim, fullWorkItemSource{}, fullWorkItemSourceNow)
	if err != nil {
		t.Fatal(err)
	}
	value := reflect.ValueOf(row)
	for i := 0; i < value.NumField(); i++ {
		name := value.Type().Field(i).Name
		if name == "DueAtAbsent" {
			continue
		}
		if value.Field(i).IsZero() {
			t.Errorf("buildWorkItemRow left %s zero", name)
		}
	}
	if *row.ParentID != "jira:P-1" || *row.EpicID != "jira:E-1" {
		t.Fatalf("hierarchy = %s, %s", *row.ParentID, *row.EpicID)
	}
	linear := linearWorkItemRowFromBase(row)
	linearValue := reflect.ValueOf(linear)
	for i := 0; i < linearValue.NumField(); i++ {
		name := linearValue.Type().Field(i).Name
		if name == "DueAtAbsent" || name == "RepoID" {
			continue
		}
		if linearValue.Field(i).IsZero() {
			t.Errorf("linearWorkItemRowFromBase left %s zero", name)
		}
	}
}
