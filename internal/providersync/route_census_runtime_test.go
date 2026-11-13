package providersync

import (
	"os"
	"testing"

	"golang.org/x/tools/go/packages"
)

// CHAOS-10268: a route value that the request sets at run time must fail the
// census, also when its name looks like a route name. The runtime fixture
// has four such sites: a local, a pass-through helper parameter (read at its
// call site), a struct field and a page option.
func TestRouteCensusRejectsARuntimeRouteValue(t *testing.T) {
	if got := routeCensusFixture(t, "runtime"); got != 4 {
		t.Fatalf("census counts %d bad sites in testdata/routecensus/runtime, want 4: a local, a helper parameter, a struct field and a page option set from the request", got)
	}
}

// The control fixture holds only static routes under the 9801 ruling
// (literal, constant *Route, concatenation with static suffix variables,
// through a helper, a slice and a struct field). The census must accept it.
func TestRouteCensusAcceptsStaticRouteFixture(t *testing.T) {
	if got := routeCensusFixture(t, "control"); got != 0 {
		t.Fatalf("census counts %d bad sites in testdata/routecensus/control, want 0", got)
	}
}

// routeCensusFixture type-checks one fixture package, so a broken fixture
// fails here and is never read as a census count, then counts its bad sites.
func routeCensusFixture(t *testing.T, dir string) int {
	t.Helper()
	pkgs, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  "../..",
	}, "./internal/providersync/testdata/routecensus/"+dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(pkgs) != 1 || len(pkgs[0].GoFiles) == 0 {
		t.Fatalf("fixture %s: loaded %d packages, want 1 with files", dir, len(pkgs))
	}
	for _, loadErr := range pkgs[0].Errors {
		t.Errorf("fixture %s: %v", dir, loadErr)
	}
	if t.Failed() {
		t.FailNow()
	}
	optionTypes := routeOptionTypes(t)
	bad := 0
	for _, name := range pkgs[0].GoFiles {
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		bad += routeCensusFile(t, name, source, optionTypes)
	}
	return bad
}
