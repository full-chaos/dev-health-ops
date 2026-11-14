package providersync

import (
	"regexp"
	"strconv"
	"strings"
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

// routeCensusKindsWants is the exact number of want sites in the kinds
// fixture: one or more per reject branch a route value can reach.
const routeCensusKindsWants = 48

// CHAOS-10268: each source kind the census cannot prove static fails with
// its file:line, never a silent skip. The kinds fixture marks each census site
// with a want comment that holds a word of the expected finding.
func TestRouteCensusRejectsEveryUnprovableSourceKind(t *testing.T) {
	pkgs := loadRouteCensusFixture(t, "kinds")
	wants := map[int]string{}
	wantComment := regexp.MustCompile(`^// want "(.+)"$`)
	for _, file := range pkgs[0].Syntax {
		for _, group := range file.Comments {
			for _, comment := range group.List {
				if match := wantComment.FindStringSubmatch(comment.Text); match != nil {
					wants[pkgs[0].Fset.Position(comment.Pos()).Line] = match[1]
				}
			}
		}
	}
	if len(wants) != routeCensusKindsWants {
		t.Fatalf("read %d want comments in the kinds fixture, want %d: a cell added or lost changes this count on purpose", len(wants), routeCensusKindsWants)
	}
	got := map[int]string{}
	for _, finding := range runRouteCensus(pkgs).findings {
		parts := strings.SplitN(finding, ":", 3)
		if len(parts) != 3 {
			t.Fatalf("finding without file:line: %q", finding)
		}
		line, err := strconv.Atoi(parts[1])
		if err != nil {
			t.Fatalf("finding without file:line: %q", finding)
		}
		if got[line] != "" {
			t.Errorf("two findings on line %d: %q and %q", line, got[line], finding)
		}
		got[line] = finding
	}
	for line, want := range wants {
		if !strings.Contains(got[line], want) {
			t.Errorf("kinds.go:%d: want a finding with %q, got %q", line, want, got[line])
		}
	}
	for line, finding := range got {
		if wants[line] == "" {
			t.Errorf("finding without a want comment: %s", finding)
		}
	}
}

// routeCensusFixture type-checks one fixture package, so a broken fixture
// fails here and is never read as a census count, then runs the census on it
// alone.
func routeCensusFixture(t *testing.T, dir string) int {
	t.Helper()
	return len(routeCensusFixtureFindings(t, dir))
}

func routeCensusFixtureFindings(t *testing.T, dir string) []string {
	t.Helper()
	findings := runRouteCensus(loadRouteCensusFixture(t, dir)).findings
	for _, finding := range findings {
		t.Logf("fixture %s: %s", dir, finding)
	}
	return findings
}

func loadRouteCensusFixture(t *testing.T, dir string) []*packages.Package {
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
	requireRouteCensusLoad(t, pkgs)
	return pkgs
}
