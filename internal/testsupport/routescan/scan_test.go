package routescan

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const fixture = `package p

const prefix = "/api/v1/auth"

const teamsPath = "/api/v1/teams"

func Routes() []Route {
	return []Route{
		{Method: http.MethodGet, Pattern: teamsPath, Handler: guard.Wrap(policy.AdminOrg, h)},
		{Method: http.MethodPost, Pattern: "/api/v1/teams",
			Handler: guard.BodyFirst(policy.Admin, h)},
		{Pattern: "/api/v1/people", Methods: []string{http.MethodGet}},
	}
}

func sso() {
	route(http.MethodGet, "/sso", g.Wrap(policy.Authenticated, h))
}

func mounts() {
	mux.HandleFunc("/query", h)
	mcpMux.Handle("/query", h)
}
`

func scanFixture(t *testing.T, mux bool) ([]Registration, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "routes.go"), []byte(fixture), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "routes_test.go"), []byte("package p\nvar _ = Route{Pattern: \"/test-only\"}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	scan := Scan
	if mux {
		scan = ScanWithMux
	}
	regs, err := scan(dir)
	if err != nil {
		t.Fatal(err)
	}
	return regs, filepath.ToSlash(filepath.Join(dir, "routes.go"))
}

func TestScanResolvesPatternsThroughConstantsAndNamesTheLevel(t *testing.T) {
	regs, file := scanFixture(t, false)
	for _, tc := range []struct {
		method, pattern, level string
		line                   int
	}{
		{"GET", "/api/v1/teams", "AdminOrg", 9},
		{"POST", "/api/v1/teams", "Admin", 10},
		{"GET", "/api/v1/people", "", 12},
		{"GET", "/api/v1/auth/sso", "Authenticated", 17},
	} {
		reg, err := Resolve(regs, file, tc.method, tc.pattern)
		if err != nil || reg.Line != tc.line || reg.Level != tc.level {
			t.Errorf("%s %s: %+v, %v; want line %d level %q", tc.method, tc.pattern, reg, err, tc.line, tc.level)
		}
	}
	for _, reg := range regs {
		if reg.Pattern == "/test-only" || reg.Pattern == "/query" {
			t.Errorf("Scan returned %+v (a _test.go file, or a mux call without ScanWithMux)", reg)
		}
	}
}

func TestResolveFailsOnAStaleOrAmbiguousSymbol(t *testing.T) {
	regs, file := scanFixture(t, true)
	if _, err := Resolve(regs, file, "DELETE", "/api/v1/teams"); err == nil || !strings.Contains(err.Error(), "source anchor stale: ") {
		t.Errorf("a method with no registration: %v", err)
	}
	if _, err := Resolve(regs, file, "POST", "/api/v1/people"); err == nil || !strings.Contains(err.Error(), "source anchor stale: ") {
		t.Errorf("a method outside the Methods list: %v", err)
	}
	if _, err := Resolve(regs, file, "POST", "/query"); err == nil || !strings.Contains(err.Error(), "source anchor ambiguous: ") || !strings.Contains(err.Error(), "2 times (lines 21, 22)") {
		t.Errorf("one path on two muxes: %v", err)
	}
	reg, err := ResolveWhere(regs, file, "POST", "/query", func(r Registration) bool { return r.Mux == "mcpMux" })
	if err != nil || reg.Line != 22 {
		t.Errorf("the mcpMux registration: %+v, %v", reg, err)
	}
}

func TestNoteLinesStopsAtAnIdentifierBoundary(t *testing.T) {
	source := []byte("func (g *Guard) WrapAdminOr(\nfunc (g *Guard) Wrap(level)\nx := Wrap\n")
	if got := NoteLines(source, "func (g *Guard) Wrap"); len(got) != 1 || got[0] != 2 {
		t.Errorf("identifier note: %v", got)
	}
	if got := NoteLines(source, "func (g *Guard)"); len(got) != 2 {
		t.Errorf("non-identifier note: %v", got)
	}
	if got := NoteLines(source, "Wrap"); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Errorf("bare identifier note: %v", got)
	}
}

func TestResolveNoteNamesOnePlaceOrFails(t *testing.T) {
	regs, file := scanFixture(t, true)
	source := []byte(fixture)
	own := func(method, pattern string) func() (Registration, error) {
		return func() (Registration, error) { return Resolve(regs, file, method, pattern) }
	}
	for _, tc := range []struct {
		name, path, note, rowFile string
		line                      int
		own                       func() (Registration, error)
		problem                   string
		drifted                   bool
	}{
		{"unique note at its line", file, "func sso(", file, 16, nil, "", false},
		{"unique note moved", file, "func sso(", file, 3, nil, "", true},
		{"note on no line", file, "func gone(", file, 16, nil, "anchor stale: ", false},
		{"weak note, other file", file, "Pattern:", "other.go", 9, nil, "anchor ambiguous: ", false},
		{"weak note on the row's registration", file, "Pattern:", file, 10, own("POST", "/api/v1/teams"), "", false},
		{"weak note, registration moved", file, "Pattern:", file, 4, own("POST", "/api/v1/teams"), "", true},
		{"weak note, row not registered", file, "Pattern:", file, 10, own("PUT", "/api/v1/teams"), "source anchor stale: ", false},
		{"weak note not on the registration", file, "Handler:", file, 10, own("GET", "/api/v1/auth/sso"), "anchor stale: the row's registration", false},
	} {
		problem, drifted := ResolveNote(source, tc.path, tc.note, tc.line, tc.rowFile, tc.own)
		if (tc.problem == "") != (problem == "") || !strings.Contains(problem, tc.problem) || drifted != tc.drifted {
			t.Errorf("%s: problem %q drifted %v; want %q, %v", tc.name, problem, drifted, tc.problem, tc.drifted)
		}
	}
}

const symbolFixture = `package p

type (
	Plain int
	Box[T any] struct{ v T }
)

// Routes is doc.
func Routes() []Route {
	return nil
}

func (g *Guard) Wrap(level string) Handler {
	return g.wrap(level)
}

func (b Box[T]) Wrap() T { return b.v }

func Wrap() {}

func init() {}

func init() {}
`

func TestResolveSymbolFindsOneDeclarationByName(t *testing.T) {
	source := []byte(symbolFixture)
	for _, tc := range []struct {
		symbol, anchor string
		line           int
	}{
		{"Routes", "", 9},
		{"Routes", "return nil", 10},
		{"Guard.Wrap", "g.wrap(level)", 14},
		{"Box.Wrap", "", 17},
		{"Wrap", "", 19},
		{"Plain", "", 4},
		{"Box", "", 5},
	} {
		line, err := ResolveSymbol(source, "p.go", tc.symbol, tc.anchor)
		if err != nil || line != tc.line {
			t.Errorf("%s %q: line %d, %v; want %d", tc.symbol, tc.anchor, line, err, tc.line)
		}
	}
}

func TestResolveSymbolFailsOnAStaleOrAmbiguousSymbolOrAnchor(t *testing.T) {
	source := []byte(symbolFixture)
	for _, tc := range []struct{ symbol, anchor, want string }{
		{"Gone", "", "symbol stale: p.go declares no func, method or type Gone"},
		{"Guard.Gone", "", "symbol stale: p.go declares no func, method or type Guard.Gone"},
		{"Guard.Routes", "", "symbol stale: "},
		{"Plain.Wrap", "", "symbol stale: "},
		{"init", "", "symbol ambiguous: p.go declares init 2 times (lines 21, 23)"},
		{"Routes", "g.wrap(level)", "anchor stale: p.go#Routes (lines 9-11) holds no line with"},
		{"Guard.Wrap", "wrapAll", "anchor stale: "},
	} {
		_, err := ResolveSymbol(source, "p.go", tc.symbol, tc.anchor)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s %q: %v; want %q", tc.symbol, tc.anchor, err, tc.want)
		}
	}
	twice := []byte("package p\n\nfunc a() {\n\tx()\n\tx()\n}\n")
	if _, err := ResolveSymbol(twice, "p.go", "a", "x()"); err == nil || !strings.Contains(err.Error(), "anchor ambiguous: \"x()\" is on 2 lines of p.go#a") {
		t.Errorf("an anchor on two lines of the symbol: %v", err)
	}
	if _, err := ResolveSymbol([]byte("package p\nfunc (\n"), "p.go", "a", ""); err == nil || !strings.Contains(err.Error(), "does not parse") {
		t.Errorf("a file that does not parse: %v", err)
	}
}
