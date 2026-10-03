package routeprofile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func servedOf(plane string, pairs ...Pair) []Served {
	var out []Served
	for _, pair := range pairs {
		out = append(out, Served{Plane: plane, Pair: pair})
	}
	return out
}

func rowOf(method, route string) Row {
	return Row{ID: method + " " + route, SurfaceKind: "rest", Method: method, Route: route, Classification: "protected"}
}

func problems(report Report) string { return strings.Join(report.Problems, "\n") }

func TestAMatchedRouteIsClean(t *testing.T) {
	report := Compare(servedOf("go-api", Pair{"GET", "/api/v1/x/{}"}), []Row{rowOf("GET", "/api/v1/x/{id}")}, nil, nil, nil)
	if !report.OK() || report.Matched != 1 {
		t.Fatalf("matched=%d problems=%s", report.Matched, problems(report))
	}
}

// (e) each guard, planted.
func TestAServedRouteWithoutARowFails(t *testing.T) {
	report := Compare(servedOf("go-api", Pair{"GET", "/api/v1/x"}, Pair{"POST", "/api/v1/planted"}), []Row{rowOf("GET", "/api/v1/x")}, nil, nil, nil)
	if report.OK() || !strings.Contains(problems(report), "POST /api/v1/planted") {
		t.Fatalf("a planted served route with no row passed: %q", problems(report))
	}
}

func TestARowWithoutAServedRouteFails(t *testing.T) {
	report := Compare(servedOf("go-api", Pair{"GET", "/api/v1/x"}), []Row{rowOf("GET", "/api/v1/x"), rowOf("DELETE", "/api/v1/gone")}, nil, nil, nil)
	if report.OK() || !strings.Contains(problems(report), "DELETE /api/v1/gone") {
		t.Fatalf("a planted profile row with no served route passed: %q", problems(report))
	}
}

func TestARowIsKeyedByMethodAndNotJustPath(t *testing.T) {
	report := Compare(servedOf("go-api", Pair{"GET", "/api/v1/x"}), []Row{rowOf("POST", "/api/v1/x")}, nil, nil, nil)
	if report.OK() {
		t.Fatal("a row for POST passed against a served GET of the same path")
	}
}

func TestACommaMethodRowIsTwoPairs(t *testing.T) {
	report := Compare(servedOf("go-api", Pair{"GET", "/health"}, Pair{"HEAD", "/health"}), []Row{rowOf("GET,HEAD", "/health")}, nil, nil, nil)
	if !report.OK() || report.Matched != 2 {
		t.Fatalf("matched=%d problems=%s", report.Matched, problems(report))
	}
}

func TestAutomaticHeadAndOptionsNeedNoRow(t *testing.T) {
	served := servedOf("go-api", Pair{"GET", "/a"}, Pair{"HEAD", "/a"}, Pair{"OPTIONS", "/a"}, Pair{"POST", "/b"}, Pair{"OPTIONS", "/b"})
	report := Compare(served, []Row{rowOf("GET", "/a"), rowOf("POST", "/b")}, nil, nil, nil)
	if !report.OK() {
		t.Fatalf("automatic methods were not exempt: %s", problems(report))
	}
	// a HEAD with no GET on its path is a real route and needs a row
	report = Compare(servedOf("go-api", Pair{"HEAD", "/only-head"}), nil, nil, nil, nil)
	if report.OK() {
		t.Fatal("a HEAD route without a GET passed as automatic")
	}
	// an OPTIONS route alone is a real route too
	report = Compare(servedOf("go-api", Pair{"OPTIONS", "/only-options"}), nil, nil, nil, nil)
	if report.OK() {
		t.Fatal("an OPTIONS route without any other method passed as automatic")
	}
}

func TestAWildcardEntryCoversItsRowAndItsRegistration(t *testing.T) {
	served := servedOf("go-api", Pair{"POST", "/api/v1/admin/ip-allowlist/{}"})
	rows := []Row{rowOf("POST", "/api/v1/admin/ip-allowlist/check")}
	wild := []Wildcard{{Method: "POST", Route: "/api/v1/admin/ip-allowlist/check", Wildcard: "/api/v1/admin/ip-allowlist/{entry_id}", Literal: "check", Site: "ipallowlist.go:433"}}
	if report := Compare(served, rows, wild, nil, nil); !report.OK() {
		t.Fatalf("a wildcard-served row failed: %s", problems(report))
	}
	// without the entry both directions fail
	if report := Compare(served, rows, nil, nil, nil); report.OK() {
		t.Fatal("a wildcard-served row passed without its closed-list entry")
	}
}

func TestAStaleWildcardEntryFails(t *testing.T) {
	rows := []Row{rowOf("POST", "/api/v1/admin/ip-allowlist/check")}
	wild := []Wildcard{{Method: "POST", Route: "/api/v1/admin/ip-allowlist/check", Wildcard: "/api/v1/admin/ip-allowlist/{entry_id}", Site: "x:1"}}
	// the wildcard is no longer registered
	if report := Compare(servedOf("go-api", Pair{"GET", "/other"}), append(rows, rowOf("GET", "/other")), wild, nil, nil); report.OK() {
		t.Fatal("a wildcard entry naming an unregistered wildcard passed")
	}
	// the literal became a registered route
	served := servedOf("go-api", Pair{"POST", "/api/v1/admin/ip-allowlist/check"}, Pair{"POST", "/api/v1/admin/ip-allowlist/{}"})
	if report := Compare(served, rows, wild, nil, nil); report.OK() || !strings.Contains(problems(report), "registered route now") {
		t.Fatalf("a wildcard entry for a now-registered route passed: %q", problems(report))
	}
	// the row is gone
	if report := Compare(servedOf("go-api", Pair{"POST", "/api/v1/admin/ip-allowlist/{}"}), nil, wild, nil, nil); report.OK() {
		t.Fatal("a wildcard entry with no profile row passed")
	}
}

func TestAPythonOnlyEntryExplainsARowAndGoesStaleWhenGoServesIt(t *testing.T) {
	rows := []Row{rowOf("GET", "/docs")}
	entries := []PythonOnly{{Method: "GET", Route: "/docs", Ref: "CHAOS-7048", Reason: "FastAPI docs dropped"}}
	if report := Compare(nil, rows, nil, entries, nil); !report.OK() {
		t.Fatalf("an explained python-only row failed: %s", problems(report))
	}
	if report := Compare(servedOf("go-api", Pair{"GET", "/docs"}), rows, nil, entries, nil); report.OK() {
		t.Fatal("a python-only entry for a route Go now serves passed")
	}
	if report := Compare(nil, nil, nil, entries, nil); report.OK() {
		t.Fatal("a python-only entry with no profile row passed")
	}
	entries[0].Ref = ""
	if report := Compare(nil, rows, nil, entries, nil); report.OK() {
		t.Fatal("a python-only entry without a ticket or decision id passed")
	}
}

func TestNormalizeWritesEveryParameterAsBraces(t *testing.T) {
	if got := Normalize("/api/v1/a/{org_id}/b/{first}/{second}"); got != "/api/v1/a/{}/b/{}/{}" {
		t.Fatalf("Normalize = %q", got)
	}
}

func TestARefusalStubNeedsNoRowAndGoesStaleWhenItHasOneOrIsGone(t *testing.T) {
	stubs := []Stub{{Method: "HEAD", Pattern: "/api/v1/billing/plans/pull-stripe", Probe: "/api/v1/billing/plans/pull-stripe", Status: 405, Site: "routes.go:92"}}
	served := servedOf("go-api", Pair{"HEAD", "/api/v1/billing/plans/pull-stripe"})
	if report := Compare(served, nil, nil, nil, stubs); !report.OK() {
		t.Fatalf("a refusal stub needed a row: %s", problems(report))
	}
	if report := Compare(served, nil, nil, nil, nil); report.OK() {
		t.Fatal("a stub registration with no stub entry and no row passed")
	}
	if report := Compare(nil, nil, nil, nil, stubs); report.OK() {
		t.Fatal("a stub entry for a route nobody registers passed")
	}
	report := Compare(served, []Row{rowOf("HEAD", "/api/v1/billing/plans/pull-stripe")}, nil, nil, stubs)
	if report.OK() {
		t.Fatal("a stub entry for a route that has a row passed")
	}
}

// A wildcard entry covers exactly its method AND its registered wildcard pattern: a served
// pair on the same wildcard with another method, or on another wildcard with the same
// method, still needs its own row.
func TestAWildcardEntryCoversOnlyItsMethodAndItsPattern(t *testing.T) {
	rows := []Row{rowOf("POST", "/api/v1/admin/ip-allowlist/check")}
	wild := []Wildcard{{Method: "POST", Route: "/api/v1/admin/ip-allowlist/check", Wildcard: "/api/v1/admin/ip-allowlist/{entry_id}", Site: "x:1"}}
	base := servedOf("go-api", Pair{"POST", "/api/v1/admin/ip-allowlist/{}"})
	for name, extra := range map[string]Pair{
		"same wildcard, another method":  {"GET", "/api/v1/admin/ip-allowlist/{}"},
		"another wildcard, same method":  {"POST", "/api/v1/unrelated/{}"},
		"another wildcard, other method": {"DELETE", "/api/v1/unrelated/{}"},
	} {
		report := Compare(append(base, servedOf("go-api", extra)...), rows, wild, nil, nil)
		if report.OK() || !strings.Contains(problems(report), "served by Go with NO profile row: "+extra.String()) {
			t.Errorf("%s: a served pair no entry covers passed: %q", name, problems(report))
		}
	}
}

// A wildcard entry whose registered wildcard is gone is stale AND leaves its row unserved
// (direction 2 does not take the entry without the registration).
func TestAWildcardEntryWithoutItsRegistrationLeavesItsRowUnserved(t *testing.T) {
	rows := []Row{rowOf("POST", "/api/v1/admin/ip-allowlist/check"), rowOf("GET", "/other")}
	wild := []Wildcard{{Method: "POST", Route: "/api/v1/admin/ip-allowlist/check", Wildcard: "/api/v1/admin/ip-allowlist/{entry_id}", Site: "x:1"}}
	report := Compare(servedOf("go-api", Pair{"GET", "/other"}), rows, wild, nil, nil)
	if len(report.StaleWildcard) != 1 || !strings.Contains(report.StaleWildcard[0], "which no Go service registers") {
		t.Errorf("stale wildcard report = %v", report.StaleWildcard)
	}
	if len(report.ProfileOnly) != 1 || report.ProfileOnly[0].String() != "POST /api/v1/admin/ip-allowlist/check" {
		t.Errorf("the row of an entry without its registration was taken as served: %v", report.ProfileOnly)
	}
}

// A closed-list line with the wrong number of columns, or an empty column, is refused.
func TestAListLineWithTheWrongColumnCountIsRefused(t *testing.T) {
	dir := t.TempDir()
	for name, line := range map[string]string{
		"five columns":  "POST\t/a\t/b/{x}\tl\tsite.go:1",
		"seven columns": "POST\t/a\t/b/{x}\tl\tsite.go:1\tunit\textra",
		"empty column":  "POST\t/a\t/b/{x}\t\tsite.go:1\tunit",
	} {
		path := filepath.Join(dir, strings.ReplaceAll(name, " ", "_")+".tsv")
		if err := os.WriteFile(path, []byte("# header\n"+line+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadWildcards(path); err == nil {
			t.Errorf("%s: a malformed list line was read", name)
		}
	}
	good := filepath.Join(dir, "good.tsv")
	if err := os.WriteFile(good, []byte("POST\t/a\t/b/{x}\tl\tsite.go:1\tunit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if rows, err := LoadWildcards(good); err != nil || len(rows) != 1 {
		t.Errorf("a well-formed line: %v %v", rows, err)
	}
}
