package ownershipwindow

import (
	"crypto/sha256"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"time"
)

var names = Names{Org: "scope_org", AsOf: "scope_as_of"}

func TestWindowClampsBothBoundsToTheFirstRecord(t *testing.T) {
	window := Window(names)
	clamp := "greatest({scope_as_of:DateTime64(3, 'UTC')}, "
	if got := strings.Count(window, clamp); got != 2 {
		t.Fatalf("both bounds must test the clamped instant, found %d clamps:\n%s", got, window)
	}
	if !strings.HasPrefix(window, "o.valid_from <= "+clamp) || !strings.Contains(window, "\n      AND (o.valid_to IS NULL OR o.valid_to > "+clamp) {
		t.Errorf("the window is not valid_from <= instant AND (valid_to IS NULL OR valid_to > instant):\n%s", window)
	}
	if strings.HasPrefix(strings.TrimSpace(window), "AND") {
		t.Errorf("the window leads with AND, which every caller adds itself:\n%s", window)
	}
	if strings.Contains(window, "now64") || strings.Contains(window, "now()") {
		t.Errorf("the window resolves its own instant instead of the caller's:\n%s", window)
	}
}

func TestFirstRecordIsTheEarliestRowOfAnActiveTeam(t *testing.T) {
	first := firstRecords(names)
	for _, want := range []string{
		"min(o.valid_from)",
		"FROM team_repo_ownership AS o FINAL",
		"o.team_id != ''",
		"(o.repo_id IS NOT NULL OR r.matched = 1)",
		"lower(r.repo) = lower(o.repo_full_name)",
		"WHERE org_id = {scope_org:String}",
		"o.org_id = {scope_org:String}",
		"GROUP BY repo_key",
		"argMax(is_active",
		"trimBoth(o.team_id) NOT IN (",
	} {
		if !strings.Contains(first, want) {
			t.Errorf("the first-record subquery lacks %q:\n%s", want, first)
		}
	}
	if strings.Contains(first, "valid_to") {
		t.Errorf("the first record is the earliest start, whatever the end of the row:\n%s", first)
	}
}

func TestEveryRenderedNameIsAGivenPlaceholder(t *testing.T) {
	for name, text := range map[string]string{
		"Window":       Window(names),
		"ActiveTeam":   ActiveTeam(names),
		"InactiveTeam": InactiveTeams(names),
	} {
		for _, chunk := range strings.Split(text, "{")[1:] {
			placeholder, _, _ := strings.Cut(chunk, ":")
			if placeholder != names.Org && placeholder != names.AsOf {
				t.Errorf("%s renders a placeholder %q the caller did not name", name, placeholder)
			}
		}
	}
}

// The package is a leaf: both readers import it, so it imports no reader.
func TestPackageImportsNoReader(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), entry.Name(), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range file.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if strings.Contains(path, "internal/teamownership") || strings.Contains(path, "internal/queryapi") {
				t.Errorf("%s imports %s: the leaf must not import a reader", entry.Name(), path)
			}
		}
	}
}

func TestFirstRecordsQueryIsTheStatementTheWindowReads(t *testing.T) {
	rows := FirstRecordsQuery(names)
	if !strings.HasPrefix(rows, "SELECT "+RepoKey+" AS repo_key, min(o.valid_from) AS first_from") {
		t.Errorf("the statement does not give (repo_key, first_from):\n%s", rows)
	}
	if !strings.Contains(firstRecords(names), rows) {
		t.Errorf("the window's first-record subquery is not built from FirstRecordsQuery:\n%s", firstRecords(names))
	}
	if strings.Contains(rows, "source") || strings.Contains(rows, names.AsOf) {
		t.Errorf("the statement filters by source or reads the instant:\n%s", rows)
	}
	if strings.Count(rows, "{scope_org:String}") != 2+strings.Count(ActiveTeam(names), "{scope_org:String}") {
		t.Errorf("the statement names a placeholder the caller did not give:\n%s", rows)
	}
}

func TestClampAsOfIsTheLaterInstant(t *testing.T) {
	early, late := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name            string
		at, first, want time.Time
	}{
		{"before the first record", early, late, late},
		{"at the first record", late, late, late},
		{"after the first record", late, early, late},
		{"no record", early, time.Time{}, early},
	} {
		if got := ClampAsOf(tc.at, tc.first); !got.Equal(tc.want) {
			t.Errorf("%s: ClampAsOf = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The text every caller of the shared window sends, taken before the bound
// variant existed: the variant adds a statement, it changes none.
func TestTheSharedWindowTextIsUnchanged(t *testing.T) {
	digest := func(s string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(s))) }
	for _, tc := range []struct {
		n    Names
		want map[string]string
	}{
		{Names{Org: "org_id", AsOf: "as_of"}, map[string]string{
			"Window":            "d83b9e92d8402e2c022cc392a50b8cd60615761d324a2c9974d2fea395495f07",
			"FirstRecordsQuery": "596f50cb78f2d2040941147145aa01ddcad421b76db9354bdd87611f4c931c14",
			"EffectiveAsOf":     "87a42a135d58da038a7a29d91f2c243580ffc2a5a19ce99b2cadfdc1e8cf22b3",
			"ActiveTeam":        "8846f2ea495ef9db7bd345ae3942eb8d1b6ecdecaefee0b48f7363575e3031ea",
			"InactiveTeams":     "ccee8cd7d682df0e5f76cddfdb9f52c0368bafb1a230ed4e54788bf696a60cfc",
		}},
		{Names{Org: "scope_org", AsOf: "scope_as_of"}, map[string]string{
			"Window":            "2475bcf2447967347ff53da4b50e97b4b7536d7d519741a2a81fb5d104e19101",
			"FirstRecordsQuery": "d2582779490d6590e38f828c352b17d660e0eadc96ffaf8aa87d4c8158949cc9",
			"EffectiveAsOf":     "d3a27f2d1b1e462a56af6e02841c99342748fed37b000eb540eab8bc0810988b",
			"ActiveTeam":        "a51de29908eaf1d61d7f7d318aea4595a3a7b10f92db8919bbfda56b59c0966f",
			"InactiveTeams":     "566314996ec759fdb94ad66100bb8b48773417926837b4c6cea69a3426835a93",
		}},
	} {
		got := map[string]string{
			"Window":            digest(Window(tc.n)),
			"FirstRecordsQuery": digest(FirstRecordsQuery(tc.n)),
			"EffectiveAsOf":     digest(EffectiveAsOf(tc.n)),
			"ActiveTeam":        digest(ActiveTeam(tc.n)),
			"InactiveTeams":     digest(InactiveTeams(tc.n)),
		}
		for name, want := range tc.want {
			if got[name] != want {
				t.Errorf("%s(%s) changed: sha256 %s, want %s", name, tc.n.Org, got[name], want)
			}
		}
	}
}

// The bound variant is the shared window with only the active-team test of the
// first record replaced: no teams read, one bound array.
func TestTheBoundWindowIsTheSharedWindowOverTheBoundSet(t *testing.T) {
	bound := WindowBoundInactive(names, "scope_inactive")
	if strings.Contains(bound, "FROM teams") || strings.Contains(bound, "is_active") {
		t.Errorf("the bound window reads teams:\n%s", bound)
	}
	if got := strings.Count(bound, "{scope_inactive:Array(String)}"); got != 2 {
		t.Errorf("the bound window names the inactive set %d times, want 2 (one first record per bound):\n%s", got, bound)
	}
	if swapped := strings.ReplaceAll(bound, BoundActiveTeam("scope_inactive"), ActiveTeam(names)); swapped != Window(names) {
		t.Errorf("the bound window differs from the shared window in more than the active-team test:\n%s", bound)
	}
	for _, chunk := range strings.Split(bound, "{")[1:] {
		placeholder, _, _ := strings.Cut(chunk, ":")
		if placeholder != names.Org && placeholder != names.AsOf && placeholder != "scope_inactive" {
			t.Errorf("the bound window renders a placeholder %q the caller did not name", placeholder)
		}
	}
}
