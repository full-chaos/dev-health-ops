package ownershipwindow

import (
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
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
