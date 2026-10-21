package releasetag_test

import (
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The refusal inventory of auto_tag: every site of the Python script that can refuse (a call that can raise, a caught
// exception, an argument parse) is a row of testdata/refusals/auto_tag.tsv, and the row names the Go answer: recorded cases
// replayed through the built cigate binary (kind case), cases where Go is stricter than Python (kind stricter), or a named
// Go test (kind go). A Python refusal with no row fails here, so a port cannot drop one unnoticed.

var refusalSite = regexp.MustCompile(`check_output\(|\.decode\(\)|^\s*except |^\s*with open\(|json\.load\(|data\.get\(|re\.match\(|\bint\(|parse_args\(|pre_num \+ 1|patch \+ 1|print\(f"(Package|Next)|\bf\.write\(`)

type refusalRow struct{ kind, refs string }

func refusalInventory(t *testing.T) map[string]refusalRow {
	t.Helper()
	rows := map[string]refusalRow{}
	for _, line := range strings.Split(mustRead(t, filepath.Join("testdata", "refusals", "auto_tag.tsv")), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) != 3 {
			t.Fatalf("inventory row %q has %d columns, want 3", line, len(parts))
		}
		if _, dup := rows[parts[0]]; dup {
			t.Fatalf("inventory row %q twice", parts[0])
		}
		rows[parts[0]] = refusalRow{parts[1], parts[2]}
	}
	return rows
}

func pythonRefusalSites(t *testing.T) []string {
	t.Helper()
	seen := map[string]int{}
	var sites []string
	for _, line := range strings.Split(mustRead(t, filepath.Join("testdata", "refusals", "auto_tag.py.frozen")), "\n") {
		if !refusalSite.MatchString(line) {
			continue
		}
		text := strings.TrimSpace(line)
		seen[text]++
		key := text
		if seen[text] > 1 {
			key = text + "#" + strconv.Itoa(seen[text])
		}
		sites = append(sites, key)
	}
	return sites
}

func TestEveryPythonRefusalOfAutoTagHasAGoRow(t *testing.T) {
	inventory := refusalInventory(t)
	sites := pythonRefusalSites(t)
	if len(sites) < 15 {
		t.Fatalf("the frozen script has %d refusing sites: the scanner is broken", len(sites))
	}
	listed := map[string]bool{}
	for _, site := range sites {
		listed[site] = true
		row, ok := inventory[site]
		if !ok {
			t.Errorf("Python refusal %q has no row in the refusal inventory", site)
			continue
		}
		if row.refs == "" {
			t.Errorf("row %q names no Go answer", site)
		}
	}
	for site := range inventory {
		if !listed[site] && !strings.HasSuffix(site, "#stricter") && !strings.HasPrefix(site, "implicit: ") {
			t.Errorf("inventory row %q matches no refusing site of the frozen script", site)
		}
		if base, ok := strings.CutSuffix(site, "#stricter"); ok && !listed[base] {
			t.Errorf("inventory row %q is stricter than a site the frozen script does not have", site)
		}
	}
}

func TestAutoTagRefusalRowsRunLiveThroughTheCLI(t *testing.T) {
	inventory := refusalInventory(t)
	bin := cigate(t)
	tests, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	var testText strings.Builder
	for _, file := range tests {
		testText.WriteString(mustRead(t, file))
	}
	ran := map[string]bool{}
	for site, row := range inventory {
		for _, ref := range strings.Split(row.refs, ";") {
			switch row.kind {
			case "case", "stricter":
				dir := filepath.Join("testdata", "parity", ref)
				if !exists(dir) {
					t.Errorf("row %q names case %s, which does not exist", site, ref)
					continue
				}
				if marked := exists(filepath.Join(dir, "stricter")); marked != (row.kind == "stricter") {
					t.Errorf("row %q is kind %s but case %s has stricter marker %v", site, row.kind, ref, marked)
				}
				if !ran[ref] {
					ran[ref] = true
					checkCase(t, bin, ref)
				}
			case "go":
				if !strings.Contains(testText.String(), "func "+ref+"(") {
					t.Errorf("row %q names Go test %s, which does not exist", site, ref)
				}
			default:
				t.Errorf("row %q has unknown kind %q", site, row.kind)
			}
		}
	}
}

func TestEveryStricterCaseIsInTheInventory(t *testing.T) {
	named := map[string]bool{}
	for _, row := range refusalInventory(t) {
		if row.kind == "stricter" {
			for _, ref := range strings.Split(row.refs, ";") {
				named[ref] = true
			}
		}
	}
	for _, name := range parityCases(t) {
		if exists(filepath.Join("testdata", "parity", name, "stricter")) && !named[name] {
			t.Errorf("stricter case %s is in no inventory row: name it", name)
		}
	}
}
