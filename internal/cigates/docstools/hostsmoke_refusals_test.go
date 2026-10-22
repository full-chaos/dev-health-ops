package docstools_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The refusal inventory of host-smoke: every refusing site of scripts/check_docs_cloudflare_host.py at the merge base
// (frozen beside the table) is a row of testdata/refusals/host-smoke.tsv. columns: site, kind (case, go or mixed: informational), refs (";"-separated; recorded host-smoke cases
// (testdata/hostsmoke/<name>) that the live test replays through the built cigate binary; a ref "go:<Test>" is a named Go test.
// Where Go is stricter or its text differs the case has a go/ directory and the row's third column says "NAMED:".

const hostSmokeRefusalRows = 23

var hostSmokeSite = regexp.MustCompile(`raise |SystemExit|return 1|^\s*except |errors\.append\(|\.open\(|parse_args\(|required=True|\.read\(\)`)

type hostSmokeRow struct{ kind, refs, note string }

func hostSmokeInventory(t *testing.T) map[string]hostSmokeRow {
	t.Helper()
	rows := map[string]hostSmokeRow{}
	for _, line := range strings.Split(mustRead(t, filepath.Join("testdata", "refusals", "host-smoke.tsv")), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) != 4 {
			t.Fatalf("host-smoke row %q has %d columns, want 4", line, len(parts))
		}
		if _, dup := rows[parts[0]]; dup {
			t.Fatalf("host-smoke row %q twice", parts[0])
		}
		rows[parts[0]] = hostSmokeRow{parts[1], parts[2], parts[3]}
	}
	return rows
}

func hostSmokeSites(t *testing.T) []string {
	t.Helper()
	var texts []string
	count := map[string]int{}
	for _, line := range strings.Split(mustRead(t, filepath.Join("testdata", "refusals", "check_docs_cloudflare_host.py.frozen")), "\n") {
		if hostSmokeSite.MatchString(line) {
			text := strings.TrimSpace(line)
			texts = append(texts, text)
			count[text]++
		}
	}
	seen := map[string]int{}
	sites := make([]string, len(texts))
	for i, text := range texts {
		seen[text]++
		sites[i] = text
		if count[text] > 1 {
			sites[i] = text + "#" + strconv.Itoa(seen[text])
		}
	}
	return sites
}

func TestEveryPythonRefusalOfHostSmokeHasAGoRow(t *testing.T) {
	inventory := hostSmokeInventory(t)
	sites := hostSmokeSites(t)
	if len(sites) == 0 {
		t.Fatal("the frozen script has no refusing site: the scanner is broken")
	}
	if len(inventory) != hostSmokeRefusalRows || len(sites) != hostSmokeRefusalRows {
		t.Errorf("%d rows and %d sites, pinned %d", len(inventory), len(sites), hostSmokeRefusalRows)
	}
	listed := map[string]bool{}
	for _, site := range sites {
		listed[site] = true
		if _, ok := inventory[site]; !ok {
			t.Errorf("Python refusal %q has no row in host-smoke.tsv", site)
		}
	}
	for site := range inventory {
		if !listed[site] {
			t.Errorf("row %q matches no refusing site of the frozen script", site)
		}
	}
}

func TestHostSmokeRefusalRowsAnswerThroughRecordedCases(t *testing.T) {
	inventory := hostSmokeInventory(t)
	var testText strings.Builder
	tests, _ := filepath.Glob("*_test.go")
	for _, file := range tests {
		testText.WriteString(mustRead(t, file))
	}
	for site, row := range inventory {
		if row.refs == "" {
			t.Errorf("row %q names no answer", site)
		}
		for _, ref := range strings.Split(row.refs, ";") {
			if name, isGo := strings.CutPrefix(ref, "go:"); isGo {
				if !strings.Contains(testText.String(), "func "+name+"(") {
					t.Errorf("row %q names Go test %s, which does not exist", site, name)
				}
				continue
			}
			if _, err := os.Stat(filepath.Join("testdata", "hostsmoke", ref, "routes.json")); err != nil {
				t.Errorf("row %q names case %s: %v", site, ref, err)
			}
		}
	}
	// Every case with a go/ override is a named difference: it must appear in the table's NAMED list.
	cases, _ := filepath.Glob(filepath.Join("testdata", "hostsmoke", "*", "go"))
	var overridden []string
	for _, dir := range cases {
		overridden = append(overridden, filepath.Base(filepath.Dir(dir)))
	}
	sort.Strings(overridden)
	listed := strings.Fields(mustRead(t, filepath.Join("testdata", "hostsmoke", "NAMED.txt")))
	sort.Strings(listed)
	if strings.Join(overridden, " ") != strings.Join(listed, " ") {
		t.Errorf("cases with a go/ override %v, NAMED.txt lists %v", overridden, listed)
	}
}
