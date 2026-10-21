package docstools_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The refusal inventories of check-docs-links, check-freshness and check-external-links: every site of the Python
// script (frozen under testdata/refusals) that refuses or can raise is a row of testdata/refusals/<verb>.tsv naming the
// Go answer: recorded pairs replayed through the built cigate binary, or named Go tests. A site with no row, a row with
// no site, a case that does not exist and a Go test that does not exist all fail. A row "<site>#<word>" adds answers
// for a site.

var linkRefusalSite = regexp.MustCompile(`raise |SystemExit|sys\.exit|return 1|^\s*except |\.read_text\(|yaml\.safe_load\(|fromisoformat\(|urlsplit\(|\.resolve\(|parse_args\(|\.port\b|\.is_dir\(\)|\.is_file\(\)|\.exists\(\)|opener\.open\(|Request\(|rglob\(`)

var linkInventories = []struct{ verb, frozen string }{
	{"check-docs-links", "check_docs_links.py.frozen"},
	{"check-freshness", "check_freshness_inventory.py.frozen"},
	{"check-external-links", "check_external_links.py.frozen"},
}

func linkSites(t *testing.T, frozen string) []string {
	t.Helper()
	seen := map[string]int{}
	var sites []string
	for _, line := range strings.Split(mustRead(t, filepath.Join("testdata", "refusals", frozen)), "\n") {
		if !linkRefusalSite.MatchString(line) {
			continue
		}
		text := strings.TrimSpace(line)
		seen[text]++
		key := text
		if seen[text] > 1 || text == "return 1" {
			key = text + "#" + strconv.Itoa(seen[text])
		}
		sites = append(sites, key)
	}
	return sites
}

func linkInventory(t *testing.T, verb string) map[string]refusalRow {
	t.Helper()
	rows := map[string]refusalRow{}
	for _, line := range strings.Split(mustRead(t, filepath.Join("testdata", "refusals", verb+".tsv")), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) != 3 || parts[2] == "" {
			t.Fatalf("%s: row %q is not site<TAB>kind<TAB>refs", verb, line)
		}
		if _, dup := rows[parts[0]]; dup {
			t.Fatalf("%s: row %q twice", verb, parts[0])
		}
		rows[parts[0]] = refusalRow{parts[1], parts[2]}
	}
	return rows
}

func TestEveryPythonRefusalOfTheLinkGatesHasAGoRow(t *testing.T) {
	for _, inv := range linkInventories {
		rows := linkInventory(t, inv.verb)
		sites := linkSites(t, inv.frozen)
		if len(sites) == 0 {
			t.Fatalf("%s: the frozen script has no refusing site: the scanner is broken", inv.verb)
		}
		listed := map[string]bool{}
		for _, site := range sites {
			listed[site] = true
			if _, ok := rows[site]; !ok {
				t.Errorf("%s: Python site %q has no row", inv.verb, site)
			}
		}
		for site := range rows {
			base := site
			if i := strings.LastIndex(site, "#"); i > 0 && !listed[site] {
				base = site[:i]
			}
			if !listed[site] && !listed[base] {
				t.Errorf("%s: row %q matches no site of the frozen script", inv.verb, site)
			}
		}
	}
}

func TestLinkGateRefusalRowsRunLiveThroughTheCLI(t *testing.T) {
	bin := buildCigate(t)
	tests, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	var testText strings.Builder
	for _, file := range tests {
		testText.WriteString(mustRead(t, file))
	}
	for _, inv := range linkInventories {
		ran := map[string]bool{}
		for site, row := range linkInventory(t, inv.verb) {
			for _, ref := range strings.Split(row.refs, ";") {
				switch row.kind {
				case "case":
					if !strings.HasPrefix(ref, inv.verb+"-") {
						t.Errorf("%s: row %q names case %s of another verb", inv.verb, site, ref)
					}
					if !ran[ref] {
						ran[ref] = true
						runLinkCase(t, bin, ref)
					}
				case "go":
					if !strings.Contains(testText.String(), "func "+ref+"(") {
						t.Errorf("%s: row %q names Go test %s, which does not exist", inv.verb, site, ref)
					}
				default:
					t.Errorf("%s: row %q has unknown kind %q", inv.verb, site, row.kind)
				}
			}
		}
	}
}

// runLinkCase replays one recorded pair through the binary: exit code, stdout and stderr (exact, the one traceback
// line, or one line with the recorded prefix).
func runLinkCase(t *testing.T, bin, name string) {
	t.Helper()
	dir := filepath.Join("testdata", "parity", name)
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("case %s: %v", name, err)
		return
	}
	work := t.TempDir()
	in := filepath.Join(dir, "in")
	_ = filepath.Walk(in, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			rel, _ := filepath.Rel(in, path)
			rel = strings.ReplaceAll(strings.TrimSuffix(rel, ".fixture"), "{E9}", "é")
			writeTree(t, filepath.Join(work, rel), mustRead(t, path), info.Mode().Perm())
		}
		return nil
	})
	seedSymlinks(t, filepath.Join(dir, "symlinks"), work)
	subst := func(s string) string { return strings.ReplaceAll(s, "{WORK}", work) }
	var args []string
	for _, a := range strings.Split(strings.TrimSuffix(mustRead(t, filepath.Join(dir, "args")), "\n"), "\n") {
		args = append(args, subst(a))
	}
	cmd := exec.Command(bin, append([]string{"docs"}, args...)...)
	cmd.Dir = work
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	code := 0
	if err := cmd.Run(); err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatal(err)
		}
		code = ee.ExitCode()
	}
	if want := strings.TrimSpace(mustRead(t, filepath.Join(dir, "rc"))); want != strconv.Itoa(code) {
		t.Errorf("%s: exit %d, recorded %s", name, code, want)
	}
	if want := subst(mustRead(t, filepath.Join(dir, "stdout"))); stdout.String() != want {
		t.Errorf("%s: stdout %q, recorded %q", name, stdout.String(), want)
	}
	got := stderr.String()
	if line, err := os.ReadFile(filepath.Join(dir, "stderr.traceback")); err == nil {
		if got != subst(string(line)) {
			t.Errorf("%s: stderr %q, recorded %q", name, got, subst(string(line)))
		}
	} else if prefix, err := os.ReadFile(filepath.Join(dir, "stderr.prefix")); err == nil {
		if want := subst(strings.TrimSuffix(string(prefix), "\n")); !strings.HasPrefix(got, want) || strings.Count(got, "\n") != 1 {
			t.Errorf("%s: stderr %q is not one line starting %q", name, got, want)
		}
	} else if want := subst(mustRead(t, filepath.Join(dir, "stderr"))); got != want {
		t.Errorf("%s: stderr %q, recorded %q", name, got, want)
	}
}
