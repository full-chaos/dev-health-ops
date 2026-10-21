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
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// The refusal inventory of check-candidate-facts: every site of the Python script that refuses (a raise, an exit, a
// stdlib call that can fail, a caught exception) is a row of testdata/refusals/check-candidate-facts.tsv, and the row
// names the Go answer: parity cases replayed through the built cigate binary, or a named Go test. A Python refusal with
// no row fails here, so a port cannot drop one unnoticed.

var refusalSite = regexp.MustCompile(`raise |SystemExit|sys\.exit|\.read_text\(|ast\.parse\(|\.is_file\(|return 1|^\s*except `)

type refusalRow struct{ kind, refs string }

// refusalInventory reads testdata/refusals/<name>: site TAB kind TAB refs.
func refusalInventory(t *testing.T, name string) map[string]refusalRow {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "refusals", name))
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string]refusalRow{}
	for _, line := range strings.Split(string(raw), "\n") {
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

// pythonRefusalSites lists the lines of a frozen script that siteRe matches; the Nth repeat of a line (and every
// "return 1") gets "#N".
func pythonRefusalSites(t *testing.T, frozen string, siteRe *regexp.Regexp) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "refusals", frozen))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	var sites []string
	for _, line := range strings.Split(string(raw), "\n") {
		if !siteRe.MatchString(line) {
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

// checkRefusalInventory fails on a refusing site with no row, a row with no Go answer, and a row that matches no
// site (rows ending in an extraSuffix name a second answer for a site, such as "#unreadable").
func checkRefusalInventory(t *testing.T, inventory map[string]refusalRow, sites []string, extraSuffixes ...string) {
	t.Helper()
	if len(sites) == 0 {
		t.Fatal("the frozen script has no refusing site: the scanner is broken")
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
		extra := false
		for _, suffix := range extraSuffixes {
			if strings.HasSuffix(site, suffix) && listed[strings.TrimSuffix(site, suffix)] {
				extra = true
			}
		}
		if !listed[site] && !extra {
			t.Errorf("inventory row %q matches no refusing site of the frozen script", site)
		}
	}
}

func buildCigate(t *testing.T) string {
	t.Helper()
	root := driftRepoRoot(t)
	bin := filepath.Join(t.TempDir(), "cigate")
	cmd := exec.Command("go", "build", "-trimpath", "-mod=readonly", "-o", bin, "./cmd/cigate")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build cigate: %v\n%s", err, out)
	}
	return bin
}

func runLiveCase(t *testing.T, bin, name string) {
	t.Helper()
	dir := filepath.Join("testdata", "parity", name)
	work := t.TempDir()
	in := filepath.Join(dir, "in")
	_ = filepath.Walk(in, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			rel, _ := filepath.Rel(in, path)
			dst := filepath.Join(work, rel)
			if mkErr := os.MkdirAll(filepath.Dir(dst), 0o755); mkErr != nil {
				t.Fatal(mkErr)
			}
			data, rerr := os.ReadFile(path)
			if rerr != nil {
				t.Fatal(rerr)
			}
			if werr := os.WriteFile(dst, data, 0o644); werr != nil {
				t.Fatal(werr)
			}
		}
		return nil
	})
	args := strings.Fields(mustRead(t, filepath.Join(dir, "args")))
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
	subst := func(s string) string { return strings.ReplaceAll(s, "{WORK}", work) }
	if want := strings.TrimSpace(mustRead(t, filepath.Join(dir, "rc"))); want != strconv.Itoa(code) {
		t.Errorf("%s: exit %d, recorded %s", name, code, want)
	}
	if want := subst(mustRead(t, filepath.Join(dir, "stdout"))); stdout.String() != want {
		t.Errorf("%s: stdout %q, recorded %q", name, stdout.String(), want)
	}
	if line, err := os.ReadFile(filepath.Join(dir, "stderr.traceback")); err == nil {
		if want := subst(string(line)); stderr.String() != want {
			t.Errorf("%s: stderr %q, recorded %q", name, stderr.String(), want)
		}
	} else if stderr.Len() != 0 {
		t.Errorf("%s: stderr %q, recorded none", name, stderr.String())
	}
}

func TestEveryPythonRefusalOfCandidateFactsHasAGoRow(t *testing.T) {
	checkRefusalInventory(t, refusalInventory(t, "check-candidate-facts.tsv"),
		pythonRefusalSites(t, "check_docs_candidate_facts.py.frozen", refusalSite), "#unreadable")
}

func TestCandidateFactsRefusalRowsRunLiveThroughTheCLI(t *testing.T) {
	inventory := refusalInventory(t, "check-candidate-facts.tsv")
	bin := buildCigate(t)
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
			case "case":
				if !ran[ref] {
					ran[ref] = true
					runLiveCase(t, bin, ref)
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

func TestCandidateFactsRegistryIsCompiledAndShaped(t *testing.T) {
	key := regexp.MustCompile("^[a-z][a-z_]*(?:\\.[a-z][a-z_]*)?$")
	themes := map[string]bool{}
	for _, theme := range units.SortedThemes {
		if !utf8.ValidString(theme) || !key.MatchString(theme) || strings.Contains(theme, ".") || themes[theme] {
			t.Errorf("theme %q is not a unique plain key", theme)
		}
		themes[theme] = true
	}
	if len(themes) == 0 {
		t.Fatal("no themes")
	}
	seen := map[string]bool{}
	for _, sub := range units.SortedSubcategories {
		theme, _, dotted := strings.Cut(sub, ".")
		if !key.MatchString(sub) || !dotted || !themes[theme] || seen[sub] {
			t.Errorf("subcategory %q is not a unique key of a known theme", sub)
		}
		seen[sub] = true
	}
	if len(seen) == 0 {
		t.Fatal("no subcategories")
	}
}

func TestCandidateFactsUnreadablePage(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode 000 file")
	}
	work := t.TempDir()
	page := filepath.Join(work, "docs", "reference", "taxonomies", "investment.md")
	if err := os.MkdirAll(filepath.Dir(page), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(page, []byte("`risk`\n"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Chdir(work)
	code, stdout, stderr := runDocs("check-candidate-facts")
	if code != 1 || stdout != "" || !strings.HasPrefix(stderr, "OSError: ") {
		t.Errorf("unreadable page: exit %d, stdout %q, stderr %q; want exit 1, no stdout, an OSError line", code, stdout, stderr)
	}
}
