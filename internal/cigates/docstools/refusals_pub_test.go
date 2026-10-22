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

// The refusal inventory of validate-publication, built like the ones of check-candidate-facts and check-code-prerequisites:
// every refusing or failing site of the frozen Python script (testdata/refusals/validate_docs_v2_publication.py.frozen, the
// script at the merge base) has a row in testdata/refusals/validate-publication.tsv. A row names the parity pairs
// (case:), the rows of the refusal table in cmd/cigate/docs_pub_refusals_test.go (table:) and the helper corpus tests (go:)
// that answer it.

const pubInventoryRows = 39

var pubRefusalSite = regexp.MustCompile(`safe_load\(|\.read_text\(|\.write_text\(|\.is_file\(|\.resolve\(|urlparse\(|with_suffix\(|DictReader\(|\.open\(|\.mkdir\(|sorted\(|\.split\(\)\[0\]|return 1|raise SystemExit|\.rglob\(|\.glob\(|relative_to\(|\.get\(.*\)\.strip\(\)|str\(`)

func pubInventory(t *testing.T) map[string]string {
	t.Helper()
	rows := map[string]string{}
	for _, line := range strings.Split(mustRead(t, filepath.Join("testdata", "refusals", "validate-publication.tsv")), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) != 2 || parts[1] == "" {
			t.Fatalf("inventory row %q has %d columns or no refs, want 2", line, len(parts))
		}
		if _, dup := rows[parts[0]]; dup {
			t.Fatalf("inventory row %q twice", parts[0])
		}
		rows[parts[0]] = parts[1]
	}
	return rows
}

func pubSites(t *testing.T) []string {
	t.Helper()
	seen := map[string]int{}
	var sites []string
	for _, line := range strings.Split(mustRead(t, filepath.Join("testdata", "refusals", "validate_docs_v2_publication.py.frozen")), "\n") {
		if !pubRefusalSite.MatchString(line) {
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

func TestEveryPythonRefusalOfValidatePublicationHasAGoRow(t *testing.T) {
	inventory := pubInventory(t)
	sites := pubSites(t)
	if len(sites) != pubInventoryRows || len(inventory) != pubInventoryRows {
		t.Fatalf("%d sites in the frozen script, %d rows, pinned %d", len(sites), len(inventory), pubInventoryRows)
	}
	table := mustRead(t, filepath.Join("..", "..", "..", "cmd", "cigate", "docs_pub_refusals_test.go"))
	var testText strings.Builder
	tests, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range tests {
		testText.WriteString(mustRead(t, file))
	}
	listed := map[string]bool{}
	for _, site := range sites {
		listed[site] = true
		refs, ok := inventory[site]
		if !ok {
			t.Errorf("Python refusal %q has no row in the refusal inventory", site)
			continue
		}
		for _, ref := range strings.Split(refs, ";") {
			kind, name, _ := strings.Cut(ref, ":")
			switch kind {
			case "case":
				if _, err := os.Stat(filepath.Join("testdata", "parity", name)); err != nil {
					t.Errorf("row %q names parity case %s, which does not exist", site, name)
				}
			case "table":
				if !strings.Contains(table, `name: "`+name+`"`) {
					t.Errorf("row %q names table row %q, which does not exist", site, name)
				}
			case "go":
				if !strings.Contains(testText.String(), "func "+name+"(") {
					t.Errorf("row %q names Go test %s, which does not exist", site, name)
				}
			default:
				t.Errorf("row %q has the unknown reference %q", site, ref)
			}
		}
	}
	for site := range inventory {
		if !listed[site] {
			t.Errorf("inventory row %q matches no refusing site of the frozen script", site)
		}
	}
}

// runLivePubCase replays a parity pair through the built cigate binary: the files of in/ (a .fixture suffix dropped),
// the symlinks, the working directory, the exit code, stdout and the one stderr line.
func runLivePubCase(t *testing.T, bin, name string) {
	t.Helper()
	dir := filepath.Join("testdata", "parity", name)
	work, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	in := filepath.Join(dir, "in")
	_ = filepath.Walk(in, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			rel, _ := filepath.Rel(in, path)
			dst := filepath.Join(work, caseRel(strings.TrimSuffix(rel, ".fixture")))
			if mkErr := os.MkdirAll(filepath.Dir(dst), 0o755); mkErr != nil {
				t.Fatal(mkErr)
			}
			if wErr := os.WriteFile(dst, []byte(mustRead(t, path)), 0o644); wErr != nil {
				t.Fatal(wErr)
			}
		}
		return nil
	})
	seedSymlinks(t, filepath.Join(dir, "symlinks"), work)
	cwd := work
	if raw, err := os.ReadFile(filepath.Join(dir, "cwd")); err == nil {
		cwd = filepath.Join(work, strings.TrimSpace(string(raw)))
		if err := os.MkdirAll(cwd, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	args := strings.Split(strings.TrimSuffix(mustRead(t, filepath.Join(dir, "args")), "\n"), "\n")
	cmd := exec.Command(bin, append([]string{"docs"}, args...)...)
	cmd.Dir = cwd
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
	} else if want := subst(mustRead(t, filepath.Join(dir, "stderr"))); stderr.String() != want {
		t.Errorf("%s: stderr %q, recorded %q", name, stderr.String(), want)
	}
}

func TestValidatePublicationRefusalCasesRunLiveThroughTheCLI(t *testing.T) {
	bin := buildCigate(t)
	ran := map[string]bool{}
	for _, refs := range pubInventory(t) {
		for _, ref := range strings.Split(refs, ";") {
			if kind, name, _ := strings.Cut(ref, ":"); kind == "case" && !ran[name] {
				ran[name] = true
				runLivePubCase(t, bin, name)
			}
		}
	}
	if len(ran) < 100 {
		t.Fatalf("only %d parity cases are named by the inventory", len(ran))
	}
}
