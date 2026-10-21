package docstools_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/cigates/docstools"
)

// The refusal inventory of check-code-prerequisites, built like the one of check-candidate-facts: every refusing site of
// the frozen Python script has a row in testdata/refusals/check-code-prerequisites.tsv naming the Go answer.

var codePrereqRefusalSite = regexp.MustCompile(`raise |SystemExit|sys\.exit|\.read_text\(|\.is_file\(|return 1|^\s*except |parse_args\(|safe_load\(`)

func codePrereqInventory(t *testing.T) map[string]refusalRow {
	t.Helper()
	rows := map[string]refusalRow{}
	for _, line := range strings.Split(mustRead(t, filepath.Join("testdata", "refusals", "check-code-prerequisites.tsv")), "\n") {
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

func codePrereqSites(t *testing.T) []string {
	t.Helper()
	seen := map[string]int{}
	var sites []string
	for _, line := range strings.Split(mustRead(t, filepath.Join("testdata", "refusals", "check_code_prerequisites.py.frozen")), "\n") {
		if !codePrereqRefusalSite.MatchString(line) {
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

func TestEveryPythonRefusalOfCodePrerequisitesHasAGoRow(t *testing.T) {
	inventory := codePrereqInventory(t)
	sites := codePrereqSites(t)
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
		if !listed[site] && !strings.HasSuffix(site, "#unreadable") {
			t.Errorf("inventory row %q matches no refusing site of the frozen script", site)
		}
	}
}

func TestCodePrerequisitesRefusalRowsRunLiveThroughTheCLI(t *testing.T) {
	inventory := codePrereqInventory(t)
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

func TestCodePrerequisitesUnreadableFiles(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode 000 file")
	}
	for name, unreadable := range map[string]string{"scope": "docs/code-prerequisite-scope.yml", "page": "docs/g.md"} {
		t.Run(name, func(t *testing.T) {
			work := t.TempDir()
			for rel, text := range map[string]string{"docs/code-prerequisite-scope.yml": "- g.md\n", "docs/g.md": "text\n"} {
				mode := os.FileMode(0o644)
				if rel == unreadable {
					mode = 0o000
				}
				if err := os.MkdirAll(filepath.Dir(filepath.Join(work, rel)), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(work, rel), []byte(text), mode); err != nil {
					t.Fatal(err)
				}
			}
			t.Chdir(work)
			code, stdout, stderr := runDocs("check-code-prerequisites")
			if code != 1 || stdout != "" || !strings.HasPrefix(stderr, "PermissionError: ") || strings.Count(stderr, "\n") != 1 {
				t.Errorf("unreadable %s: exit %d, stdout %q, stderr %q; want exit 1, no stdout, one PermissionError line", name, code, stdout, stderr)
			}
		})
	}
}

// Named difference from argparse: an abbreviated option is refused, not accepted.
func TestCodePrerequisitesRefusesAbbreviatedOptions(t *testing.T) {
	for _, arg := range []string{"--sc", "--scope-", "--docs", "--help-me", "-hx", "--"} {
		code, stdout, stderr := runDocs("check-code-prerequisites", arg, "x")
		if code != 2 || stdout != "" || !strings.Contains(stderr, "unrecognized arguments: "+arg) || strings.Count(stderr, "\n") != 1 {
			t.Errorf("%s: exit %d, stdout %q, stderr %q; want exit 2 and one unrecognized-arguments line", arg, code, stdout, stderr)
		}
	}
}

// The scope reader accepts a narrow grammar (an empty file, or a block sequence of plain or quoted scalars with
// whole-line comments); PyYAML accepts more. testdata/stricter-scope holds scope files Python loads (python-rc and
// python-stdout record its answer) that the Go verb refuses with the one load-error line and exit code 1.
func TestCodePrerequisitesIsStricterThanPython(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("testdata", "stricter-scope"))
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) < 10 {
		t.Fatalf("no stricter-scope cases under %s: %v", root, err)
	}
	for _, entry := range entries {
		dir := filepath.Join(root, entry.Name())
		t.Run(entry.Name(), func(t *testing.T) {
			if strings.TrimSpace(mustRead(t, filepath.Join(dir, "python-rc"))) == "1" && mustRead(t, filepath.Join(dir, "python-stdout")) == "" {
				t.Fatal("Python refused this input too: it belongs under parity, not stricter-scope")
			}
			work := t.TempDir()
			in := filepath.Join(dir, "in")
			_ = filepath.Walk(in, func(path string, info os.FileInfo, err error) error {
				if err == nil && info.Mode().IsRegular() {
					rel, _ := filepath.Rel(in, path)
					writeTree(t, filepath.Join(work, rel), mustRead(t, path), 0o644)
				}
				return nil
			})
			t.Chdir(work)
			code, stdout, stderr := runDocs("check-code-prerequisites")
			want := strings.ReplaceAll(mustRead(t, filepath.Join(dir, "stderr.traceback")), "{WORK}", work)
			if code != 1 || stdout != "" || stderr != want {
				t.Errorf("exit %d, stdout %q, stderr %q; want exit 1, no stdout, stderr %q", code, stdout, stderr, want)
			}
		})
	}
}

func TestCodePrerequisitesUnicodeTableMatchesGo(t *testing.T) {
	if unicode.Version != docstools.CodePrereqUnicodeGoVersion {
		t.Fatalf("Go unicode.Version is %s, the word table of check-code-prerequisites was generated for %s: generate it again (testdata/README.md, \"Unicode word table\")", unicode.Version, docstools.CodePrereqUnicodeGoVersion)
	}
	n := 0
	for _, rg := range docstools.CodePrereqGoOnlyWord {
		for r := rg[0]; r <= rg[1]; r++ {
			n++
			if !unicode.IsLetter(r) && !unicode.IsNumber(r) {
				t.Errorf("U+%04X is in the table but is no Go word character", r)
			}
		}
	}
	if n != docstools.CodePrereqUnicodeGoOnly {
		t.Errorf("the table holds %d code points, the header says %d", n, docstools.CodePrereqUnicodeGoOnly)
	}
}
