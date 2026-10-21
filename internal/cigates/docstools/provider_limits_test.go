package docstools_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

const (
	navWithPage       = "nav:\n  - Reference:\n    - reference/limits-and-compatibility/provider-limits.md\n"
	providerLimitsRel = "docs/reference/limits-and-compatibility/provider-limits.md"
)

// providerLimitsTree is a synthetic tree (never the real mkdocs.yml: a Go test that read it would need a go.yml
// filter, and go.yml must not list docs paths). The real tree is checked by docs-drift.yml.
func providerLimitsTree(t *testing.T, nav string, page bool) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "mkdocs.yml"), []byte(nav), 0o644); err != nil {
		t.Fatal(err)
	}
	if page {
		writeTree(t, filepath.Join(dir, filepath.FromSlash(providerLimitsRel)), "# Provider limits\n", 0o644)
	}
	return dir
}

// providerLimitsCases builds the tree for each inventory case; the key set must equal the inventory's case column.
var providerLimitsCases = map[string]func(t *testing.T) string{
	"page-absent": func(t *testing.T) string { return providerLimitsTree(t, navWithPage, false) },
	"page-is-dir": func(t *testing.T) string {
		dir := providerLimitsTree(t, navWithPage, false)
		mustMkdir(t, filepath.Join(dir, filepath.FromSlash(providerLimitsRel)))
		return dir
	},
	"page-dangling-symlink": func(t *testing.T) string {
		dir := providerLimitsTree(t, navWithPage, false)
		page := filepath.Join(dir, filepath.FromSlash(providerLimitsRel))
		mustMkdir(t, filepath.Dir(page))
		mustSymlink(t, "gone.md", page)
		return dir
	},
	"mkdocs-absent": func(t *testing.T) string {
		dir := providerLimitsTree(t, navWithPage, true)
		mustRemove(t, filepath.Join(dir, "mkdocs.yml"))
		return dir
	},
	"mkdocs-is-dir": func(t *testing.T) string {
		dir := providerLimitsTree(t, navWithPage, true)
		mustRemove(t, filepath.Join(dir, "mkdocs.yml"))
		mustMkdir(t, filepath.Join(dir, "mkdocs.yml"))
		return dir
	},
	"mkdocs-unreadable": func(t *testing.T) string {
		if os.Geteuid() == 0 {
			t.Skip("root reads a mode 000 file")
		}
		dir := providerLimitsTree(t, navWithPage, true)
		if err := os.Chmod(filepath.Join(dir, "mkdocs.yml"), 0); err != nil {
			t.Fatal(err)
		}
		return dir
	},
	"mkdocs-invalid-utf8": func(t *testing.T) string { return providerLimitsTree(t, navWithPage+"# \xff\n", true) },
	"mkdocs-surrogate":    func(t *testing.T) string { return providerLimitsTree(t, navWithPage+"# \xed\xa0\x80\n", true) },
	"nav-target-absent": func(t *testing.T) string {
		return providerLimitsTree(t, "nav:\n  - Reference:\n    - reference/other.md\n", true)
	},
	"nav-target-case-differs": func(t *testing.T) string {
		return providerLimitsTree(t, "nav:\n  - Reference:\n    - Reference/Limits-and-compatibility/Provider-limits.md\n", true)
	},
}

type providerLimitsRefusal struct {
	site, refusal, name, stderr string
	rc                          int
}

func readProviderLimitsInventory(t *testing.T) []providerLimitsRefusal {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "provider_limits_refusals.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var rows []providerLimitsRefusal
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cols := strings.Split(line, "\t")
		if len(cols) != 5 {
			t.Fatalf("inventory line %d: %d columns, want 5", n, len(cols))
		}
		rc, err := strconv.Atoi(cols[3])
		if err != nil || rc == 0 || cols[4] == "" {
			t.Fatalf("inventory line %d: a refusal row needs a nonzero rc and a stderr reason", n)
		}
		rows = append(rows, providerLimitsRefusal{site: cols[0], refusal: cols[1], name: cols[2], rc: rc, stderr: cols[4]})
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("empty refusal inventory")
	}
	return rows
}

var (
	cigateDir  string
	cigateOnce sync.Once
	cigateBin  string
	cigateErr  error
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "cigate-docs-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cigateDir = dir
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// liveCigate builds cmd/cigate once, so the inventory runs the CLI a workflow runs.
func liveCigate(t *testing.T) string {
	t.Helper()
	cigateOnce.Do(func() {
		root, err := moduleroot.Root()
		if err != nil {
			cigateErr = err
			return
		}
		cigateBin = filepath.Join(cigateDir, "cigate")
		cmd := exec.Command("go", "build", "-buildvcs=false", "-o", cigateBin, "./cmd/cigate")
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			cigateErr = errors.New(err.Error() + ": " + string(out))
		}
	})
	if cigateErr != nil {
		t.Fatalf("build cigate: %v", cigateErr)
	}
	return cigateBin
}

func runLiveProviderLimits(t *testing.T, dir string, args ...string) (int, string, string) {
	t.Helper()
	cmd := exec.Command(liveCigate(t), append([]string{"docs", "check-provider-limits"}, args...)...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	var exit *exec.ExitError
	if err != nil && !errors.As(err, &exit) {
		t.Fatal(err)
	}
	return cmd.ProcessState.ExitCode(), stdout.String(), stderr.String()
}

func TestCheckProviderLimitsPassesAWiredPage(t *testing.T) {
	code, stdout, stderr := runLiveProviderLimits(t, providerLimitsTree(t, navWithPage, true))
	if code != 0 || stderr != "" || stdout != "Provider limits page check passed\n" {
		t.Fatalf("rc %d stdout %q stderr %q", code, stdout, stderr)
	}
}

// Python's is_file() and read_text() follow symlinks, so a symlinked page or mkdocs.yml passed there.
func TestCheckProviderLimitsFollowsSymlinksLikePython(t *testing.T) {
	dir := providerLimitsTree(t, navWithPage, false)
	page := filepath.Join(dir, filepath.FromSlash(providerLimitsRel))
	writeTree(t, filepath.Join(dir, "real.md"), "# Provider limits\n", 0o644)
	mustMkdir(t, filepath.Dir(page))
	mustSymlink(t, filepath.Join(dir, "real.md"), page)
	mustRemove(t, filepath.Join(dir, "mkdocs.yml"))
	writeTree(t, filepath.Join(dir, "nav.yml"), navWithPage, 0o644)
	mustSymlink(t, "nav.yml", filepath.Join(dir, "mkdocs.yml"))
	if code, stdout, stderr := runLiveProviderLimits(t, dir); code != 0 || stderr != "" {
		t.Fatalf("rc %d stdout %q stderr %q", code, stdout, stderr)
	}
}

func TestCheckProviderLimitsUsageRefusal(t *testing.T) {
	code, stdout, stderr := runLiveProviderLimits(t, providerLimitsTree(t, navWithPage, true), "extra")
	if code != 2 || stdout != "" || !strings.Contains(stderr, "usage: cigate docs check-provider-limits") {
		t.Fatalf("rc %d stdout %q stderr %q", code, stdout, stderr)
	}
}

// TestCheckProviderLimitsRefusalInventory runs every Python refusal in the inventory through the live CLI: same
// nonzero exit, empty stdout, the reason on stderr.
func TestCheckProviderLimitsRefusalInventory(t *testing.T) {
	rows := readProviderLimitsInventory(t)
	seen := map[string]bool{}
	for _, row := range rows {
		if seen[row.name] {
			t.Fatalf("inventory case %q appears twice", row.name)
		}
		seen[row.name] = true
		build, ok := providerLimitsCases[row.name]
		if !ok {
			t.Errorf("Python refusal %s (%s) has no Go case %q", row.site, row.refusal, row.name)
			continue
		}
		t.Run(row.name, func(t *testing.T) {
			code, stdout, stderr := runLiveProviderLimits(t, build(t))
			if code != row.rc || stdout != "" || !strings.Contains(stderr, row.stderr) {
				t.Fatalf("%s (%s): rc %d stdout %q stderr %q, want rc %d, empty stdout, stderr naming %q",
					row.site, row.refusal, code, stdout, stderr, row.rc, row.stderr)
			}
		})
	}
	var extra []string
	for name := range providerLimitsCases {
		if !seen[name] {
			extra = append(extra, name)
		}
	}
	sort.Strings(extra)
	if len(extra) > 0 {
		t.Errorf("Go cases with no inventory row: %v", extra)
	}
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
}

func mustSymlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
}

func mustRemove(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
}
