package venuechanged

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// closureDir is where the dependency closure of every venue test package is committed.
const closureDir = "ci/venue_oracle_closure.d"

// venuePackages is the sorted set of package directories with a `run` row in the registry.
func venuePackages(t *testing.T, root string) []string {
	t.Helper()
	seen := map[string]bool{}
	files, _ := filepath.Glob(filepath.Join(root, "ci", "venue_oracle_registry.d", "*.tsv"))
	for _, file := range files {
		for _, row := range tsvRows(t, file) {
			if len(row) == 3 && row[2] == "run" {
				seen[row[0]] = true
			}
		}
	}
	var pkgs []string
	for pkg := range seen {
		pkgs = append(pkgs, pkg)
	}
	sort.Strings(pkgs)
	return pkgs
}

func closureFile(root, pkg string) string {
	return filepath.Join(root, filepath.FromSlash(closureDir), strings.ReplaceAll(pkg, "/", "__")+".txt")
}

// computeClosures asks the Go toolchain for the packages each test binary links: the in-module
// directories of `go list -test` Deps of the package's `.test` binary (its own, in-package and
// external test files included), minus the package's own directory (the package rule selects
// that one).
func computeClosures(t *testing.T, root string, pkgs []string) map[string][]string {
	t.Helper()
	args := []string{"list", "-mod=readonly", "-tags=integration", "-test", "-f", `{{.ImportPath}}{{"\t"}}{{join .Deps " "}}`}
	for _, pkg := range pkgs {
		args = append(args, "./"+pkg)
	}
	cmd := exec.Command("go", args...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.Output()
	if err != nil {
		detail := ""
		if exit, ok := err.(*exec.ExitError); ok {
			detail = string(exit.Stderr)
		}
		t.Fatalf("go list -test over %d venue package(s): %v\n%s", len(pkgs), err, detail)
	}
	closures := map[string][]string{}
	for _, line := range strings.Split(string(out), "\n") {
		importPath, deps, ok := strings.Cut(line, "\t")
		if !ok || !strings.HasSuffix(importPath, ".test") {
			continue
		}
		pkg := strings.TrimPrefix(strings.TrimSuffix(importPath, ".test"), modulePrefix)
		set := map[string]bool{}
		for _, dep := range strings.Fields(deps) {
			rel, in := strings.CutPrefix(dep, modulePrefix)
			if !in || rel == pkg {
				continue
			}
			// "pkg [pkg.test]" style test variants are not in Deps; an external test package
			// ("pkg_test") maps to its directory.
			rel = strings.TrimSuffix(rel, "_test")
			if rel == pkg {
				continue
			}
			set[rel] = true
		}
		var list []string
		for dir := range set {
			list = append(list, dir)
		}
		sort.Strings(list)
		closures[pkg] = list
	}
	for _, pkg := range pkgs {
		if _, ok := closures[pkg]; !ok {
			t.Fatalf("go list -test gave no test binary for the venue package %s", pkg)
		}
	}
	return closures
}

// committedClosures reads ci/venue_oracle_closure.d/ (package dir -> dependency dirs).
func committedClosures(t *testing.T, root string, pkgs []string) map[string]map[string]bool {
	t.Helper()
	closures := map[string]map[string]bool{}
	for _, pkg := range pkgs {
		data, err := os.ReadFile(closureFile(root, pkg))
		if err != nil {
			t.Fatalf("%s: no committed dependency closure (%v); regenerate with: %s", pkg, err, regenerate)
		}
		set := map[string]bool{}
		for _, line := range strings.Split(string(data), "\n") {
			if line != "" {
				set[line] = true
			}
		}
		closures[pkg] = set
	}
	return closures
}

const regenerate = "VENUE_CLOSURE_WRITE=1 go test ./internal/testsupport/venuechanged -run TestClosureFilesMatchTheTestBinaryDependencies"

// TestClosureFilesMatchTheTestBinaryDependencies holds the derived trigger set of the pull
// request venue job (CHAOS-9935): for every venue test package, ci/venue_oracle_closure.d/ lists
// the in-module packages its test binary links, and `venue-oracles --changed` selects the
// package's tests when a changed file lies in one of them. The file is what the Go toolchain says
// today, so a hand-written trigger row can no longer be the only thing between a dependency
// change and a skipped oracle.
func TestClosureFilesMatchTheTestBinaryDependencies(t *testing.T) {
	root := repoRoot(t)
	pkgs := venuePackages(t, root)
	if len(pkgs) == 0 {
		t.Fatal("no run rows found in ci/venue_oracle_registry.d/")
	}
	want := computeClosures(t, root, pkgs)
	write := os.Getenv("VENUE_CLOSURE_WRITE") == "1"
	if write {
		if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(closureDir)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	known := map[string]bool{}
	for _, pkg := range pkgs {
		body := strings.Join(want[pkg], "\n") + "\n"
		known[filepath.Base(closureFile(root, pkg))] = true
		if write {
			if err := os.WriteFile(closureFile(root, pkg), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		got, err := os.ReadFile(closureFile(root, pkg))
		if err != nil {
			t.Errorf("%s: no committed dependency closure (%v); regenerate with: %s", pkg, err, regenerate)
			continue
		}
		if string(got) != body {
			t.Errorf("%s: %s is stale against `go list -test`: %s\nregenerate with: %s", pkg, closureFile(root, pkg), diffLines(string(got), body), regenerate)
		}
	}
	entries, _ := os.ReadDir(filepath.Join(root, filepath.FromSlash(closureDir)))
	for _, entry := range entries {
		if !known[entry.Name()] {
			if write {
				_ = os.Remove(filepath.Join(root, filepath.FromSlash(closureDir), entry.Name()))
				continue
			}
			t.Errorf("%s/%s belongs to no registered venue test package: remove it (or regenerate with: %s)", closureDir, entry.Name(), regenerate)
		}
	}
	t.Logf("%d venue package(s) with a committed dependency closure", len(pkgs))
}

// diffLines names the lines that differ between two newline lists, a few of each.
func diffLines(got, want string) string {
	in := func(text string) map[string]bool {
		set := map[string]bool{}
		for _, line := range strings.Split(text, "\n") {
			if line != "" {
				set[line] = true
			}
		}
		return set
	}
	g, w := in(got), in(want)
	var added, removed []string
	for line := range w {
		if !g[line] {
			added = append(added, line)
		}
	}
	for line := range g {
		if !w[line] {
			removed = append(removed, line)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	cut := func(list []string) string {
		if len(list) > 5 {
			return strings.Join(list[:5], ", ") + fmt.Sprintf(" (+%d more)", len(list)-5)
		}
		return strings.Join(list, ", ")
	}
	return fmt.Sprintf("now linked and not listed: [%s]; listed and no longer linked: [%s]", cut(added), cut(removed))
}
