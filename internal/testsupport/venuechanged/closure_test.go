package venuechanged

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

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

func closureFile(dir, pkg string) string {
	return filepath.Join(dir, strings.ReplaceAll(pkg, "/", "__")+".txt")
}

// listedPackage is the part of `go list -json` the derivation reads. Paths are taken from the JSON as they are: no field is
// split on a delimiter a path may contain, and no directory is derived from an import path by editing its text.
type listedPackage struct {
	ImportPath      string
	Dir             string
	ForTest         string
	Deps            []string
	EmbedFiles      []string
	TestEmbedFiles  []string
	XTestEmbedFiles []string
}

func decodeGoList(r io.Reader) ([]listedPackage, error) {
	var out []listedPackage
	decoder := json.NewDecoder(r)
	for {
		var pkg listedPackage
		if err := decoder.Decode(&pkg); err == io.EOF {
			return out, nil
		} else if err != nil {
			return nil, fmt.Errorf("read go list -json: %w", err)
		}
		out = append(out, pkg)
	}
}

// closuresFromListing derives, for each venue package directory (relative to root), the closure lines of its test binary: the
// directory of every package the `.test` binary links that lies under root (so a repo-local replacement module counts, a module
// in the cache or the toolchain does not), minus the package's own directory, then `~dir` for every directory below a linked
// package that holds a file the package or its tests embed. Directories come from the package's Dir, never from its import path.
func closuresFromListing(root string, pkgs []string, listing []listedPackage) (map[string][]string, error) {
	byImport := map[string]listedPackage{}
	for _, pkg := range listing {
		byImport[pkg.ImportPath] = pkg
	}
	rel := func(dir string) (string, bool) {
		r, err := filepath.Rel(root, dir)
		if err != nil || r == ".." || strings.HasPrefix(r, ".."+string(filepath.Separator)) {
			return "", false
		}
		return filepath.ToSlash(r), true
	}
	embedsOf := map[string]map[string]bool{}
	for _, pkg := range listing {
		dir, ok := rel(pkg.Dir)
		if !ok {
			continue
		}
		for _, files := range [][]string{pkg.EmbedFiles, pkg.TestEmbedFiles, pkg.XTestEmbedFiles} {
			for _, file := range files {
				under := path.Dir(filepath.ToSlash(file))
				if under == "." {
					continue
				}
				if embedsOf[dir] == nil {
					embedsOf[dir] = map[string]bool{}
				}
				embedsOf[dir][dir+"/"+under] = true
			}
		}
	}
	closures := map[string][]string{}
	for _, own := range pkgs {
		var main *listedPackage
		for i := range listing {
			dir, ok := rel(listing[i].Dir)
			if ok && dir == own && listing[i].ForTest == "" && strings.HasSuffix(listing[i].ImportPath, ".test") {
				main = &listing[i]
			}
		}
		if main == nil {
			return nil, fmt.Errorf("go list -test gave no test binary for the venue package %s", own)
		}
		dirs := map[string]bool{}
		embedded := map[string]bool{}
		linked := []string{own}
		for _, dep := range main.Deps {
			pkg, ok := byImport[dep]
			if !ok {
				return nil, fmt.Errorf("the test binary of %s links %s, which go list -deps did not list", own, dep)
			}
			dir, ok := rel(pkg.Dir)
			if !ok || dir == own {
				continue
			}
			dirs[dir] = true
			linked = append(linked, dir)
		}
		for _, dir := range linked {
			for under := range embedsOf[dir] {
				embedded["~"+under] = true
			}
		}
		var lines []string
		for dir := range dirs {
			lines = append(lines, dir)
		}
		sort.Strings(lines)
		var tail []string
		for line := range embedded {
			tail = append(tail, line)
		}
		sort.Strings(tail)
		closures[own] = append(lines, tail...)
	}
	return closures, nil
}

// computeClosures asks the Go toolchain for the packages each test binary links (`go list -json -deps -test`, with the
// integration tag the venue tests are built under) and derives the closure lines from it.
func computeClosures(t *testing.T, root string, pkgs []string) map[string][]string {
	t.Helper()
	args := []string{"list", "-buildvcs=false", "-mod=readonly", "-tags=integration", "-json", "-deps", "-test"}
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
		t.Fatalf("go list -json -deps -test over %d venue package(s): %v\n%s", len(pkgs), err, detail)
	}
	listing, err := decodeGoList(bytes.NewReader(out))
	if err != nil {
		t.Fatal(err)
	}
	closures, err := closuresFromListing(root, pkgs, listing)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateClosures(pkgs, closures); err != nil {
		t.Fatal(err)
	}
	return closures
}

// validateClosures refuses a derivation that came back empty or incomplete: a venue test binary always links in-module packages, and
// the repository embeds files (migrations, templates), so a closure with no package, or a whole derivation with no `~dir` line, means
// `go list` output was not read, not that nothing is linked. Silent zeros here would select nothing for every pull request.
func validateClosures(pkgs []string, closures map[string][]string) error {
	embedded := false
	for _, pkg := range pkgs {
		lines, ok := closures[pkg]
		if !ok {
			return fmt.Errorf("go list -test gave no test binary for the venue package %s", pkg)
		}
		dirs := 0
		for _, line := range lines {
			if strings.HasPrefix(line, "~") {
				embedded = true
			} else {
				dirs++
			}
		}
		if dirs == 0 {
			return fmt.Errorf("the venue package %s links no in-module package according to go list: the derivation is empty, not the closure", pkg)
		}
	}
	if !embedded {
		return fmt.Errorf("no venue package has an embedded-file directory (~dir): the EmbedFiles fields of go list were not read")
	}
	return nil
}

// closureSets is computeClosures as sets (package dir -> closure lines).
func closureSets(t *testing.T, root string, pkgs []string) map[string]map[string]bool {
	t.Helper()
	sets := map[string]map[string]bool{}
	for pkg, lines := range computeClosures(t, root, pkgs) {
		sets[pkg] = map[string]bool{}
		for _, line := range lines {
			sets[pkg][line] = true
		}
	}
	return sets
}

// TestClosureFilesMatchTheTestBinaryDependencies derives the trigger set of the pull request venue job (CHAOS-9935): for
// every venue test package, the in-module packages its test binary links, and `venue-oracles --changed` selects the
// package's tests when a changed file lies in one of them. The closure is what the Go toolchain says for the tree under
// test, never a committed copy (CHAOS-13101): with VENUE_CLOSURE_WRITE=1 it writes one <package>.txt per venue package
// into VENUE_CLOSURE_DIR, which ci/check_go.sh reads.
func TestClosureFilesMatchTheTestBinaryDependencies(t *testing.T) {
	root := repoRoot(t)
	pkgs := venuePackages(t, root)
	if len(pkgs) == 0 {
		t.Fatal("no run rows found in ci/venue_oracle_registry.d/")
	}
	want := computeClosures(t, root, pkgs)
	if os.Getenv("VENUE_CLOSURE_WRITE") != "1" {
		t.Logf("%d venue package(s) with a derived dependency closure", len(pkgs))
		return
	}
	dir := os.Getenv("VENUE_CLOSURE_DIR")
	if dir == "" {
		t.Fatal("VENUE_CLOSURE_WRITE=1 needs VENUE_CLOSURE_DIR, the directory to write the closure files into")
	}
	if err := writeClosures(dir, want); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d venue package closure(s) written to %s", len(pkgs), dir)
}

// writeClosures writes one <package>.txt per closure into dir, the form ci/check_go.sh reads.
func writeClosures(dir string, closures map[string][]string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for pkg, lines := range closures {
		if err := os.WriteFile(closureFile(dir, pkg), []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			return err
		}
	}
	return nil
}
