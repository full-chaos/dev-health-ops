// Package frozengoldens guards the Python-recorded pgmigrate goldens (CHAOS-10365 froze them; no tool can record
// them again) and the pins that hold them. frozen_goldens.tsv lists the sha256 of every frozen file and the sha256
// literals of the test sources in the pinned directories. A change to any of them, or to the manifest, is refused
// here (content check) and by the git half (a diff against the base of the change), so the only way through is a
// human-approved change to this guard itself (CHAOS-10751).
package frozengoldens

import (
	"bufio"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// ManifestPath is the manifest's own path: it is in the guarded set of the git half.
const ManifestPath = "internal/cigates/frozengoldens/frozen_goldens.tsv"

// PinnedDirs are the directories whose *_test.go sources are scanned for sha256 literals (the pins).
var PinnedDirs = []string{"internal/pgmigrate", "internal/api/health"}

// GoldenDirs are the directories whose golden files must all be listed in the manifest.
var GoldenDirs = []string{"internal/pgmigrate/testdata/golden", "internal/api/health/testdata/golden"}

//go:embed frozen_goldens.tsv
var manifestFile []byte

// Manifest is the parsed frozen_goldens.tsv.
type Manifest struct {
	Files map[string]string   // path -> sha256 of the content
	Pins  map[string][]string // test source path -> sorted sha256 literals it holds
	Shape map[string]string   // test source path -> Shape of that source (golden-open, finish and skip counts)
}

// Load parses the embedded manifest.
func Load() (Manifest, error) { return Parse(strings.NewReader(string(manifestFile))) }

// Parse reads a manifest: `file<TAB>path<TAB>sha256` and `pins<TAB>path<TAB>sha256,sha256<TAB>shape`; # lines are comments.
func Parse(r io.Reader) (Manifest, error) {
	m := Manifest{Files: map[string]string{}, Pins: map[string][]string{}, Shape: map[string]string{}}
	scanner := bufio.NewScanner(r)
	for line := 1; scanner.Scan(); line++ {
		text := scanner.Text()
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		fields := strings.Split(text, "\t")
		if len(fields) < 3 || len(fields) > 4 {
			return m, fmt.Errorf("manifest line %d: want 3 or 4 tab-separated fields", line)
		}
		switch fields[0] {
		case "file":
			m.Files[fields[1]] = fields[2]
		case "pins":
			pins := strings.Split(fields[2], ",")
			sort.Strings(pins)
			m.Pins[fields[1]] = pins
			if len(fields) == 4 {
				m.Shape[fields[1]] = fields[3]
			}
		default:
			return m, fmt.Errorf("manifest line %d: unknown kind %q", line, fields[0])
		}
	}
	return m, scanner.Err()
}

// GuardedDirs are directories the git half guards whole, whatever the manifest lists: an existing file in them that
// is modified, deleted or renamed against the base is refused, so a manifest weakened in the same change set (an entry
// dropped, a golden deleted) does not clear itself.
var GuardedDirs = []string{"internal/pgmigrate/testdata/golden", "internal/pgmigrate/baseline", "internal/api/health/testdata/golden"}

// GuardedPaths is every path the git half refuses a change to: the manifest itself, its frozen files and the guarded
// directories.
func (m Manifest) GuardedPaths() []string {
	out := append([]string{ManifestPath}, GuardedDirs...)
	for path := range m.Files {
		out = append(out, path)
	}
	sort.Strings(out)
	return out
}

var pinLiteral = regexp.MustCompile(`"([0-9a-f]{64})"`)

var (
	goldenOpen   = regexp.MustCompile(`\b(OpenGolden|openStatesGolden|frozenPython)\(|\bgoldens\.Outputs\(`)
	goldenFinish = regexp.MustCompile(`\.(Finish|SkipDiff)\(t\)`)
	skipToken    = regexp.MustCompile(`\bt\.Skip(f|Now)?\(|\.SkipNow\(`)
)

// Shape is what a test source that holds pins still does with them: how many golden-open calls, golden finish calls
// and skip tokens it has. A frozen comparison replaced by t.Skip, a dropped call or an added skip changes it. An early
// return inside a test body is not seen here; the venueoracle TestMain already fails a golden opened and not consumed.
func Shape(source []byte) string {
	return fmt.Sprintf("opens=%d,finishes=%d,skips=%d", len(goldenOpen.FindAll(source, -1)), len(goldenFinish.FindAll(source, -1)), len(skipToken.FindAll(source, -1)))
}

// Check compares the tree under root with the manifest and returns one line per difference: a frozen file whose
// content changed or is missing, a golden file the manifest does not list, a test source whose sha256 literals
// differ from the listed ones (changed, added or removed), and a test source with literals the manifest lacks.
func Check(root string, m Manifest) []string {
	var problems []string
	for path, want := range m.Files {
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
		if err != nil {
			problems = append(problems, path+": missing ("+err.Error()+")")
			continue
		}
		sum := sha256.Sum256(data)
		if got := hex.EncodeToString(sum[:]); got != want {
			problems = append(problems, fmt.Sprintf("%s: content changed (sha256 %s, frozen %s)", path, got, want))
		}
	}
	for _, dir := range GoldenDirs {
		entries, err := os.ReadDir(filepath.Join(root, filepath.FromSlash(dir)))
		if err != nil {
			problems = append(problems, dir+": "+err.Error())
			continue
		}
		for _, entry := range entries {
			path := dir + "/" + entry.Name()
			if _, listed := m.Files[path]; !entry.IsDir() && !listed {
				problems = append(problems, path+": a golden file the manifest does not list")
			}
		}
	}
	seen := map[string]bool{}
	for _, dir := range PinnedDirs {
		files, _ := filepath.Glob(filepath.Join(root, filepath.FromSlash(dir), "*_test.go"))
		for _, file := range files {
			data, err := os.ReadFile(file)
			if err != nil {
				problems = append(problems, file+": "+err.Error())
				continue
			}
			rel, _ := filepath.Rel(root, file)
			rel = filepath.ToSlash(rel)
			found := map[string]bool{}
			for _, match := range pinLiteral.FindAllSubmatch(data, -1) {
				found[string(match[1])] = true
			}
			var got []string
			for pin := range found {
				got = append(got, pin)
			}
			sort.Strings(got)
			want, listed := m.Pins[rel]
			seen[rel] = true
			if len(got) == 0 && !listed {
				continue
			}
			if want, ok := m.Shape[rel]; ok && Shape(data) != want {
				problems = append(problems, fmt.Sprintf("%s: the frozen comparisons changed (%s, frozen %s)", rel, Shape(data), want))
			}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				problems = append(problems, fmt.Sprintf("%s: sha256 pins are [%s], frozen [%s]", rel, strings.Join(got, ","), strings.Join(want, ",")))
			}
		}
	}
	for rel := range m.Pins {
		if !seen[rel] {
			problems = append(problems, rel+": a test source the manifest pins is missing")
		}
	}
	sort.Strings(problems)
	return problems
}

// MergeBase is the commit this change branched from: the merge base of HEAD and origin/main in dir.
func MergeBase(dir string) (string, error) {
	out, err := exec.Command("git", "-C", dir, "merge-base", "HEAD", "origin/main").Output()
	if err != nil {
		return "", fmt.Errorf("no merge base of HEAD and origin/main: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// Changed lists the paths among guarded that differ between base and HEAD in dir.
func Changed(dir, base string, guarded []string) ([]string, error) {
	args := append([]string{"-C", dir, "diff", "--name-only", "--diff-filter=MDRT", base + "...HEAD", "--"}, guarded...)
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		return nil, fmt.Errorf("git diff %s...HEAD: %w", base, err)
	}
	var changed []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line != "" {
			changed = append(changed, line)
		}
	}
	return changed, nil
}
