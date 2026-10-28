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
	"strconv"
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
var GuardedDirs = []string{"internal/pgmigrate/testdata/golden", "internal/pgmigrate/baseline", "internal/pgmigrate/archive", "internal/api/health/testdata/golden"}

// AddGuardedDirs are the guarded directories where an ADDED file is a change too: a second baseline or a new archived
// file is not history the base holds.
var AddGuardedDirs = []string{"internal/pgmigrate/baseline", "internal/pgmigrate/archive"}

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

var opensCount = regexp.MustCompile(`opens=(\d+)`)

// BaseManifest is the manifest as the base commit holds it; ok is false when the base has none (the change set that
// introduces the guard).
func BaseManifest(dir, base string) (m Manifest, ok bool, err error) {
	out, err := exec.Command("git", "-C", dir, "show", base+":"+ManifestPath).Output()
	if err != nil {
		if exec.Command("git", "-C", dir, "cat-file", "-e", base+":"+ManifestPath).Run() != nil {
			return Manifest{}, false, nil
		}
		return Manifest{}, false, fmt.Errorf("git show %s:%s: %w", base, ManifestPath, err)
	}
	m, err = Parse(strings.NewReader(string(out)))
	return m, err == nil, err
}

// CheckNotBelowBase holds every pinned test source to the base manifest's count of golden-open calls: the count never
// goes down against the base. Editing the manifest in the same change set cannot lower the bar, because the bar is
// the BASE's manifest; the git half refuses that edit as well, and this check names the removed comparison.
func CheckNotBelowBase(root string, base Manifest) []string {
	var problems []string
	for rel, shape := range base.Shape {
		want := opensCount.FindStringSubmatch(shape)
		if want == nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			problems = append(problems, rel+": a test source the base manifest pins is missing")
			continue
		}
		before, _ := strconv.Atoi(want[1])
		after, _ := strconv.Atoi(opensCount.FindStringSubmatch(Shape(data))[1])
		if after < before {
			problems = append(problems, fmt.Sprintf("%s: golden-open calls went from %d to %d against the base", rel, before, after))
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

// Exemption waives the git half's findings of ONE exact change set, keyed to content: the base tree's manifest and
// head.json, the archive manifest at HEAD, the exact set of guarded paths the change set touches, and the content of
// every file the guard reads at HEAD (InputDigest). A matching exemption waives only the changed-path findings for its
// Paths and the below-base findings whose exact text is in WaivedBelowBase; every other finding stands.
type Exemption struct {
	BaseManifestSHA256    string
	BaseHeadJSONSHA256    string
	ArchiveManifestSHA256 string
	Paths                 []string
	ContentSHA256         string
	WaivedBelowBase       []string
	Cites                 string
}

// inputPaths is every path the guard reads at HEAD: the manifest, every path the HEAD or the base manifest names,
// every file under the guarded and golden dirs and every *_test.go entry directly in the pinned dirs.
func inputPaths(root string, head, base Manifest) ([]string, error) {
	set := map[string]bool{ManifestPath: true}
	for _, m := range []Manifest{head, base} {
		for path := range m.Files {
			set[path] = true
		}
		for path := range m.Pins {
			set[path] = true
		}
		for path := range m.Shape {
			set[path] = true
		}
	}
	var pinned []string
	for _, dir := range PinnedDirs {
		pinned = append(pinned, dir+"/")
	}
	for _, listing := range []struct {
		flags  []string
		paths  []string
		suffix string
	}{
		{[]string{"-r"}, append(append([]string(nil), GuardedDirs...), GoldenDirs...), ""},
		{nil, pinned, "_test.go"},
	} {
		args := append(append([]string{"-C", root, "ls-tree", "-z", "--name-only", "--full-tree"}, listing.flags...), "HEAD", "--")
		out, err := exec.Command("git", append(args, listing.paths...)...).Output()
		if err != nil {
			return nil, fmt.Errorf("git ls-tree HEAD: %w", err)
		}
		for _, path := range strings.Split(string(out), "\x00") {
			if path != "" && strings.HasSuffix(path, listing.suffix) {
				set[path] = true
			}
		}
	}
	paths := make([]string, 0, len(set))
	for path := range set {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths, nil
}

// InputDigest is the sha256 over "<path>\t<object type>\t<sha256 of the object at HEAD>\n" for every path the guard
// reads (or "<path>\tabsent\n"), sorted: one value that changes with any byte of any input, or any input added,
// deleted or renamed.
func InputDigest(root string, head, base Manifest) (string, error) {
	paths, err := inputPaths(root, head, base)
	if err != nil {
		return "", err
	}
	cmd := exec.Command("git", "-C", root, "cat-file", "--batch")
	var input strings.Builder
	for _, path := range paths {
		input.WriteString("HEAD:" + path + "\n")
	}
	cmd.Stdin = strings.NewReader(input.String())
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git cat-file --batch: %w", err)
	}
	reader := bufio.NewReader(strings.NewReader(string(out)))
	digest := sha256.New()
	for _, path := range paths {
		header, err := reader.ReadString('\n')
		if err != nil {
			return "", fmt.Errorf("git cat-file --batch: %s: %w", path, err)
		}
		fields := strings.Fields(header)
		if len(fields) == 2 && fields[1] == "missing" {
			fmt.Fprintf(digest, "%s\tabsent\n", path)
			continue
		}
		if len(fields) != 3 {
			return "", fmt.Errorf("git cat-file --batch: %s: unexpected header %q", path, header)
		}
		size, err := strconv.Atoi(fields[2])
		if err != nil {
			return "", fmt.Errorf("git cat-file --batch: %s: %w", path, err)
		}
		body := make([]byte, size+1)
		if _, err := io.ReadFull(reader, body); err != nil {
			return "", fmt.Errorf("git cat-file --batch: %s: %w", path, err)
		}
		sum := sha256.Sum256(body[:size])
		fmt.Fprintf(digest, "%s\t%s\t%s\n", path, fields[1], hex.EncodeToString(sum[:]))
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// Exemptions are the change sets whose own findings the git half waives.
var Exemptions []Exemption

// matches reports whether the change set under root against base is exactly the one e names.
func (e Exemption) matches(root, base string, changed []string, head, baseManifest Manifest) (bool, error) {
	if len(changed) != len(e.Paths) {
		return false, nil
	}
	sorted, want := append([]string(nil), changed...), append([]string(nil), e.Paths...)
	sort.Strings(sorted)
	sort.Strings(want)
	for i := range sorted {
		if sorted[i] != want[i] {
			return false, nil
		}
	}
	for path, want := range map[string]string{ManifestPath: e.BaseManifestSHA256, "internal/pgmigrate/baseline/head.json": e.BaseHeadJSONSHA256} {
		out, err := exec.Command("git", "-C", root, "show", base+":"+path).Output()
		if err != nil {
			return false, nil
		}
		if sum := sha256.Sum256(out); hex.EncodeToString(sum[:]) != want {
			return false, nil
		}
	}
	data, err := os.ReadFile(filepath.Join(root, "internal", "pgmigrate", "archive", "chain_manifest.tsv"))
	if err != nil {
		return false, nil
	}
	if sum := sha256.Sum256(data); hex.EncodeToString(sum[:]) != e.ArchiveManifestSHA256 {
		return false, nil
	}
	content, err := InputDigest(root, head, baseManifest)
	if err != nil {
		return false, nil
	}
	return content == e.ContentSHA256, nil
}

// GitHalf is the git half's verdict on the tree under root against base: the guarded paths the change set touches and
// the frozen comparisons it removes against the base manifest, one line each.
func GitHalf(root, base string, m Manifest, exemptions []Exemption) ([]string, error) {
	changed, err := Changed(root, base, m.GuardedPaths())
	if err != nil {
		return nil, err
	}
	baseManifest, hasBase, err := BaseManifest(root, base)
	if err != nil {
		return nil, err
	}
	waivedPaths, waivedBelowBase := map[string]bool{}, map[string]bool{}
	for _, exemption := range exemptions {
		if !hasBase {
			break
		}
		ok, err := exemption.matches(root, base, changed, m, baseManifest)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		for _, path := range exemption.Paths {
			waivedPaths[path] = true
		}
		for _, problem := range exemption.WaivedBelowBase {
			waivedBelowBase[problem] = true
		}
	}
	var problems []string
	if hasBase {
		for _, problem := range CheckNotBelowBase(root, baseManifest) {
			if !waivedBelowBase[problem] {
				problems = append(problems, "a frozen comparison was removed against the base: "+problem)
			}
		}
	}
	for _, path := range changed {
		if !waivedPaths[path] {
			problems = append(problems, "this change set edits a frozen golden, a pin source list or the manifest: "+path)
		}
	}
	return problems, nil
}

// Changed lists the paths among guarded that differ between base and HEAD in dir: modified, deleted, renamed or
// retyped anywhere guarded, and added under AddGuardedDirs.
func Changed(dir, base string, guarded []string) ([]string, error) {
	seen := map[string]bool{}
	var changed []string
	for _, query := range []struct {
		filter string
		paths  []string
	}{{"MDRT", guarded}, {"A", AddGuardedDirs}} {
		args := append([]string{"-C", dir, "diff", "--name-only", "--diff-filter=" + query.filter, base + "...HEAD", "--"}, query.paths...)
		out, err := exec.Command("git", args...).Output()
		if err != nil {
			return nil, fmt.Errorf("git diff %s...HEAD: %w", base, err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if line != "" && !seen[line] {
				seen[line] = true
				changed = append(changed, line)
			}
		}
	}
	sort.Strings(changed)
	return changed, nil
}
