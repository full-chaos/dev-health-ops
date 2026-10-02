// Package ratchet holds the closed lists of the repository to their size on the merge base.
//
// A closed list (a file of one entry per line) may shrink and never grow: the lists of Python-recorded files that still
// have no kind, of goldens that have no stamp, of test packages that still start their own Python child. Each used to be
// held by a number written beside the code (`const unclassifiedDayOne = 974`) that had to equal the list's length, so two
// pull requests that each removed one entry both edited that one line and the second one conflicted, each time costing a
// merge of main, a vet and a CI run. Here the size is read from git instead: the check compares the list in the working
// tree with the same list at the merge base and fails when it is longer. No pull request edits a shared number, and the
// check cannot pass without reading the base: a base that cannot be read is a failure, never a pass.
package ratchet

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// ManifestPath is the file that names the lists, relative to the repository root.
const ManifestPath = "ci/ratchets.tsv"

// Entry is one closed list: its name for messages and its path from the repository root.
type Entry struct {
	Name string
	Path string
}

// ReadManifest reads ManifestPath: `name<TAB>path<TAB>why`, `#` lines and blank lines ignored. An empty manifest is an
// error (a gate that checks nothing passes everything).
func ReadManifest(repo string) ([]Entry, error) {
	raw, err := os.ReadFile(filepath.Join(repo, filepath.FromSlash(ManifestPath)))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", ManifestPath, err)
	}
	var entries []Entry
	seen := map[string]bool{}
	for number, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 3 || strings.TrimSpace(fields[0]) == "" || strings.TrimSpace(fields[1]) == "" || strings.TrimSpace(fields[2]) == "" {
			return nil, fmt.Errorf("%s line %d: want name<TAB>path<TAB>why, got %q", ManifestPath, number+1, line)
		}
		entry := Entry{Name: strings.TrimSpace(fields[0]), Path: strings.TrimSpace(fields[1])}
		if seen[entry.Name] || seen[entry.Path] {
			return nil, fmt.Errorf("%s line %d: %q or %q is listed twice", ManifestPath, number+1, entry.Name, entry.Path)
		}
		seen[entry.Name], seen[entry.Path] = true, true
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("%s names no list: a ratchet that checks nothing passes everything", ManifestPath)
	}
	return entries, nil
}

// Count is the number of entries of a closed list: the lines that are not blank and do not start with `#`.
func Count(data []byte) int {
	count := 0
	for _, line := range strings.Split(string(data), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			count++
		}
	}
	return count
}

func git(repo string, args ...string) ([]byte, error) {
	command := exec.Command("git", append([]string{"-C", repo}, args...)...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// Resolve is the commit the lists are compared with. base, when given (the CI sets it to the pull request's base), must
// name a commit that exists in the checkout. Without it: on a branch, the merge base with origin/main; on origin/main
// itself, its parent. Anything else is an error that says how to supply the base.
func Resolve(repo, base string) (string, error) {
	if base != "" {
		out, err := git(repo, "rev-parse", "--verify", "--quiet", base+"^{commit}")
		if err != nil {
			return "", fmt.Errorf("the base %q is not a commit of this checkout (fetch it: git fetch --no-tags origin %s): %w", base, base, err)
		}
		return strings.TrimSpace(string(out)), nil
	}
	if _, err := git(repo, "rev-parse", "--verify", "--quiet", "origin/main^{commit}"); err != nil {
		return "", errors.New("no base to compare the closed lists with: set RATCHET_BASE_SHA (or pass -base), or fetch origin/main (git fetch origin main)")
	}
	head, err := git(repo, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	main, err := git(repo, "rev-parse", "origin/main")
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(head)) == strings.TrimSpace(string(main)) {
		parent, err := git(repo, "rev-parse", "HEAD^")
		if err != nil {
			return "", fmt.Errorf("HEAD is origin/main and has no parent to compare with: %w", err)
		}
		return strings.TrimSpace(string(parent)), nil
	}
	merge, err := git(repo, "merge-base", "HEAD", "origin/main")
	if err != nil {
		return "", fmt.Errorf("no merge base of HEAD and origin/main (a shallow checkout? fetch more history, or set RATCHET_BASE_SHA): %w", err)
	}
	return strings.TrimSpace(string(merge)), nil
}

// Result is what one list measured.
type Result struct {
	Entry     Entry
	BaseCount int
	NowCount  int
}

// Check measures one list at the base commit and in the working tree. It returns an error when either cannot be read,
// and a non-nil failure text when the list grew.
func Check(repo, base string, entry Entry) (Result, string, error) {
	result := Result{Entry: entry}
	baseData, err := git(repo, "show", base+":"+entry.Path)
	if err != nil {
		return result, "", fmt.Errorf("%s: cannot read %s at the base %s: %w (a list that is new or moved has no base to be compared with: this is a failure, not a pass)", entry.Name, entry.Path, base, err)
	}
	nowData, err := os.ReadFile(filepath.Join(repo, filepath.FromSlash(entry.Path)))
	if err != nil {
		return result, "", fmt.Errorf("%s: cannot read %s in the working tree: %w", entry.Name, entry.Path, err)
	}
	result.BaseCount, result.NowCount = Count(baseData), Count(nowData)
	if result.BaseCount == 0 {
		return result, "", fmt.Errorf("%s: the list at the base holds no entry: an empty list has nothing to be held to", entry.Name)
	}
	if result.NowCount > result.BaseCount {
		return result, fmt.Sprintf("%s: %s holds %d entries and held %d on the base: a closed list only shrinks", entry.Name, entry.Path, result.NowCount, result.BaseCount), nil
	}
	return result, "", nil
}
