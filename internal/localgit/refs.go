package localgit

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// errPackedRefsNotUTF8 is the UnicodeDecodeError GitPython raises reading
// packed-refs, which it opens as strict UTF-8.
var errPackedRefsNotUTF8 = errors.New("packed-refs is not valid UTF-8 (GitPython reads it as strict UTF-8)")

// commonDir is repo.common_dir: the directory holding refs/ and packed-refs
// (the main .git of a linked worktree).
func (r Repo) commonDir(ctx context.Context) (string, error) {
	out, err := r.run(ctx, "rev-parse", "--git-common-dir")
	if err != nil {
		return "", err
	}
	dir := strings.TrimSuffix(string(out), "\n")
	if !filepath.IsAbs(dir) {
		dir = filepath.Join(r.Root, dir)
	}
	return dir, nil
}

// refPaths is SymbolicReference._iter_items(common_path): GitPython does not ask
// git for refs, it reads them. The loose refs are every FILE below
// <common_dir>/<common_path> (whatever its name: git itself ignores a ref name it
// finds invalid, GitPython does not) and the packed refs are the first
// space-separated field pair of every packed-refs line whose path starts with
// common_path (no trailing slash). The union is sorted by code point.
func (r Repo) refPaths(ctx context.Context, commonPath string) ([]string, error) {
	dir, err := r.commonDir(ctx)
	if err != nil {
		return nil, err
	}
	set := map[string]bool{}
	root := filepath.Join(dir, filepath.FromSlash(commonPath))
	_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil // os.walk: an unreadable directory is skipped, a symlink to one is not entered
		}
		if entry.Name() == "packed-refs" {
			return nil
		}
		if rel, err := filepath.Rel(dir, path); err == nil {
			set[filepath.ToSlash(rel)] = true
		}
		return nil
	})
	entries, err := readPackedRefs(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.path, commonPath) {
			set[entry.path] = true
		}
	}
	paths := make([]string, 0, len(set))
	for path := range set {
		if strings.HasPrefix(path, "refs/") { // Reference.from_path accepts only refs/...
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	return paths, nil
}

// refName is Reference.name: the path without its first two components (a path
// of fewer than three components is its own name).
func refName(path string) string {
	tokens := strings.Split(path, "/")
	if len(tokens) < 3 {
		return path
	}
	return strings.Join(tokens[2:], "/")
}

type packedRef struct{ sha, path string }

// readPackedRefs is SymbolicReference._iter_packed_refs: the file is read as
// strict UTF-8 text, every line stripped, blank lines, comments and peeled (^)
// lines skipped, and the rest split at the first space. A missing file is no
// entries; a header that names a packing without `peeled`, a non-UTF-8 file or a
// line with no space is an error (TypeError, UnicodeDecodeError, ValueError).
func readPackedRefs(dir string) ([]packedRef, error) {
	packed, err := os.ReadFile(filepath.Join(dir, "packed-refs"))
	if err != nil {
		return nil, nil // OSError: no packed-refs
	}
	if !utf8.Valid(packed) {
		return nil, errPackedRefsNotUTF8
	}
	var entries []packedRef
	for _, line := range strings.Split(string(packed), "\n") {
		line = pythonparity.Strip(line)
		if strings.HasPrefix(line, "#") {
			if strings.HasPrefix(line, "# pack-refs with:") && !strings.Contains(line, "peeled") {
				return nil, fmt.Errorf("packed-refs header %q: GitPython does not understand this packing (TypeError)", line)
			}
			continue
		}
		if line == "" || strings.HasPrefix(line, "^") {
			continue
		}
		sha, path, found := strings.Cut(line, " ")
		if !found {
			return nil, fmt.Errorf("packed-refs line %q has no path (GitPython cannot unpack it)", line)
		}
		entries = append(entries, packedRef{sha, path})
	}
	return entries, nil
}

// checkRefNameValid is SymbolicReference._check_ref_name_valid: false is the
// ValueError.
func checkRefNameValid(path string) bool {
	previous, oneBefore := rune(-1), rune(-1)
	for _, c := range path {
		switch {
		case strings.ContainsRune(" ~^:?*[\\", c):
			return false
		case c == '.':
			if previous == -1 || previous == '/' || previous == '.' {
				return false
			}
		case c == '/':
			if previous == '/' || previous == -1 {
				return false
			}
		case c == '{' && previous == '@':
			return false
		case c < 32 || c == 127:
			return false
		}
		oneBefore, previous = previous, c
	}
	switch {
	case previous == '.', previous == '/':
		return false
	case previous == '@' && oneBefore == -1:
		return false
	}
	for _, component := range pathParts(path) {
		if strings.HasSuffix(component, ".lock") {
			return false
		}
	}
	return true
}

var reHexSHA = regexp.MustCompile(`^[0-9A-Fa-f]{40}$`)

// errRefCrash marks a failure of the ref reader Python does not catch: the run
// ends there.
var errRefCrash = errors.New("GitPython raises an exception it does not catch here")

// dereferenceRef is SymbolicReference.dereference_recursive: the sha a ref names,
// following `ref: <path>` pointers. skip is any ValueError, TypeError or
// UnicodeDecodeError, which infer_open_pull_requests_from_refs catches and
// answers by skipping the ref; an error is one it does not (AssertionError on an
// empty file, IndexError on `ref:` alone, a loop it would follow forever).
func dereferenceRef(dir, refPath string) (sha string, skip bool, err error) {
	for hops := 0; hops < 100; hops++ {
		if !checkRefNameValid(refPath) {
			return "", true, nil
		}
		var tokens []string
		content, readErr := os.ReadFile(filepath.Join(dir, filepath.FromSlash(refPath)))
		if readErr == nil {
			if !utf8.Valid(content) {
				return "", true, nil // UnicodeDecodeError is a ValueError
			}
			tokens = pythonparity.SplitWhitespace(pythonparity.RStrip(string(content)))
			if len(tokens) == 0 {
				return "", false, fmt.Errorf("%w: empty ref file %s (AssertionError)", errRefCrash, refPath)
			}
		} else {
			entries, packedErr := readPackedRefs(dir)
			if packedErr != nil {
				return "", true, nil
			}
			for _, entry := range entries {
				if entry.path == refPath {
					tokens = []string{entry.sha, entry.path}
					break
				}
			}
			if tokens == nil {
				return "", true, nil // ValueError: the reference does not exist
			}
		}
		switch {
		case tokens[0] == "ref:":
			if len(tokens) < 2 {
				return "", false, fmt.Errorf("%w: ref file %s is `ref:` alone (IndexError)", errRefCrash, refPath)
			}
			refPath = tokens[1]
		case reHexSHA.MatchString(tokens[0]):
			return tokens[0], false, nil
		default:
			return "", true, nil // ValueError: Failed to parse reference information
		}
	}
	return "", false, fmt.Errorf("%w: ref %s never resolves (GitPython would loop forever)", errRefCrash, refPath)
}

// pathParts is pathlib.PurePosixPath(path).parts for a relative path: empty and
// "." components are dropped.
func pathParts(path string) []string {
	var parts []string
	for _, part := range strings.Split(path, "/") {
		if part == "" || part == "." {
			continue
		}
		parts = append(parts, part)
	}
	return parts
}
