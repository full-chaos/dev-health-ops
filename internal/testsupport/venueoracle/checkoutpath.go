package venueoracle

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// A golden's key holds, by name AND value, every entry of the Python environment that is not per-run (see
// perRunPythonEnv). A value that holds the absolute path of a checkout (the Python root the recording runs
// from, or the repository the test runs in) is then keyed to that one path: the golden replays in the
// checkout it was recorded in and fails in every other (a second worktree, a CI runner), as the GitHub App
// install golden did before Options.PythonPathRel (CHAOS-7306). The recorder refuses such a value at
// record time (CHAOS-7849), naming the entry and never printing its value.

// checkoutRoots are the absolute checkout paths a keyed value must not hold: the Python root of the
// recording and the root of the module the test runs in.
func checkoutRoots(options Options) []string {
	var roots []string
	add := func(path string) {
		if path == "" {
			return
		}
		absolute, err := filepath.Abs(path)
		if err != nil {
			return
		}
		absolute = filepath.Clean(absolute)
		if len(absolute) < 4 { // "/" and the like are not a checkout
			return
		}
		for _, held := range roots {
			if held == absolute {
				return
			}
		}
		roots = append(roots, absolute)
	}
	add(options.Root)
	if wd, err := os.Getwd(); err == nil {
		for dir := wd; ; dir = filepath.Dir(dir) {
			if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
				add(dir)
				break
			}
			if filepath.Dir(dir) == dir {
				break
			}
		}
	}
	return roots
}

// holdsPath reports whether value holds root as a path: root followed by a path separator, the end of the
// value or a PATH-list colon, and not preceded by a name character (so "/x/ops" is not found inside
// "/x/ops2" or "/y/x/ops").
func holdsPath(value, root string) bool {
	for start := 0; ; {
		at := strings.Index(value[start:], root)
		if at < 0 {
			return false
		}
		at += start
		end := at + len(root)
		before := at == 0 || strings.ContainsRune(":=\"' ", rune(value[at-1]))
		after := end == len(value) || strings.ContainsRune("/:\"' ", rune(value[end])) || value[end] == filepath.Separator
		if before && after {
			return true
		}
		start = at + 1
	}
}

// keyedCheckoutPathErr is an error when an entry the key holds by value holds the absolute path of a
// checkout. Entries keyed by name only (perRunPythonEnv, when its name's supplier is the one the list
// names) are not read: their value is not in the key. The error names the entries, never their values.
func keyedCheckoutPathErr(entries []envEntry, roots []string) error {
	if len(roots) == 0 {
		return nil
	}
	type held struct {
		value  string
		byTest bool
	}
	values := map[string]held{}
	for _, entry := range entries {
		name, value, found := strings.Cut(entry.text, "=")
		if found && name != "" {
			values[name] = held{value, entry.byTest}
		}
	}
	var names []string
	for name, entry := range values {
		if listed, perRun := perRunPythonEnv[name]; perRun && listed.byTest == entry.byTest {
			continue
		}
		for _, root := range roots {
			if holdsPath(entry.value, root) {
				names = append(names, name)
				break
			}
		}
	}
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)
	return fmt.Errorf("recording: the Python environment entr%s %s hold%s the absolute path of a checkout, and the golden's key holds a value by name and value: the golden would replay in this checkout only and fail in every other (a second worktree, a CI runner); give the harness a root-relative name (Options.PythonPathRel for a directory on PYTHONPATH) or keep the path out of the environment",
		plural(len(names), "y", "ies"), strings.Join(names, ", "), plural(len(names), "s", ""))
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// declaredCheckoutPathErr is keyedCheckoutPathErr for the entries a producer call declares (the request's
// key holds them by name and value): none may hold the path of the Python root or of the module.
func declaredCheckoutPathErr(declared map[string]string, root string) error {
	entries := make([]envEntry, 0, len(declared))
	for name, value := range declared {
		entries = append(entries, envEntry{text: name + "=" + value, byTest: true})
	}
	return keyedCheckoutPathErr(entries, checkoutRoots(Options{Root: root}))
}
