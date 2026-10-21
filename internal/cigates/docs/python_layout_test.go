package docs_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The Python application tree under src/dev_health_ops/ is gone; only a few config files remain there. A docs page that
// names a path under it must name a path that still exists, or carry pythonRemovedNote on the same line.

const pythonRemovedNote = "removed with the Python api"

var (
	pythonLayoutPath = regexp.MustCompile(`src/dev_health_ops(?:/[A-Za-z0-9_.\-/]*)?`)
	// Port records list the Python modules by design: each row is a Python path and its Go replacement.
	pythonPortRecordPages = map[string]bool{
		"docs/go-migration-matrix.md":                          true,
		"docs/reference/runtime/python-go-live-path-ledger.md": true,
	}
)

func (d docTree) markdownUnder(dir string) []string {
	seen := map[string]bool{}
	_ = filepath.WalkDir(filepath.Join(d.root, filepath.FromSlash(dir)), func(p string, e fs.DirEntry, err error) error {
		if err == nil && !e.IsDir() && strings.HasSuffix(p, ".md") {
			rel, _ := filepath.Rel(d.root, p)
			seen[filepath.ToSlash(rel)] = true
		}
		return nil
	})
	for rel, v := range d.over {
		if strings.HasPrefix(rel, dir+"/") && strings.HasSuffix(rel, ".md") {
			seen[rel] = v != nil
		}
	}
	var out []string
	for rel, ok := range seen {
		if ok {
			out = append(out, rel)
		}
	}
	sort.Strings(out)
	return out
}

func (d docTree) pathExists(rel string) bool {
	if d.exists(rel) {
		return true
	}
	info, err := os.Stat(filepath.Join(d.root, filepath.FromSlash(rel)))
	return err == nil && info.IsDir()
}

func pythonLayoutGuard(d docTree) []string {
	var out []string
	for _, rel := range d.markdownUnder("docs") {
		if pythonPortRecordPages[rel] {
			continue
		}
		text, err := d.read(rel)
		if err != nil {
			out = append(out, fmt.Sprintf("%s: %v", rel, err))
			continue
		}
		for i, line := range strings.Split(text, "\n") {
			if strings.Contains(line, pythonRemovedNote) {
				continue
			}
			for _, ref := range pythonLayoutPath.FindAllString(line, -1) {
				ref = strings.TrimRight(ref, ".")
				if d.pathExists(strings.TrimSuffix(ref, "/")) {
					continue
				}
				out = append(out, fmt.Sprintf("%s:%d names %s, which no longer exists: name the Go owner, or write %q on that line when no Go owner exists", rel, i+1, ref, pythonRemovedNote))
			}
		}
	}
	return out
}

func TestDocsNameNoDeletedPythonPath(t *testing.T) {
	gone := "src/dev_health_ops/api/main.py"
	assertGuard(t, pythonLayoutGuard, []plantCase{
		{"a new page names a deleted Python module", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "# Planted\n\nThe app is `"+gone+"`.\n")
		}, "docs/reference/planted.md:3 names " + gone},
		{"a source_of_truth line names a deleted Python module", func(t *testing.T, d docTree) docTree {
			return d.edit(t, "docs/reference/taxonomies/investment.md", "source_of_truth:\n", "source_of_truth:\n  - "+gone+"\n")
		}, "docs/reference/taxonomies/investment.md:"},
		{"a deleted Python package directory", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "See src/dev_health_ops/providers/.\n")
		}, "src/dev_health_ops/providers/"},
		{"a port record page does not exempt another page", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/runtime/planted.md", gone+"\n")
		}, "docs/reference/runtime/planted.md:1"},
		{"the note on another line does not exempt the line", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", pythonRemovedNote+"\n"+gone+"\n")
		}, "docs/reference/planted.md:2"},
	})
}

func TestDocsPythonLayoutGuardAcceptsLivePathsAndTheNote(t *testing.T) {
	real := realTree(t)
	before := map[string]bool{}
	for _, v := range pythonLayoutGuard(real) {
		before[v] = true
	}
	for _, c := range []struct{ name, rel, page string }{
		{"a remaining config file", "docs/reference/planted.md", "src/dev_health_ops/config/status_mapping.yaml\n"},
		{"the note on the same line", "docs/reference/planted.md", "src/dev_health_ops/api/main.py (" + pythonRemovedNote + ")\n"},
		{"a port record page", "docs/go-migration-matrix.md", "src/dev_health_ops/api/main.py\n"},
	} {
		for _, v := range pythonLayoutGuard(real.added(c.rel, c.page)) {
			if !before[v] {
				t.Errorf("%s: the guard refused it: %s", c.name, v)
			}
		}
	}
}
