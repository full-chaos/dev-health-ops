package docs_test

import (
	"strings"
	"testing"
)

func TestDocsRepoPathGuardBypasses(t *testing.T) {
	assertGuard(t, docsRepoPathGuard, []plantCase{
		{"a YAML flow list in front matter", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "---\nsource_of_truth: [internal/nosuchpackage/a.go, cmd/dho/main.go]\n---\n# P\n")
		}, "docs/reference/planted.md:2 names internal/nosuchpackage/a.go"},
		{"a front matter value with prose after the path", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "---\nsource_of_truth:\n  - internal/nosuchpackage/b.go (the owner)\n---\n# P\n")
		}, "docs/reference/planted.md:3 names internal/nosuchpackage/b.go"},
		{"the note for one path does not hide a second missing path on the line", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "`src/dev_health_ops/api/main.py` (removed with the Python api) and `internal/nosuchpackage/c.go`\n")
		}, "docs/reference/planted.md:1 names internal/nosuchpackage/c.go"},
		{"the note for the second path does not hide the first", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "`internal/nosuchpackage/d.go` and `src/dev_health_ops/api/main.py` (removed with the Python api)\n")
		}, "docs/reference/planted.md:1 names internal/nosuchpackage/d.go"},
	})
}

func TestDocsRepoPathGuardNoteExemptsOnlyItsPath(t *testing.T) {
	d := realTree(t).added("docs/reference/planted.md", "`src/dev_health_ops/api/main.py` (removed with the Python api), `cmd/dho/main.go`\n")
	for _, v := range docsRepoPathGuard(d) {
		if len(v) > 28 && v[:28] == "docs/reference/planted.md:1 " {
			t.Errorf("a noted path or a tracked path was refused: %s", v)
		}
	}
}

func TestDocsRepoPathGuardNoteIsPositional(t *testing.T) {
	assertGuard(t, docsRepoPathGuard, []plantCase{
		{"two identical missing paths, the note after the second", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "`internal/nosuchpackage/e.go` then `internal/nosuchpackage/e.go` (removed with the Python api)\n")
		}, "docs/reference/planted.md:1 names internal/nosuchpackage/e.go"},
		{"two identical Python paths in a table row, the note after the second", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "| `src/dev_health_ops/api/main.py` | `src/dev_health_ops/api/main.py` (removed with the Python api) |\n")
		}, "docs/reference/planted.md:1 names src/dev_health_ops/api/main.py"},
	})
}

func TestDocsRepoPathGuardFences(t *testing.T) {
	missing := "internal/nosuchpackage/f.go"
	assertGuard(t, docsRepoPathGuard, []plantCase{
		{"text after a closed fence is checked", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "~~~text\nx\n~~~\n`"+missing+"`\n")
		}, "docs/reference/planted.md:4 names " + missing},
		{"a shorter closing run does not close the fence, a longer one does", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "````text\n```\n`internal/nosuchpackage/g.go`\n````\n`"+missing+"`\n")
		}, "docs/reference/planted.md:5 names " + missing},
		{"a different fence character does not close the fence", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "~~~text\n```\n~~~\n`"+missing+"`\n")
		}, "docs/reference/planted.md:4 names " + missing},
	})
	for _, c := range []struct{ name, page string }{
		{"a tilde fence", "~~~text\n`internal/nosuchpackage/h.go`\n~~~\n"},
		{"a long tilde fence closed by a longer run", "~~~~text\n`internal/nosuchpackage/h.go`\n~~~\n~~~~~\n"},
		{"a backtick fence inside a tilde fence", "~~~text\n```\n`internal/nosuchpackage/h.go`\n```\n~~~\n"},
	} {
		for _, v := range docsRepoPathGuard(realTree(t).added("docs/reference/planted.md", c.page)) {
			if strings.Contains(v, "docs/reference/planted.md") {
				t.Errorf("%s: a path inside a fence was reported: %s", c.name, v)
			}
		}
	}
}

func TestDocsRepoPathGuardIndentedFence(t *testing.T) {
	missing := "internal/nosuchpackage/i.go"
	assertGuard(t, docsRepoPathGuard, []plantCase{
		{"a fence indented four spaces is code, not a fence", func(t *testing.T, d docTree) docTree {
			return d.added("docs/reference/planted.md", "    ```text\n`"+missing+"`\n")
		}, "docs/reference/planted.md:2 names " + missing},
	})
	page := "   ```text\n`internal/nosuchpackage/j.go`\n   ```\n"
	for _, v := range docsRepoPathGuard(realTree(t).added("docs/reference/planted.md", page)) {
		if strings.Contains(v, "docs/reference/planted.md") {
			t.Errorf("a fence indented three spaces was not recognised: %s", v)
		}
	}
}
