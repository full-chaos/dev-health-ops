package docs_test

import "testing"

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
