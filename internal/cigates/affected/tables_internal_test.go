package affected

import "testing"

// The glob and refusal cases of the deleted ci/go_relevance.sh corpus, as classes.tsv globs answer them.
func TestClassGlobs(t *testing.T) {
	cases := []struct {
		glob, path string
		want       bool
	}{
		{"docs/*.md", "docs/sub/a.md", false},
		{"docs/*.md", "docs/a.md", true},
		{"scripts/**", "scripts/a/b/c.sh", true},
		{"**/testdata/**", "testdata/x", true},
		{"**/testdata/**", "pkg/sub/testdata/x.txt", true},
		{"docs/c.rgo", "docs/cargo", false},
		{"Makefile", "docs/Makefile", false},
		{"internal/a+b(c)/*", "internal/a+b(c)/x.txt", true},
		{"docs/*.md", "docs/r\u00e9sum\u00e9.md", true},
		{"docs/?.md", "docs/\u0663.md", true},
		{".codex-review*", ".codex-review-context.md", true},
		{"ci/check_go.sh", "ci/check_go.sh.bak", false},
	}
	for _, c := range cases {
		if got := globMatch(c.glob, c.path); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", c.glob, c.path, got, c.want)
		}
	}
	for _, bad := range []string{"", "/abs/**", "docs/", "docs/**x", "docs/["} {
		if err := checkGlob(bad); err == nil {
			t.Errorf("checkGlob(%q) accepted a malformed glob", bad)
		}
	}
}

// A walk's filter bounds what it covers; an unexpressed walk covers everything.
func TestWalkRowFilterCovers(t *testing.T) {
	cases := []struct {
		root, depth, filter, path string
		want                      bool
	}{
		{".", depthTree, "*", "README.md", true},
		{".", depthTree, "*.go", "README.md", false},
		{".", depthTree, "*.go", "internal/x/x.go", true},
		{".", depthTree, "*_test.go", "internal/x/x.go", false},
		{"docs", depthDir, "*.md", "docs/a.md", true},
		{"docs", depthDir, "*.md", "docs/sub/a.md", false},
		{"docs", depthTree, "*.go", "docs/a.md", false},
		{unexpressed, "-", "-", "README.md", true},
	}
	for _, c := range cases {
		w := walkRow{root: c.root, depth: c.depth, filter: c.filter}
		if got := w.covers(c.path); got != c.want {
			t.Errorf("walk %s %s %s covers(%q) = %v, want %v", c.root, c.depth, c.filter, c.path, got, c.want)
		}
	}
	for _, f := range []string{"*", "*.go", "*_test.go"} {
		if !validFilter(f) {
			t.Errorf("validFilter(%q) = false", f)
		}
	}
	for _, f := range []string{"", "-", ".go", "**.go", "*/x.go", "*.[ch]"} {
		if validFilter(f) {
			t.Errorf("validFilter(%q) = true", f)
		}
	}
}
