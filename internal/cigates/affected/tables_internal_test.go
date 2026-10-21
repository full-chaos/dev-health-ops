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
