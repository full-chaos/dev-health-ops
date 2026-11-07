package authruntime

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The session store and decisionstate must read one refresh lifetime. Every
// session.Config and decisionstate.Config literal in this package takes it from
// sessionRefreshTTL(), and that function returns session.DefaultRefreshTTL: a
// literal or a second default in any of them fails here.
func TestRefreshTTLHasOneSource(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	literal := regexp.MustCompile(`\b(session|decisionstate)\.Config\{`)
	var seen int
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		for _, at := range literal.FindAllStringIndex(text, -1) {
			seen++
			rest := text[at[1]:]
			end := strings.Index(rest, "\n\t})")
			if end < 0 {
				end = strings.Index(rest, "})")
			}
			if !strings.Contains(rest[:end], "RefreshTTL: sessionRefreshTTL()") {
				t.Errorf("%s: %s literal does not take RefreshTTL from sessionRefreshTTL()", name, text[at[0]:at[1]-1])
			}
		}
	}
	if seen < 3 {
		t.Errorf("found %d session.Config or decisionstate.Config literals, want at least 3 (two stores and the decision reader)", seen)
	}
	fn := regexp.MustCompile(`(?s)func sessionRefreshTTL\(\) time\.Duration \{\s*return session\.DefaultRefreshTTL\s*\}`)
	var defined bool
	for _, name := range files {
		if raw, err := os.ReadFile(name); err == nil && !strings.HasSuffix(name, "_test.go") && fn.Match(raw) {
			defined = true
		}
	}
	if !defined {
		t.Error("sessionRefreshTTL() is not defined as `return session.DefaultRefreshTTL`")
	}
}
