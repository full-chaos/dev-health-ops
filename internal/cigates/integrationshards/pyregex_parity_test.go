package integrationshards_test

import (
	"regexp"
	"testing"
)

func TestPortedRegexesKeepPythonUnicodeSemantics(t *testing.T) {
	t.Parallel()
	build := regexp.MustCompile(`(?m)^//go:build.*` + pyBefore + `integration` + pyAfter)
	fn := regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]+)` + pyS + `*\(`)
	for name, c := range map[string]struct{ got, want bool }{
		"integration after a non-breaking space": {build.MatchString("//go:build x integration"), true},
		"integration glued to an accented word":  {build.MatchString("//go:build caféintegration"), false},
		"integration before an accented letter":  {build.MatchString("//go:build integrationé"), false},
		"func with a non-breaking space":         {fn.MatchString("func TestX () {"), true},
	} {
		if c.got != c.want {
			t.Errorf("%s: got %v, want %v", name, c.got, c.want)
		}
	}
}
