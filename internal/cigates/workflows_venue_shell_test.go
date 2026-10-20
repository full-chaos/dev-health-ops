package cigates_test

import (
	"strings"
	"testing"
)

// CHAOS-10177 (B2): the venue guards read shell that a workflow step or ci/check_go.sh runs. A raw substring match
// passes on a comment, an echo or a quoted string, so every shell check works on venueShellCode: code lines with their
// indentation, comments removed, and matches anchored to the start of a line at a stated nesting level.

type venueShLine struct {
	indent int
	text   string
}

// venueShellCode returns the code lines of src: blank lines and full-line comments dropped, a trailing " #..." cut, indentation
// kept relative to the least-indented code line.
func venueShellCode(src string) []venueShLine {
	var out []venueShLine
	minIndent := -1
	for _, raw := range strings.Split(src, "\n") {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if i := strings.Index(trimmed, " #"); i >= 0 {
			trimmed = strings.TrimSpace(trimmed[:i])
		}
		indent := len(raw) - len(strings.TrimLeft(raw, " \t"))
		if minIndent < 0 || indent < minIndent {
			minIndent = indent
		}
		out = append(out, venueShLine{indent, trimmed})
	}
	for i := range out {
		out[i].indent -= minIndent
	}
	return out
}

func TestShellCodeIgnoresCommentsAndKeepsNesting(t *testing.T) {
	got := venueShellCode("  set -e\n  # check_x\n\n  if false; then\n    check_x # tail\n  fi\n")
	want := []venueShLine{{0, "set -e"}, {0, "if false; then"}, {2, "check_x"}, {0, "fi"}}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("line %d: got %v want %v", i, got[i], want[i])
		}
	}
}
