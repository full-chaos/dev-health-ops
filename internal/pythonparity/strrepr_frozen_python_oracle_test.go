package pythonparity_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

const pythonStrReprProgram = `
import json, sys
texts = json.loads(sys.stdin.read())
ranges, start = [], None
for cp in range(sys.maxunicode + 2):
    nonprintable = cp <= sys.maxunicode and not chr(cp).isprintable()
    if nonprintable and start is None:
        start = cp
    elif not nonprintable and start is not None:
        ranges.append([start, cp - 1])
        start = None
print(json.dumps({"repr": [repr(t) for t in texts], "nonprintable_ranges": ranges}))
`

// TestStrReprMatchesFrozenPython compares IsPrintable with what
// str.isprintable() answered for every code point (surrogates included), and
// StrRepr with what repr() answered for a corpus of quoting and escape cases,
// executed once on the pinned build and frozen.
func TestStrReprMatchesFrozenPython(t *testing.T) {
	corpus := []string{
		"", "external-ingest.v2", "it's", `say "hi"`, `both ' and "`, `back\slash`, "tab\tnl\nnr\r", "\x00\x1f\x7f",
		"\u0085\u00a0\u00ad", "é ü ß", "\u2028\u2029", "\u200b\ufeff", "\U0001f600", "\U000e0001", "\u3000x", "\ue000", "\U0010ffff",
	}
	input, _ := json.Marshal(corpus)
	output := frozenPython(t, "strrepr.golden.json",
		programoracle.Program{Name: "str repr", Text: pythonStrReprProgram, Stdin: input})[0]
	lines := strings.Split(strings.TrimSpace(output), "\n")
	var want struct {
		Repr               []string
		NonprintableRanges [][2]rune `json:"nonprintable_ranges"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// The answer names the non-printable code points as inclusive ranges, in
	// ascending order, so the golden stays small.
	if len(want.NonprintableRanges) == 0 {
		t.Fatal("the answer names no non-printable range: the derivation is broken")
	}
	nonprintable := make([]bool, 0x110000)
	for _, bounds := range want.NonprintableRanges {
		if bounds[0] < 0 || bounds[0] > bounds[1] || bounds[1] > 0x10ffff {
			t.Fatalf("malformed non-printable range %v", bounds)
		}
		for r := bounds[0]; r <= bounds[1]; r++ {
			nonprintable[r] = true
		}
	}
	differences := 0
	for r := rune(0); r <= 0x10ffff; r++ {
		if pythonparity.IsPrintable(r) == nonprintable[r] {
			differences++
			if differences <= 10 {
				t.Errorf("U+%04X: Go printable=%v, Python printable=%v", r, pythonparity.IsPrintable(r), !nonprintable[r])
			}
		}
	}
	for index, text := range corpus {
		if got := pythonparity.StrRepr(text); got != want.Repr[index] {
			t.Errorf("repr(%q): go %s, python %s", text, got, want.Repr[index])
		}
	}
	t.Logf("0x110000 code points and %d reprs compared; %d printability differences", len(corpus), differences)
}
