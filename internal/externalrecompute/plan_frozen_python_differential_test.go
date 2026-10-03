package externalrecompute

// plan_python_differential_test.go compares pythonInt against the answers of the
// interpreter that owns the other half of this parity seam (executed once on
// the pinned build and frozen), instead of against a table someone typed from
// memory.
//
// This file exists because a hand-written table already failed once. The r1 fix
// asserted parity from such a table and still shipped the wrong whitespace set:
// it trimmed 0x1c-0x1f, borrowed from a note about str.strip(), which int()
// does NOT strip. Measured against a real python3, int("\x1c3") rejects while
// int("\xa03") returns 3 -- so the "fix" accepted values CPython refuses. r2
// found it by running the interpreter. A table cannot catch the case nobody
// thought of; the interpreter can.

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// pythonIntDifferentialCases are chosen to cover every rule pythonInt claims to
// implement, plus the exotica that broke it. Control characters are written as
// Go escapes so the file stays plain ASCII on disk.
var pythonIntDifferentialCases = []string{
	// plain
	"3", "0", "007", "+5", "-5", "42",
	// whitespace int() DOES strip
	" 3 ", "\t7\n", "\v3\f", "\r3", " 3", "3 ", " 3", "3",
	// separators int() does NOT strip -- the r1 defect
	"\x1c3", "\x1d3", "\x1e3", "\x1f3", "3\x1c",
	// underscore rules
	"1_0", "1_000", "_10", "10_", "1__0", "-_5", "+1_0",
	// rejects
	"", "   ", "x", "3.5", "1 0", "+", "-", "0x10", "3a", "--5",
	// numeral systems and magnitudes this port deliberately does not handle
	"٣", "١٢٣", "+٣",
	"99999999999999999999999999999999",
	// non-ASCII white space around a value, and a second sign
	"\u00a03", "\u00853", "3\u00a0", "3\u0085", "\u00a0\u00a03\u0085",
	"++5", "+-5", "-+5", "-++5",
}

// pythonIntProgram answers, per case (base64 of the value's UTF-8 bytes, so no
// shell, argv or source-encoding layer can alter the exotic bytes under test),
// "ok:<int(value)>" or "reject".
const pythonIntProgram = `import base64, json, sys
out = []
for item in json.loads(sys.stdin.read()):
    raw = base64.b64decode(item).decode("utf-8")
    try:
        out.append("ok:%d" % int(raw))
    except Exception:
        out.append("reject")
print(json.dumps(out))
`

func TestPythonIntMatchesFrozenCPython(t *testing.T) {
	encoded := make([]string, len(pythonIntDifferentialCases))
	for index, raw := range pythonIntDifferentialCases {
		encoded[index] = base64.StdEncoding.EncodeToString([]byte(raw))
	}
	input, err := json.Marshal(encoded)
	if err != nil {
		t.Fatal(err)
	}
	output := frozenPython(t, "python-int.golden.json",
		programoracle.Program{Name: "int()", Text: pythonIntProgram, Stdin: input})[0]
	var answers []string
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &answers); err != nil {
		t.Fatalf("decode the frozen answer: %v", err)
	}
	if len(answers) != len(pythonIntDifferentialCases) {
		t.Fatalf("the frozen answer holds %d results for %d cases", len(answers), len(pythonIntDifferentialCases))
	}

	for index, raw := range pythonIntDifferentialCases {
		goValue, goOK := pythonInt(raw)
		answer := answers[index]
		pythonOK := strings.HasPrefix(answer, "ok:")
		pythonValue := 0
		if pythonOK {
			var err error
			pythonValue, err = strconv.Atoi(strings.TrimPrefix(answer, "ok:"))
			if err != nil {
				// CPython is arbitrary-precision; a value Go cannot even hold
				// is one of the two documented non-matches below.
				pythonValue, pythonOK = 0, false
			}
		}

		if pythonOK && !goOK {
			// Two DELIBERATE non-matches, both safe because ValidateCapEnv
			// refuses at startup rather than defaulting: a Go rejection is a
			// named error the operator sees, never a silently widened bound.
			// Everything else disagreeing here is a defect.
			// The exemption is for a numeral system only: white space around the
			// value (NBSP, NEL) is not one, and Go must accept it as CPython does.
			if isPlainASCII(strings.TrimFunc(raw, unicode.IsSpace)) {
				t.Fatalf("pythonInt(%q) rejected an ASCII value CPython accepts as %d",
					raw, pythonValue)
			}
			continue
		}
		if goOK != pythonOK || (goOK && goValue != pythonValue) {
			t.Fatalf("pythonInt(%q) = (%d, %v); CPython int() = (%d, %v)",
				raw, goValue, goOK, pythonValue, pythonOK)
		}
	}
}

// isPlainASCII separates "we disagree about an ordinary value" (a defect) from
// "CPython understands a numeral system this port documents that it does not"
// (declared).
func isPlainASCII(raw string) bool {
	for _, character := range raw {
		if character > 127 {
			return false
		}
	}
	return true
}
