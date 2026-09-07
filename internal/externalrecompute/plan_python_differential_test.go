package externalrecompute

// plan_python_differential_test.go compares pythonInt against the interpreter
// that owns the other half of this parity seam, instead of against a table
// someone typed from memory.
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
	"os/exec"
	"strconv"
	"strings"
	"testing"
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
}

func TestPythonIntMatchesRealCPython(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
	// The value crosses the process boundary base64-encoded so no shell,
	// argv, or source-encoding layer can alter the exotic bytes under test --
	// which would silently turn this differential into a comparison of two
	// mangled strings that happen to agree.
	const script = `import base64,sys
raw = base64.b64decode(sys.argv[1]).decode("utf-8")
try:
    sys.stdout.write("ok:%d" % int(raw))
except Exception:
    sys.stdout.write("reject")`

	for _, raw := range pythonIntDifferentialCases {
		goValue, goOK := pythonInt(raw)
		out, err := exec.Command("python3", "-c", script,
			base64.StdEncoding.EncodeToString([]byte(raw))).Output()
		if err != nil {
			t.Fatalf("python3 for %q: %v", raw, err)
		}
		pythonOK := strings.HasPrefix(string(out), "ok:")
		pythonValue := 0
		if pythonOK {
			pythonValue, err = strconv.Atoi(strings.TrimPrefix(string(out), "ok:"))
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
			if isPlainASCII(raw) {
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
