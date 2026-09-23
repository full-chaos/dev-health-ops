package pythonparity

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

const pythonStrReprProgram = `
import json, sys
texts = json.loads(sys.stdin.read())
nonprintable = [cp for cp in range(sys.maxunicode + 1) if not chr(cp).isprintable()]
print(json.dumps({"repr": [repr(t) for t in texts], "nonprintable": nonprintable}))
`

// TestStrReprMatchesLivePython compares IsPrintable with str.isprintable()
// for every code point (surrogates included), and StrRepr with repr() for a
// corpus of quoting and escape cases.
func TestStrReprMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := []string{
		"", "external-ingest.v2", "it's", `say "hi"`, `both ' and "`, `back\slash`, "tab\tnl\nnr\r", "\x00\x1f\x7f",
		"\u0085\u00a0\u00ad", "é ü ß", "\u2028\u2029", "\u200b\ufeff", "\U0001f600", "\U000e0001", "\u3000x", "\ue000", "\U0010ffff",
	}
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonStrReprProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want struct {
		Repr         []string
		Nonprintable []rune
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	nonprintable := make(map[rune]bool, len(want.Nonprintable))
	for _, r := range want.Nonprintable {
		nonprintable[r] = true
	}
	differences := 0
	for r := rune(0); r <= 0x10ffff; r++ {
		if IsPrintable(r) == nonprintable[r] {
			differences++
			if differences <= 10 {
				t.Errorf("U+%04X: Go printable=%v, Python printable=%v", r, IsPrintable(r), !nonprintable[r])
			}
		}
	}
	for index, text := range corpus {
		if got := StrRepr(text); got != want.Repr[index] {
			t.Errorf("repr(%q): go %s, python %s", text, got, want.Repr[index])
		}
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "pythonparity-strrepr"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("0x110000 code points and %d reprs compared; %d printability differences", len(corpus), differences)
}
