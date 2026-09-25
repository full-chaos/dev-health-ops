package httpapi

import (
	"encoding/json"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

const pythonLimitProgram = `
import json, sys
from limits import parse_many
out = []
for text in json.loads(sys.stdin.read()):
    try:
        items = parse_many(text)
    except ValueError:
        out.append(None)
        continue
    out.append([[item.amount, item.get_expiry()] for item in items])
print(json.dumps(out))
`

// TestParseLimitMatchesLivePython compares ParseLimit with limits.parse_many,
// the parser slowapi applies to a limit string. Where Go accepts, Python
// must yield exactly that one limit; where Go refuses, Python must refuse or
// yield something Go documents it does not serve the same way (several
// limits, a zero count).
func TestParseLimitMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := []string{
		"3/hour", "3/hours", "10/15minutes", "5 per day", "5per day", "5 PER DAY", "1/SECOND", "2/month", "2/year",
		"3/0hour", "3/00hour", "0/hour", "3/hourss", "3/h", "", "/hour", "3/", "3 hour", " 3 / hour ", "3/hour\n",
		"1/second; 5/minute", "1/second,5/minute", "1/second|5/minute", "3/hour;", "03/hour", "3/2 hours",
		"3/Khour", "3/ſecond", "3 /hour", "3/hour ", "99999999999999999999/hour", "3/99999999999hours",
		"1000/hour", "500/hour",
	}
	pieces := []string{"1", "3", "15", "/", " per ", " ", "hour", "hours", "minute", "second", "s", "day", ";", "0", "x"}
	random := rand.New(rand.NewSource(6260))
	for range 4000 {
		var b strings.Builder
		for range 1 + random.Intn(6) {
			b.WriteString(pieces[random.Intn(len(pieces))])
		}
		corpus = append(corpus, b.String())
	}
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonLimitProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want [][][2]float64
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python returned %d results for %d cases", len(want), len(corpus))
	}
	accepted, mismatches := 0, 0
	for index, text := range corpus {
		limit, err := ParseLimit("x", text)
		expected := want[index]
		switch {
		case err == nil && (len(expected) != 1 || expected[0][0] != float64(limit.Count) || expected[0][1] != limit.Window.Seconds()):
			mismatches++
			t.Errorf("%q: go %d per %s, python %v", text, limit.Count, limit.Window, expected)
		case err == nil:
			accepted++
		case len(expected) == 1 && expected[0][0] > 0 && expected[0][0] < 9e18 && expected[0][1] < 1e10:
			// Python reads one positive, in-range limit that Go refuses: only a Unicode
			// whitespace spelling may differ this way.
			if !strings.ContainsFunc(text, func(r rune) bool { return r > 0x7f }) {
				mismatches++
				t.Errorf("%q: go refuses (%v), python %v", text, err, expected)
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d cases differ", mismatches, len(corpus))
	}
	if accepted == 0 {
		t.Fatal("no case was accepted; the corpus cannot show the accepted branch agrees")
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "httpapi-limit-string"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d cases compared, %d accepted; 0 mismatches", len(corpus), accepted)
}
