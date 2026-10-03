package httpapi_test

import (
	"encoding/json"
	"math/rand"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
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

// TestParseLimitMatchesFrozenPython compares ParseLimit with limits.parse_many,
// the parser slowapi applies to a limit string. Where Go accepts, Python
// must yield exactly that one limit; where Go refuses, Python must refuse or
// yield something Go documents it does not serve the same way (several
// limits, a zero count).
func TestParseLimitMatchesFrozenPython(t *testing.T) {
	corpus := []string{
		"3/hour", "3/hours", "10/15minutes", "5 per day", "5per day", "5 PER DAY", "1/SECOND", "2/month", "2/year",
		"3/0hour", "3/00hour", "0/hour", "3/hourss", "3/h", "", "/hour", "3/", "3 hour", " 3 / hour ", "3/hour\n",
		"1/second; 5/minute", "1/second,5/minute", "1/second|5/minute", "3/hour;", "03/hour", "3/2 hours",
		"3/Khour", "3/ſecond", "3 /hour", "3/hour ", "99999999999999999999/hour", "3/99999999999hours",
		"1000/hour", "500/hour",
	}
	// Declared divergence (CHAOS-7861): the recorded Python answer accepts these
	// as one positive limit; Go refuses them, so an operator-set value stops the
	// process at startup instead of serving a different limit. Each needs the
	// refusal below, and the refusal text pins which Go check refused it.
	declaredRefusals := map[string]string{
		"3/2147483648seconds": "the window multiple is out of range", // limitspec.go multiple above 2^31-1
		"3/3000000000seconds": "the window multiple is out of range",
		"3/2600000hours":      "the window is out of range", // 9.36e9 s overflows time.Duration
	}
	corpus = append(corpus, "3/2147483648seconds", "3/3000000000seconds", "3/2600000hours")
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
	output := frozenPython(t, "parse-limit.golden.json",
		programoracle.Program{Name: "parse-limit", Text: pythonLimitProgram, Stdin: []byte(string(input))})[0]
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want [][][2]float64
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python returned %d results for %d cases", len(want), len(corpus))
	}
	accepted, mismatches, zeroRefused, declaredSeen := 0, 0, 0, 0
	for index, text := range corpus {
		limit, err := httpapi.ParseLimit("x", text)
		expected := want[index]
		if reason, declared := declaredRefusals[text]; declared {
			switch {
			case len(expected) != 1 || expected[0][0] <= 0:
				mismatches++
				t.Errorf("%q: declared divergence expects python to accept one positive limit, python %v", text, expected)
			case err == nil:
				mismatches++
				t.Errorf("%q: declared refusal, go now accepts it (%d per %s)", text, limit.Count, limit.Window)
			case !strings.Contains(err.Error(), reason):
				mismatches++
				t.Errorf("%q: go refusal changed class: %q, want text %q", text, err, reason)
			default:
				declaredSeen++
			}
			continue
		}
		switch {
		case len(expected) == 1 && expected[0][0] == 0:
			// Python's parser reads "0/hour" as one limit of count 0; Go documents
			// that it refuses a zero count, and this pins the refusal.
			if err == nil {
				mismatches++
				t.Errorf("%q: python reads a limit of count 0, go accepts it (%d per %s): a zero count must be refused", text, limit.Count, limit.Window)
			} else {
				zeroRefused++
			}
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
	if declaredSeen != len(declaredRefusals) {
		t.Fatalf("%d of %d declared refusals were checked", declaredSeen, len(declaredRefusals))
	}
	if zeroRefused == 0 {
		t.Fatal("no zero-count case was refused; the corpus cannot show the refusal Go documents")
	}
	t.Logf("%d cases compared, %d accepted, %d zero-count refused; 0 mismatches", len(corpus), accepted, zeroRefused)
}
