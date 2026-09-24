package pythonparity

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

const pythonSequenceRatioProgram = `
import difflib, json, sys
pairs = json.loads(sys.stdin.read())
print(json.dumps([difflib.SequenceMatcher(a=a, b=b).ratio() for a, b in pairs]))
`

// sequenceRatioCorpus mixes hand-picked names, the autojunk boundary (b of
// 199/200/201 code points with a popular element), and seeded random pairs
// over tiny alphabets, where ties between equal-length matches decide the
// answer.
func sequenceRatioCorpus() [][2]string {
	pairs := [][2]string{
		{"", ""}, {"a", ""}, {"", "a"}, {"alice smith", "alice smyth"}, {"bob", "bobby"},
		{"jane doe", "doe jane"}, {"日本語", "日本人"}, {"aaaa", "aa"}, {"abcabc", "abc"},
		{"the quick brown fox", "the quick brown fox"}, {"İstanbul", "istanbul"},
	}
	for _, n := range []int{199, 200, 201, 250} {
		popular := strings.Repeat("a", n-10) + "bcdefghijk"
		pairs = append(pairs, [2]string{"xxaaaaayybcdz", popular}, [2]string{strings.Repeat("ab", 60), popular})
	}
	rng := rand.New(rand.NewSource(20260924))
	alphabets := []string{"ab", "abc", "abcd ", "aeiou bcd"}
	for i := 0; i < 400; i++ {
		alphabet := []rune(alphabets[i%len(alphabets)])
		gen := func(maxLen int) string {
			n := rng.Intn(maxLen + 1)
			out := make([]rune, n)
			for k := range out {
				out[k] = alphabet[rng.Intn(len(alphabet))]
			}
			return string(out)
		}
		maxLen := 12
		if i%10 == 0 {
			maxLen = 320
		}
		pairs = append(pairs, [2]string{gen(maxLen), gen(maxLen)})
	}
	return pairs
}

// TestSequenceRatioMatchesLivePython compares SequenceRatio with
// difflib.SequenceMatcher(a, b).ratio() over the corpus above, exact float
// equality.
func TestSequenceRatioMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := sequenceRatioCorpus()
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonSequenceRatioProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []float64
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d ratios for %d pairs", len(want), len(corpus))
	}
	mismatches := 0
	for index, pair := range corpus {
		if got := SequenceRatio(pair[0], pair[1]); got != want[index] {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("SequenceRatio(%q, %q) = %v, python %v", pair[0], pair[1], got, want[index])
			}
		}
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "pythonparity-seqratio"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d pairs compared, %d mismatches", len(corpus), mismatches)
}
