package pythonparity_test

import (
	"encoding/json"
	"math/rand"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
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

// TestSequenceRatioMatchesFrozenPython compares SequenceRatio with what
// difflib.SequenceMatcher(a, b).ratio() answered over the corpus above,
// executed once on the pinned build and frozen. Each ratio is compared by its
// literal text (Python's float repr), never through a decoded float.
func TestSequenceRatioMatchesFrozenPython(t *testing.T) {
	corpus := sequenceRatioCorpus()
	input, _ := json.Marshal(corpus)
	output := frozenPython(t, "seqratio.golden.json",
		programoracle.Program{Name: "sequence ratio", Text: pythonSequenceRatioProgram, Stdin: input})[0]
	decoder := json.NewDecoder(strings.NewReader(output))
	decoder.UseNumber()
	var want []json.Number
	if err := decoder.Decode(&want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d ratios for %d pairs", len(want), len(corpus))
	}
	mismatches := 0
	for index, pair := range corpus {
		if got := pythonparity.Repr(pythonparity.SequenceRatio(pair[0], pair[1])); got != want[index].String() {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("pythonparity.SequenceRatio(%q, %q) = %s, python %s", pair[0], pair[1], got, want[index])
			}
		}
	}
	t.Logf("%d pairs compared, %d mismatches", len(corpus), mismatches)
}
