package pyunicodedata

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

const nfcProgram = `
import json, sys, unicodedata
out = []
u32 = unicodedata.ucd_3_2_0
for cps in json.load(sys.stdin):
    text = "".join(map(chr, cps or []))
    out.append([
        [ord(c) for c in unicodedata.normalize("NFC", text)],
        [ord(c) for c in unicodedata.normalize("NFKC", text)],
        [ord(c) for c in u32.normalize("NFKC", text)],
    ])
json.dump(out, sys.stdout)
`

// nfcCorpus is every code point alone, every code point after a base
// letter and before a combining mark, Hangul jamo sequences, long runs of
// marks (past the Stream-Safe limit), and seeded random mixes of bases,
// marks, jamo, surrogates and code points unassigned in Unicode 16.
func nfcCorpus() [][]rune {
	var corpus [][]rune
	for r := rune(0); r <= 0x10ffff; r++ {
		corpus = append(corpus, []rune{r})
		if r < 0x30000 {
			corpus = append(corpus, []rune{'e', r, 0x0301})
		}
	}
	// Hangul: the code points just past the syllable block that are
	// multiples of the trailing-consonant count from its start, each
	// followed by a trailing consonant (not a composition in Python).
	for _, first := range []rune{0xd7a4, 0xd7c0, 0xd7dc, 0xabff, 0xac00, 0xd7a3} {
		corpus = append(corpus, []rune{first, 0x11a8}, []rune{first, 0x1161}, []rune{0x1100, first})
	}
	marks := []rune{0x0301, 0x0327, 0x0316, 0x05b0, 0x0345, 0x093c, 0x094d, 0x3099, 0x1d165, 0x0e38, 0x0f71, 0x0f72}
	bases := []rune{'a', 'e', 'A', 'o', 0x00e7, 0x0915, 0x304b, 0x0391, 0x03b1, 0x1100, 0x1161, 0x11a8, 0xac00, 0x0b47, 0x1025}
	for n := 25; n <= 70; n += 3 {
		for _, mark := range marks {
			run := []rune{'a'}
			for i := 0; i < n; i++ {
				run = append(run, mark)
			}
			corpus = append(corpus, append(run, 'b'))
		}
	}
	random := rand.New(rand.NewSource(1600))
	extra := []rune{0xd800, 0xdc00, 0x0378, 0x1f8ff, 0x034f, ' '}
	for i := 0; i < 50000; i++ {
		length := random.Intn(80)
		text := make([]rune, length)
		for j := range text {
			switch k := random.Intn(10); {
			case k < 6:
				text[j] = marks[random.Intn(len(marks))]
			case k < 9:
				text[j] = bases[random.Intn(len(bases))]
			default:
				text[j] = extra[random.Intn(len(extra))]
			}
		}
		corpus = append(corpus, text)
	}
	return corpus
}

// TestNFCMatchesLivePython compares NFC, NFKC and NFKC32 with
// unicodedata.normalize("NFC" / "NFKC", ...) and
// unicodedata.ucd_3_2_0.normalize("NFKC", ...) over nfcCorpus.
func TestNFCMatchesLivePython(t *testing.T) {
	regenerate := os.Getenv("DEV_HEALTH_REGENERATE_TABLES") == "1"
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" && !regenerate {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := nfcCorpus()
	payload, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", nfcProgram)
	command.Stdin = strings.NewReader(string(payload))
	var stderr strings.Builder
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, []byte(stderr.String())))
	}
	var answers [][3][]rune
	if err := json.Unmarshal(output, &answers); err != nil {
		t.Fatal(err)
	}
	want := make([][]rune, len(answers))
	for index, answer := range answers {
		want[index] = answer[0]
	}
	differences := 0
	report := func(kind string, input, got, expected []rune) {
		differences++
		if differences <= 20 {
			t.Errorf("%s %U:\n  go     %U\n  python %U", kind, input, got, expected)
		}
	}
	for index, input := range corpus {
		if got := NFC(input); !equal(got, answers[index][0]) {
			report("NFC", input, got, answers[index][0])
		}
		if got := NFKC(input); !equal(got, answers[index][1]) {
			report("NFKC", input, got, answers[index][1])
		}
		if got := NFKC32(input); !equal(got, answers[index][2]) {
			report("NFKC32", input, got, answers[index][2])
		}
	}
	t.Logf("%d inputs, %d differences", len(corpus), differences)
	if differences > 0 {
		t.Fatalf("%d differences", differences)
	}
	// The golden keeps every 4th input NFC changes among the code point
	// sweeps, every long-mark input, and every 100th random mix.
	sweepEnd := 0x110000 + 0x30000
	longEnd := sweepEnd + 16*12
	var golden []nfcCase
	for index, input := range corpus {
		keep := (index < sweepEnd && index%4 == 0 && !equal(input, want[index])) ||
			(index >= sweepEnd && index < longEnd) || (index >= longEnd && index%100 == 0)
		if keep {
			golden = append(golden, nfcCase{Input: input, Want: want[index]})
		}
	}
	checkJSONLines(t, nfcGoldenPath, golden, regenerate)
	if regenerate {
		return
	}
	writeProof(t, "pythonparity-pyunicodedata-nfc")
}

func equal(a, b []rune) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
