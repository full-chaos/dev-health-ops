package pyunicodedata_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyunicodedata"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// nfcProgram normalizes each input three ways. Its answers are frozen as block
// digests, one set per form, one canonical line per answer: the normalized
// text as a code point list.
const nfcProgram = programoracle.BlocksPython + `
import sys, unicodedata
u32 = unicodedata.ucd_3_2_0
nfc, nfkc, nfkc32 = [], [], []
for cps in json.load(sys.stdin):
    value = "".join(map(chr, cps or []))
    nfc.append(code_points(map(ord, unicodedata.normalize("NFC", value))))
    nfkc.append(code_points(map(ord, unicodedata.normalize("NFKC", value))))
    nfkc32.append(code_points(map(ord, u32.normalize("NFKC", value))))
print(json.dumps({"NFC": block_digests(nfc), "NFKC": block_digests(nfkc), "NFKC32": block_digests(nfkc32)}))
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

// TestNFCMatchesFrozenPython compares NFC, NFKC and NFKC32 with
// unicodedata.normalize("NFC" / "NFKC", ...) and
// unicodedata.ucd_3_2_0.normalize("NFKC", ...) over nfcCorpus.
func TestNFCMatchesFrozenPython(t *testing.T) {
	regenerate := os.Getenv("DEV_HEALTH_REGENERATE_TABLES") == "1"
	corpus := nfcCorpus()
	payload, err := json.Marshal(corpus)
	if err != nil {
		t.Fatal(err)
	}
	output := frozenPython(t, "nfc.golden.json", programoracle.Program{Name: "normalize", Text: nfcProgram, Stdin: payload})[0]
	var frozen map[string]json.RawMessage
	if err := json.Unmarshal([]byte(output), &frozen); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// The NFC answers here. The block digests prove each is the Python
	// answer, so the slice cut for the golden below is a slice of the Python
	// answers.
	want := make([][]rune, len(corpus))
	forms := []struct {
		name      string
		normalize func([]rune) []rune
	}{{"NFC", pyunicodedata.NFC}, {"NFKC", pyunicodedata.NFKC}, {"NFKC32", pyunicodedata.NFKC32}}
	if len(frozen) != len(forms) {
		t.Fatalf("the frozen answer holds %d forms, want %d", len(frozen), len(forms))
	}
	for _, form := range forms {
		line := func(dst []byte, index int) []byte {
			got := form.normalize(corpus[index])
			if form.name == "NFC" {
				want[index] = got
			}
			return programoracle.AppendCodePoints(dst, got)
		}
		programoracle.RequireBlocks(t, form.name, string(frozen[form.name]), len(corpus), line, func(index int) string {
			return fmt.Sprintf("%s %U: go %U", form.name, corpus[index], form.normalize(corpus[index]))
		})
	}
	// golang.org/x/text composes a supplementary-plane base with a following
	// mark as its low-16-bit twin would: U+10057 U+0301 as U+1E82. Python
	// leaves it.
	twin := 2*0x10057 + 1
	if !equal(corpus[twin], []rune{'e', 0x10057, 0x0301}) {
		t.Fatalf("answer %d of the sweep is for %U", twin, corpus[twin])
	}
	programoracle.RequireFindsDefect(t, "a supplementary-plane base composed as its BMP twin", string(frozen["NFC"]), twin,
		func(dst []byte, index int) []byte {
			return programoracle.AppendCodePoints(dst, pyunicodedata.NFC(corpus[index]))
		},
		func(dst []byte) []byte { return programoracle.AppendCodePoints(dst, []rune{'e', 0x1E82}) })
	t.Logf("%d inputs compared in blocks for %d forms, 0 differences", len(corpus), len(forms))
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
