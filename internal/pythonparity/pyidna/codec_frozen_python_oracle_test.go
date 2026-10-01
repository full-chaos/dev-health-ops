package pyidna_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyidna"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// The program encodes with the interpreter's own "idna" codec; "!" stands
// where str.encode("idna") raises. The code point sweep is generated inside
// Python, so the corpus is not a Go-side guess: every code point between two
// letters, then every code point on its own. Its 2.2 million answers are
// frozen as block digests, one canonical line per answer: the encoded host as
// a text field. The corpus answers are frozen as text.
const pythonIDNAProgram = programoracle.BlocksPython + `
import sys
def enc(s):
    try:
        return s.encode("idna").decode("ascii")
    except UnicodeError:
        return "!"
payload = json.loads(sys.stdin.read())
points = [cp for cp in range(0x110000) if not 0xD800 <= cp <= 0xDFFF]
lines = [text(enc("a" + chr(cp) + "b")) for cp in points] + [text(enc(chr(cp))) for cp in points]
print(json.dumps({"sweep": block_digests(lines), "corpus": [enc(s) for s in payload]}))
`

func idnaCorpus() []string {
	corpus := []string{
		"", "example.com", "a..b", ".a", "a.", "ａ.com", "０.０.０.０", "ＧＩＴ.com", "straße.de", "ß", "ẞ", "İ", "ǅ", "ﬃ", "㎏", "ⓐⓑ", "a\u200db", "a\u00adb",
		"\u2100", "예", "例え.jp", "例え。jp", "例え．jp", "例え｡jp", "xn--a.b", "xn--é", "Xn--é", "é.xn--a", "a.é.b", "éé", "é。", "\u05d0", "\u05d0a", "a\u05d0",
		"\u05d0\u05d1", "\u05d01", "1\u05d0", "\u05d01\u05d0", "\u0627ل", "\u0627a", "\u0627\u0661", "\u06f1\u0627", "a\u0301", "\u0301", "\ufb1d", "\ufeff", "\u1806",
		"a\u2028b", "a\ue000b", "a\ufffdb", "a\u0341b", "\U0001d7ce", "\U0001f600", "\U000e0001", "\U0010ffff", strings.Repeat("é", 20), strings.Repeat("é", 30),
		strings.Repeat("a", 63), strings.Repeat("a", 64), strings.Repeat("a", 63) + ".b", strings.Repeat("a", 64) + ".b", strings.Repeat("a", 63) + "é", strings.Repeat("a", 64) + "é", strings.Repeat("é", 63), strings.Repeat("ａ", 63), strings.Repeat("ａ", 64), strings.Repeat("a", 63) + ".é",
	}
	pool := []rune{'a', 'B', '1', '-', '.', '。', 'é', 'ß', 'ａ', '０', '\u05d0', '\u05d1', '\u0627', '\u0661', '\u200d', '\u00ad', '\u2028', '\ufb1d', '\u0301', '\ufeff', '\u1806', '\u2100', 'İ', 'ǅ', 'ﬃ', '例', '\U0001d7ce'}
	random := rand.New(rand.NewSource(6513))
	for range 30000 {
		var builder strings.Builder
		for range 1 + random.Intn(6) {
			builder.WriteRune(pool[random.Intn(len(pool))])
		}
		corpus = append(corpus, builder.String())
	}
	return corpus
}

// TestCodecEncodeMatchesFrozenPython compares CodecEncode with
// str.encode("idna") for every code point between two letters and on its own,
// and for a corpus of bidirectional, mapping and length cases.
func TestCodecEncodeMatchesFrozenPython(t *testing.T) {
	corpus := idnaCorpus()
	input, err := json.Marshal(corpus)
	if err != nil {
		t.Fatal(err)
	}
	output := frozenPython(t, "codec.golden.json", programoracle.Program{Name: "codec", Text: pythonIDNAProgram, Stdin: input})[0]
	var want struct {
		Sweep  json.RawMessage `json:"sweep"`
		Corpus []string        `json:"corpus"`
	}
	if err := json.Unmarshal([]byte(output), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	encode := func(text string) string {
		got, err := pyidna.CodecEncode(text)
		if err != nil {
			return "!"
		}
		return got
	}
	var points []rune
	for cp := rune(0); cp <= 0x10FFFF; cp++ {
		if cp < 0xD800 || cp > 0xDFFF {
			points = append(points, cp)
		}
	}
	sweepInput := func(index int) string {
		if index < len(points) {
			return "a" + string(points[index]) + "b"
		}
		return string(points[index-len(points)])
	}
	line := func(dst []byte, index int) []byte {
		return programoracle.AppendText(dst, encode(sweepInput(index)))
	}
	programoracle.RequireBlocks(t, "code point sweep", string(want.Sweep), 2*len(points), line, func(index int) string {
		return fmt.Sprintf("%+q: go %q", sweepInput(index), encode(sweepInput(index)))
	})
	// A port on UTS 46 keeps the sharp s; the codec's nameprep maps it to "ss".
	if sweepInput(0xDF) != "a\u00dfb" {
		t.Fatalf("answer 0xDF of the sweep is for %+q", sweepInput(0xDF))
	}
	programoracle.RequireFindsDefect(t, "sharp s kept between two letters", string(want.Sweep), 0xDF, line, func(dst []byte) []byte {
		return programoracle.AppendText(dst, "xn--"+string(pyidna.PunycodeEncode([]rune("a\u00dfb"))))
	})
	if len(want.Corpus) != len(corpus) {
		t.Fatalf("python answered %d of %d corpus cases", len(want.Corpus), len(corpus))
	}
	mismatches := 0
	for position, text := range corpus {
		if got := encode(text); got != want.Corpus[position] {
			mismatches++
			if mismatches <= 30 {
				t.Errorf("%+q: go %q, python %q", text, got, want.Corpus[position])
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d corpus cases differ", mismatches, len(corpus))
	}
	t.Logf("%d sweep answers in blocks and %d corpus cases compared; 0 mismatches", 2*len(points), len(corpus))
}
