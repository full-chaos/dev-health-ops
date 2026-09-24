package pyidna

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

// The program answers, per input, the punycode-encoded host or "!" where
// str.encode("idna") raises. Inputs travel as JSON strings; the code point
// sweep is generated inside Python so the corpus is not a Go-side guess.
const pythonIDNAProgram = `
import json, sys
def enc(s):
    try:
        return s.encode("idna").decode("ascii")
    except UnicodeError:
        return "!"
payload = json.loads(sys.stdin.read())
sweep = [enc("a" + chr(cp) + "b") for cp in range(0x110000) if not 0xD800 <= cp <= 0xDFFF]
single = [enc(chr(cp)) for cp in range(0x110000) if not 0xD800 <= cp <= 0xDFFF]
print(json.dumps({"sweep": sweep, "single": single, "corpus": [enc(s) for s in payload]}))
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

// TestCodecEncodeMatchesLivePython compares CodecEncode with str.encode("idna")
// for every code point on its own and between two letters, and for a corpus
// of bidirectional, mapping and length cases.
func TestCodecEncodeMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := idnaCorpus()
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonIDNAProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want struct{ Sweep, Single, Corpus []string }
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	mismatches, total := 0, 0
	check := func(text, expected string) {
		total++
		got, err := CodecEncode(text)
		if err != nil {
			got = "!"
		}
		if got != expected {
			mismatches++
			if mismatches <= 30 {
				t.Errorf("%+q: go %q, python %q", text, got, expected)
			}
		}
	}
	index := 0
	for cp := rune(0); cp <= 0x10FFFF; cp++ {
		if cp >= 0xD800 && cp <= 0xDFFF {
			continue
		}
		check("a"+string(cp)+"b", want.Sweep[index])
		check(string(cp), want.Single[index])
		index++
	}
	if index != len(want.Sweep) || index != len(want.Single) {
		t.Fatalf("python returned %d/%d sweep results for %d code points", len(want.Sweep), len(want.Single), index)
	}
	for position, text := range corpus {
		check(text, want.Corpus[position])
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d cases differ", mismatches, total)
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "pythonparity-idna"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d cases compared; 0 mismatches", total)
}
