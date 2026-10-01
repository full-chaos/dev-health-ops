package pythonparity_test

import (
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

const pythonUTF8ReplaceProgram = `
import json, sys
inputs = json.loads(sys.stdin.read())
print(json.dumps([bytes.fromhex(h).decode("utf-8", "replace").encode("utf-8").hex() for h in inputs]))
`

// utf8ReplaceCorpus is every 1- and 2-byte string, every 3-byte string whose
// lead is a multi-byte lead, every 4-byte string with a 4-byte lead over the
// continuation boundary bytes, and a seeded fuzz biased to bytes >= 0x80.
func utf8ReplaceCorpus() []string {
	var corpus []string
	for a := 0; a < 256; a++ {
		corpus = append(corpus, string([]byte{byte(a)}))
		for b := 0; b < 256; b++ {
			corpus = append(corpus, string([]byte{byte(a), byte(b)}))
		}
	}
	for a := 0xE0; a <= 0xEF; a++ {
		for b := 0x70; b <= 0xC2; b++ {
			for c := 0x70; c <= 0xC2; c++ {
				corpus = append(corpus, string([]byte{byte(a), byte(b), byte(c)}))
			}
		}
	}
	edges := []byte{0x00, 0x41, 0x7F, 0x80, 0x8F, 0x90, 0x9F, 0xA0, 0xBF, 0xC0, 0xC2, 0xE0, 0xF0, 0xF4, 0xFF}
	for a := 0xF0; a <= 0xF7; a++ {
		for _, b := range edges {
			for _, c := range edges {
				for _, d := range edges {
					corpus = append(corpus, string([]byte{byte(a), b, c, d}))
				}
			}
		}
	}
	random := rand.New(rand.NewSource(6332))
	for range 20000 {
		value := make([]byte, 1+random.Intn(12))
		for index := range value {
			if random.Intn(4) == 0 {
				value[index] = byte(random.Intn(0x80))
			} else {
				value[index] = byte(0x80 + random.Intn(0x80))
			}
		}
		corpus = append(corpus, string(value))
	}
	return corpus
}

// TestDecodeUTF8ReplaceMatchesFrozenPython compares DecodeUTF8Replace with
// what bytes.decode("utf-8", "replace") answered on utf8ReplaceCorpus,
// executed once on the pinned build and frozen.
func TestDecodeUTF8ReplaceMatchesFrozenPython(t *testing.T) {
	corpus := utf8ReplaceCorpus()
	hexes := make([]string, len(corpus))
	for index, value := range corpus {
		hexes[index] = hex.EncodeToString([]byte(value))
	}
	input, _ := json.Marshal(hexes)
	output := frozenPython(t, "utf8replace.golden.json",
		programoracle.Program{Name: "utf-8 replace", Text: pythonUTF8ReplaceProgram, Stdin: input})[0]
	lines := strings.Split(strings.TrimSpace(output), "\n")
	var want []string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python returned %d results for %d inputs", len(want), len(corpus))
	}
	mismatches := 0
	for index, value := range corpus {
		if got := hex.EncodeToString([]byte(pythonparity.DecodeUTF8Replace(value))); got != want[index] {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("% x: go %s, python %s", value, got, want[index])
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d inputs differ", mismatches, len(corpus))
	}
	t.Logf("%d byte strings compared; 0 mismatches", len(corpus))
}
