package pythonparity

import (
	"encoding/hex"
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

// TestDecodeUTF8ReplaceMatchesLivePython compares DecodeUTF8Replace with
// bytes.decode("utf-8", "replace") on utf8ReplaceCorpus.
func TestDecodeUTF8ReplaceMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := utf8ReplaceCorpus()
	hexes := make([]string, len(corpus))
	for index, value := range corpus {
		hexes[index] = hex.EncodeToString([]byte(value))
	}
	input, _ := json.Marshal(hexes)
	command := exec.Command(python, "-c", pythonUTF8ReplaceProgram)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python returned %d results for %d inputs", len(want), len(corpus))
	}
	mismatches := 0
	for index, value := range corpus {
		if got := hex.EncodeToString([]byte(DecodeUTF8Replace(value))); got != want[index] {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("% x: go %s, python %s", value, got, want[index])
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d inputs differ", mismatches, len(corpus))
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "pythonparity-utf8-replace"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d byte strings compared; 0 mismatches", len(corpus))
}
