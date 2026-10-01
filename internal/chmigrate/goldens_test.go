package chmigrate_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// TestPythonGoldensMatchTheirPin fails when a golden recorded from the Python
// chain is edited or replaced without its pin: the pin names the 40-hex
// commit the Python producer ran on and the sha256 of each recorded file.
func TestPythonGoldensMatchTheirPin(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "python_goldens.pin.json"))
	if err != nil {
		t.Fatal(err)
	}
	var pin struct {
		PythonBuild string            `json:"python_build"`
		SHA256      map[string]string `json:"sha256"`
	}
	if err := json.Unmarshal(data, &pin); err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(pin.PythonBuild) {
		t.Fatalf("python_build %q is not a 40-hex commit", pin.PythonBuild)
	}
	for _, name := range []string{"python_chain_contract2.json", "python_split.json"} {
		want, ok := pin.SHA256[name]
		if !ok {
			t.Fatalf("%s has no pin", name)
		}
		raw, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if got := hex.EncodeToString(sum[:]); got != want {
			t.Errorf("%s sha256 %s, pinned %s: re-record it from the Python build and update the pin together", name, got, want)
		}
	}
}
