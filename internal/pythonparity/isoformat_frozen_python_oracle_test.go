package pythonparity_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// TestIsoformatUTCMatchesFrozenPython compares byte for byte with what CPython
// answered, executed once on the pinned build and frozen.
func TestIsoformatUTCMatchesFrozenPython(t *testing.T) {
	var input strings.Builder
	expected := map[string]string{}
	for _, moment := range pythonparity.IsoformatOracleCases() {
		key := fmt.Sprintf("%d %d", moment[0], moment[1])
		input.WriteString(key + "\n")
		expected[key] = pythonparity.IsoformatUTC(time.Unix(moment[0], moment[1]).UTC())
	}

	output := frozenPython(t, "isoformat.golden.json",
		programoracle.Program{Name: "isoformat", Stdin: []byte(input.String())})[0]

	var got map[string]string
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("parse oracle output: %v\n%s", err, output)
	}
	if len(got) != len(expected) {
		t.Fatalf("oracle returned %d results for %d inputs", len(got), len(expected))
	}
	for key, want := range expected {
		if got[key] != want {
			t.Errorf("input %q: python %q, go %q", key, got[key], want)
		}
	}
}
