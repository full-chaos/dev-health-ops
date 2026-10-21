package scripts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The lock probe of ci/local_validate.sh reads a sub-second clock from bash and date only: Python is dead, and the
// probe must neither call python3 or perl nor name them in its refusal message.
func TestLocalValidateLockProbeRunsNoPython(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "local_validate.sh"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	start := strings.Index(src, "lock_probe_time() {")
	end := strings.Index(src, `printf 'lock-probe: releasing`)
	if start < 0 || end < start {
		t.Fatal("lock probe code not found in ci/local_validate.sh")
	}
	for _, word := range []string{"python3", "perl"} {
		if strings.Contains(src[start:end], word) {
			t.Errorf("the lock probe time source names %q", word)
		}
	}
}
