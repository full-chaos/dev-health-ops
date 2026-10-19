package authflowvenue_test

import (
	"os"
	"strings"
	"testing"
)

// The corpus changes the target's environment only through venueoracle.SetTargetEnv, which skips the case on an
// external target. A direct Setenv after the first batch would change this process only, and an external target
// would answer under its own configuration: a silent difference.
func TestCorpusChangesTargetEnvOnlyThroughSetTargetEnv(t *testing.T) {
	raw, err := os.ReadFile("auth_flow_venue_oracle_integration_test.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	first := strings.Index(text, "r.batch(")
	if first < 0 {
		t.Fatal("no r.batch call found: the guard measures nothing")
	}
	for _, call := range []string{"t.Setenv(", "os.Setenv(", "os.Unsetenv("} {
		if strings.Contains(text[first:], call) {
			t.Errorf("%s appears after the first batch: use venueoracle.SetTargetEnv", call)
		}
	}
}
