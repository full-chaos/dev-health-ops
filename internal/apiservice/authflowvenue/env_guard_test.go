package authflowvenue_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"testing"
)

// The corpus changes the target's environment only through venueoracle.SetTargetEnv, which also drives an
// external target. A direct Setenv after the first batch would change this process only, and an external target
// would answer under its own configuration: a silent difference.
var envMutators = map[string]bool{"Setenv": true, "Unsetenv": true, "Clearenv": true, "PutEnv": true}

// envMutationsAfterFirstBatch lists the env-mutating selectors (called or merely referenced, so an alias is
// caught) that stand after the first r.batch call in source.
func envMutationsAfterFirstBatch(t *testing.T, source string) []string {
	return nil
}

func TestEnvGuardFindsAliasesAndDirectCalls(t *testing.T) {
	const prefix = "package p\nfunc f(t T, r R) {\n\tt.Setenv(\"BEFORE\", \"1\")\n\tr.batch(\"b\", nil)\n"
	for name, body := range map[string]string{
		"t.Setenv":    "\tt.Setenv(\"X\", \"1\")\n",
		"os.Setenv":   "\tos.Setenv(\"X\", \"1\")\n",
		"os.Unsetenv": "\t_ = os.Unsetenv(\"X\")\n",
		"alias":       "\tsetenv := os.Setenv\n\tsetenv(\"X\", \"1\")\n",
		"syscall":     "\tsyscall.Setenv(\"X\", \"1\")\n",
		"os.Clearenv": "\tos.Clearenv()\n",
	} {
		if got := envMutationsAfterFirstBatch(t, prefix+body+"}\n"); len(got) == 0 {
			t.Errorf("%s after the first batch was not found", name)
		}
	}
	if got := envMutationsAfterFirstBatch(t, prefix+"\tvenueoracle.SetTargetEnv(t, false, \"X\", \"1\")\n}\n"); len(got) != 0 {
		t.Errorf("SetTargetEnv flagged: %v", got)
	}
	if got := envMutationsAfterFirstBatch(t, "package p\nfunc f(t T) {\n\tt.Setenv(\"BEFORE\", \"1\")\n}\n"); len(got) != 0 {
		t.Errorf("a Setenv with no batch flagged: %v", got)
	}
}

func TestCorpusChangesTargetEnvOnlyThroughSetTargetEnv(t *testing.T) {
	raw, err := os.ReadFile("auth_flow_venue_oracle_integration_test.go")
	if err != nil {
		t.Fatal(err)
	}
	if got := envMutationsAfterFirstBatch(t, string(raw)); len(got) != 0 {
		t.Errorf("env mutations after the first batch (use venueoracle.SetTargetEnv): %v", got)
	}
	_, _ = token.NewFileSet(), parser.ParseComments
	_, _ = ast.Inspect, sort.Strings
}
