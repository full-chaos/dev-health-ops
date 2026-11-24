package adminops

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// The database Boundary and the credentials typed on the command line are one
// redaction: the Boundary's escaped login "svc:" ends where the typed value
// "A9mQ2" starts, and neither may hide the other's first byte.
func TestFinishLLMRedactsTheDatabaseAndTheTypedValuesAsOneUnion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := cli.Env{
		Lookup: func(key string) (string, bool) {
			if key == "MIGRATION_DATABASE_URI" {
				return "postgres://svc%3A:pw-x1@db.example/app", true
			}
			return "", false
		},
		Stdout: &stdout, Stderr: &stderr,
	}
	_, redact, ok := llmSettingsConfig(env)
	if !ok {
		t.Fatalf("llmSettingsConfig refused: %q", stderr.String())
	}
	code := finishLLM(env, redact, errors.New("cause svc%3A9mQ2 end"), "A9mQ2")
	if code == cli.ExitOK {
		t.Fatalf("exit %d", code)
	}
	if out := stderr.String(); strings.Contains(out, "9mQ2") || !strings.Contains(out, "cause [REDACTED] end") {
		t.Errorf("the typed value is not fully hidden: %q", out)
	}
}
