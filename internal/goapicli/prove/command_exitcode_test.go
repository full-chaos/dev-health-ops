package prove

import (
	"bytes"
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// TestCommand_UnknownFlagExitsUsage pins the dho exit-code contract at the
// Command boundary: an unknown flag is a syntax error (ExitUsage=2), not the
// verb's own generic failure code -- a caller checking for "usage error" vs
// "the operation ran and failed" must be able to tell them apart.
func TestCommand_UnknownFlagExitsUsage(t *testing.T) {
	var stderr bytes.Buffer
	code := Command().Run(context.Background(), cli.Env{
		Args:   []string{"-not-a-real-flag"},
		Stdout: &bytes.Buffer{},
		Stderr: &stderr,
	})
	if code != cli.ExitUsage {
		t.Fatalf("exit = %d, want cli.ExitUsage (%d); stderr: %s", code, cli.ExitUsage, stderr.String())
	}
}

// TestCommand_HelpExitsOK pins that a help request is success, not a
// failure -- an operator running `dho goapi prove -h` gets exit 0.
func TestCommand_HelpExitsOK(t *testing.T) {
	code := Command().Run(context.Background(), cli.Env{
		Args:   []string{"-h"},
		Stdout: &bytes.Buffer{},
		Stderr: &bytes.Buffer{},
	})
	if code != cli.ExitOK {
		t.Fatalf("-h exit = %d, want cli.ExitOK (%d)", code, cli.ExitOK)
	}
}
