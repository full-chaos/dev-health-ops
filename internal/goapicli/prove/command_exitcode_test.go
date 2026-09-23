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

// TestCommand_RemovedBearerExecFlagsFailLoudAsUnknown pins that
// -proof-bearer-exec, -edge-bearer-exec and -proof-bearer-secret-file --
// the subprocess-exec minting flags folded away in favour of in-process
// minting -- are UNKNOWN flags now, not silently accepted or silently
// ignored: a stale prod round script still passing one of them gets a
// loud exit 2 (the shared unknown-flag contract), never a quiet
// successful run down the old, now-nonexistent path.
func TestCommand_RemovedBearerExecFlagsFailLoudAsUnknown(t *testing.T) {
	for _, flag := range []string{
		"-proof-bearer-exec=[\"mint-envelope\",\"-org\",\"o\"]",
		"-edge-bearer-exec=[\"mint-edge-token\",\"-org\",\"o\"]",
		"-proof-bearer-secret-file=/tmp/whatever",
	} {
		t.Run(flag, func(t *testing.T) {
			var stderr bytes.Buffer
			code := Command().Run(context.Background(), cli.Env{
				Args:   []string{flag},
				Stdout: &bytes.Buffer{},
				Stderr: &stderr,
			})
			if code != cli.ExitUsage {
				t.Fatalf("exit = %d, want cli.ExitUsage (%d); stderr: %s", code, cli.ExitUsage, stderr.String())
			}
		})
	}
}
