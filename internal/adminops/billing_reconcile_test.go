package adminops

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// reconcileRun runs `billing reconcile args...` with an empty environment.
func reconcileRun(args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := RunBillingReconcile(context.Background(), cli.Env{
		Args: args, Lookup: func(string) (string, bool) { return "", false }, Stdout: &stdout, Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

// TestBillingReconcileRefusesBadArgumentsBeforeAnyConnection: a bad --org-id or
// --since is refused before the database is opened (Python raises before its
// session), an unknown flag or a positional argument is a usage error (argparse's 2).
func TestBillingReconcileRefusesBadArgumentsBeforeAnyConnection(t *testing.T) {
	for _, test := range []struct {
		args []string
		code int
		text string
	}{
		{[]string{"--org-id", "not-a-uuid"}, cli.ExitFailure, "--org-id"},
		{[]string{"--org-id", "12345678-1234-1234-1234-12345678123"}, cli.ExitFailure, "not a valid UUID"},
		{[]string{"--since", "yesterday"}, cli.ExitFailure, "--since"},
		{[]string{"--since", "2026-13-01"}, cli.ExitFailure, "not an ISO date"},
		{[]string{"--nope"}, cli.ExitUsage, ""},
		{[]string{"extra"}, cli.ExitUsage, "positional arguments"},
	} {
		code, stdout, stderr := reconcileRun(test.args...)
		if code != test.code || stdout != "" || !strings.Contains(stderr, test.text) {
			t.Errorf("%v: exit %d stdout %q stderr %q, want exit %d and %q on stderr", test.args, code, stdout, stderr, test.code, test.text)
		}
	}
}

// TestBillingReconcileTakesTheFormsPythonTakes: an empty --org-id or --since is
// "not given", and Python's uuid.UUID and datetime.fromisoformat forms are read;
// each gets as far as the database, which this environment does not configure.
func TestBillingReconcileTakesTheFormsPythonTakes(t *testing.T) {
	for _, args := range [][]string{
		{"--org-id", ""}, {"--since", ""},
		{"--org-id", "AAAAAAAA-0000-4000-8000-00000000000A"},
		{"--org-id", "urn:uuid:aaaaaaaa-0000-4000-8000-00000000000a"},
		{"--org-id", "{aaaaaaaa-0000-4000-8000-00000000000a}"},
		{"--org-id", "aaaaaaaa00004000800000000000000a"},
		{"--since", "2026-06-01"}, {"--since", "20260601"}, {"--since", "2026-W23-1"},
		{"--since", "2026-06-01T12:30:00+02:00"}, {"--since", "2026-06-01T12:30:00Z"},
	} {
		code, _, stderr := reconcileRun(args...)
		if code != cli.ExitFailure || strings.Contains(stderr, "argument error") {
			t.Errorf("%v: exit %d stderr %q, want the refusal of the missing database, not of the argument", args, code, stderr)
		}
	}
}
