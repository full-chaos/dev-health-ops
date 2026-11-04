package usersbackfill

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// A set AUTH_ORGS_OWNED no longer refuses orgs-backfill: the command goes on to connect, and writes one ERROR line
// that names the key and not its value.
func TestOrgsBackfillIgnoresAndReportsTheRemovedOrgsOwnedKey(t *testing.T) {
	var verb cli.Command
	for _, child := range Command().Children {
		if child.Name == "orgs-backfill" {
			verb = child
		}
	}
	if verb.Run == nil {
		t.Fatal("no orgs-backfill verb")
	}
	original := connectFn
	t.Cleanup(func() { connectFn = original })
	for _, value := range []string{"true", "false", "zz-not-a-switch-4471"} {
		reached := false
		connectFn = func(context.Context, cli.Env, string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
			reached = true
			return nil, authschema.ValidatedIdentifier{}, cli.ExitFailure
		}
		lookup := func(key string) (string, bool) {
			if key == "AUTH_ORGS_OWNED" {
				return value, true
			}
			return "", false
		}
		var stderr bytes.Buffer
		code := verb.Run(context.Background(), cli.Env{Args: []string{"-dry-run"}, Lookup: lookup, Stdout: &bytes.Buffer{}, Stderr: &stderr})
		if !reached || code != cli.ExitFailure {
			t.Errorf("%q: exit %d, connect reached %v; stderr: %s", value, code, reached, stderr.String())
		}
		errorLines := 0
		for _, line := range strings.Split(stderr.String(), "\n") {
			if strings.Contains(line, `"level":"ERROR"`) && strings.Contains(line, "AUTH_ORGS_OWNED") {
				errorLines++
				if strings.Contains(line, "zz-not-a-switch") {
					t.Errorf("%q: the value reached the log", value)
				}
			}
		}
		if errorLines != 1 {
			t.Errorf("%q: %d ERROR lines name the key, want 1; stderr: %s", value, errorLines, stderr.String())
		}
	}
}
