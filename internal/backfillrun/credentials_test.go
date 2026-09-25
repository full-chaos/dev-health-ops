package backfillrun

import (
	"bytes"
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// `dho backfill run (a raw pool: the error comes from the first query)` opens PostgreSQL itself: a login and password that
// only PGUSER and PGPASSWORD supply are in the driver's failure text and must not
// reach the verb's output.
func TestCredentialsFromTheEnvironmentAreRedacted(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	values := map[string]string{"POSTGRES_URI": refusing.URI}
	lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	env := cli.Env{Args: []string{"--config-id", "00000000-0000-4000-8000-000000000001", "--no-wait"}, Lookup: lookup, Stdout: &stdout, Stderr: &stderr}
	code := runVerb(context.Background(), env)
	refusing.RequireConnected(t)
	if code == cli.ExitOK || stderr.Len() == 0 {
		t.Fatalf("exit %d, stderr %q: the refusing server must fail the verb with a message", code, stderr.String())
	}
	if leaks := refusing.Leaks(stdout.String() + stderr.String()); len(leaks) > 0 {
		t.Errorf("the output carries %v:\n%s", leaks, stderr.String())
	}
}
