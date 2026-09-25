package maintenancecli

import (
	"bytes"
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// `dho maintenance cleanup-tokens` opens PostgreSQL itself: a login and password that
// only PGUSER and PGPASSWORD supply are in the driver's failure text and must not
// reach the verb's output.
func TestCredentialsFromTheEnvironmentAreRedacted(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	values := map[string]string{"POSTGRES_URI": refusing.URI}
	lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	env := cli.Env{Args: nil, Lookup: lookup, Stdout: &stdout, Stderr: &stderr}
	code := runCleanupTokens(context.Background(), env)
	if code == cli.ExitOK || stderr.Len() == 0 {
		t.Fatalf("exit %d, stderr %q: the refusing server must fail the verb with a message", code, stderr.String())
	}
	if leaks := refusing.Leaks(stdout.String() + stderr.String()); len(leaks) > 0 {
		t.Errorf("the output carries %v:\n%s", leaks, stderr.String())
	}
}
