package adminops

import (
	"bytes"
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// `dho admin users list (a raw pool: the error comes from the first query)` opens PostgreSQL itself: a login and password that
// only PGUSER and PGPASSWORD supply are in the driver's failure text and must not
// reach the verb's output.
func TestCredentialsFromTheEnvironmentAreRedacted(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	values := map[string]string{"MIGRATION_DATABASE_URI": refusing.URI}
	lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	env := cli.Env{Args: []string{"--limit", "5"}, Lookup: lookup, Stdout: &stdout, Stderr: &stderr}
	code := runUsersList(context.Background(), env)
	if code == cli.ExitOK || stderr.Len() == 0 {
		t.Fatalf("exit %d, stderr %q: the refusing server must fail the verb with a message", code, stderr.String())
	}
	if leaks := refusing.Leaks(stdout.String() + stderr.String()); len(leaks) > 0 {
		t.Errorf("the output carries %v:\n%s", leaks, stderr.String())
	}
}
