package admincli

import (
	"bytes"
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// `admin features seed` opens PostgreSQL itself: a login and password that only
// PGUSER and PGPASSWORD supply must not reach its output.
func TestSeedRedactsCredentialsThatComeFromTheEnvironment(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	lookup := func(key string) (string, bool) {
		if key == "POSTGRES_URI" {
			return refusing.URI, true
		}
		return "", false
	}
	var stdout, stderr bytes.Buffer
	code := cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{
		Args: []string{"admin", "features", "seed"}, Lookup: lookup, Stdout: &stdout, Stderr: &stderr,
	})
	if code == cli.ExitOK || stderr.Len() == 0 {
		t.Fatalf("exit %d, stderr %q: the refusing server must fail the verb with a message", code, stderr.String())
	}
	if leaks := refusing.Leaks(stdout.String() + stderr.String()); len(leaks) > 0 {
		t.Errorf("the output carries %v:\n%s", leaks, stderr.String())
	}
}
