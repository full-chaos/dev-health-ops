package pgmigrate

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// A login and password that only PGUSER and PGPASSWORD supply are in the driver's
// failure text, and no verb that opens PostgreSQL itself prints them.
func TestVerbsRedactCredentialsThatComeFromTheEnvironment(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	for _, uri := range refusing.URIs() {
		verbsRedact(t, refusing, uri)
	}
}

func verbsRedact(t *testing.T, refusing fakepg.Refusing, uri string) {
	t.Helper()
	resolve := ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
		return secrets.NewValue(uri), "test", true
	})
	lookup := func(key string) (string, bool) {
		if key == CutoverEnv {
			return "1", true
		}
		return "", false
	}
	for _, verb := range []string{"upgrade", "status", "current"} {
		var run func(context.Context, cli.Env) int
		for _, child := range Command(resolve).Children {
			if child.Name == verb {
				run = child.Run
			}
		}
		var stdout, stderr bytes.Buffer
		before := refusing.Connections()
		code := run(context.Background(), cli.Env{Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
		refusing.RequireConnectedSince(t, before)
		if code == cli.ExitOK {
			t.Fatalf("%s: the refusing server let the verb succeed", verb)
		}
		if leaks := refusing.Leaks(stdout.String() + stderr.String()); len(leaks) > 0 {
			t.Errorf("%s: the output carries %v:\n%s", verb, leaks, stderr.String())
		}
		if stderr.Len() == 0 {
			t.Errorf("%s: no error was printed: the test measures nothing", verb)
		}
	}
}
