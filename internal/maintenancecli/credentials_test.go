package maintenancecli

import (
	"bytes"
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// `dho maintenance cleanup-tokens` opens PostgreSQL itself: over every form of the
// connection string (URI and keyword, userinfo, pool parameters valid, invalid and
// zero, service and password files) no login or password the driver resolved reaches
// its output.
func TestRedactsResolvedCredentialsOverEveryConnectionForm(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	refusing.RunGrid(t, false, func(t *testing.T, dsn string) string {
		values := map[string]string{"POSTGRES_URI": dsn}
		lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
		var stdout, stderr bytes.Buffer
		runCleanupTokens(context.Background(), cli.Env{Args: nil, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
		return stdout.String() + stderr.String()
	})
}
