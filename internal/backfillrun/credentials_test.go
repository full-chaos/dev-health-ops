package backfillrun

import (
	"bytes"
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// `dho backfill run (a pool: the error comes from the first query)` opens PostgreSQL itself: over every form of the
// connection string (URI and keyword, userinfo, pool parameters valid, invalid and
// zero, service and password files) no login or password the driver resolved reaches
// its output.
func TestRedactsResolvedCredentialsOverEveryConnectionForm(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	refusing.RunGrid(t, true, func(t *testing.T, dsn string) string {
		values := map[string]string{"POSTGRES_URI": dsn}
		lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
		var stdout, stderr bytes.Buffer
		runVerb(context.Background(), cli.Env{Args: []string{"--config-id", "00000000-0000-4000-8000-000000000001", "--no-wait"}, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
		return stdout.String() + stderr.String()
	})
}
