package fixturescli

import (
	"bytes"
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// `dho fixtures finalize-synthetic-sync (a pool)` opens PostgreSQL itself: over every form of the
// connection string (URI and keyword, userinfo, pool parameters valid, invalid and
// zero, service and password files) no login or password the driver resolved reaches
// its output.
func TestRedactsResolvedCredentialsOverEveryConnectionForm(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	refusing.RunGrid(t, true, func(t *testing.T, dsn string) string {
		values := map[string]string{"POSTGRES_URI": dsn, AllowEnvVar: "1"}
		lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
		var stdout, stderr bytes.Buffer
		runFinalizeSynthetic(context.Background(), cli.Env{Args: []string{"--target", "cicd", "--repo-name", "acme/api", "--org", "org-1"}, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
		return stdout.String() + stderr.String()
	})
}
