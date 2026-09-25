package admincli

import (
	"bytes"
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// `admin features seed` opens PostgreSQL with a raw pgx connection: over every form
// of the connection string (URI and keyword, userinfo, pool parameters, service and
// password files) no login or password the driver resolved reaches its output.
func TestSeedRedactsResolvedCredentialsOverEveryConnectionForm(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	refusing.RunGrid(t, false, func(t *testing.T, dsn string) string {
		lookup := func(key string) (string, bool) {
			if key == "POSTGRES_URI" {
				return dsn, true
			}
			return "", false
		}
		var stdout, stderr bytes.Buffer
		cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{
			Args: []string{"admin", "features", "seed"}, Lookup: lookup, Stdout: &stdout, Stderr: &stderr,
		})
		return stdout.String() + stderr.String()
	})
}
