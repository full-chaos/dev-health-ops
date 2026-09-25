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

// `dho migrate postgres upgrade|status|current` and the flat `migrate status` open
// PostgreSQL with a raw pgx connection: over every form of the connection string no
// login or password the driver resolved reaches their output.
func TestVerbsRedactResolvedCredentialsOverEveryConnectionForm(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	refusing.RunGrid(t, false, func(t *testing.T, dsn string) string {
		resolve := ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
			return secrets.NewValue(dsn), "test", true
		})
		lookup := func(key string) (string, bool) {
			if key == CutoverEnv {
				return "1", true
			}
			return "", false
		}
		var out bytes.Buffer
		verbs := Command(resolve).Children
		for _, alias := range Aliases(resolve) {
			if alias.Name == "status" {
				verbs = append(verbs, alias)
			}
		}
		for _, child := range verbs {
			switch child.Name {
			case "upgrade", "status", "current":
				var stderr bytes.Buffer
				child.Run(context.Background(), cli.Env{Lookup: lookup, Stdout: &out, Stderr: &stderr})
				out.Write(stderr.Bytes())
			}
		}
		return out.String()
	})
}
