package synccli

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// `dho sync teams --provider github|gitlab|linear` resolves the provider token,
// then opens the ClickHouse store. A store-open failure whose text echoes the token
// must not print it: the catalog print union is the sink, the database and the
// driver-resolved credentials, and held no provider token. Every value is made up.
func TestCatalogStoreOpenErrorKeepsTheProviderTokenHidden(t *testing.T) {
	providers := map[string][]string{
		"github": {"--provider", "github", "--owner", "acme", "--org", "o", "--auth", openCredential, "--structure"},
		"gitlab": {"--provider", "gitlab", "--owner", "acme", "--org", "o", "--auth", openCredential, "--structure"},
		"linear": {"--provider", "linear", "--org", "o", "--auth", openCredential, "--structure"},
	}
	for provider, args := range providers {
		t.Run(provider, func(t *testing.T) {
			env := validEnv()
			env["CLICKHOUSE_URI"] = "clickhouse://zz9-cat-login:zz9-cat-pass@127.0.0.1:9000/d"
			d := defaultDeps()
			d.openStore = func(context.Context, string) (driver.Conn, error) { return nil, errors.New(openServer) }
			code, stdout, stderr := run(t, env, d, args...)
			if code == 0 || !strings.Contains(stderr, "handshake echoed") {
				t.Fatalf("exit %d: the open failure never reached the printing path (or lost the server's text): %q", code, stderr)
			}
			if strings.Contains(stdout+stderr, openCredential) {
				t.Errorf("the provider token printed: %q", stderr)
			}
		})
	}
}
