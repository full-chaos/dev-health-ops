package synccli

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// The provider token the run was given reaches the print path (the run holds it
// as run.Credential before the ClickHouse sink is opened), but the print union
// is built from the sink and the database only, and the batch union is built
// after the open. A store-open failure whose text echoes the token must not
// print it. Every value is made up.
const (
	openCredential = "zz9tok-synthetic-aaaaaaaa"
	openServer     = "dial refused: handshake echoed Authorization " + openCredential + " end"
)

func openCredentialEnv() map[string]string {
	return map[string]string{"CLICKHOUSE_URI": "clickhouse://zz9-open-login:zz9-open-pass@127.0.0.1:9000/d", "ORG_ID": "org-1"}
}

func TestStoreOpenErrorKeepsTheProviderTokenHidden(t *testing.T) {
	executor := InlineExecutor(InlineDeps{
		OpenStore: func(context.Context, string) (driver.Conn, error) { return nil, errors.New(openServer) },
		List: func(context.Context, providersync.InProcessRun, providersync.InProcessListing) ([]providersync.ListedRepository, error) {
			return nil, nil
		},
	})
	for name, c := range map[string]struct {
		target string
		args   []string
	}{
		"single repository": {"prs", []string{"--provider", "github", "--owner", "acme", "--repo", "api", "--auth", openCredential}},
		"github batch":      {"git", []string{"--provider", "github", "-s", "acme/*", "--auth", openCredential}},
		"gitlab":            {"prs", []string{"--provider", "gitlab", "--project-id", "77", "--auth", openCredential}},
	} {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := runVerb(t, c.target, executor, c.args, openCredentialEnv())
			if code == 0 || !strings.Contains(stderr, "handshake echoed") {
				t.Fatalf("exit %d: the open failure never reached the printing path (or lost the server's text): %q", code, stderr)
			}
			if strings.Contains(stdout+stderr, openCredential) {
				t.Errorf("the provider token printed: %q", stderr)
			}
		})
	}
}
