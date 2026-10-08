package synccli

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

type teamCatalogCarryConn struct {
	driver.Conn
	queries int
	batches int
	err     error
}

func (conn *teamCatalogCarryConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	conn.queries++
	return nil, conn.err
}

func (conn *teamCatalogCarryConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	conn.batches++
	return nil, errors.New("no batch")
}

func (conn *teamCatalogCarryConn) Close() error { return nil }

// refusingTransport stands in for the network: a collector that runs
// before the carry reaches it, and it answers with an error.
type refusingTransport struct{ calls *int }

func (transport refusingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	*transport.calls++
	return nil, errors.New("no network in this test")
}

// TestEveryCLITeamCatalogCollectorCarriesFirstCensus builds every provider
// of the team catalog verb and runs it with a store whose first read fails:
// each must stop on the carry's count read.
func TestEveryCLITeamCatalogCollectorCarriesFirstCensus(t *testing.T) {
	if len(catalogProviderSpecs) < 3 {
		t.Fatalf("catalog providers = %d", len(catalogProviderSpecs))
	}
	failed := errors.New("count read failed")
	for provider := range catalogProviderSpecs {
		conn := &teamCatalogCarryConn{err: failed}
		env := cli.Env{Lookup: func(string) (string, bool) { return "", false }, Stderr: io.Discard}
		calls := 0
		d := defaultDeps()
		d.doer = &http.Client{Transport: refusingTransport{calls: &calls}}
		collector, credential, client, code := buildCatalogCollector(env, d, catalogRequest{provider: provider, orgID: "org-1"}, "acme", "token-for-test", conn)
		if collector == nil || code != 0 {
			t.Fatalf("%s: no collector (exit %d)", provider, code)
		}
		_, err := collector.CollectTeamCatalog(context.Background(),
			providersync.TeamCatalogReference{OrgID: "org-1", SyncRunID: "run", Strict: true}, credential, client,
			providersync.TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Now())
		if !errors.Is(err, failed) || !strings.Contains(err.Error(), "team id carry: count") || conn.queries != 1 || conn.batches != 0 || calls != 0 {
			t.Errorf("%s: err = %v, reads = %d, batches = %d, requests = %d; want only the carry's count read and its error", provider, err, conn.queries, conn.batches, calls)
		}
	}
}

// The Atlassian teams verb writes without a collector: it carries before
// its write, and a failed carry writes nothing.
func TestTheAtlassianTeamsVerbCarriesBeforeItWrites(t *testing.T) {
	failed := errors.New("count read failed")
	conn := &teamCatalogCarryConn{err: failed}
	d := stubDeps(&recorded{}, emptyClient{}, nil)
	d.openStore = func(context.Context, string) (driver.Conn, error) { return conn, nil }
	code, stdout, stderr := run(t, validEnv(), d, "--provider", "jira", "--org", "o", "--allow-empty")
	if code != cli.ExitFailure || !strings.Contains(stderr, `"code":"write_failed"`) || !strings.Contains(stderr, "team id carry: count") ||
		conn.queries != 1 || conn.batches != 0 || stdout != "" {
		t.Fatalf("exit %d, reads %d, batches %d, stdout %q, stderr %s", code, conn.queries, conn.batches, stdout, stderr)
	}
}
