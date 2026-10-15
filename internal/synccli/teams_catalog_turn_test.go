package synccli

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

const catalogTurnOrg = "20000000-0000-4000-8000-000000000009"

// catalogRunWithFailingCarry runs `dho sync teams --provider github` against a store whose first read fails: a run
// that reaches the collector shows as a query on conn, one that does not shows none.
func catalogRunWithFailingCarry(t *testing.T, org string, extraArgs []string, values map[string]string, d deps) (code int, stderr string, conn *teamCatalogCarryConn) {
	t.Helper()
	conn = &teamCatalogCarryConn{err: errors.New("count read failed")}
	d.openStore = func(context.Context, string) (driver.Conn, error) { return conn, nil }
	d.doer = &http.Client{Transport: refusingTransport{calls: new(int)}}
	env := map[string]string{"CLICKHOUSE_URI": dsnValue}
	for key, value := range values {
		env[key] = value
	}
	var errOut strings.Builder
	args := append([]string{"--provider", "github", "--owner", "acme", "--org", org, "--auth", "token-for-test"}, extraArgs...)
	code = runTeams(context.Background(), cli.Env{Args: args, Lookup: lookup(env), Stdout: io.Discard, Stderr: &errOut}, d)
	return code, errOut.String(), conn
}

// A run with no PostgreSQL DSN cannot take the catalog turn the worker takes; it says so and still runs.
func TestCatalogTeamsWithoutAPostgresDSNWarnsThatNoTurnIsTaken(t *testing.T) {
	_, stderr, conn := catalogRunWithFailingCarry(t, catalogTurnOrg, nil, nil, defaultDeps())

	for _, want := range []string{`"level":"WARN"`, `team catalog turn not taken`, `"reason":"no_postgres_dsn"`, `"provider":"github"`} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr lacks %s: %s", want, stderr)
		}
	}
	if !strings.Contains(stderr, "a concurrent worker or CLI run of this organization and provider is not excluded") {
		t.Errorf("the warning does not say what is not excluded: %s", stderr)
	}
	if conn.queries != 1 {
		t.Errorf("the run made %d store reads, want 1 (it still runs without the turn)", conn.queries)
	}
}

// An organization id that is not a UUID cannot key a lease: the worker only ever holds UUID organizations.
func TestCatalogTeamsWithANonUUIDOrganizationWarnsAndRunsWithoutATurn(t *testing.T) {
	_, stderr, conn := catalogRunWithFailingCarry(t, "org-not-a-uuid", nil, map[string]string{PostgresURIKey: "postgres://u:p@127.0.0.1:1/db"}, defaultDeps())

	if !strings.Contains(stderr, `team catalog turn not taken`) || !strings.Contains(stderr, `"reason":"org_not_uuid"`) {
		t.Errorf("stderr lacks the org_not_uuid warning: %s", stderr)
	}
	if conn.queries != 1 {
		t.Errorf("the run made %d store reads, want 1", conn.queries)
	}
}
