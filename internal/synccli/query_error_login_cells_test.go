package synccli

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"atlassian/atlassian"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// A ClickHouse server that accepted the login can name it in a later error
// (the authentication and access-denied texts do). The sink URI below lists two
// hosts, one of them an IPv6 literal: the ClickHouse driver accepts that form and
// settles the login and password from it, and net/url (which the print union
// parses the URI with) does not, so the union holds the URI's bytes only. Every
// value is made up.
const (
	queryLogin   = "zz9-login-synthetic"
	queryPass    = "zz9-pass-synthetic"
	queryServer  = "code: 497, message: " + queryLogin + ": Not enough privileges to run the query"
	queryHostURI = "clickhouse://" + queryLogin + ":" + queryPass + "@127.0.0.1:1,[::ffff:127.0.0.1]:9000/d"
)

// queryFailsConn is a connection that opened and fails every statement with the
// server's text.
type queryFailsConn struct{ driver.Conn }

func (queryFailsConn) Close() error { return nil }
func (queryFailsConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errors.New(queryServer)
}
func (queryFailsConn) Exec(context.Context, string, ...any) error { return errors.New(queryServer) }
func (queryFailsConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, errors.New(queryServer)
}

func TestQueryErrorKeepsTheClickHouseLoginHiddenOnCatalogTeams(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	env := validEnv()
	env["CLICKHOUSE_URI"] = queryHostURI
	d := defaultDeps()
	d.openStore = func(context.Context, string) (driver.Conn, error) { return queryFailsConn{}, nil }
	code, stdout, stderr := run(t, env, d, "--provider", "github", "--owner", "acme", "--org", "o", "--auth", "ghp-test", "--structure")
	if code == 0 || !strings.Contains(stderr, "Not enough privileges") {
		t.Fatalf("exit %d: the query error never reached the printing path (or lost the server's text): %q", code, stderr)
	}
	for _, secret := range []string{queryLogin, queryPass} {
		if strings.Contains(stdout+stderr+logs.String(), secret) {
			t.Errorf("%q printed or logged:\nstderr %q\nlogs %q", secret, stderr, logs.String())
		}
	}
}

func TestQueryErrorKeepsTheClickHouseLoginHiddenOnSyncTargets(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	executor := InlineExecutor(InlineDeps{
		OpenStore: func(context.Context, string) (driver.Conn, error) { return queryFailsConn{}, nil },
		Run: func(context.Context, providersync.InProcessRun) (providersync.CompleteRouteExecutionResult, error) {
			slog.Error("provider write failed", "error", errors.New(queryServer))
			return providersync.CompleteRouteExecutionResult{}, errors.New(queryServer)
		},
	})
	env := map[string]string{"CLICKHOUSE_URI": queryHostURI, "ORG_ID": "org-1"}
	code, stdout, stderr := runVerb(t, "prs", executor, []string{"--provider", "github", "--owner", "acme", "--repo", "api", "--auth", "ghp-test"}, env)
	if code == 0 || !strings.Contains(stderr, "Not enough privileges") {
		t.Fatalf("exit %d: the query error never reached the printing path (or lost the server's text): %q", code, stderr)
	}
	for _, secret := range []string{queryLogin, queryPass} {
		if strings.Contains(stdout+stderr+logs.String(), secret) {
			t.Errorf("%q printed or logged:\nstderr %q\nlogs %q", secret, stderr, logs.String())
		}
	}
}

// oneTeamClient answers one team, so the run writes through the connection.
type oneTeamClient struct{ emptyClient }

func (oneTeamClient) SearchTeams(context.Context, string, string, string, int) ([]atlassian.AtlassianTeam, error) {
	return []atlassian.AtlassianTeam{{ID: "ari:cloud:identity::team/ZZ9-1", DisplayName: "Platform", State: "ACTIVE"}}, nil
}

func TestQueryErrorKeepsTheClickHouseLoginHiddenOnAtlassianTeams(t *testing.T) {
	env := validEnv()
	env["CLICKHOUSE_URI"] = queryHostURI
	d := stubDeps(&recorded{}, oneTeamClient{}, nil)
	d.openStore = func(context.Context, string) (driver.Conn, error) { return queryFailsConn{}, nil }
	code, stdout, stderr := run(t, env, d, "--provider", "jira", "--org", "o")
	if code == 0 || !strings.Contains(stderr, "Not enough privileges") {
		t.Fatalf("exit %d: the query error never reached the printing path (or lost the server's text): %q", code, stderr)
	}
	for _, secret := range []string{queryLogin, queryPass} {
		if strings.Contains(stdout+stderr, secret) {
			t.Errorf("%q printed: %q", secret, stderr)
		}
	}
}
