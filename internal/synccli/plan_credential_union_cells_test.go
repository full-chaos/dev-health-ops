package synccli

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// A provider credential the database lookup resolved (no --auth) is held by the
// executor's plan after resolveDBLookups; runTarget's own plan is the one before
// it. Every value is made up.
func dbCredentialExecutor(creds *GitHubCredentials, open func(context.Context, string) (driver.Conn, error), run func(context.Context, providersync.InProcessRun) (providersync.CompleteRouteExecutionResult, error)) Executor {
	return InlineExecutor(InlineDeps{
		OpenStore: open,
		Run:       run,
		GitHubCredential: func(context.Context, string, string, cli.Env) (*GitHubCredentials, error) {
			return creds, nil
		},
	})
}

func dbCredentialEnv() map[string]string {
	env := openCredentialEnv()
	env["POSTGRES_URI"] = "postgres://zz9-open-db:zz9-open-dbpass@pg.example:5432/db"
	return env
}

// A non-open error of the run (a provider or sink error under the executor)
// that echoes the database-resolved token must not print or log it.
func TestDatabaseResolvedProviderTokenIsHiddenInANonOpenErrorAndTheLog(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	executor := dbCredentialExecutor(&GitHubCredentials{Mode: CredentialPAT, Name: "database", Token: openCredential},
		func(context.Context, string) (driver.Conn, error) { return fakeStore{}, nil },
		func(context.Context, providersync.InProcessRun) (providersync.CompleteRouteExecutionResult, error) {
			slog.Error("provider write failed", "error", errors.New(openServer))
			return providersync.CompleteRouteExecutionResult{}, errors.New(openServer)
		})
	code, stdout, stderr := runVerb(t, "prs", executor, []string{"--provider", "github", "--owner", "acme", "--repo", "api"}, dbCredentialEnv())
	if code == 0 || !strings.Contains(stderr, "handshake echoed") || !strings.Contains(logs.String(), "handshake echoed") {
		t.Fatalf("exit %d: the error never reached the printing path or the log: stderr %q logs %q", code, stderr, logs.String())
	}
	for name, text := range map[string]string{"stderr": stdout + stderr, "log": logs.String()} {
		if strings.Contains(text, openCredential) {
			t.Errorf("the provider token is in the %s: %q", name, text)
		}
	}
}

// An echo of a private key usually drops the final line feed (a trimmed
// value): the key as the plan holds it ends in a line feed, so the raw match
// misses the echo.
const (
	openPEMBody = "-----BEGIN PRIVATE KEY-----\nzz9-synthetic-key-aaaa\n-----END PRIVATE KEY-----"
)

func TestAppPrivateKeyEchoedWithoutItsFinalLineFeedIsHidden(t *testing.T) {
	executor := dbCredentialExecutor(&GitHubCredentials{Mode: CredentialApp, Name: "database", AppID: "1", PrivateKey: openPEMBody + "\n", InstallationID: "2"},
		func(context.Context, string) (driver.Conn, error) {
			return nil, errors.New("handshake echoed " + openPEMBody + " end")
		}, nil)
	code, stdout, stderr := runVerb(t, "prs", executor, []string{"--provider", "github", "--owner", "acme", "--repo", "api"}, dbCredentialEnv())
	if code == 0 || !strings.Contains(stderr, "handshake echoed") {
		t.Fatalf("exit %d: the open failure never reached the printing path (or lost the server's text): %q", code, stderr)
	}
	if strings.Contains(stdout+stderr, "zz9-synthetic-key-aaaa") {
		t.Errorf("the private key printed: %q", stderr)
	}
}
