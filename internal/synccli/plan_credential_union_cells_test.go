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

// The final line feed is the common case; every whitespace kind the plan's key can
// end in (unicode.IsSpace) is covered.
func TestAppPrivateKeyEchoedWithoutItsFinalLineFeedIsHidden(t *testing.T) {
	// The key the plan holds ends in whitespace of several kinds; an echo drops it all.
	for name, suffix := range map[string]string{
		"final line feed":       "\n",
		"carriage return":       "\r\n",
		"blank line with space": "\n \n",
		"no-break space":        "\n\u00a0\n",
		"tab and line feeds":    "\n\t\n\n",
	} {
		t.Run(name, func(t *testing.T) {
			executor := dbCredentialExecutor(&GitHubCredentials{Mode: CredentialApp, Name: "database", AppID: "1", PrivateKey: openPEMBody + suffix, InstallationID: "2"},
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
		})
	}
}

func TestDatabaseResolvedProviderTokenIsHiddenInEveryLogLineOfABatch(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	executor := InlineExecutor(InlineDeps{
		OpenStore: func(context.Context, string) (driver.Conn, error) {
			slog.Warn("store opened", "note", openServer)
			return fakeStore{}, nil
		},
		List: func(context.Context, providersync.InProcessRun, providersync.InProcessListing) ([]providersync.ListedRepository, error) {
			return githubRepos("acme/api", "acme/web", "acme/docs"), nil
		},
		Run: func(_ context.Context, run providersync.InProcessRun) (providersync.CompleteRouteExecutionResult, error) {
			slog.Error("provider write failed", "repo", run.SourceExternalID, "error", errors.New(openServer))
			return providersync.CompleteRouteExecutionResult{}, nil
		},
		GitHubCredential: func(context.Context, string, string, cli.Env) (*GitHubCredentials, error) {
			return &GitHubCredentials{Mode: CredentialPAT, Name: "database", Token: openCredential}, nil
		},
	})
	code, _, stderr := runVerb(t, "git", executor, []string{"--provider", "github", "-s", "acme/*"}, dbCredentialEnv())
	if code != 0 {
		t.Fatalf("exit %d: %q", code, stderr)
	}
	// 1 store-open line + 3 repositories x 2 datasets' lines at least: every one must be there, none with the token.
	if got := strings.Count(logs.String(), "handshake echoed"); got < 4 {
		t.Fatalf("the batch logged %d lines the cell can check, want at least 4: %q", got, logs.String())
	}
	if strings.Contains(logs.String(), openCredential) {
		t.Errorf("the provider token was logged: %q", logs.String())
	}
}

// Registering a second value never drops the first (no reset between runs).
func TestCredentialRegistrarKeepsEveryRegisteredValue(t *testing.T) {
	registrar := &credentialRegistrar{}
	registrar.add("zz9-first-credential")
	registrar.add("zz9-second-credential")
	if got := registrar.redact("a zz9-first-credential b zz9-second-credential c"); got != "a [REDACTED] b [REDACTED] c" {
		t.Errorf("got %q", got)
	}
}
