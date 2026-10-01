package synccli

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"atlassian/atlassian"

	"github.com/full-chaos/dev-health-ops/internal/providersync"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// Credential-shaped test constants: not real credentials.
const (
	plantedLogin    = "planted_login_c6644"
	plantedPassword = "planted-pw-c6644"
)

func plantedEnv() map[string]string {
	return map[string]string{
		"CLICKHOUSE_URI": "clickhouse://" + plantedLogin + ":" + plantedPassword + "@localhost:8123/default",
		"ORG_ID":         "org-1",
	}
}

// What a server or driver prints when a connection or a statement fails: the
// ClickHouse authentication-failure and access-denied texts name the login, and a
// driver error can carry the password it was given.
func plantedServerText() string {
	return plantedLogin + ": Authentication failed: password is incorrect, or there is no user with such name (password " +
		plantedPassword + ")"
}

// A database or connection error printed by `dho sync <target>` carries neither
// the ClickHouse login nor the password, at every place the text is built: the
// store open, and the in-process run (the writes it makes). Nothing may appear
// on stdout or stderr.
func TestSyncTargetPrintsNeitherClickHouseLoginNorPassword(t *testing.T) {
	githubArgs := []string{"--provider", "github", "--owner", "acme", "--repo", "api", "--auth", "ghp-test"}
	for name, build := range map[string]func() *inlineHarness{
		"the store open fails": func() *inlineHarness {
			return &inlineHarness{openFn: func(string) error { return errors.New(plantedServerText()) }}
		},
		"the in-process run fails": func() *inlineHarness {
			return &inlineHarness{failOn: "prs", err: errors.New(plantedServerText())}
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := build()
			code, stdout, stderr := runVerb(t, "prs", h.executor(), githubArgs, plantedEnv())
			if code != cli.ExitFailure {
				t.Fatalf("exit %d, want a failure that reached the error path; stderr %q", code, stderr)
			}
			for _, secret := range []string{plantedLogin, plantedPassword} {
				if strings.Contains(stdout, secret) || strings.Contains(stderr, secret) {
					t.Errorf("%q printed:\nstdout %q\nstderr %q", secret, stdout, stderr)
				}
			}
		})
	}
}

// What the in-process run logs through the process logger (the sinks under the
// executor log database errors) is redacted too: a warning and an error that
// carry the login and the password, logged by a stub run, never reach the log.
func TestSyncTargetLogsNeitherClickHouseLoginNorPassword(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	executor := InlineExecutor(InlineDeps{
		OpenStore: func(context.Context, string) (driver.Conn, error) { return fakeStore{}, nil },
		Run: func(context.Context, providersync.InProcessRun) (providersync.CompleteRouteExecutionResult, error) {
			slog.Error("provider write failed", "error", errors.New(plantedServerText()))
			slog.Warn("guard read failed: " + plantedServerText())
			slog.Default().With("cause", plantedServerText()).Info("x")
			return providersync.CompleteRouteExecutionResult{}, nil
		},
	})
	code, stdout, stderr := runVerb(t, "prs", executor,
		[]string{"--provider", "github", "--owner", "acme", "--repo", "api", "--auth", "ghp-test"}, plantedEnv())
	if code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	out := logs.String()
	if !strings.Contains(out, "provider write failed") || !strings.Contains(out, "Authentication failed") {
		t.Fatalf("the run logged nothing the test can check:\n%s", out)
	}
	for _, secret := range []string{plantedLogin, plantedPassword} {
		if strings.Contains(out, secret) || strings.Contains(stdout, secret) || strings.Contains(stderr, secret) {
			t.Errorf("%q logged:\n%s", secret, out)
		}
	}
}

// The batch path prints a failure per repository and a listing error through
// its own redact() (batch.go): neither holds the login or the password.
func TestSyncBatchPrintsNeitherClickHouseLoginNorPassword(t *testing.T) {
	for name, build := range map[string]func() *batchHarness{
		"the listing fails": func() *batchHarness {
			return &batchHarness{listErr: errors.New(plantedServerText())}
		},
		"a repository's run fails": func() *batchHarness {
			return &batchHarness{repos: githubRepos("acme/api"), fail: func(providersync.InProcessRun) error { return errors.New(plantedServerText()) }}
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := build()
			code, stdout, stderr := runVerb(t, "git", h.executor(), githubBatchArgs, plantedEnv())
			if code == cli.ExitOK {
				t.Fatalf("exit %d: the failure never reached the printing path; stderr %q", code, stderr)
			}
			if !strings.Contains(stderr, "Authentication failed") {
				t.Fatalf("the server's text is not in stderr, so the run proves nothing: %q", stderr)
			}
			for _, secret := range []string{plantedLogin, plantedPassword} {
				if strings.Contains(stdout, secret) || strings.Contains(stderr, secret) {
					t.Errorf("%q printed:\nstdout %q\nstderr %q", secret, stdout, stderr)
				}
			}
		})
	}
}

// loggingClient and loggingDoer stand for the collector and the writer under
// `dho sync teams`: they log a database error with the login and the password
// through the process logger, as the providersync collectors do, then fail.
type loggingClient struct{ failingClient }

func (c loggingClient) SearchTeams(ctx context.Context, a, b, d string, n int) ([]atlassian.AtlassianTeam, error) {
	slog.Default().WarnContext(ctx, "roster_preservation_failed", "error", errors.New(plantedServerText()))
	return c.failingClient.SearchTeams(ctx, a, b, d, n)
}

type loggingDoer struct{}

func (loggingDoer) Do(request *http.Request) (*http.Response, error) {
	slog.Default().WarnContext(request.Context(), "roster_preservation_failed", "error", errors.New(plantedServerText()))
	return nil, errors.New(plantedServerText())
}

// `dho sync teams` (the Atlassian verb and the GitHub/GitLab/Linear catalog
// verb) logs nothing that holds the ClickHouse login or the password: the
// collector's own log lines pass through the same boundary as the error the verb
// prints.
func TestSyncTeamsLogsNeitherClickHouseLoginNorPassword(t *testing.T) {
	for name, build := range map[string]func() (map[string]string, deps, []string){
		"atlassian": func() (map[string]string, deps, []string) {
			env := validEnv()
			env["CLICKHOUSE_URI"] = plantedEnv()["CLICKHOUSE_URI"]
			d := stubDeps(&recorded{}, loggingClient{failingClient{err: errors.New(plantedServerText())}}, nil)
			return env, d, []string{"--provider", "jira", "--org", "org-1"}
		},
		"catalog": func() (map[string]string, deps, []string) {
			env := map[string]string{"CLICKHOUSE_URI": plantedEnv()["CLICKHOUSE_URI"]}
			d := stubDeps(&recorded{}, failingClient{}, nil)
			d.doer = loggingDoer{}
			return env, d, []string{"--provider", "github", "--org", "org-1", "--owner", "acme", "--auth", "ghp-test"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{})))
			t.Cleanup(func() { slog.SetDefault(previous) })
			env, d, args := build()
			code, stdout, stderr := run(t, env, d, args...)
			if code == cli.ExitOK {
				t.Fatalf("exit %d: the verb did not reach the failing collector; stderr %q", code, stderr)
			}
			if !strings.Contains(logs.String(), "roster_preservation_failed") || !strings.Contains(logs.String(), "Authentication failed") {
				t.Fatalf("the collector logged nothing the test can check:\nlogs %s\nstderr %s", logs.String(), stderr)
			}
			for _, secret := range []string{plantedLogin, plantedPassword} {
				if strings.Contains(logs.String(), secret) || strings.Contains(stdout, secret) || strings.Contains(stderr, secret) {
					t.Errorf("%q printed or logged:\nlogs %s\nstdout %q\nstderr %q", secret, logs.String(), stdout, stderr)
				}
			}
		})
	}
}
