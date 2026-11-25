package synccli

import (
	"context"
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// Verdict cells for residuals of the credential redaction. Every value is made
// up. The ClickHouse sink login "svc:" has the escaped spelling "svc%3A", which
// ends where the database password "A7kZ2" starts in the server text below.

const (
	residualSink     = "clickhouse://svc%3A:zz9q@c.example:9000/d"
	residualDB       = "postgres://acct:A7kZ2@pg.example:5432/db"
	residualServer   = "refused for svc%3A7kZ2 end"
	residualLeakTail = "7kZ2"
)

func residualEnv() map[string]string {
	return map[string]string{"CLICKHOUSE_URI": residualSink, "POSTGRES_URI": residualDB, "ORG_ID": "org-1"}
}

// R1: the text the sink-only (inline.go, local.go) or sink-and-token (batch.go)
// redaction produced is redacted again by planBoundary (sink and database).
func TestResidualR1TextRedactedTwiceKeepsTheDatabasePasswordHidden(t *testing.T) {
	githubArgs := []string{"--provider", "github", "--owner", "acme", "--repo", "api", "--auth", "ghp-test"}
	openFails := func(context.Context, string) (driver.Conn, error) { return nil, errors.New(residualServer) }
	for name, c := range map[string]struct {
		executor Executor
		target   string
		args     []string
	}{
		"inline store open": {InlineExecutor(InlineDeps{OpenStore: openFails}), "prs", githubArgs},
		"local store open": {InlineExecutor(InlineDeps{OpenStore: openFails}), "git",
			[]string{"--provider", "local", "--repo-path", "/repo", "--org", "o", "--since", "2026-01-02"}},
		"batch listing": {InlineExecutor(InlineDeps{
			OpenStore: func(context.Context, string) (driver.Conn, error) { return fakeStore{}, nil },
			List: func(context.Context, providersync.InProcessRun, providersync.InProcessListing) ([]providersync.ListedRepository, error) {
				return nil, errors.New(residualServer)
			},
		}), "git", githubBatchArgs},
	} {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := runVerb(t, c.target, c.executor, c.args, residualEnv())
			if code == 0 {
				t.Fatalf("exit %d: the failure never reached the printing path; stderr %q", code, stderr)
			}
			if !strings.Contains(stderr, "refused for") {
				t.Fatalf("the server's text is not in stderr, so the cell proves nothing: %q", stderr)
			}
			if strings.Contains(stdout+stderr, residualLeakTail) {
				t.Errorf("the database password's tail printed: %q", stderr)
			}
		})
	}
}

// R2: the Atlassian token goes to NewBoundaryWith as its first argument (the
// DSN argument), so only its raw bytes are matched; the escaped spelling of a
// token holding "=" must be hidden too.
func TestResidualR2AtlassianTokenEscapedSpellingIsRedacted(t *testing.T) {
	b := settings{token: "tok=Q9zWx", email: "sync@example.test"}.boundary()
	for _, text := range []string{"echo tok%3DQ9zWx end", "echo sync%40example.test end"} {
		if got := b.RedactText(text); !strings.Contains(got, "[REDACTED]") || strings.Contains(got, "Q9zWx") || strings.Contains(got, "example.test") {
			t.Errorf("%q: got %q", text, got)
		}
	}
}

// P3 synccli.go:249: the ClickHouse sink Boundary and the Atlassian credential
// are one redaction in `dho sync teams --provider jira`.
func TestResidualRunTeamsRedactsTheSinkAndTheTokenAsOneUnion(t *testing.T) {
	env := validEnv()
	env["CLICKHOUSE_URI"] = residualSink
	env["ATLASSIAN_API_TOKEN"] = "A7kZ2"
	rec := &recorded{}
	code, _, stderr := run(t, env, stubDeps(rec, failingClient{}, errors.New("refused for svc%3A7kZ2 end")), "--provider", "jira", "--org", "o")
	if code == 0 || !strings.Contains(stderr, "clickhouse_unavailable") {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if strings.Contains(stderr, residualLeakTail) {
		t.Errorf("the token's tail printed: %q", stderr)
	}
}

// P3 teamscatalog.go:197: the sink and the database Boundaries are one redaction
// in `dho sync teams --provider github`.
func TestResidualRunCatalogTeamsRedactsTheSinkAndTheDatabaseAsOneUnion(t *testing.T) {
	env := validEnv()
	env["CLICKHOUSE_URI"] = residualSink
	env["POSTGRES_URI"] = residualDB
	rec := &recorded{}
	code, _, stderr := run(t, env, stubDeps(rec, failingClient{}, errors.New("refused for svc%3A7kZ2 end")),
		"--provider", "github", "--owner", "acme", "--org", "o", "--auth", "ghp-test", "--structure")
	if code == 0 || rec.opened == 0 {
		t.Fatalf("exit %d, opened %d: the failure never reached the redactor; stderr %q", code, rec.opened, stderr)
	}
	if strings.Contains(stderr, residualLeakTail) {
		t.Errorf("the database password's tail printed: %q", stderr)
	}
}

// The text a batch print path already redacted with its whole union is not
// redacted a second time: a database password that spells part of the marker
// ("RED") would otherwise rewrite the marker it just got.
func TestBatchListErrorIsNotRedactedASecondTime(t *testing.T) {
	env := map[string]string{"CLICKHOUSE_URI": residualSink, "POSTGRES_URI": "postgres://acct:RED@pg.example:5432/db", "ORG_ID": "org-1"}
	executor := InlineExecutor(InlineDeps{
		OpenStore: func(context.Context, string) (driver.Conn, error) { return fakeStore{}, nil },
		List: func(context.Context, providersync.InProcessRun, providersync.InProcessListing) ([]providersync.ListedRepository, error) {
			return nil, errors.New("refused for svc%3A end")
		},
	})
	_, _, stderr := runVerb(t, "git", executor, githubBatchArgs, env)
	if !strings.Contains(stderr, "list repositories: refused for [REDACTED] end") {
		t.Errorf("got %q", stderr)
	}
}

// Only a text that is wholly the already-redacted one skips the plan boundary:
// a caller that wraps a redactedError with text of its own gets that text
// redacted.
func TestWrappedRedactedErrorGetsItsOuterTextRedacted(t *testing.T) {
	env := map[string]string{"CLICKHOUSE_URI": residualSink, "POSTGRES_URI": residualDB, "ORG_ID": "org-1"}
	executor := func(context.Context, Plan, cli.Env) error {
		return fmt.Errorf("outer A7kZ2 text: %w", redactedError{text: "inner [REDACTED] text"})
	}
	_, _, stderr := runVerb(t, "git", executor, githubBatchArgs, env)
	if !strings.Contains(stderr, "outer [REDACTED] text: inner [REDACTED] text") {
		t.Errorf("got %q", stderr)
	}
}
