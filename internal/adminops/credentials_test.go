package adminops

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// `dho admin users list (a pool: the error comes from the first query)` opens PostgreSQL itself: over every form of the
// connection string (URI and keyword, userinfo, pool parameters valid, invalid and
// zero, service and password files) no login or password the driver resolved reaches
// its output.
func TestRedactsResolvedCredentialsOverEveryConnectionForm(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	refusing.RunGrid(t, true, func(t *testing.T, dsn string) string {
		values := map[string]string{"MIGRATION_DATABASE_URI": dsn}
		lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
		var stdout, stderr bytes.Buffer
		runUsersList(context.Background(), cli.Env{Args: []string{"--limit", "5"}, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
		return stdout.String() + stderr.String()
	})
}

// A pod without MIGRATION_DATABASE_URI dials POSTGRES_URI; a failure there keeps
// the server's error text, with only the resolved credentials redacted.
func TestPostgresURIFallbackFailureKeepsTheErrorText(t *testing.T) {
	echoing := fakepg.StartEchoing(t)
	values := map[string]string{"POSTGRES_URI": echoing.URI}
	lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	code := runUsersList(context.Background(), cli.Env{Args: []string{"--limit", "5"}, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	echoing.RequireConnected(t)
	output := stdout.String() + stderr.String()
	if code != cli.ExitFailure {
		t.Fatalf("exit %d, want %d; output %q", code, cli.ExitFailure, output)
	}
	if leaks := echoing.Leaks(output); len(leaks) > 0 {
		t.Fatalf("output carries resolved credentials %q: %q", leaks, output)
	}
	if !strings.Contains(stderr.String(), `"code":"admin_failed"`) || !strings.Contains(stderr.String(), "server echo") || !strings.Contains(stderr.String(), "XX000") {
		t.Fatalf("the failure lost the server's error text: %q", stderr.String())
	}
}

// The POSTGRES_URI fallback redacts every connection form as MIGRATION_DATABASE_URI does.
func TestRedactsResolvedCredentialsOverEveryConnectionFormOfThePostgresURIFallback(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	refusing.RunGrid(t, true, func(t *testing.T, dsn string) string {
		values := map[string]string{"POSTGRES_URI": dsn}
		lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
		var stdout, stderr bytes.Buffer
		runUsersList(context.Background(), cli.Env{Args: []string{"--limit", "5"}, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
		return stdout.String() + stderr.String()
	})
}
