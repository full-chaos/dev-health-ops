package adminops

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// accountVerbs are the users and orgs verbs, each with the arguments that reach
// its first database statement.
var accountVerbs = []struct {
	name  string
	run   func(context.Context, cli.Env) int
	args  []string
	stdin string
}{
	{"users create", runUsersCreate, []string{"--email", "someone@service.dev-health.invalid", "--password-stdin"}, "long-enough-line\n"},
	{"users list", runUsersList, []string{"--limit", "5"}, ""},
	{"users update", runUsersUpdate, []string{"--email", "someone@service.dev-health.invalid", "--full-name", "Some One"}, ""},
	{"orgs create", runOrgsCreate, []string{"--name", "Some Org"}, ""},
	{"orgs list", runOrgsList, []string{"--limit", "5"}, ""},
	{"orgs delete", runOrgsDelete, []string{"--org-id", "00000000-0000-4000-8000-000000000001", "--dry-run"}, ""},
}

// Every users and orgs verb dials API_DATABASE_URI over POSTGRES_URI (the
// go-api pod has both; POSTGRES_URI there or in a worker pod cannot write
// users), and its failure keeps the server's text with the API login and
// password redacted.
func TestAccountVerbsDialAPIDatabaseOverPostgresURI(t *testing.T) {
	for _, verb := range accountVerbs {
		t.Run(verb.name, func(t *testing.T) {
			api := fakepg.StartEchoing(t)
			domain := fakepg.StartRefusing(t)
			const login, password = "apilogin_qx81", "apipassword_vw42"
			values := map[string]string{
				"API_DATABASE_URI": fmt.Sprintf("postgres://%s:%s@%s:%d/appdb?sslmode=disable", login, password, api.Host, api.Port),
				"POSTGRES_URI":     domain.URI,
			}
			lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
			var stdout, stderr bytes.Buffer
			code := verb.run(context.Background(), cli.Env{Args: verb.args, Lookup: lookup, Stdin: strings.NewReader(verb.stdin), Stdout: &stdout, Stderr: &stderr})
			output := stdout.String() + stderr.String()
			api.RequireConnected(t)
			if domain.Connections() != 0 {
				t.Fatalf("dialed POSTGRES_URI although API_DATABASE_URI is set; output %q", output)
			}
			if code != cli.ExitFailure {
				t.Fatalf("exit %d, want %d; output %q", code, cli.ExitFailure, output)
			}
			if strings.Contains(output, login) || strings.Contains(output, password) {
				t.Fatalf("output carries the API_DATABASE_URI login or password: %q", output)
			}
			if !strings.Contains(output, "server echo") {
				t.Fatalf("the failure lost the server's error text: %q", output)
			}
		})
	}
}

// MIGRATION_DATABASE_URI, when set, still wins over API_DATABASE_URI.
func TestAccountVerbsDialMigrationDatabaseFirst(t *testing.T) {
	migration := fakepg.StartEchoing(t)
	api := fakepg.StartRefusing(t)
	values := map[string]string{"MIGRATION_DATABASE_URI": migration.URI, "API_DATABASE_URI": api.URI, "POSTGRES_URI": api.URI}
	lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	runUsersList(context.Background(), cli.Env{Args: []string{"--limit", "5"}, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	migration.RequireConnected(t)
	if api.Connections() != 0 {
		t.Fatalf("dialed API_DATABASE_URI although MIGRATION_DATABASE_URI is set; output %q", stdout.String()+stderr.String())
	}
}

// The API_DATABASE_URI source redacts every connection form as MIGRATION_DATABASE_URI does.
func TestRedactsResolvedCredentialsOverEveryConnectionFormOfAPIDatabaseURI(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	refusing.RunGrid(t, true, func(t *testing.T, dsn string) string {
		values := map[string]string{"API_DATABASE_URI": dsn}
		lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
		var stdout, stderr bytes.Buffer
		runUsersList(context.Background(), cli.Env{Args: []string{"--limit", "5"}, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
		return stdout.String() + stderr.String()
	})
}

// The users and orgs verbs name the go-api pod in their help.
func TestAccountVerbsHelpNamesTheGoAPIPod(t *testing.T) {
	for _, verb := range accountVerbs {
		var stdout, stderr bytes.Buffer
		verb.run(context.Background(), cli.Env{Args: []string{"-h"}, Lookup: func(string) (string, bool) { return "", false }, Stdout: &stdout, Stderr: &stderr})
		if !strings.Contains(stderr.String(), "API_DATABASE_URI") || !strings.Contains(stderr.String(), "go-api pod") {
			t.Errorf("%s help does not name API_DATABASE_URI and the go-api pod: %q", verb.name, stderr.String())
		}
	}
}
