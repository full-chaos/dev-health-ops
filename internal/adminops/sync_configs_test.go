package adminops

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// `admin sync-configs artifact-excludes set|show` refuse a bad command line before they connect, and never echo the
// typed configuration id.
func TestArtifactExcludesVerbsRefuseBadArgumentsBeforeConnecting(t *testing.T) {
	noDatabase := func(string) (string, bool) { return "", false }
	const id = "0b5e0f3a-1111-4111-8111-111111111111"
	cases := []struct {
		name string
		run  func(context.Context, cli.Env) int
		args []string
	}{
		{"set needs --config-id", runArtifactExcludesSet, []string{"--prefix", "ci-x-", "--reason", "r"}},
		{"set needs a UUID", runArtifactExcludesSet, []string{"--config-id", "not-a-uuid", "--prefix", "ci-x-", "--reason", "r"}},
		{"set needs --prefix or --clear", runArtifactExcludesSet, []string{"--config-id", id, "--reason", "r"}},
		{"set refuses --prefix with --clear", runArtifactExcludesSet, []string{"--config-id", id, "--prefix", "ci-x-", "--clear", "--reason", "r"}},
		{"set needs --reason", runArtifactExcludesSet, []string{"--config-id", id, "--prefix", "ci-x-"}},
		{"set refuses a blank --reason", runArtifactExcludesSet, []string{"--config-id", id, "--clear", "--reason", " "}},
		{"set refuses a positional", runArtifactExcludesSet, []string{"--config-id", id, "--clear", "--reason", "r", "x"}},
		{"show needs --config-id", runArtifactExcludesShow, nil},
		{"show needs a UUID", runArtifactExcludesShow, []string{"--config-id", "x"}},
	}
	for _, c := range cases {
		var stdout, stderr bytes.Buffer
		code := c.run(context.Background(), cli.Env{Args: c.args, Stdout: &stdout, Stderr: &stderr, Lookup: noDatabase})
		if code != cli.ExitUsage {
			t.Errorf("%s: exit %d, want %d (stderr %q)", c.name, code, cli.ExitUsage, stderr.String())
		}
		if stdout.Len() != 0 {
			t.Errorf("%s: stdout = %q", c.name, stdout.String())
		}
		if strings.Contains(stderr.String(), id) {
			t.Errorf("%s: stderr echoes the configuration id", c.name)
		}
	}
}

// artifactExcludesVerbs are the artifact-excludes verbs, each with the arguments that reach its first database statement.
var artifactExcludesVerbs = []struct {
	name string
	run  func(context.Context, cli.Env) int
	args []string
}{
	{"set", runArtifactExcludesSet, []string{"--config-id", "0b5e0f3a-1111-4111-8111-111111111111", "--prefix", "ci-x-", "--reason", "r"}},
	{"show", runArtifactExcludesShow, []string{"--config-id", "0b5e0f3a-1111-4111-8111-111111111111"}},
}

// The artifact-excludes verbs dial the admin database of the users and orgs verbs (CHAOS-14565): in the go-api pod
// (API_DATABASE_URI, the api role) they reach the api role, never POSTGRES_URI (a worker's domain role, which cannot
// write integrations), and the failure text redacts the API login and password.
func TestArtifactExcludesVerbsDialAPIDatabaseOverPostgresURI(t *testing.T) {
	for _, verb := range artifactExcludesVerbs {
		t.Run(verb.name, func(t *testing.T) {
			api := fakepg.StartEchoing(t)
			domain := fakepg.StartRefusing(t)
			const login, password = "apilogin_ax81", "apipassword_ax42"
			values := map[string]string{
				"API_DATABASE_URI": fmt.Sprintf("postgres://%s:%s@%s:%d/appdb?sslmode=disable", login, password, api.Host, api.Port),
				"POSTGRES_URI":     domain.URI,
			}
			lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
			var stdout, stderr bytes.Buffer
			code := verb.run(context.Background(), cli.Env{Args: verb.args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
			output := stdout.String() + stderr.String()
			api.RequireConnected(t)
			if domain.Connections() != 0 {
				t.Fatalf("dialed POSTGRES_URI although API_DATABASE_URI is set; output %q", output)
			}
			if code != cli.ExitFailure || strings.Contains(output, login) || strings.Contains(output, password) {
				t.Fatalf("exit %d, output %q: want exit 1 and no API login or password", code, output)
			}
		})
	}
}

// MIGRATION_DATABASE_URI, when set, still wins over API_DATABASE_URI, as for the users and orgs verbs.
func TestArtifactExcludesVerbsDialMigrationDatabaseFirst(t *testing.T) {
	for _, verb := range artifactExcludesVerbs {
		migration := fakepg.StartEchoing(t)
		api := fakepg.StartRefusing(t)
		values := map[string]string{"MIGRATION_DATABASE_URI": migration.URI, "API_DATABASE_URI": api.URI, "POSTGRES_URI": api.URI}
		lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
		var stdout, stderr bytes.Buffer
		verb.run(context.Background(), cli.Env{Args: verb.args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
		migration.RequireConnected(t)
		if api.Connections() != 0 {
			t.Fatalf("%s dialed API_DATABASE_URI although MIGRATION_DATABASE_URI is set; output %q", verb.name, stdout.String()+stderr.String())
		}
	}
}

// The api role holds what the verbs need (CHAOS-14565): table-wide UPDATE on integrations and sync_configurations (the
// row locks FOR NO KEY UPDATE and the writes), with either sessions flag; and the help names the go-api pod.
func TestAPIPostureGrantsTheArtifactExcludesTables(t *testing.T) {
	for _, sessionsOwnedByAuth := range []bool{false, true} {
		found := map[string]bool{}
		for _, table := range postgres.APIPosture(sessionsOwnedByAuth).RequiredTables {
			if table.TableName == "integrations" || table.TableName == "sync_configurations" {
				found[table.TableName] = table.AllowUpdate
			}
		}
		if !found["integrations"] || !found["sync_configurations"] {
			t.Errorf("sessions owned %v: the api posture grants UPDATE integrations=%v sync_configurations=%v, want both", sessionsOwnedByAuth,
				found["integrations"], found["sync_configurations"])
		}
	}
	for _, verb := range artifactExcludesVerbs {
		var stdout, stderr bytes.Buffer
		verb.run(context.Background(), cli.Env{Args: []string{"-h"}, Lookup: func(string) (string, bool) { return "", false }, Stdout: &stdout, Stderr: &stderr})
		if !strings.Contains(stderr.String(), "API_DATABASE_URI") || !strings.Contains(stderr.String(), "go-api pod") {
			t.Errorf("%s help does not name API_DATABASE_URI and the go-api pod: %q", verb.name, stderr.String())
		}
	}
}

// The artifact-excludes verbs redact the resolved API_DATABASE_URI credentials over every connection form, as the users
// and orgs verbs do (their failure text goes through the redactor of the DSN they dialed).
func TestArtifactExcludesVerbsRedactEveryConnectionFormOfAPIDatabaseURI(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	for _, verb := range artifactExcludesVerbs {
		refusing.RunGrid(t, true, func(t *testing.T, dsn string) string {
			values := map[string]string{"API_DATABASE_URI": dsn}
			lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
			var stdout, stderr bytes.Buffer
			verb.run(context.Background(), cli.Env{Args: verb.args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
			return stdout.String() + stderr.String()
		})
	}
}
