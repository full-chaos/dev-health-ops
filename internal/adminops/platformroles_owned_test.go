package adminops

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func platformRolesLookup(owned string) func(string) (string, bool) {
	values := map[string]string{"API_DATABASE_URI": "postgres://user@[::1/db", "AUTH_PLATFORM_ROLES_OWNED": owned}
	return func(key string) (string, bool) { value, ok := values[key]; return value, ok }
}

// While auth-service owns the platform roles, dho users create/update with
// --superuser or --no-superuser refuse before a DSN is resolved; a malformed
// switch is a configuration error.
func TestDhoUsersSuperuserRefusesWhileOwned(t *testing.T) {
	for _, verb := range []struct {
		name  string
		run   func(context.Context, cli.Env) int
		args  []string
		stdin string
	}{
		{"users create --superuser", runUsersCreate, []string{"--email", "a@example.test", "--password-stdin", "--superuser"}, "long-enough-line\n"},
		{"users update --superuser", runUsersUpdate, []string{"--email", "a@example.test", "--superuser"}, ""},
		{"users update --no-superuser", runUsersUpdate, []string{"--email", "a@example.test", "--no-superuser"}, ""},
	} {
		t.Run(verb.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := verb.run(context.Background(), cli.Env{Args: verb.args, Lookup: platformRolesLookup("true"), Stdin: strings.NewReader(verb.stdin), Stdout: &stdout, Stderr: &stderr})
			if code != cli.ExitRefused || !strings.Contains(stderr.String(), "platform roles") {
				t.Fatalf("exit %d stderr %q stdout %q, want ExitRefused with the platform roles text", code, stderr.String(), stdout.String())
			}
			stdout.Reset()
			stderr.Reset()
			code = verb.run(context.Background(), cli.Env{Args: verb.args, Lookup: platformRolesLookup("yes"), Stdin: strings.NewReader(verb.stdin), Stdout: &stdout, Stderr: &stderr})
			if code == cli.ExitRefused || code == 0 || !strings.Contains(stderr.String(), "AUTH_PLATFORM_ROLES_OWNED") {
				t.Fatalf("malformed switch: exit %d stderr %q, want a configuration error naming the switch", code, stderr.String())
			}
		})
	}
}
