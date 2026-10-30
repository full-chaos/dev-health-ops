package adminops

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
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

// The dho admin Operator carries the switch and AUTH_DATABASE_SCHEMA into its
// store, so its users reads take is_superuser from the platform role rule in
// that schema while owned. The pool is built without connecting.
func TestOperatorCarriesThePlatformRoleRead(t *testing.T) {
	dsn := func(secrets.LookupEnv, io.Writer) (secrets.Value, bool) {
		return secrets.NewValue("postgres://nobody:none@127.0.0.1:1/none?connect_timeout=1"), true
	}
	for _, c := range []struct {
		owned, schema string
		wantOwned     bool
		wantSchema    string
	}{
		{"true", "acp", true, "acp"},
		{"", "", false, "auth"},
	} {
		values := map[string]string{"AUTH_PLATFORM_ROLES_OWNED": c.owned, "AUTH_DATABASE_SCHEMA": c.schema}
		env := cli.Env{Lookup: func(key string) (string, bool) { value, ok := values[key]; return value, ok && value != "" },
			Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}}
		op, closePool, code := operatorFor(context.Background(), env, dsn)
		closePool()
		if code != 0 || op.PlatformRolesOwned != c.wantOwned || op.AuthSchema != c.wantSchema {
			t.Errorf("%+v: owned %v schema %q code %d, want %v %q", c, op.PlatformRolesOwned, op.AuthSchema, code, c.wantOwned, c.wantSchema)
		}
	}
	values := map[string]string{"AUTH_DATABASE_SCHEMA": "a;b"}
	env := cli.Env{Lookup: func(key string) (string, bool) { value, ok := values[key]; return value, ok },
		Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}}
	if _, closePool, code := operatorFor(context.Background(), env, dsn); code == 0 {
		closePool()
		t.Error("an unusable auth schema was accepted")
	}
}
