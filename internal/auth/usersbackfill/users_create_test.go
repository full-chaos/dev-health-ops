package usersbackfill

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const goodPassword = "Zq7-correct-horse-battery" // gitleaks:allow

func envLookup(values map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := values[k]; return v, ok }
}

func runUsersCreateVerb(t *testing.T, values map[string]string, stdin string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := authVerb(t, "users-create").Run(context.Background(), cli.Env{
		Args: args, Lookup: envLookup(values), Stdin: strings.NewReader(stdin), Stdout: &stdout, Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

func usersCreateNoConnect(t *testing.T) {
	t.Helper()
	orig := connectFn
	connectFn = func(context.Context, cli.Env, string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
		t.Fatal("connect was reached: the verb must refuse before any database access")
		return nil, authschema.ValidatedIdentifier{}, cli.ExitFailure
	}
	t.Cleanup(func() { connectFn = orig })
}

var createArgs = []string{"-email", "new@example.com", "-password-stdin"}

func TestUsersCreateRefusesBeforeConnectUnlessUsersOwned(t *testing.T) {
	usersCreateNoConnect(t)
	for _, values := range []map[string]string{{}, {"AUTH_USERS_OWNED": "false"}, {"AUTH_USERS_OWNED": ""}} {
		code, stdout, stderr := runUsersCreateVerb(t, values, goodPassword+"\n", createArgs...)
		if code != cli.ExitRefused || stdout != "" || !strings.Contains(stderr, "AUTH_USERS_OWNED") || !strings.Contains(stderr, "dho admin users create") {
			t.Errorf("values %v: exit %d, stdout %q, stderr %q", values, code, stdout, stderr)
		}
	}
	if code, _, _ := runUsersCreateVerb(t, map[string]string{"AUTH_USERS_OWNED": "maybe"}, goodPassword+"\n", createArgs...); code != cli.ExitFailure {
		t.Errorf("malformed switch: exit %d, want %d", code, cli.ExitFailure)
	}
}

func TestUsersCreateTakesThePasswordFromStdinOnly(t *testing.T) {
	usersCreateNoConnect(t)
	owned := map[string]string{"AUTH_USERS_OWNED": "true"}
	cases := map[string][]string{
		"no password-stdin flag": {"-email", "new@example.com"},
		"a password argument":    {"-email", "new@example.com", "-password", goodPassword},
		"no email":               {"-password-stdin"},
		"a positional argument":  {"-email", "new@example.com", "-password-stdin", goodPassword},
	}
	for name, args := range cases {
		code, stdout, stderr := runUsersCreateVerb(t, owned, goodPassword+"\n", args...)
		if code != cli.ExitUsage || stdout != "" || strings.Contains(stderr, goodPassword) {
			t.Errorf("%s: exit %d, stdout %q, stderr %q", name, code, stdout, stderr)
		}
	}
	if code, _, _ := runUsersCreateVerb(t, owned, "", createArgs...); code != cli.ExitUsage {
		t.Errorf("empty stdin: exit %d, want %d", code, cli.ExitUsage)
	}
	// An env variable is no password source: the password stays unread without stdin.
	if code, _, _ := runUsersCreateVerb(t, map[string]string{"AUTH_USERS_OWNED": "true", "PASSWORD": goodPassword, "AUTH_USER_PASSWORD": goodPassword}, "", createArgs...); code != cli.ExitUsage {
		t.Errorf("password in env: exit %d, want %d", code, cli.ExitUsage)
	}
}

func TestUsersCreateRefusesAWeakPasswordBeforeConnectWithoutEchoingIt(t *testing.T) {
	usersCreateNoConnect(t)
	for _, weak := range []string{"short1", "alllettersnodigits", "password1234"} {
		code, stdout, stderr := runUsersCreateVerb(t, map[string]string{"AUTH_USERS_OWNED": "true"}, weak+"\n", createArgs...)
		if code != cli.ExitRefused || stdout != "" || strings.Contains(stderr, weak) || !strings.Contains(stderr, "password validation failed") {
			t.Errorf("%q: exit %d, stdout %q, stderr %q", weak, code, stdout, stderr)
		}
	}
}

func TestUsersCreateSuperuserRefusesBeforeConnectWhileRolesNotOwned(t *testing.T) {
	usersCreateNoConnect(t)
	for _, roles := range []string{"", "false"} {
		code, stdout, stderr := runUsersCreateVerb(t, map[string]string{"AUTH_USERS_OWNED": "true", "AUTH_PLATFORM_ROLES_OWNED": roles}, goodPassword+"\n", append(createArgs, "-superuser")...)
		if code != cli.ExitRefused || stdout != "" || strings.Contains(stderr, goodPassword) ||
			!strings.Contains(stderr, "AUTH_PLATFORM_ROLES_OWNED=true") || !strings.Contains(stderr, "auth-platform-roles-flip-and-rollback") || !strings.Contains(stderr, "without --superuser") {
			t.Errorf("roles %q: exit %d, stdout %q, stderr %q", roles, code, stdout, stderr)
		}
	}
}

func TestUsersCreateConnectsWhenOwnedAndRefusesSuperuserOnlyAfterTheRead(t *testing.T) {
	orig := connectFn
	t.Cleanup(func() { connectFn = orig })
	reached := 0
	connectFn = func(context.Context, cli.Env, string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
		reached++
		return nil, authschema.ValidatedIdentifier{}, cli.ExitFailure
	}
	values := map[string]string{"AUTH_USERS_OWNED": "true", "AUTH_PLATFORM_ROLES_OWNED": "true"}
	if code, _, _ := runUsersCreateVerb(t, values, goodPassword+"\n", createArgs...); code != cli.ExitFailure || reached != 1 {
		t.Errorf("plain create: exit %d, connect reached %d", code, reached)
	}
	if code, _, _ := runUsersCreateVerb(t, values, goodPassword+"\n", append(createArgs, "-superuser")...); code != cli.ExitFailure || reached != 2 {
		t.Errorf("superuser create: exit %d, connect reached %d", code, reached)
	}
}
