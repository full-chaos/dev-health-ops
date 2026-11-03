package usersbackfill

import (
	"bytes"
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func authVerb(t *testing.T, name string) cli.Command {
	t.Helper()
	for _, c := range Command().Children {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no %s verb", name)
	return cli.Command{}
}

const (
	prTarget = "11111111-1111-4111-8111-111111111111"
	prActor  = "22222222-2222-4222-8222-222222222222"
)

var platformRoleWriteArgs = map[string][]string{
	"platform-role-grant":  {"-principal", prTarget, "-role", "platform_admin", "-granted-by", prActor},
	"platform-role-revoke": {"-principal", prTarget, "-role", "platform_admin", "-revoked-by", prActor},
}

func runAuthVerb(t *testing.T, name string, lookup func(string) (string, bool), args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := authVerb(t, name).Run(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	return code, stdout.String(), stderr.String()
}

func ownedLookup(value string) func(string) (string, bool) {
	return func(k string) (string, bool) { return value, k == "AUTH_PLATFORM_ROLES_OWNED" }
}

// While public users.is_superuser is the authority, a platform role write is
// refused before any database access; a malformed switch fails.
func TestPlatformRoleWriteVerbsRefuseBeforeConnectWhileNotOwned(t *testing.T) {
	orig := connectFn
	connectFn = func(context.Context, cli.Env, string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
		t.Fatal("connect was reached: the switch gate must refuse before any database access")
		return nil, authschema.ValidatedIdentifier{}, cli.ExitFailure
	}
	t.Cleanup(func() { connectFn = orig })
	for name, args := range platformRoleWriteArgs {
		for _, value := range []string{"", "false"} {
			code, stdout, stderr := runAuthVerb(t, name, ownedLookup(value), args...)
			if code != cli.ExitRefused || !bytes.Contains([]byte(stderr), []byte("AUTH_PLATFORM_ROLES_OWNED")) || stdout != "" {
				t.Errorf("%s with switch %q: exit %d, stderr %q, stdout %q", name, value, code, stderr, stdout)
			}
		}
		if code, _, _ := runAuthVerb(t, name, ownedLookup("maybe"), args...); code != cli.ExitFailure {
			t.Errorf("%s with a malformed switch: exit %d, want %d", name, code, cli.ExitFailure)
		}
	}
}

// With the switch on, the write verbs pass the gate and reach the database.
func TestPlatformRoleWriteVerbsConnectWhileOwned(t *testing.T) {
	orig := connectFn
	t.Cleanup(func() { connectFn = orig })
	for name, args := range platformRoleWriteArgs {
		reached := false
		connectFn = func(context.Context, cli.Env, string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
			reached = true
			return nil, authschema.ValidatedIdentifier{}, cli.ExitFailure
		}
		if code, _, _ := runAuthVerb(t, name, ownedLookup("true"), args...); code != cli.ExitFailure || !reached {
			t.Errorf("%s with the switch on: exit %d, connect reached %v", name, code, reached)
		}
	}
}

func TestPlatformRoleVerbsRefuseBadArguments(t *testing.T) {
	orig := connectFn
	connectFn = func(context.Context, cli.Env, string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
		t.Fatal("connect was reached on bad arguments")
		return nil, authschema.ValidatedIdentifier{}, cli.ExitFailure
	}
	t.Cleanup(func() { connectFn = orig })
	for name, bad := range map[string][][]string{
		"platform-role-grant": {
			{"extra"},
			{"-principal", prTarget, "-granted-by", prActor},
			{"-principal", "not-a-uuid", "-role", "platform_admin", "-granted-by", prActor},
			{"-principal", prTarget, "-role", "platform_admin"},
			{"-principal", "00000000-0000-0000-0000-000000000000", "-role", "platform_admin", "-granted-by", prActor},
			{"-principal", prTarget, "-role", "platform_admin", "-granted-by", prActor, "-expires-at", "tomorrow"},
		},
		"platform-role-revoke": {
			{"extra"},
			{"-principal", prTarget, "-revoked-by", prActor},
			{"-principal", prTarget, "-role", "platform_admin"},
			{"-principal", prTarget, "-role", "platform_admin", "-revoked-by", "x"},
		},
		"platform-role-list": {{"extra"}, {"-nope"}},
	} {
		for _, args := range bad {
			if code, _, _ := runAuthVerb(t, name, ownedLookup("true"), args...); code != cli.ExitUsage {
				t.Errorf("%s %v: exit %d, want %d", name, args, code, cli.ExitUsage)
			}
		}
	}
}

// The list verb is read-only and needs no switch.
func TestPlatformRoleListNeedsNoSwitch(t *testing.T) {
	orig := connectFn
	t.Cleanup(func() { connectFn = orig })
	reached := false
	connectFn = func(context.Context, cli.Env, string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
		reached = true
		return nil, authschema.ValidatedIdentifier{}, cli.ExitFailure
	}
	if code, _, _ := runAuthVerb(t, "platform-role-list", ownedLookup("")); code != cli.ExitFailure || !reached {
		t.Fatalf("list with the switch off: exit %d, connect reached %v", code, reached)
	}
}
