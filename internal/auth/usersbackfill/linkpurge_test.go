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

func purgeVerb(t *testing.T) cli.Command {
	t.Helper()
	for _, c := range Command().Children {
		if c.Name == "legacy-link-purge" {
			return c
		}
	}
	t.Fatal("no legacy-link-purge verb")
	return cli.Command{}
}

func runPurgeVerb(t *testing.T, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := purgeVerb(t).Run(context.Background(), cli.Env{
		Args:   args,
		Lookup: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
		Stdout: &stdout, Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

func failOnConnect(t *testing.T) {
	t.Helper()
	old := connectFn
	connectFn = func(context.Context, cli.Env, string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
		t.Error("the verb opened a database connection before its gates passed")
		return nil, authschema.ValidatedIdentifier{}, cli.ExitFailure
	}
	t.Cleanup(func() { connectFn = old })
}

func TestLegacyLinkPurgeRefusesWhenUsersAreNotOwned(t *testing.T) {
	failOnConnect(t)
	for name, env := range map[string]map[string]string{
		"unset": {}, "false": {"AUTH_USERS_OWNED": "false"},
	} {
		code, stdout, stderr := runPurgeVerb(t, env)
		if code != cli.ExitRefused {
			t.Errorf("%s: exit %d, want %d (stderr %q)", name, code, cli.ExitRefused, stderr)
		}
		if stdout != "" || !strings.Contains(stderr, "AUTH_USERS_OWNED") {
			t.Errorf("%s: stdout %q stderr %q", name, stdout, stderr)
		}
	}
}

func TestLegacyLinkPurgeRefusesAMalformedSwitch(t *testing.T) {
	failOnConnect(t)
	if code, _, _ := runPurgeVerb(t, map[string]string{"AUTH_USERS_OWNED": "maybe"}); code != cli.ExitFailure {
		t.Fatalf("exit %d, want %d", code, cli.ExitFailure)
	}
}

func TestLegacyLinkPurgeRefusesBadArguments(t *testing.T) {
	failOnConnect(t)
	owned := map[string]string{"AUTH_USERS_OWNED": "true"}
	for _, args := range [][]string{{"extra"}, {"-batch", "0"}, {"-batch", "20000"}, {"-nope"}, {"-run-id", "Has Space"}, {"-run-id", ""}} {
		if code, _, _ := runPurgeVerb(t, owned, args...); code != cli.ExitUsage {
			t.Errorf("args %v: exit %d, want %d", args, code, cli.ExitUsage)
		}
	}
}

func TestLegacyLinkPurgeAcceptsTheFlagsOfTheBackfillJob(t *testing.T) {
	failOnConnect(t)
	if code, _, stderr := runPurgeVerb(t, map[string]string{}, "--batch=500", "--dry-run", "--run-id=roll-1"); code != cli.ExitRefused {
		t.Fatalf("exit %d, want the owned-switch refusal %d (stderr %q)", code, cli.ExitRefused, stderr)
	}
}

func TestLegacyLinkPurgeReportIsCountsOnly(t *testing.T) {
	got := PurgeReport{EmailVerificationTokens: 3, PasswordResetTokens: 2}.Lines()
	want := []string{"mode=purge", "email_verification_tokens_purged=3", "password_reset_tokens_purged=2"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("lines = %v, want %v", got, want)
	}
	dry := PurgeReport{DryRun: true, EmailVerificationTokens: 1}.Lines()
	if dry[0] != "mode=dry-run" || dry[1] != "email_verification_tokens_would_purge=1" {
		t.Fatalf("dry lines = %v", dry)
	}
}
