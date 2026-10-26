package usersbackfill

import (
	"bytes"
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func oauthSSOCountVerb(t *testing.T) cli.Command {
	t.Helper()
	for _, c := range Command().Children {
		if c.Name == "oauth-sso-count" {
			return c
		}
	}
	t.Fatal("no oauth-sso-count verb")
	return cli.Command{}
}

func TestOAuthSSOCountVerbRefusesArguments(t *testing.T) {
	for _, args := range [][]string{{"extra"}, {"-dry-run"}, {"-batch", "5"}} {
		code := oauthSSOCountVerb(t).Run(context.Background(), cli.Env{Args: args,
			Lookup: func(string) (string, bool) { return "", false }, Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
		if code != cli.ExitUsage {
			t.Fatalf("args %v: exit %d, want %d", args, code, cli.ExitUsage)
		}
	}
}

func TestOAuthSSOCountVerbWithoutADatabaseFailsWithoutRunning(t *testing.T) {
	var stderr, stdout bytes.Buffer
	code := oauthSSOCountVerb(t).Run(context.Background(), cli.Env{
		Lookup: func(string) (string, bool) { return "", false }, Stdout: &stdout, Stderr: &stderr})
	if code != cli.ExitFailure || !bytes.Contains(stderr.Bytes(), []byte("AUTH_DATABASE_URI")) || stdout.Len() != 0 {
		t.Fatalf("exit %d, stderr %q, stdout %q", code, stderr.String(), stdout.String())
	}
}

func TestOAuthSSOKeysOddSocialBaseURLFormsAreSelfHostedUnbindable(t *testing.T) {
	row := uuid.New()
	for base, wantKey := range map[string]bool{
		"https://gitlab.com":      true,
		"https://gitlab.com/":     true,
		"https://GitLab.com/":     false,
		"https://gitlab.com:443":  false,
		"https://gitlab.com/path": false,
		"http://gitlab.com":       false,
	} {
		key, selfHosted := OAuthSSOKey(cand(uuid.New(), row, "gitlab", "5", base))
		if (key != nil) != wantKey || selfHosted == wantKey {
			t.Fatalf("base_url %q: key=%v selfHosted=%v, want key=%v", base, key, selfHosted, wantKey)
		}
	}
}
