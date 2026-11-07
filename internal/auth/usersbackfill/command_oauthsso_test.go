package usersbackfill

import (
	"bytes"
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
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

func TestOAuthSSOKeysBaseURLFormsKeyByLowercaseHost(t *testing.T) {
	row := uuid.New()
	for base, wantIssuer := range map[string]string{
		"https://gitlab.com":               "https://gitlab.com",
		"https://gitlab.com/":              "https://gitlab.com",
		"https://GitLab.com/":              "https://gitlab.com",
		"https://gitlab.com//":             "https://gitlab.com",
		"https://gitlab.com/path":          "https://gitlab.com",
		"https://gitlab.com:443":           "https://gitlab.com:443",
		"http://gitlab.com":                "http://gitlab.com",
		"https://Git.Corp.Example:8443/gl": "https://git.corp.example:8443",
		"git.corp.example":                 "",
		"ftp://git.corp.example":           "",
	} {
		key := OAuthSSOKey(cand(uuid.New(), row, "gitlab", "5", base))
		if wantIssuer == "" {
			if key != nil {
				t.Fatalf("base_url %q: key %+v, want none", base, key)
			}
			continue
		}
		if key == nil || key.Issuer != wantIssuer || key.Provider != identity.Provider(row.String()) {
			t.Fatalf("base_url %q: key %+v, want issuer %q", base, key, wantIssuer)
		}
	}
}
