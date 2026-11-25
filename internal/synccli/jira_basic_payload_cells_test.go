package synccli

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// Cells owed by READ-AUTH-7046-2. Every value is made up.

func basicPayloads(email, token string) []string {
	raw := []byte(email + ":" + token)
	return []string{
		base64.StdEncoding.EncodeToString(raw), base64.RawStdEncoding.EncodeToString(raw),
		base64.URLEncoding.EncodeToString(raw), base64.RawURLEncoding.EncodeToString(raw),
	}
}

// The run boundary holds the payload of the pair the run sends: the final one, mixed or not.
func TestSettingsBoundaryHidesTheBasicPayloadOfTheFinalPair(t *testing.T) {
	t.Logf("cell: TestSettingsBoundaryHidesTheBasicPayloadOfTheFinalPair")
	const storedEmail, storedToken = "stored@example.test", "zz9tok-stored-aaaaaaaa"
	const overrideEmail, overrideToken = "override@example.test", "zz9tok-override-bbbbbbbb"
	pairs := map[string]settings{
		"stored pair":                      {email: storedEmail, token: storedToken},
		"override email, stored token":     {email: overrideEmail, token: storedToken},
		"stored email, override token":     {email: storedEmail, token: overrideToken},
		"override email, override token":   {email: overrideEmail, token: overrideToken},
		"fully configured environment run": mustSettingsFromEnv(t, envOverrides{email: overrideEmail, token: overrideToken, base: "https://acme.atlassian.net", cloudID: "cloud", organizationID: "org"}),
	}
	for name, s := range pairs {
		for _, payload := range basicPayloads(s.email, s.token) {
			if got := s.boundary().RedactText("rejected: Authorization: Basic " + payload); strings.Contains(got, payload) {
				t.Errorf("%s: payload %q survives in %q", name, payload, got)
			}
		}
	}
}

func mustSettingsFromEnv(t *testing.T, o envOverrides) settings {
	t.Helper()
	s, err := settingsFromEnv(o)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// A full run on the environment path: the client's error echoes the Authorization header.
func TestRunHidesTheBasicPayloadWhenTheGatewayEchoesIt(t *testing.T) {
	t.Logf("cell: TestRunHidesTheBasicPayloadWhenTheGatewayEchoesIt")
	env := validEnv()
	payload := basicPayloads(env["ATLASSIAN_EMAIL"], env["ATLASSIAN_API_TOKEN"])[0]
	rec := &recorded{}
	d := stubDeps(rec, failingClient{err: errors.New("rejected request, Authorization: Basic " + payload)}, nil)
	code, stdout, stderr := run(t, env, d, "--provider", "jira", "--org", "o")
	if code == cli.ExitOK || !strings.Contains(stderr, "rejected request") {
		t.Fatalf("exit %d: the client error never reached the printing path: %q", code, stderr)
	}
	if strings.Contains(stdout+stderr, payload) {
		t.Errorf("the Basic payload printed: %q", stderr)
	}
}

// L3: the discovery error is text only, so no later Unwrap reaches the unredacted failure.
func TestRedactCredentialErrorIsTextOnly(t *testing.T) {
	t.Logf("cell: TestRedactCredentialErrorIsTextOnly")
	failure := errors.New("rejected the credential zz9tok-synthetic-aaaaaaaa")
	got := redactCredentialError(failure, "zz9tok-synthetic-aaaaaaaa")
	if strings.Contains(got.Error(), "zz9tok-synthetic-aaaaaaaa") {
		t.Fatalf("the value survives: %q", got.Error())
	}
	if unwrapped := errors.Unwrap(got); unwrapped != nil {
		t.Errorf("the unredacted failure is still reachable: %v", unwrapped)
	}
}

type unusedDecryptor struct{}

func (unusedDecryptor) Decrypt(secrets.Value) ([]byte, error) { return nil, errors.New("unused") }

// L2: the pool connects on its first query, so the connect failure of the stored-credential read
// carries the database login unless resolveTeamsSettings redacts it.
func TestResolveTeamsSettingsHidesTheDatabaseLoginOfALazyConnectFailure(t *testing.T) {
	t.Logf("cell: TestResolveTeamsSettingsHidesTheDatabaseLoginOfALazyConnectFailure")
	const login, password = "zz9login-synthetic", "zz9pw-synthetic-aaaaaaaa"
	dsn := "postgresql://" + login + ":" + password + "@127.0.0.1:1/synthdb?connect_timeout=2"
	env := cli.Env{Lookup: func(key string) (string, bool) {
		if key == PostgresURIKey {
			return dsn, true
		}
		return "", false
	}}
	d := defaultDeps()
	d.decryptor = func(cli.Env) (providerfoundation.CredentialDecryptor, error) { return unusedDecryptor{}, nil }
	_, err := resolveTeamsSettings(context.Background(), env, d, "org-under-test", "")
	if err == nil {
		t.Fatal("expected the connect failure")
	}
	for _, secret := range []string{login, password} {
		if strings.Contains(err.Error(), secret) {
			t.Errorf("%q survives in %q", secret, err.Error())
		}
	}
}
