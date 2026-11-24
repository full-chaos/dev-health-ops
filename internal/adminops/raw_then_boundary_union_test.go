package adminops

import (
	"bytes"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func accountEnv(t *testing.T, extra map[string]string, stdout, stderr *bytes.Buffer) cli.Env {
	t.Helper()
	values := map[string]string{"API_DATABASE_URI": "postgres://acct:Q9zWx@db.example/app"}
	for key, value := range extra {
		values[key] = value
	}
	return cli.Env{Lookup: func(key string) (string, bool) { value, ok := values[key]; return value, ok }, Stdout: stdout, Stderr: stderr}
}

// The raw values (the analytics DSN, a PagerDuty client id) and the account
// password are one redaction: "Kx7ab" and "abQ9z" overlap each other, and the
// password "Q9zWx" starts inside the second. No byte of any of them may stay.
func TestOrgDeleteRedactsTheRawValuesAndTheAccountPasswordAsOneUnion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := accountEnv(t, map[string]string{"PAGER_DUTY_CLIENT_ID": "abQ9z"}, &stdout, &stderr)
	_, redact, ok := deleteConfigFrom(env, "Kx7ab")
	if !ok {
		t.Fatalf("deleteConfigFrom refused: %q", stderr.String())
	}
	if got := redact(errors.New("e Kx7abQ9zWx f")).Error(); got != "e [REDACTED] f" {
		t.Errorf("got %q", got)
	}
}

// The Stripe key "abab" overlaps itself in the text ("ababab") and ends where
// the account password "abQ9" starts.
func TestFinishSyncRedactsTheStripeKeyAndTheAccountPasswordAsOneUnion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := accountEnv(t, map[string]string{"API_DATABASE_URI": "postgres://acct:abQ9@db.example/app"}, &stdout, &stderr)
	if code := finishSync(env, secrets.NewValue("abab"), errors.New("e abababQ9 f")); code == cli.ExitOK {
		t.Fatalf("exit %d", code)
	}
	if got := stdout.String(); got != "Error: e [REDACTED] f\n" {
		t.Errorf("got %q", got)
	}
}

// billingFailureText serves both billing verbs that print a Stripe failure
// (reconcile and the sync verbs): the same union for the same text.
func TestBillingFailureTextIsOneUnionOfTheKeyAndTheAccountDatabase(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := accountEnv(t, map[string]string{"API_DATABASE_URI": "postgres://acct:abQ9@db.example/app"}, &stdout, &stderr)
	if got := billingFailureText(env, secrets.NewValue("abab"), errors.New("e abababQ9 f")); got != "e [REDACTED] f" {
		t.Errorf("got %q", got)
	}
	if got := billingFailureText(env, secrets.Value{}, errors.New("e abQ9 f")); got != "e [REDACTED] f" {
		t.Errorf("no key configured: got %q", got)
	}
	if got := billingFailureText(cli.Env{Lookup: func(string) (string, bool) { return "", false }, Stderr: &stderr}, secrets.NewValue("abab"), errors.New("e abab f")); got != "redacted" {
		t.Errorf("no account database: got %q", got)
	}
}
