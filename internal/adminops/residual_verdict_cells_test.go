package adminops

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// Verdict cells for residuals of the credential redaction. Every value is made up.

// P3 billing_reconcile.go: the Stripe key and the account database credentials
// are one redaction in the text a failed reconcile prints. The server echoes
// "<login> <password>"; the key "q4 env_p" spans the end of the login, the space
// and the start of the password.
func TestResidualBillingReconcileRedactsTheKeyAndTheDatabaseCredentialsAsOneUnion(t *testing.T) {
	r := fakepg.StartEchoing(t)
	values := map[string]string{"API_DATABASE_URI": r.URI, "STRIPE_SECRET_KEY": "q4 env_p"}
	var stdout, stderr bytes.Buffer
	code := RunBillingReconcile(context.Background(), cli.Env{
		Lookup: func(key string) (string, bool) { value, ok := values[key]; return value, ok }, Stdout: &stdout, Stderr: &stderr})
	if code == cli.ExitOK {
		t.Fatalf("exit %d: %q", code, stderr.String())
	}
	r.RequireConnected(t)
	if !strings.Contains(stderr.String(), "reconcile failed") {
		t.Fatalf("the failure never reached the printing path: %q", stderr.String())
	}
	for _, secret := range []string{r.Login, r.Password, "q4 env_p", "env_p", "mv7"} {
		if strings.Contains(stderr.String(), secret) {
			t.Errorf("%q printed: %q", secret, stderr.String())
		}
	}
}

// P3 orgdelete.go: a refusal prints its Message unredacted. The delete refuses an
// organization id that is not a UUID with a fixed text; an id that holds a
// credential value is not echoed.
func TestResidualOrgDeleteRefusalMessageDoesNotEchoTheOrganizationId(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := accountEnv(t, map[string]string{"CLICKHOUSE_URI": "clickhouse://chuser:Hn4tM@ch.example:9000/d"}, &stdout, &stderr)
	env.Args = []string{"--org-id", "Hn4tM-chuser", "--dry-run"}
	if code := runOrgsDelete(context.Background(), env); code == cli.ExitOK {
		t.Fatalf("exit %d", code)
	}
	if out := stdout.String(); !strings.Contains(out, "Error: Invalid organization id") || strings.Contains(out, "Hn4tM") || strings.Contains(out, "chuser") {
		t.Errorf("stdout %q stderr %q", out, stderr.String())
	}
}

// P3 orgdelete.go: the ClickHouse DSN is one raw value of the redactor; a server
// text that carries only its password (or its login) must be hidden too.
func TestResidualOrgDeleteRedactsTheClickHousePasswordAndLoginAlone(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := accountEnv(t, nil, &stdout, &stderr)
	_, redact, ok := deleteConfigFrom(env, "clickhouse://chuser:Hn4tM@ch.example:9000/d")
	if !ok {
		t.Fatalf("deleteConfigFrom refused: %q", stderr.String())
	}
	for _, secret := range []string{"Hn4tM", "chuser"} {
		if got := redact(errors.New("auth failed for " + secret + " end")).Error(); strings.Contains(got, secret) {
			t.Errorf("%q printed: %q", secret, got)
		}
	}
}
