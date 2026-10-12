package adminops

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// billingVerbs are the verbs that read or write the billing tables.
var billingVerbs = []struct {
	name string
	run  func(context.Context, cli.Env) int
	args []string
}{
	{"billing seed", runBillingSeed, nil},
	{"billing list", runBillingList, nil},
	{"billing pull-stripe", runBillingPullStripe, nil},
	{"billing sync-stripe", runBillingSyncStripe, nil},
	{"billing reconcile", RunBillingReconcile, nil},
}

// Every billing verb dials the DSN key order of the users and orgs verbs
// (MIGRATION_DATABASE_URI, API_DATABASE_URI, POSTGRES_URI): in the go-api pod,
// API_DATABASE_URI, the role the api's billing routes use, beats the domain
// role in POSTGRES_URI.
func TestBillingVerbsDialAPIDatabaseOverPostgresURI(t *testing.T) {
	for _, verb := range billingVerbs {
		t.Run(verb.name, func(t *testing.T) {
			api := fakepg.StartEchoing(t)
			domain := fakepg.StartRefusing(t)
			values := map[string]string{
				"API_DATABASE_URI":  fmt.Sprintf("postgres://apilogin_qx81:apipassword_vw42@%s:%d/appdb?sslmode=disable", api.Host, api.Port),
				"POSTGRES_URI":      domain.URI,
				"STRIPE_SECRET_KEY": "stripe-test-value-not-a-credential",
			}
			lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
			var stdout, stderr bytes.Buffer
			verb.run(context.Background(), cli.Env{Args: verb.args, Lookup: lookup, Stdin: strings.NewReader(""), Stdout: &stdout, Stderr: &stderr})
			output := stdout.String() + stderr.String()
			api.RequireConnected(t)
			if domain.Connections() != 0 {
				t.Fatalf("dialed POSTGRES_URI although API_DATABASE_URI is set; output %q", output)
			}
			if strings.Contains(output, "apilogin_qx81") || strings.Contains(output, "apipassword_vw42") {
				t.Fatalf("output carries the API_DATABASE_URI login or password: %q", output)
			}
		})
	}
}

// MIGRATION_DATABASE_URI, when set, still wins for the billing verbs.
func TestBillingVerbsDialMigrationDatabaseFirst(t *testing.T) {
	for _, verb := range billingVerbs {
		t.Run(verb.name, func(t *testing.T) {
			migration := fakepg.StartEchoing(t)
			api := fakepg.StartRefusing(t)
			values := map[string]string{"MIGRATION_DATABASE_URI": migration.URI, "API_DATABASE_URI": api.URI, "POSTGRES_URI": api.URI, "STRIPE_SECRET_KEY": "stripe-test-value-not-a-credential"}
			lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
			var stdout, stderr bytes.Buffer
			verb.run(context.Background(), cli.Env{Args: verb.args, Lookup: lookup, Stdin: strings.NewReader(""), Stdout: &stdout, Stderr: &stderr})
			migration.RequireConnected(t)
			if api.Connections() != 0 {
				t.Fatalf("dialed API_DATABASE_URI although MIGRATION_DATABASE_URI is set")
			}
		})
	}
}

// Without any database variable the refusal names the go-api pod, not river-migrate.
func TestBillingVerbsRefusalNamesGoAPIPod(t *testing.T) {
	for _, verb := range billingVerbs {
		var stdout, stderr bytes.Buffer
		code := verb.run(context.Background(), cli.Env{Args: verb.args, Lookup: func(string) (string, bool) { return "", false }, Stdin: strings.NewReader(""), Stdout: &stdout, Stderr: &stderr})
		out := stdout.String() + stderr.String()
		if code == cli.ExitOK || strings.Contains(out, "river-migrate") || !strings.Contains(out, "API_DATABASE_URI") {
			t.Errorf("%s: code %d output %q", verb.name, code, out)
		}
	}
}
