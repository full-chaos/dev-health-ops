package synccli

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func githubDBPlan(dbURL string) Plan {
	return Plan{OrgSource: OrgFromDBFirst, DB: &dbURL, GitHub: &GitHubCredentials{Mode: CredentialDB}}
}

func noCredentialLookups(t *testing.T) dbLookups {
	t.Helper()
	return dbLookups{
		FirstOrg: firstOrganization,
		GitHubCredential: func(context.Context, string, string, cli.Env) (*GitHubCredentials, error) {
			t.Fatal("the credential lookup must not run without an organization")
			return nil, nil
		},
	}
}

func TestFirstOrganizationLogsAnUnreachableDatabaseWithoutTheDSN(t *testing.T) {
	log := captureLookupLog(t)
	const dbURL = "postgresql://dho_user:s3cret-pw@127.0.0.1:1/dho?connect_timeout=2"
	plan, refusal := resolveDBLookups(context.Background(), noCredentialLookups(t), githubDBPlan(dbURL), cli.Env{})
	if refusal == nil || refusal.Message != missingGitHubCredentials || plan.Org != nil {
		t.Fatalf("refusal = %+v, org = %v; want the missing-credentials refusal and no organization", refusal, plan.Org)
	}
	out := log.String()
	if n := strings.Count(out, `"msg":"synccli.github_credential_lookup_failed"`); n != 1 {
		t.Fatalf("%d failure lines, want 1: %s", n, out)
	}
	for _, want := range []string{`"level":"WARN"`, `"reason":"database_connect_failed"`, `"org_id":""`, `"error_class":"`, `"error_type":"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q: %s", want, out)
		}
	}
	if strings.Contains(out, `"error_type":""`) || strings.Contains(out, `"error_class":""`) {
		t.Fatalf("log has an empty class or type: %s", out)
	}
	for _, secret := range []string{"s3cret-pw", "dho_user", "127.0.0.1"} {
		if strings.Contains(out, secret) {
			t.Fatalf("log carries %q: %s", secret, out)
		}
	}
}

func TestFirstOrganizationNoDatabaseReasonsAreLogged(t *testing.T) {
	for _, tc := range []struct {
		name   string
		dbURL  string
		reason string
	}{
		{"unusable URL", "mysql://dho_user:s3cret-pw@127.0.0.1:1/dho", "database_url_not_usable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := captureLookupLog(t)
			if _, refusal := resolveDBLookups(context.Background(), noCredentialLookups(t), githubDBPlan(tc.dbURL), cli.Env{}); refusal == nil {
				t.Fatal("want a refusal")
			}
			out := log.String()
			for _, want := range []string{`"reason":"` + tc.reason + `"`, `"error_type":"`} {
				if !strings.Contains(out, want) {
					t.Fatalf("log lacks %q: %s", want, out)
				}
			}
			if strings.Contains(out, `"error_type":""`) || strings.Contains(out, "s3cret-pw") {
				t.Fatalf("log has an empty type or the password: %s", out)
			}
		})
	}
}

func TestNoOrganizationIsLoggedAsOrganizationNotFound(t *testing.T) {
	log := captureLookupLog(t)
	lookups := dbLookups{
		FirstOrg: func(context.Context, string) (string, error) {
			return "", lookupFailure(reasonOrganizationNotFound)
		},
		GitHubCredential: noCredentialLookups(t).GitHubCredential,
	}
	if _, refusal := resolveDBLookups(context.Background(), lookups, githubDBPlan("postgresql://db/x"), cli.Env{}); refusal == nil {
		t.Fatal("want a refusal")
	}
	if !strings.Contains(log.String(), `"reason":"organization_not_found"`) {
		t.Fatalf("log lacks organization_not_found: %s", log.String())
	}
}

const firstOrgSecretText = "dial postgresql://dho_user:s3cret-pw@127.0.0.1:1/dho refused"

func connectFailingFirstOrg(context.Context, string) (string, error) {
	return "", lookupFailureWith(reasonDatabaseConnectFailed, errors.New(firstOrgSecretText))
}

func assertFirstOrgFailureLogged(t *testing.T, out string) {
	t.Helper()
	if n := strings.Count(out, `"msg":"synccli.github_credential_lookup_failed"`); n != 1 {
		t.Fatalf("%d failure lines, want 1: %s", n, out)
	}
	for _, want := range []string{`"level":"WARN"`, `"reason":"database_connect_failed"`, `"error_class":"`, `"error_type":"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q: %s", want, out)
		}
	}
	for _, secret := range []string{"s3cret-pw", "dho_user", "127.0.0.1", "refused"} {
		if strings.Contains(out, secret) {
			t.Fatalf("log carries %q: %s", secret, out)
		}
	}
}

func TestFirstOrganizationFailureIsLoggedOnTheTokenPath(t *testing.T) {
	log := captureLookupLog(t)
	dbURL := "postgresql://db/x"
	plan := Plan{OrgSource: OrgFromDBFirst, DB: &dbURL, Target: "git"}
	lookups := dbLookups{FirstOrg: connectFailingFirstOrg}
	got, refusal := resolveDBLookups(context.Background(), lookups, plan, cli.Env{})
	want := notYet(plan, "a sync with no organization (none found in PostgreSQL; pass --org or set ORG_ID)", ticketDBLookups)
	if refusal == nil || refusal.Message != want.Message || refusal.Code != want.Code || got.Org != nil {
		t.Fatalf("refusal = %+v, org = %v; want the unchanged not-yet refusal and no organization", refusal, got.Org)
	}
	assertFirstOrgFailureLogged(t, log.String())
}

func TestFirstOrganizationFailureIsLoggedOnTheLocalPath(t *testing.T) {
	log := captureLookupLog(t)
	h := &localHarness{}
	deps := InlineDeps{
		OpenStore: func(context.Context, string) (driver.Conn, error) { h.opened++; return fakeStore{}, nil },
		FirstOrg:  connectFailingFirstOrg,
		Local: func(_ context.Context, plan Plan, _ driver.Conn, _ cli.Env, _ time.Time) error {
			h.plans = append(h.plans, plan)
			return nil
		},
	}
	code, _, stderr := runVerb(t, "git", InlineExecutor(deps), []string{"--provider", "local", "--repo-path", "/repo"}, dbEnv)
	if code != cli.ExitOK || len(h.plans) != 1 || h.plans[0].Org != nil {
		t.Fatalf("exit %d plans %+v: %s", code, h.plans, stderr)
	}
	assertFirstOrgFailureLogged(t, log.String())
}
