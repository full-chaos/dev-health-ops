//go:build integration

package synccli

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"atlassian/atlassian"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The discovery fails with ATLASSIAN_EMAIL / ATLASSIAN_API_TOKEN set. The gateway's error names
// the override values and the Basic payload of the pair that would be sent; none of them prints.
// Every value is made up. Each shape is its own test, so each passes or fails by name.
const (
	overrideStoredEmail, overrideStoredToken = "stored@example.test", "zz9tok-stored-aaaaaaaa"
	overrideEmail, overrideToken             = "override@example.test", "zz9tok-override-bbbbbbbb"
)

func TestAtlassianDiscoveryErrorKeepsTheOverrideEmailHidden(t *testing.T) {
	t.Logf("cell: TestAtlassianDiscoveryErrorKeepsTheOverrideEmailHidden")
	checkDiscoveryErrorHidesOverrides(t, overrideEmail, "", overrideEmail, overrideStoredToken)
}

func TestAtlassianDiscoveryErrorKeepsTheOverrideTokenHidden(t *testing.T) {
	t.Logf("cell: TestAtlassianDiscoveryErrorKeepsTheOverrideTokenHidden")
	checkDiscoveryErrorHidesOverrides(t, "", overrideToken, overrideStoredEmail, overrideToken)
}

func TestAtlassianDiscoveryErrorKeepsBothOverridesHidden(t *testing.T) {
	t.Logf("cell: TestAtlassianDiscoveryErrorKeepsBothOverridesHidden")
	checkDiscoveryErrorHidesOverrides(t, overrideEmail, overrideToken, overrideEmail, overrideToken)
}

func checkDiscoveryErrorHidesOverrides(t *testing.T, email, token, effectiveEmail, effectiveToken string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	orgID, _, uri := startSeededPostgres(t, ctx,
		map[string]string{"base_url": "https://acme.atlassian.net", "atlassian_cloud_id": "pinned-cloud-id"},
		map[string]string{"email": overrideStoredEmail, "api_token": overrideStoredToken})

	leaks := append([]string{email, token}, basicPayloads(effectiveEmail, effectiveToken)...)
	message := "Unauthorized request for"
	for _, leak := range leaks {
		if leak != "" {
			message += " [" + leak + "]"
		}
	}
	d := defaultDeps()
	// The verb closes the pool it opens, so it gets its own.
	d.openPostgres = func(ctx context.Context, _ string) (*pgxpool.Pool, error) { return pgxpool.New(ctx, uri) }
	d.decryptor = func(cli.Env) (providerfoundation.CredentialDecryptor, error) { return testDecryptor(), nil }
	d.doer = http.DefaultClient
	d.newOrganizationResolver = func(string, atlassian.AuthProvider) atlassianteams.OrganizationResolver {
		return messageResolver{message: message}
	}
	env := map[string]string{
		"POSTGRES_URI":   uri,
		"CLICKHOUSE_URI": "clickhouse://zz9-disc-login:zz9-disc-pass@127.0.0.1:9000/d",
	}
	if email != "" {
		env["ATLASSIAN_EMAIL"] = email
	}
	if token != "" {
		env["ATLASSIAN_API_TOKEN"] = token
	}
	code, stdout, stderr := run(t, env, d, "--provider", "jira", "--org", orgID)
	if code == cli.ExitOK || !strings.Contains(stderr, "Unauthorized request") {
		t.Fatalf("exit %d: the discovery error never reached the printing path: %q", code, stderr)
	}
	for _, leak := range leaks {
		if leak != "" && strings.Contains(stdout+stderr, leak) {
			t.Errorf("%q printed: %q", leak, stderr)
		}
	}
}
