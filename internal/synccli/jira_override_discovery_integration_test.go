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

// The discovery fails with ATLASSIAN_EMAIL / ATLASSIAN_API_TOKEN set. The
// gateway's error names the override values and the Basic payload of the pair that would be
// sent; none of them prints. Every value is made up.
func TestAtlassianDiscoveryErrorKeepsTheOverrideCredentialsHidden(t *testing.T) {
	t.Logf("cell: TestAtlassianDiscoveryErrorKeepsTheOverrideCredentialsHidden")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const storedEmail, storedToken = "stored@example.test", "zz9tok-stored-aaaaaaaa"
	const overrideEmail, overrideToken = "override@example.test", "zz9tok-override-bbbbbbbb"
	orgID, pool, uri := startSeededPostgres(t, ctx,
		map[string]string{"base_url": "https://acme.atlassian.net", "atlassian_cloud_id": "pinned-cloud-id"},
		map[string]string{"email": storedEmail, "api_token": storedToken})

	for name, c := range map[string]struct{ email, token, effectiveEmail, effectiveToken string }{
		"email only": {overrideEmail, "", overrideEmail, storedToken},
		"token only": {"", overrideToken, storedEmail, overrideToken},
		"both":       {overrideEmail, overrideToken, overrideEmail, overrideToken},
	} {
		leaks := []string{c.email, c.token}
		leaks = append(leaks, basicPayloads(c.effectiveEmail, c.effectiveToken)...)
		message := "Unauthorized request for"
		for _, leak := range leaks {
			if leak != "" {
				message += " [" + leak + "]"
			}
		}
		d := defaultDeps()
		d.openPostgres = func(context.Context, string) (*pgxpool.Pool, error) { return pool, nil }
		d.decryptor = func(cli.Env) (providerfoundation.CredentialDecryptor, error) { return testDecryptor(), nil }
		d.doer = http.DefaultClient
		d.newOrganizationResolver = func(string, atlassian.AuthProvider) atlassianteams.OrganizationResolver {
			return messageResolver{message: message}
		}
		env := map[string]string{
			"POSTGRES_URI":   uri,
			"CLICKHOUSE_URI": "clickhouse://zz9-disc-login:zz9-disc-pass@127.0.0.1:9000/d",
		}
		if c.email != "" {
			env["ATLASSIAN_EMAIL"] = c.email
		}
		if c.token != "" {
			env["ATLASSIAN_API_TOKEN"] = c.token
		}
		code, stdout, stderr := run(t, env, d, "--provider", "jira", "--org", orgID)
		if code == cli.ExitOK || !strings.Contains(stderr, "Unauthorized request") {
			t.Fatalf("%s: exit %d: the discovery error never reached the printing path: %q", name, code, stderr)
		}
		for _, leak := range leaks {
			if leak != "" && strings.Contains(stdout+stderr, leak) {
				t.Errorf("%s: %q printed: %q", name, leak, stderr)
			}
		}
	}
}
