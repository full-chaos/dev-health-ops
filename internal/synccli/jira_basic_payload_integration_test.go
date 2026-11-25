//go:build integration

package synccli

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"atlassian/atlassian"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// READ-AUTH-7046-2 cells that need the stored credential row. Every value is made up.

type messageResolver struct{ message string }

func (r messageResolver) Execute(context.Context, string, map[string]any, string, []string, int) (*atlassian.Result, error) {
	return nil, &atlassian.GraphQLOperationError{Errors: []atlassian.GraphQLError{{Message: r.message}}}
}

func startSeededPostgres(t *testing.T, ctx context.Context, config, secretValues map[string]string) (string, *pgxpool.Pool, string) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(ctx) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	applyRealSchema(t, ctx, pool)
	orgID := uuid.NewString()
	seedJiraCredential(t, ctx, pool, orgID, config, secretValues)
	return orgID, pool, instance.URI
}

// The discovery authenticates with the stored pair: a gateway that echoes the Authorization
// header puts base64(email:token) into the error that runTeams prints.
func TestAtlassianDiscoveryErrorKeepsTheBasicPayloadHidden(t *testing.T) {
	t.Logf("cell: TestAtlassianDiscoveryErrorKeepsTheBasicPayloadHidden")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const email, token = "sync@example.test", "zz9tok-synthetic-aaaaaaaa"
	orgID, pool, uri := startSeededPostgres(t, ctx,
		map[string]string{"base_url": "https://acme.atlassian.net", "atlassian_cloud_id": "pinned-cloud-id"},
		map[string]string{"email": email, "api_token": token})
	payload := basicPayloads(email, token)[0]

	d := defaultDeps()
	d.openPostgres = func(context.Context, string) (*pgxpool.Pool, error) { return pool, nil }
	d.decryptor = func(cli.Env) (providerfoundation.CredentialDecryptor, error) { return testDecryptor(), nil }
	d.doer = http.DefaultClient
	d.newOrganizationResolver = func(string, atlassian.AuthProvider) atlassianteams.OrganizationResolver {
		return messageResolver{message: "Unauthorized request, Authorization: Basic " + payload + " was refused"}
	}
	env := map[string]string{
		"POSTGRES_URI":   uri,
		"CLICKHOUSE_URI": "clickhouse://zz9-disc-login:zz9-disc-pass@127.0.0.1:9000/d",
	}
	code, stdout, stderr := run(t, env, d, "--provider", "jira", "--org", orgID)
	if code == cli.ExitOK || !strings.Contains(stderr, "Unauthorized request") {
		t.Fatalf("exit %d: the discovery error never reached the printing path: %q", code, stderr)
	}
	if strings.Contains(stdout+stderr, payload) {
		t.Errorf("the Basic payload printed: %q", stderr)
	}
}

// ATLASSIAN_EMAIL and ATLASSIAN_API_TOKEN replace the stored pair after the discovery used it.
// A gateway error that names the stored account must not print the stored email or token.
func TestOverrideRunKeepsTheReplacedStoredPairHidden(t *testing.T) {
	t.Logf("cell: TestOverrideRunKeepsTheReplacedStoredPairHidden")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const storedEmail, storedToken = "stored@example.test", "zz9tok-stored-aaaaaaaa"
	orgID, pool, uri := startSeededPostgres(t, ctx,
		map[string]string{
			"base_url": "https://acme.atlassian.net", "atlassian_cloud_id": "pinned-cloud-id",
			"atlassian_organization_id": "pinned-org-id",
		},
		map[string]string{"email": storedEmail, "api_token": storedToken})

	d := defaultDeps()
	d.openPostgres = func(context.Context, string) (*pgxpool.Pool, error) { return pool, nil }
	d.decryptor = func(cli.Env) (providerfoundation.CredentialDecryptor, error) { return testDecryptor(), nil }
	d.doer = http.DefaultClient
	d.newClient = func(string, atlassian.AuthProvider) atlassianteams.Client {
		return failingClient{err: errors.New("rejected: the account " + storedEmail + " with the token " + storedToken + " has no access")}
	}
	d.openStore = func(context.Context, string) (driver.Conn, error) { return closer{}, nil }
	env := map[string]string{
		"POSTGRES_URI":        uri,
		"CLICKHOUSE_URI":      "clickhouse://zz9-disc-login:zz9-disc-pass@127.0.0.1:9000/d",
		"ATLASSIAN_EMAIL":     "override@example.test",
		"ATLASSIAN_API_TOKEN": "zz9tok-override-bbbbbbbb",
	}
	code, stdout, stderr := run(t, env, d, "--provider", "jira", "--org", orgID)
	if code == cli.ExitOK || !strings.Contains(stderr, "has no access") {
		t.Fatalf("exit %d: the collect error never reached the printing path: %q", code, stderr)
	}
	for _, secret := range []string{storedEmail, storedToken} {
		if strings.Contains(stdout+stderr, secret) {
			t.Errorf("the replaced stored value %q printed: %q", secret, stderr)
		}
	}
}
