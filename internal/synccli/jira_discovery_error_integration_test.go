//go:build integration

package synccli

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"atlassian/atlassian"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The org's stored jira credential authenticates the live organization-id
// discovery (no ATLASSIAN_ORGANIZATION_ID, none in the stored config). The
// gateway's error text is written verbatim by the vendored GraphQL formatter and
// wrapped with %w; `dho sync teams --provider jira` printed it before the
// redaction boundary existed. A gateway that echoes the token must not print it.
// Every value is made up.
const discoveryToken = "zz9tok-synthetic-aaaaaaaa"

type echoingOrganizationResolver struct{}

func (echoingOrganizationResolver) Execute(context.Context, string, map[string]any, string, []string, int) (*atlassian.Result, error) {
	return nil, &atlassian.GraphQLOperationError{Errors: []atlassian.GraphQLError{{Message: "Unauthorized request, the credential " + discoveryToken + " was refused"}}}
}

func TestAtlassianDiscoveryErrorKeepsTheStoredTokenHidden(t *testing.T) {
	t.Logf("cell: TestAtlassianDiscoveryErrorKeepsTheStoredTokenHidden")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(ctx) }()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	applyRealSchema(t, ctx, pool)

	orgID := uuid.NewString()
	seedJiraCredential(t, ctx, pool, orgID,
		map[string]string{"base_url": "https://acme.atlassian.net", "atlassian_cloud_id": "pinned-cloud-id"},
		map[string]string{"email": "sync@example.test", "api_token": discoveryToken})

	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	d := defaultDeps()
	d.openPostgres = func(context.Context, string) (*pgxpool.Pool, error) { return pool, nil }
	d.decryptor = func(cli.Env) (providerfoundation.CredentialDecryptor, error) { return testDecryptor(), nil }
	d.doer = http.DefaultClient
	d.newOrganizationResolver = func(string, atlassian.AuthProvider) atlassianteams.OrganizationResolver {
		return echoingOrganizationResolver{}
	}
	env := map[string]string{
		"POSTGRES_URI":   instance.URI,
		"CLICKHOUSE_URI": "clickhouse://zz9-disc-login:zz9-disc-pass@127.0.0.1:9000/d",
	}
	code, stdout, stderr := run(t, env, d, "--provider", "jira", "--org", orgID)
	t.Logf("exit %d; stderr %q; slog default saw %q", code, stderr, logs.String())
	if code == cli.ExitOK || !strings.Contains(stderr, "Unauthorized request") {
		t.Fatalf("exit %d: the discovery error never reached the printing path (or lost the gateway's text): %q", code, stderr)
	}
	if strings.Contains(stdout+stderr, discoveryToken) {
		t.Errorf("the stored token printed: %q", stderr)
	}
	if strings.Contains(logs.String(), discoveryToken) {
		t.Errorf("the stored token was logged: %q", logs.String())
	}
}
