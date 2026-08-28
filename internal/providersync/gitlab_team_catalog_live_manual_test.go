//go:build manuallive

package providersync

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// gitlabLiveGroupPathResolver ports resolveTeamCatalogIntegration/the
// sync_options read team_catalog_clients.go's teamCatalogSelectionsResolver
// already does (cmd/dev-health-worker), scoped to just the one key GitLab
// needs. Only wired up in THIS manual proof -- production wiring for
// GroupPathResolver is the cmd/dev-health-worker layer's job once the
// group_path threading question (asked of team-lead) is settled.
type gitlabLiveGroupPathResolver struct{ pool *pgxpool.Pool }

func (resolver gitlabLiveGroupPathResolver) ResolveGroupPath(ctx context.Context, ref TeamCatalogReference) (string, error) {
	var groupPath string
	err := resolver.pool.QueryRow(ctx, `
SELECT COALESCE(sync_options->>'owner', sync_options->>'group', sync_options->>'group_path', '')
FROM public.sync_configurations
WHERE org_id = $1 AND integration_id = $2::uuid AND parent_id IS NULL
ORDER BY created_at, id
LIMIT 1`, ref.OrgID, ref.IntegrationID).Scan(&groupPath)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrInvalidConfiguration
	}
	if err != nil {
		return "", err
	}
	return groupPath, nil
}

// TestGitLabTeamCatalogAgainstRealLocalStack is a ONE-OFF, manually-invoked
// proof (CHAOS-4432) that GitLabTeamCatalogCollector (the claim-free
// TeamCatalogCollector adapter, CHAOS-4431 shape) produces the correct rows
// against the REAL local shared stack: real GitLab credential (decrypted
// via the same Fernet path production uses), real GitLab API, real
// ClickHouse.
//
// Deliberately NOT part of any normal test run (build tag manuallive, never
// passed to go test by ci/check_go.sh or CI). Never prints a secret value --
// only env KEY presence, row counts, and non-secret identifiers.
//
// Run from an ops worktree with the local stack up:
//
//	docker compose exec -T postgres env  # (do not run; illustrative only)
//	go test -tags=manuallive ./internal/providersync/ \
//	  -run TestGitLabTeamCatalogAgainstRealLocalStack -v
//
// Requires POSTGRES_URI-shaped access to the compose Postgres, the compose
// ClickHouse, and SETTINGS_ENCRYPTION_KEY -- all read from environment
// variables this test does not set itself; export them from the running
// go-worker container's env before invoking (never echoed).
func TestGitLabTeamCatalogAgainstRealLocalStack(t *testing.T) {
	orgID := strings.TrimSpace(os.Getenv("DEV_HEALTH_LIVE_GITLAB_ORG_ID"))
	pgDSN := os.Getenv("DEV_HEALTH_LIVE_POSTGRES_DSN")
	chDSN := os.Getenv("DEV_HEALTH_LIVE_CLICKHOUSE_DSN")
	encryptionKey := os.Getenv("SETTINGS_ENCRYPTION_KEY")
	if orgID == "" || pgDSN == "" || chDSN == "" || encryptionKey == "" {
		t.Skip("manual live proof requires DEV_HEALTH_LIVE_GITLAB_ORG_ID, " +
			"DEV_HEALTH_LIVE_POSTGRES_DSN, DEV_HEALTH_LIVE_CLICKHOUSE_DSN, " +
			"SETTINGS_ENCRYPTION_KEY")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		t.Fatalf("open postgres pool: %v", err)
	}
	defer pool.Close()

	var integrationID, credentialID, syncRunID string
	if err := pool.QueryRow(ctx, `
SELECT i.id::text, i.credential_id::text, sr.id::text
FROM integrations i
LEFT JOIN sync_runs sr ON sr.integration_id = i.id AND sr.org_id = i.org_id
WHERE i.org_id = $1 AND i.provider = 'gitlab' AND i.is_active = TRUE
ORDER BY sr.created_at DESC NULLS LAST
LIMIT 1`, orgID).Scan(&integrationID, &credentialID, &syncRunID); err != nil {
		t.Fatalf("resolve gitlab integration for org: %v", err)
	}
	if syncRunID == "" {
		// No prior sync run for this integration -- SyncRunID only needs to
		// be non-empty for ref.validate(); it is not looked up by anything
		// downstream of this collector.
		syncRunID = "00000000-0000-4000-8000-000000000000"
	}
	ref := TeamCatalogReference{OrgID: orgID, SyncRunID: syncRunID, IntegrationID: integrationID}
	t.Logf("resolved integration_id=%s (credential id/token/group_path withheld)", integrationID)

	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(encryptionKey), "")
	if err != nil {
		t.Fatalf("construct decryptor: %v", err)
	}
	resolver := providerfoundation.CredentialResolver{
		Repository: providerfoundation.PostgresCredentialRepository{Pool: pool},
		Decryptor:  decryptor,
	}
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	credential, err := resolver.Resolve(ctx, lease, providerfoundation.TenantScope{
		OrgID: orgID, Provider: "gitlab", IntegrationID: integrationID, CredentialID: credentialID,
	})
	if err != nil {
		t.Fatalf("resolve credential: %v", err)
	}

	client, err := providerfoundation.NewGitLabClient(
		credential, http.DefaultClient,
		providerfoundation.RetryPolicy{MaxAttempts: 3, InitialWait: 200 * time.Millisecond, MaxWait: 2 * time.Second},
		lease,
	)
	if err != nil {
		t.Fatalf("construct gitlab client: %v", err)
	}
	client.Lease = lease

	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(chDSN))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer func() { _ = conn.Close() }()

	before := gitlabTeamCatalogLiveCounts(t, ctx, conn, orgID)
	t.Logf("BEFORE (local, real data, org %s): %+v", orgID, before)

	collector := GitLabTeamCatalogCollector{
		Handler: GitLabTeamCatalogRouteHandler{GroupPathResolver: gitlabLiveGroupPathResolver{pool: pool}},
		Sink:    GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: lease},
	}
	selections := TeamCatalogSelections{Teams: true, Projects: true, Members: true}
	result, err := collector.CollectTeamCatalog(ctx, ref, credential, client, selections, time.Now())
	if err != nil {
		t.Fatalf("collect team catalog: %v", err)
	}
	t.Logf("TeamCatalogResult: teams_written=%d members_written=%d memberships_written=%d projects_written=%d ownership_written=%d team_keys=%v",
		result.TeamsWritten, result.MembersWritten, result.MembershipsWritten, result.ProjectsWritten, result.OwnershipWritten, result.TeamKeys)

	after := gitlabTeamCatalogLiveCounts(t, ctx, conn, orgID)
	t.Logf("AFTER (local, real data, org %s): %+v", orgID, after)
	t.Logf("Go client wrote gitlab rows for org %s: teams=%d team_project_ownership=%d team_memberships=%d projects=%d",
		orgID, after.teams, after.ownership, after.memberships, after.projects)
}

type gitlabTeamCatalogLiveRowCounts struct {
	teams, ownership, memberships, projects uint64
}

func gitlabTeamCatalogLiveCounts(t *testing.T, ctx context.Context, conn driver.Conn, orgID string) gitlabTeamCatalogLiveRowCounts {
	t.Helper()
	var counts gitlabTeamCatalogLiveRowCounts
	scan := func(query string, dest *uint64) {
		if err := conn.QueryRow(ctx, query, orgID).Scan(dest); err != nil {
			t.Fatalf("count query failed: %v", err)
		}
	}
	scan(`SELECT count() FROM teams FINAL WHERE org_id = ? AND provider = 'gitlab'`, &counts.teams)
	scan(`SELECT count() FROM team_project_ownership FINAL WHERE org_id = ? AND provider = 'gitlab'`, &counts.ownership)
	scan(`SELECT count() FROM team_memberships FINAL WHERE org_id = ? AND provider = 'gitlab'`, &counts.memberships)
	scan(`SELECT count() FROM projects FINAL WHERE org_id = ? AND provider = 'gitlab'`, &counts.projects)
	return counts
}
