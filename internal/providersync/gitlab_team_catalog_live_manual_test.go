//go:build manuallive

package providersync

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestGitLabTeamCatalogAgainstRealLocalStack is a ONE-OFF, manually-invoked
// proof (CHAOS-4432) that GitLabTeamCatalogRouteHandler + GitLabTeamCatalog
// ClickHouseEffects produce the correct rows against the REAL local shared
// stack: real GitLab credential (decrypted via the same Fernet path
// production uses), real GitLab API, real ClickHouse.
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

	var integrationID, credentialID, groupPath string
	if err := pool.QueryRow(ctx, `
SELECT i.id::text, i.credential_id::text,
       COALESCE(sc.sync_options->>'owner', sc.sync_options->>'group', sc.sync_options->>'group_path', '')
FROM integrations i
JOIN sync_configurations sc ON sc.integration_id = i.id AND sc.parent_id IS NULL
WHERE i.org_id = $1 AND i.provider = 'gitlab' AND i.is_active = TRUE
LIMIT 1`, orgID).Scan(&integrationID, &credentialID, &groupPath); err != nil {
		t.Fatalf("resolve gitlab integration for org: %v", err)
	}
	if groupPath == "" {
		t.Fatal("no group_path/owner found in sync_options for this org's gitlab integration")
	}
	t.Logf("resolved integration_id=%s group_path=%s (credential id/token withheld)", integrationID, groupPath)

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

	claim := Claim{
		Unit: Unit{
			ID: uuid.NewString(), SyncRunID: uuid.NewString(), OrgID: orgID,
			IntegrationID: integrationID, SourceID: uuid.NewString(),
			SourceExternalID: groupPath, SourceName: groupPath,
			Provider: "gitlab", Dataset: "work-items", CostClass: CostMedium,
			Mode: "incremental", CredentialID: credentialID, AuthSource: "integration_credential",
			DatasetOptions: map[string]any{
				"owner": groupPath, "auto_import_teams": true, "auto_import_projects": true, "auto_import_members": true,
			},
		},
		Owner: uuid.NewString(), Attempt: 1, LeaseExpiresAt: time.Now().Add(10 * time.Minute),
	}

	batch, err := (GitLabTeamCatalogRouteHandler{}).CollectTeamCatalog(ctx, claim, credential, client, time.Now())
	if err != nil {
		t.Fatalf("collect team catalog: %v", err)
	}
	t.Logf("collected: teams=%d ownership=%d memberships=%d native_projects=%d evidence=%+v",
		len(batch.Rows.Teams), len(batch.Rows.Ownership), len(batch.Rows.Memberships), len(batch.Rows.Projects), batch.Evidence)

	sink := GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}
	for _, effect := range batch.Effects.Batches() {
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatalf("write effect destination=%s: %v", effect.Destination, err)
		}
		inspection, err := sink.InspectEffect(ctx, claim, effect)
		if err != nil {
			t.Fatalf("inspect effect destination=%s: %v", effect.Destination, err)
		}
		if inspection != EffectExact {
			t.Fatalf("readback destination=%s inspection=%s, want exact", effect.Destination, inspection)
		}
	}

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
