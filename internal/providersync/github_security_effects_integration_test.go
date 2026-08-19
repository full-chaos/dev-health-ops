//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const securityAlertsDDL = `
CREATE TABLE security_alerts (
  org_id String, repo_id UUID, alert_id String, source String,
  severity Nullable(String), state Nullable(String), package_name Nullable(String),
  cve_id Nullable(String), url Nullable(String), title Nullable(String),
  description Nullable(String), created_at DateTime64(3, 'UTC'),
  fixed_at Nullable(DateTime64(3, 'UTC')), dismissed_at Nullable(DateTime64(3, 'UTC')),
  last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, alert_id)`

func TestGitHubSecurityReadbackExcludesSameNaturalKeyFromOtherTenant(t *testing.T) {
	ctx, sink := newGitHubSecurityIntegrationSink(t)
	claim := nativeTestClaim("github", "security")
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	row := securityAlertRow{OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", AlertID: "dependabot:1", Source: "dependabot", CreatedAt: now, LastSynced: now}
	otherClaim, otherRow := claim, row
	otherClaim.OrgID, otherRow.OrgID = "other-org", "other-org"
	if err := sink.WriteEffect(ctx, otherClaim, securityAlertEffect(t, otherRow)); err != nil {
		t.Fatal(err)
	}
	inspection, err := sink.InspectEffect(ctx, claim, securityAlertEffect(t, row))
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("foreign row inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, securityAlertEffect(t, row)); err != nil {
		t.Fatal(err)
	}
	inspection, err = sink.InspectEffect(ctx, claim, securityAlertEffect(t, row))
	if err != nil || inspection != EffectExact {
		t.Fatalf("tenant row inspection=%s error=%v", inspection, err)
	}
}

func newGitHubSecurityIntegrationSink(t *testing.T) (context.Context, GitHubSecurityClickHouseEffects) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, securityAlertsDDL); err != nil {
		t.Fatal(err)
	}
	return ctx, GitHubSecurityClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
}

func securityAlertEffect(t *testing.T, row securityAlertRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("security_alerts", EffectReadbackRequired, []securityAlertRow{row})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}
