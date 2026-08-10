//go:build integration

package providersync

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// This integration proof authors no DDL.  security_alerts is created by the
// real 032/033 migrations and rebuilt by 042 so org_id is the first component
// of the ReplacingMergeTree key.  A hand-written table here would only prove a
// second schema and could never catch drift in the production migration chain.
func TestGitLabSecurityEffectsAgainstMigratedSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	conn := gitLabSecurityMigratedConnection(t, ctx)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := GitLabSecurityClickHouseEffects{Conn: conn, Lease: lease}

	t.Run("migration exposes concrete tenant key and payload columns", func(t *testing.T) {
		rows, err := conn.Query(ctx, `
SELECT name, type
FROM system.columns
WHERE database = currentDatabase() AND table = 'security_alerts'
ORDER BY name`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		columns := make(map[string]string)
		for rows.Next() {
			var name, columnType string
			if err := rows.Scan(&name, &columnType); err != nil {
				t.Fatal(err)
			}
			columns[name] = columnType
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		want := map[string]string{
			"org_id":       "String",
			"repo_id":      "UUID",
			"alert_id":     "String",
			"source":       "String",
			"severity":     "Nullable(String)",
			"state":        "Nullable(String)",
			"package_name": "Nullable(String)",
			"cve_id":       "Nullable(String)",
			"url":          "Nullable(String)",
			"title":        "Nullable(String)",
			"description":  "Nullable(String)",
			"created_at":   "DateTime64(3, 'UTC')",
			"fixed_at":     "Nullable(DateTime64(3, 'UTC'))",
			"dismissed_at": "Nullable(DateTime64(3, 'UTC'))",
			"last_synced":  "DateTime64(3, 'UTC')",
		}
		if len(columns) != len(want) {
			t.Fatalf("migrated security_alerts columns=%v want exactly=%v", columns, want)
		}
		for name, expected := range want {
			if got := strings.ReplaceAll(columns[name], " ", ""); got != strings.ReplaceAll(expected, " ", "") {
				t.Fatalf("column %s type=%q want=%q (all=%v)", name, columns[name], expected, columns)
			}
		}

		var engine, sortingKey string
		if err := conn.QueryRow(ctx, `
SELECT engine, sorting_key
FROM system.tables
WHERE database = currentDatabase() AND name = 'security_alerts'`).Scan(&engine, &sortingKey); err != nil {
			t.Fatal(err)
		}
		if engine != "ReplacingMergeTree" {
			t.Fatalf("security_alerts engine=%q want ReplacingMergeTree", engine)
		}
		if normalized := strings.ReplaceAll(sortingKey, " ", ""); normalized != "org_id,repo_id,alert_id" {
			t.Fatalf("security_alerts sorting key=%q want org_id,repo_id,alert_id", sortingKey)
		}
	})

	// Stop background merges so count() proves physical replay/version rows;
	// InspectEffect still uses FINAL and must resolve the logical winner.
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES security_alerts`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Exec(ctx, `SYSTEM START MERGES security_alerts`); err != nil {
			t.Errorf("resume security_alerts merges: %v", err)
		}
	})

	claim := nativeTestClaim("gitlab", "security")
	foreignClaim := claim
	foreignClaim.OrgID = "org-gitlab-foreign"
	now := time.Date(2026, 8, 10, 12, 0, 0, 123000000, time.UTC)

	t.Run("same natural key remains tenant fenced", func(t *testing.T) {
		foreign := gitLabSecurityIntegrationRow(foreignClaim, "tenant-collision", now)
		current := gitLabSecurityIntegrationRow(claim, "tenant-collision", now)
		if err := sink.WriteEffect(ctx, foreignClaim, gitLabSecurityIntegrationEffect(t, foreign)); err != nil {
			t.Fatal(err)
		}
		if inspection, err := sink.InspectEffect(ctx, claim, gitLabSecurityIntegrationEffect(t, current)); err != nil || inspection != EffectAbsent {
			t.Fatalf("foreign row inspection=%s error=%v", inspection, err)
		}
		if err := sink.WriteEffect(ctx, claim, gitLabSecurityIntegrationEffect(t, current)); err != nil {
			t.Fatal(err)
		}
		if inspection, err := sink.InspectEffect(ctx, claim, gitLabSecurityIntegrationEffect(t, current)); err != nil || inspection != EffectExact {
			t.Fatalf("current row inspection=%s error=%v", inspection, err)
		}
		assertGitLabSecurityPhysicalRows(t, ctx, conn, current, 2)
	})

	t.Run("FINAL selects newer replay version", func(t *testing.T) {
		stale := gitLabSecurityIntegrationRow(claim, "versioned", now)
		newer := stale
		newer.LastSynced = now.Add(time.Minute)
		newer.Title = gitLabSecurityStringPointer("newer")
		if err := sink.WriteEffect(ctx, claim, gitLabSecurityIntegrationEffect(t, stale)); err != nil {
			t.Fatal(err)
		}
		if err := sink.WriteEffect(ctx, claim, gitLabSecurityIntegrationEffect(t, newer)); err != nil {
			t.Fatal(err)
		}
		if inspection, err := sink.InspectEffect(ctx, claim, gitLabSecurityIntegrationEffect(t, stale)); err != nil || inspection != EffectConflict {
			t.Fatalf("stale row inspection=%s error=%v", inspection, err)
		}
		if inspection, err := sink.InspectEffect(ctx, claim, gitLabSecurityIntegrationEffect(t, newer)); err != nil || inspection != EffectExact {
			t.Fatalf("newer row inspection=%s error=%v", inspection, err)
		}
		assertGitLabSecurityPhysicalRows(t, ctx, conn, stale, 2)
	})
}

func gitLabSecurityMigratedConnection(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func gitLabSecurityIntegrationRow(claim Claim, alertID string, synced time.Time) gitLabSecurityAlertRow {
	created := synced.Add(-time.Hour)
	severity := "high"
	state := "detected"
	cve := "CVE-2026-3124"
	url := "https://gitlab.example/acme/api/-/security/vulnerability/" + alertID
	packageName := "example-package"
	return gitLabSecurityAlertRow{
		OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79",
		AlertID: alertID, Source: "gitlab_vulnerability", Severity: &severity,
		State: &state, PackageName: &packageName, CVEID: &cve, URL: &url,
		Title: gitLabSecurityStringPointer("security finding"), CreatedAt: created,
		LastSynced: synced,
	}
}

func gitLabSecurityIntegrationEffect(t *testing.T, row gitLabSecurityAlertRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("security_alerts", EffectReadbackRequired, []gitLabSecurityAlertRow{row})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

func assertGitLabSecurityPhysicalRows(
	t *testing.T,
	ctx context.Context,
	conn driver.Conn,
	row gitLabSecurityAlertRow,
	want uint64,
) {
	t.Helper()
	var got uint64
	if err := conn.QueryRow(ctx, `
SELECT count()
FROM security_alerts
WHERE repo_id = ? AND alert_id = ?`, row.RepoID, row.AlertID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("physical security_alerts rows=%d want=%d (%s)", got, want, fmt.Sprintf("%s/%s", row.OrgID, row.AlertID))
	}
}

func gitLabSecurityStringPointer(value string) *string { return &value }
