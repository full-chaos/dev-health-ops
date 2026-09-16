//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestRepositoryShadowLoadReturnsNewestNullSettingsAsDefaultNotStaleValue is
// the executed proof for repos.settings (Nullable(String)): a
// coalesce(argMax(settings, last_synced), '{}') wraps the OUTER argMax
// result, which does not stop argMax from skipping a newest row whose
// settings is NULL. Seed an older row with real settings JSON and a newer
// row with settings = NULL for the same (org_id, provider, repo); before the
// fix this returns the older JSON, after the fix it returns the '{}'
// default (the newest row's actual, NULL, value).
func TestRepositoryShadowLoadReturnsNewestNullSettingsAsDefaultNotStaleValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	}()
	options, err := stdclickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	// Minimal schema matching 000_raw_tables.sql's repos table exactly on
	// the columns this reader projects (settings is genuinely
	// Nullable(String) in production, unlike the pre-existing
	// repository_shadow_clickhouse_integration_test.go fixture's ad-hoc
	// NOT NULL `settings String` schema, which cannot reproduce this bug).
	if err := conn.Exec(ctx, `
CREATE TABLE repos (
	id UUID, org_id String, repo String, provider String,
	settings Nullable(String),
	last_synced DateTime64(9, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, id)`); err != nil {
		t.Fatal(err)
	}

	older := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	staleSettings := `{"source":"github","default_branch":"main"}`

	if err := conn.Exec(ctx, "INSERT INTO repos VALUES (?, ?, ?, ?, ?, ?)",
		"77777777-7777-4777-8777-777777777777", "org-4547", "acme/api", "github", staleSettings, older,
	); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "INSERT INTO repos VALUES (?, ?, ?, ?, ?, ?)",
		"77777777-7777-4777-8777-777777777777", "org-4547", "acme/api", "github", nil, newer,
	); err != nil {
		t.Fatal(err)
	}

	claim := nativeTestClaim("github", "repo-metadata")
	claim.OrgID = "org-4547"
	claim.SourceExternalID = "acme/api"

	source := ClickHouseRepositoryShadowSource{Conn: conn}
	envelopes, err := source.Load(ctx, claim)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(envelopes) != 1 {
		t.Fatalf("Load returned %d envelopes, want 1", len(envelopes))
	}

	// default_branch is derived from the settings JSON Load actually reads
	// (see Load's attribute-extraction loop). The newest physical row's
	// settings is NULL, so a correct dedup must fall through to the "{}"
	// default and leave default_branch empty -- asserting through Load's
	// real return value, not a separately reimplemented query, so this test
	// actually exercises repository_shadow.go's own SQL.
	if got := envelopes[0].Attributes["default_branch"]; got != "" {
		t.Fatalf("Attributes[default_branch] = %q, want \"\" (newest row's NULL settings) -- argMax skipped the NULL and returned the stale settings %q instead",
			got, staleSettings)
	}
}
