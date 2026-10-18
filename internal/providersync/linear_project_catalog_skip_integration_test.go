//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
)

func writeLinearProjectsBatch(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, rows ...projectmembership.CatalogRow,
) {
	t.Helper()
	raws := make([]json.RawMessage, len(rows))
	for index, row := range rows {
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		raws[index] = raw
	}
	effect, err := BuildEffectBatch("projects", EffectReadbackRequired, raws)
	if err != nil {
		t.Fatal(err)
	}
	identity := LinearWorkItemEffectIdentity{
		OrgID: orgID, Provider: "linear", Dataset: "work-items", Generation: "generation-1",
		Destination: "projects", ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	adapter := LinearProjectCatalogClickHouseAdapter{Delegate: GitHubProjectCatalogClickHouseAdapter{Conn: conn}}
	if err := adapter.WriteLinearWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatal(err)
	}
}

func linearProjectRowCount(t *testing.T, ctx context.Context, conn driver.Conn, orgID, id string) uint64 {
	t.Helper()
	var count uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM projects WHERE org_id = ? AND provider = 'linear' AND id = ?`, orgID, id,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func linearProjectNewestName(t *testing.T, ctx context.Context, conn driver.Conn, orgID, id string) string {
	t.Helper()
	var name string
	if err := conn.QueryRow(ctx,
		`SELECT name FROM projects FINAL WHERE org_id = ? AND provider = 'linear' AND id = ?`, orgID, id,
	).Scan(&name); err != nil {
		t.Fatal(err)
	}
	return name
}

// The ensure rows of a work-items unit are written only when they can change the newest stored row.
func TestLinearProjectEnsureRowsAreWrittenOnlyWhenTheyChangeTheNewestRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES projects`); err != nil {
		t.Fatal(err)
	}
	orgID := "org-ensure-skip"
	first := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	later := first.Add(24 * time.Hour)

	t.Run("an identical ensure row in a later window is not written again", func(t *testing.T) {
		id := "project-same"
		writeLinearProjectsBatch(t, ctx, conn, orgID, linearEnsureProjectsRow(orgID, id, "Same", first))
		writeLinearProjectsBatch(t, ctx, conn, orgID, linearEnsureProjectsRow(orgID, id, "Same", later))
		if got := linearProjectRowCount(t, ctx, conn, orgID, id); got != 1 {
			t.Fatalf("stored rows = %d, want 1", got)
		}
	})

	t.Run("a changed name at the same version lands", func(t *testing.T) {
		id := "project-renamed"
		writeLinearProjectsBatch(t, ctx, conn, orgID, linearEnsureProjectsRow(orgID, id, "", first))
		writeLinearProjectsBatch(t, ctx, conn, orgID, linearEnsureProjectsRow(orgID, id, "Named", later))
		if got := linearProjectNewestName(t, ctx, conn, orgID, id); got != "Named" {
			t.Fatalf("newest name = %q, want Named", got)
		}
	})

	t.Run("an ensure row under a newer catalog row is not written", func(t *testing.T) {
		id := "project-catalog"
		catalog := linearEnsureProjectsRow(orgID, id, "Catalog", first)
		catalog.UpdatedAt = first
		writeLinearProjectsBatch(t, ctx, conn, orgID, catalog)
		writeLinearProjectsBatch(t, ctx, conn, orgID, linearEnsureProjectsRow(orgID, id, "Catalog", later))
		if got := linearProjectRowCount(t, ctx, conn, orgID, id); got != 1 {
			t.Fatalf("stored rows = %d, want 1", got)
		}
	})
}
