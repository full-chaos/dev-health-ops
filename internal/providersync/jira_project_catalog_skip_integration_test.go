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

func writeJiraProjectsBatch(
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
	identity := GitHubWorkItemEffectIdentity{
		OrgID: orgID, Provider: "jira", Dataset: "work-items", Generation: "generation-1",
		Destination: "projects", ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	adapter := JiraProjectCatalogClickHouseAdapter{Delegate: GitHubProjectCatalogClickHouseAdapter{Conn: conn}}
	if err := adapter.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatal(err)
	}
}

func jiraProjectStored(t *testing.T, ctx context.Context, conn driver.Conn, orgID, id string) (count uint64, name string, updatedAt time.Time) {
	t.Helper()
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM projects WHERE org_id = ? AND provider = 'jira' AND id = ?`, orgID, id,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx,
		`SELECT name, updated_at FROM projects FINAL WHERE org_id = ? AND provider = 'jira' AND id = ?`, orgID, id,
	).Scan(&name, &updatedAt); err != nil {
		t.Fatal(err)
	}
	return count, name, updatedAt.UTC()
}

// Jira ensure rows carry the window's wall clock as updated_at; identical content must not be rewritten.
func TestJiraProjectEnsureSkipsIdenticalContentAtAnyVersion(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES projects`); err != nil {
		t.Fatal(err)
	}
	orgID := "org-jira-ensure-skip"
	first := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	later := first.Add(24 * time.Hour)
	ensure := func(id, name string, at time.Time) projectmembership.CatalogRow {
		return projectmembership.EnsureProjectsRow(orgID, "jira", id, "KEY", name, at).
			WithState(projectmembership.LinearProjectState(""))
	}

	t.Run("identical content in a later window writes no row and keeps the old updated_at", func(t *testing.T) {
		id := "jira-same"
		writeJiraProjectsBatch(t, ctx, conn, orgID, ensure(id, "Same", first))
		writeJiraProjectsBatch(t, ctx, conn, orgID, ensure(id, "Same", later))
		count, _, updatedAt := jiraProjectStored(t, ctx, conn, orgID, id)
		if count != 1 || !updatedAt.Equal(first) {
			t.Fatalf("rows = %d updated_at = %s, want 1 row at %s", count, updatedAt, first)
		}
	})

	t.Run("changed content in a later window writes a new row", func(t *testing.T) {
		id := "jira-renamed"
		writeJiraProjectsBatch(t, ctx, conn, orgID, ensure(id, "Old", first))
		writeJiraProjectsBatch(t, ctx, conn, orgID, ensure(id, "New", later))
		count, name, updatedAt := jiraProjectStored(t, ctx, conn, orgID, id)
		if count != 2 || name != "New" || !updatedAt.Equal(later) {
			t.Fatalf("rows = %d name = %q updated_at = %s, want 2 rows, New at %s", count, name, updatedAt, later)
		}
	})
}
