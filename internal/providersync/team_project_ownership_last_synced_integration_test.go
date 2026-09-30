//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// Every provider writer of team_project_ownership stamps last_synced with the
// time of the write and keeps the provider's own time in updated_at: a row with
// an old provider time that lands late must still look new to a consumer cursor.
func TestTeamProjectOwnershipWritersStampIngestTimeAgainstMigratedSchema(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	provider := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	projectKey := "PLAT"

	writers := map[string]func() error{
		"jira": func() error {
			return JiraTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}.writeOwnership(ctx,
				[]jiraTeamCatalogOwnershipRow{normalizeJiraOwnershipRow("org-stamp", "PLAT", projectKey, provider)})
		},
		"gitlab": func() error {
			return GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}.writeOwnership(ctx,
				[]gitlabTeamCatalogOwnershipRow{normalizeGitLabOwnershipRow("org-stamp", "gl:org", projectKey, gitlabTeamCatalogBaseSpecificity, provider)})
		},
		"linear": func() error {
			return LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: lease}.writeOwnership(ctx,
				[]linearReferenceOwnershipRow{{
					OrgID: "org-stamp", Provider: "linear", TeamID: "ENG", ProjectID: "project-1", ProjectKey: &projectKey,
					Source: "native", IsPrimary: 1, Specificity: 100, Priority: 10, ValidFrom: provider, UpdatedAt: provider,
				}})
		},
	}
	for name, write := range writers {
		before := time.Now().UTC().Add(-time.Second)
		if err := write(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		after := time.Now().UTC().Add(time.Second)

		var updatedMillis, syncedMillis int64
		query := `SELECT toUnixTimestamp64Milli(updated_at), toUnixTimestamp64Milli(last_synced) FROM team_project_ownership FINAL WHERE org_id = 'org-stamp' AND provider = ?`
		if err := conn.QueryRow(ctx, query, name).Scan(&updatedMillis, &syncedMillis); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if updatedMillis != provider.UnixMilli() {
			t.Fatalf("%s: updated_at = %d, want the provider time %d to stay in its own column", name, updatedMillis, provider.UnixMilli())
		}
		if got := time.UnixMilli(syncedMillis).UTC(); got.Before(before) || got.After(after) {
			t.Fatalf("%s: last_synced = %s, want the ingest time between %s and %s", name, got, before, after)
		}
	}
}

// A write delayed between building the batch and sending it (the lease check sits there) must be
// stamped when the server runs the insert, after the delay: a stamp taken before the delay would
// land behind a consumer cursor that had already read a newer row.
func TestTeamProjectOwnershipLastSyncedIsTakenAfterADelayedLeaseCheck(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	provider := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	projectKey := "PLAT"
	var released time.Time
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error {
		time.Sleep(400 * time.Millisecond)
		released = time.Now().UTC()
		return nil
	})

	writers := map[string]func() error{
		"jira": func() error {
			return JiraTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}.writeOwnership(ctx,
				[]jiraTeamCatalogOwnershipRow{normalizeJiraOwnershipRow("org-delay", "PLAT", projectKey, provider)})
		},
		"gitlab": func() error {
			return GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}.writeOwnership(ctx,
				[]gitlabTeamCatalogOwnershipRow{normalizeGitLabOwnershipRow("org-delay", "gl:org", projectKey, gitlabTeamCatalogBaseSpecificity, provider)})
		},
		"linear": func() error {
			return LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: lease}.writeOwnership(ctx,
				[]linearReferenceOwnershipRow{{
					OrgID: "org-delay", Provider: "linear", TeamID: "ENG", ProjectID: "project-1", ProjectKey: &projectKey,
					Source: "native", IsPrimary: 1, Specificity: 100, Priority: 10, ValidFrom: provider, UpdatedAt: provider,
				}})
		},
	}
	for name, write := range writers {
		if err := write(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var syncedMillis int64
		query := `SELECT toUnixTimestamp64Milli(last_synced) FROM team_project_ownership FINAL WHERE org_id = 'org-delay' AND provider = ?`
		if err := conn.QueryRow(ctx, query, name).Scan(&syncedMillis); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if syncedMillis < released.UnixMilli() {
			t.Fatalf("%s: last_synced = %d is before the delayed lease check returned at %d: the stamp was taken before the delay", name, syncedMillis, released.UnixMilli())
		}
	}
}
