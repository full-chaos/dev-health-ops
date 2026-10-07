//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// CHAOS-8821. Which stored work items sit under the nil repository id is
// decided by the sink writers, by symbol (the repository id each writer stores):
//   - linear: projectLinearWorkItem, RepoID uuid.Nil always
//     (internal/providersync/linear_work_items_effects.go).
//   - jira: the jira row builder sets RepoID nil
//     (internal/providersync/jira_work_items_rows.go) and projectWorkItem
//     turns nil into uuid.Nil (github_work_items_direct_effects_clickhouse.go).
//   - github, gitlab: their sync routes refuse a row without a repository
//     (github_work_items_rows.go and gitlab_work_items_rows.go require
//     RepoID non-nil), so these rows carry the repository id.
//   - ingest paths: internal_ingest.go stores uuid.Nil for every provider;
//     external_clickhouse.go stores uuid.Nil for every system that is not
//     github or gitlab.
//
// The discoverer must follow the stored repo_id, never a provider name.
func TestRepositoryDiscovererReturnsNilRepositoryOnlyForStoredNilRepositoryItems(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	discoverer, err := NewClickHouseRepositoryDiscoverer(conn)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name         string
		provider     string
		storeNilRepo bool
		wantNil      bool
	}{
		{name: "github", provider: "github", storeNilRepo: false, wantNil: false},
		{name: "gitlab", provider: "gitlab", storeNilRepo: false, wantNil: false},
		{name: "jira", provider: "jira", storeNilRepo: true, wantNil: true},
		{name: "linear", provider: "linear", storeNilRepo: true, wantNil: true},
		// The decision is the stored repo_id, not the provider name:
		{name: "linear_item_stored_with_a_repository", provider: "linear", storeNilRepo: false, wantNil: false},
		{name: "unlisted_provider_stored_without_a_repository", provider: "someothertracker", storeNilRepo: true, wantNil: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			orgID := uuid.NewString()
			repoID := uuid.New()
			if err := conn.Exec(ctx, `INSERT INTO repos (id, org_id, repo, provider, created_at, last_synced) VALUES (?, ?, 'acme/api', 'github', ?, ?)`,
				repoID, orgID, day, day); err != nil {
				t.Fatal(err)
			}
			itemRepo := repoID
			if tc.storeNilRepo {
				itemRepo = uuid.Nil
			}
			completed := day.Add(10 * time.Hour)
			batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
				repo_id, work_item_id, provider, type, status, created_at, completed_at, org_id, last_synced)`)
			if err != nil {
				t.Fatal(err)
			}
			if err := batch.Append(itemRepo, tc.provider+":1", tc.provider, "issue", "done", day.AddDate(0, 0, -2), &completed, orgID, day); err != nil {
				t.Fatal(err)
			}
			if err := batch.Send(); err != nil {
				t.Fatal(err)
			}
			discovered, err := discoverer.RepositoryIDs(ctx, orgID)
			if err != nil {
				t.Fatal(err)
			}
			holdsNil := false
			for _, id := range discovered {
				if string(id) == uuid.Nil.String() {
					holdsNil = true
				}
			}
			if holdsNil != tc.wantNil {
				t.Fatalf("discovered %v: holds nil repository = %v, want %v", discovered, holdsNil, tc.wantNil)
			}
			wantLen := 1
			if tc.wantNil {
				wantLen = 2
			}
			if len(discovered) != wantLen {
				t.Fatalf("discovered %v, want %d ids (no extra partition member without a nil-repository item)", discovered, wantLen)
			}
		})
	}
	t.Run("work_items_of_another_organization_do_not_add_it", func(t *testing.T) {
		withNil, withoutNil := uuid.NewString(), uuid.NewString()
		for _, org := range []string{withNil, withoutNil} {
			if err := conn.Exec(ctx, `INSERT INTO repos (id, org_id, repo, provider, created_at, last_synced) VALUES (?, ?, 'acme/api', 'github', ?, ?)`,
				uuid.New(), org, day, day); err != nil {
				t.Fatal(err)
			}
		}
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, type, status, created_at, org_id, last_synced)`)
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Append(uuid.Nil, "linear:X-1", "linear", "issue", "open", day, withNil, day); err != nil {
			t.Fatal(err)
		}
		if err := batch.Send(); err != nil {
			t.Fatal(err)
		}
		discovered, err := discoverer.RepositoryIDs(ctx, withoutNil)
		if err != nil {
			t.Fatal(err)
		}
		if len(discovered) != 1 || string(discovered[0]) == uuid.Nil.String() {
			t.Fatalf("discovered %v for the organization without a nil-repository item", discovered)
		}
	})
}
