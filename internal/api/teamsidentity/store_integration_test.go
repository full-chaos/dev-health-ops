//go:build integration

package teamsidentity

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// createTeamsIdentitiesTables is the real teams/identities DDL, transcribed
// verbatim from src/dev_health_ops/migrations/clickhouse/{002,011,024,025,
// 051,065,079}_*.sql (teams) and {054,065}_*.sql (identities) -- the exact
// column set/types/engine/ORDER BY those migrations produce, not a
// hand-guessed shape. See .remember/lanes/gwc-w1-acr/chaos-6251-pr1-reference.md
// for the full derivation.
func createTeamsIdentitiesTables(t *testing.T, ctx context.Context, conn interface {
	Exec(context.Context, string, ...any) error
}) {
	t.Helper()
	statements := []string{
		`CREATE TABLE teams (
			id String,
			team_uuid UUID,
			name String,
			description Nullable(String),
			members Array(String),
			manual_members Array(String) DEFAULT [],
			project_keys Array(String) DEFAULT [],
			repo_patterns Array(String) DEFAULT [],
			is_active UInt8 DEFAULT 1,
			updated_at DateTime64(6),
			last_synced DateTime64(6) DEFAULT now(),
			org_id String DEFAULT 'default',
			provider String DEFAULT '',
			native_team_key Nullable(String),
			parent_team_id Nullable(String),
			source_id Nullable(UUID) DEFAULT NULL
		) ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (id)`,
		`CREATE TABLE identities (
			org_id String DEFAULT 'default',
			canonical_id String,
			identity_uuid UUID,
			display_name Nullable(String),
			email Nullable(String),
			provider_identities String DEFAULT '{}',
			team_ids Array(String) DEFAULT [],
			is_active UInt8 DEFAULT 1,
			updated_at DateTime64(6),
			source_id Nullable(UUID) DEFAULT NULL
		) ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (org_id, canonical_id)`,
	}
	for _, statement := range statements {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("create table: %v", err)
		}
	}
}

func startTeamsIdentitiesStore(t *testing.T) (Store, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	conn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	createTeamsIdentitiesTables(t, ctx, conn)
	return Store{Conn: conn}, ctx
}

// TestTeamCRUDRoundTrip proves the whole team CRUD path against a real
// ClickHouse: create, get, list, update (via CreateOrUpdateTeam again),
// add/remove members (CHAOS-4321 manual_members provenance), delete.
func TestTeamCRUDRoundTrip(t *testing.T) {
	store, ctx := startTeamsIdentitiesStore(t)
	const orgID = "org-1"

	desc := "first description"
	created, err := store.CreateOrUpdateTeam(ctx, orgID, TeamWrite{
		TeamID: "team-a", Name: "Team A", Description: &desc,
		RepoPatterns: &[]string{"repo-*"}, ProjectKeys: &[]string{"PROJ"},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.TeamID != "team-a" || created.Name != "Team A" || created.Description == nil || *created.Description != desc {
		t.Fatalf("created team mismatch: %+v", created)
	}
	if len(created.RepoPatterns) != 1 || created.RepoPatterns[0] != "repo-*" {
		t.Fatalf("repo_patterns not stored: %+v", created.RepoPatterns)
	}

	got, err := store.GetTeam(ctx, orgID, "team-a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got == nil || got.ID != created.ID {
		t.Fatalf("get mismatch: %+v", got)
	}

	list, err := store.ListTeams(ctx, orgID, true)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("list len=%d want=1", len(list))
	}

	// PATCH-shaped update: omit repo_patterns/project_keys (nil pointer) so
	// the existing values are carried forward, only name changes.
	updated, err := store.CreateOrUpdateTeam(ctx, orgID, TeamWrite{
		TeamID: "team-a", Name: "Team A Renamed", Description: got.Description,
		RepoPatterns: &got.RepoPatterns, ProjectKeys: &got.ProjectKeys,
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.ID != created.ID {
		t.Fatalf("team_uuid must be stable across updates: created=%s updated=%s", created.ID, updated.ID)
	}
	if updated.Name != "Team A Renamed" {
		t.Fatalf("name not updated: %+v", updated)
	}
	if len(updated.RepoPatterns) != 1 || updated.RepoPatterns[0] != "repo-*" {
		t.Fatalf("repo_patterns not carried forward: %+v", updated.RepoPatterns)
	}

	// CHAOS-4321: AddMembers unions into both members and manual_members.
	afterAdd, err := store.AddMembers(ctx, orgID, "team-a", []string{"alice@example.com"})
	if err != nil {
		t.Fatalf("add members: %v", err)
	}
	if afterAdd == nil || len(afterAdd.Members) != 1 || afterAdd.Members[0] != "alice@example.com" {
		t.Fatalf("members not added: %+v", afterAdd)
	}
	if len(afterAdd.ManualMembers) != 1 || afterAdd.ManualMembers[0] != "alice@example.com" {
		t.Fatalf("manual_members not tracked: %+v", afterAdd)
	}

	// r2 (CHAOS-6310) finding #5: the EARLIER omitted-field update (above,
	// "PATCH-shaped update") ran before any member ever existed, so a
	// regression that CLEARED manual_members on an omitted Members/
	// ManualMembers field would have been indistinguishable from correctly
	// preserving an already-empty roster -- that update could not have
	// caught it. This one runs AFTER AddMembers has populated a real,
	// non-empty roster, with Members/ManualMembers both omitted (nil), so a
	// clearing regression is now observable: the roster must still be
	// exactly {alice@example.com} afterward, not wiped by the update this
	// field is not part of.
	afterOmittedUpdate, err := store.CreateOrUpdateTeam(ctx, orgID, TeamWrite{
		TeamID: "team-a", Name: "Team A Renamed Again", Description: got.Description,
		RepoPatterns: &got.RepoPatterns, ProjectKeys: &got.ProjectKeys,
	})
	if err != nil {
		t.Fatalf("omitted-field update after members exist: %v", err)
	}
	if len(afterOmittedUpdate.Members) != 1 || afterOmittedUpdate.Members[0] != "alice@example.com" {
		t.Fatalf("members cleared by an update that omitted the field: %+v", afterOmittedUpdate)
	}
	if len(afterOmittedUpdate.ManualMembers) != 1 || afterOmittedUpdate.ManualMembers[0] != "alice@example.com" {
		t.Fatalf("manual_members cleared by an update that omitted the field: %+v", afterOmittedUpdate)
	}

	afterRemove, err := store.RemoveMembers(ctx, orgID, "team-a", map[string]bool{"alice@example.com": true})
	if err != nil {
		t.Fatalf("remove members: %v", err)
	}
	if afterRemove == nil || len(afterRemove.Members) != 0 || len(afterRemove.ManualMembers) != 0 {
		t.Fatalf("members not removed: %+v", afterRemove)
	}

	deleted, err := store.DeleteTeam(ctx, orgID, "team-a")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted {
		t.Fatal("delete returned false for an existing team")
	}
	afterDelete, err := store.GetTeam(ctx, orgID, "team-a")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if afterDelete != nil {
		t.Fatalf("team still visible after DELETE: %+v", afterDelete)
	}
	deletedAgain, err := store.DeleteTeam(ctx, orgID, "team-a")
	if err != nil {
		t.Fatalf("delete again: %v", err)
	}
	if deletedAgain {
		t.Fatal("delete returned true for an already-deleted team")
	}
}

// TestIdentityCRUDRoundTrip proves the identity CRUD path, including
// provider_identities JSON round-trip and find-by-provider-identity.
func TestIdentityCRUDRoundTrip(t *testing.T) {
	store, ctx := startTeamsIdentitiesStore(t)
	const orgID = "org-1"

	email := "bob@example.com"
	bobProviders := pybody.NewOrderedStringListDict()
	bobProviders.Set("github", []string{"bob-gh"})
	created, err := store.CreateOrUpdateIdentity(ctx, orgID, IdentityWrite{
		CanonicalID: "bob", Email: &email,
		ProviderIdentities: bobProviders,
		TeamIDs:            &[]string{},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.CanonicalID != "bob" || created.Email == nil || *created.Email != email {
		t.Fatalf("created identity mismatch: %+v", created)
	}
	createdGithub, _ := created.ProviderIdentities.Get("github")
	if len(createdGithub) != 1 || createdGithub[0] != "bob-gh" {
		t.Fatalf("provider_identities not round-tripped: %+v", created.ProviderIdentities)
	}

	found, err := store.FindIdentityByProviderIdentity(ctx, orgID, "github", "bob-gh")
	if err != nil {
		t.Fatalf("find by provider identity: %v", err)
	}
	if found == nil || found.CanonicalID != "bob" {
		t.Fatalf("find_by_provider_identity mismatch: %+v", found)
	}

	notFound, err := store.FindIdentityByProviderIdentity(ctx, orgID, "github", "nope")
	if err != nil {
		t.Fatalf("find by provider identity (miss): %v", err)
	}
	if notFound != nil {
		t.Fatalf("expected no match, got: %+v", notFound)
	}

	list, err := store.ListIdentities(ctx, orgID, true)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("list len=%d want=1", len(list))
	}

	deleted, err := store.DeleteIdentity(ctx, orgID, "bob")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted {
		t.Fatal("delete returned false for an existing identity")
	}
	afterDelete, err := store.GetIdentity(ctx, orgID, "bob")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if afterDelete != nil {
		t.Fatalf("identity still visible after DELETE: %+v", afterDelete)
	}
}
