//go:build integration

package teamattribution

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// CHAOS-6637: the loaders must take the newest row per sort key FIRST and
// apply validity to that row, not filter validity first. A retraction is a
// second physical row under the SAME sort key (org, ..., source, valid_from)
// carrying valid_to and a newer updated_at; until the ReplacingMergeTree
// pair merges, both rows exist. A loader that filters `valid_to` before it
// resolves the newest version drops the retraction and keeps the stale
// open-ended row, so a retracted ownership or membership stays attributed.
//
// Each case writes an open row, then its retraction, STOPs merges so both
// stay physical (the test fails loudly if the pair is not unmerged), and
// reads through the real loader for every provider the contract names.

var retractionProviders = []string{"jira", "gitlab", "github", "linear"}

func TestLoadersHonorARetractionBeforeTheRowPairMerges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := openTeamAttributionSchema(ctx, t)

	for _, table := range []string{"team_project_ownership", "team_repo_ownership", "team_memberships"} {
		if err := conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatalf("stop merges on %s: %v", table, err)
		}
	}

	opened := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	retracted := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	beforeRetraction := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	afterRetraction := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	const org = "org-6637"

	for _, provider := range retractionProviders {
		team := "team-" + provider
		// Open row, then the retraction under the same sort key: the same
		// valid_from, valid_to set, newer updated_at.
		for _, row := range []struct {
			validTo   *time.Time
			updatedAt time.Time
		}{{nil, opened}, {&retracted, retracted}} {
			var validTo any
			if row.validTo != nil {
				validTo = *row.validTo
			}
			if err := conn.Exec(ctx, `INSERT INTO team_project_ownership
				(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
				VALUES (?, ?, ?, ?, ?, 'native', 1, 110, 10, ?, ?, ?)`,
				org, provider, team, "proj-"+provider, "KEY", opened, validTo, row.updatedAt); err != nil {
				t.Fatalf("insert project ownership (%s): %v", provider, err)
			}
			if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
				(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
				VALUES (?, ?, ?, NULL, ?, 'exact', 'native', 1, 110, 10, ?, ?, ?)`,
				org, provider, team, "acme/"+provider, opened, validTo, row.updatedAt); err != nil {
				t.Fatalf("insert repo ownership (%s): %v", provider, err)
			}
			if err := conn.Exec(ctx, `INSERT INTO team_memberships
				(org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
				VALUES (?, ?, ?, ?, NULL, NULL, 'native', 1, 100, 10, ?, ?, ?)`,
				org, provider, team, provider+":member", opened, validTo, row.updatedAt); err != nil {
				t.Fatalf("insert membership (%s): %v", provider, err)
			}
		}
	}

	// The pair must be physically unmerged, or the test proves nothing.
	for _, table := range []string{"team_project_ownership", "team_repo_ownership", "team_memberships"} {
		var physical uint64
		if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s WHERE org_id = ?", table), org).Scan(&physical); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if want := uint64(2 * len(retractionProviders)); physical != want {
			t.Fatalf("%s holds %d physical rows, want %d (open + retraction per provider, unmerged)", table, physical, want)
		}
	}

	source := ClickHouseFactSource{Conn: conn}
	loaders := []struct {
		name string
		load func(asOf time.Time) (int, map[string]bool, error)
	}{
		{"LoadProjects", func(asOf time.Time) (int, map[string]bool, error) {
			facts, err := source.LoadProjects(ctx, org, asOf)
			seen := map[string]bool{}
			for _, f := range facts {
				seen[f.Provider] = true
			}
			return len(facts), seen, err
		}},
		{"LoadRepos", func(asOf time.Time) (int, map[string]bool, error) {
			facts, err := source.LoadRepos(ctx, org, asOf)
			seen := map[string]bool{}
			for _, f := range facts {
				seen[f.Provider] = true
			}
			return len(facts), seen, err
		}},
		{"LoadProviderMembers", func(asOf time.Time) (int, map[string]bool, error) {
			facts, err := source.LoadProviderMembers(ctx, org, asOf)
			seen := map[string]bool{}
			for _, f := range facts {
				seen[f.Provider] = true
			}
			return len(facts), seen, err
		}},
	}
	for _, loader := range loaders {
		// Before the retraction takes effect the open row still holds, for
		// every provider (control: the loader is not simply returning nothing).
		count, seen, err := loader.load(beforeRetraction)
		if err != nil {
			t.Fatalf("%s(before retraction): %v", loader.name, err)
		}
		if count != len(retractionProviders) {
			t.Errorf("%s at %s returned %d facts, want %d (the open row, one per provider): %v",
				loader.name, beforeRetraction.Format(time.DateOnly), count, len(retractionProviders), seen)
		}
		// After it, the retraction is honored for every provider.
		count, seen, err = loader.load(afterRetraction)
		if err != nil {
			t.Fatalf("%s(after retraction): %v", loader.name, err)
		}
		if count != 0 {
			t.Errorf("%s at %s returned %d facts for providers %v, want 0: the retraction row is invisible until the pair merges",
				loader.name, afterRetraction.Format(time.DateOnly), count, seen)
		}
	}
}

// A re-open is the retraction's mirror: the newest row under the sort key has
// valid_to NULL while an older row carries a real valid_to. valid_to is
// Nullable, so a plain argMax(valid_to, ...) skips the NULL and returns the
// older, closed date -- re-closing a row the newest version re-opened.
func TestLoadersHonorAReopenBeforeTheRowPairMerges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := openTeamAttributionSchema(ctx, t)

	for _, table := range []string{"team_project_ownership", "team_repo_ownership", "team_memberships"} {
		if err := conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatalf("stop merges on %s: %v", table, err)
		}
	}
	opened := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	closed := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	reopened := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	const org = "org-6637-reopen"

	for _, row := range []struct {
		validTo   any
		updatedAt time.Time
	}{{closed, closed}, {nil, reopened}} {
		if err := conn.Exec(ctx, `INSERT INTO team_project_ownership
			(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'jira', 'team-a', 'proj-1', 'KEY', 'native', 1, 110, 10, ?, ?, ?)`,
			org, opened, row.validTo, row.updatedAt); err != nil {
			t.Fatalf("insert project ownership: %v", err)
		}
		if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
			(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'jira', 'team-a', NULL, 'acme/api', 'exact', 'native', 1, 110, 10, ?, ?, ?)`,
			org, opened, row.validTo, row.updatedAt); err != nil {
			t.Fatalf("insert repo ownership: %v", err)
		}
		if err := conn.Exec(ctx, `INSERT INTO team_memberships
			(org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'jira', 'team-a', 'jira:member', NULL, NULL, 'native', 1, 100, 10, ?, ?, ?)`,
			org, opened, row.validTo, row.updatedAt); err != nil {
			t.Fatalf("insert membership: %v", err)
		}
	}

	source := ClickHouseFactSource{Conn: conn}
	projects, err := source.LoadProjects(ctx, org, asOf)
	if err != nil || len(projects) != 1 {
		t.Errorf("LoadProjects after a re-open: %d facts, err %v; want 1", len(projects), err)
	}
	repos, err := source.LoadRepos(ctx, org, asOf)
	if err != nil || len(repos) != 1 {
		t.Errorf("LoadRepos after a re-open: %d facts, err %v; want 1", len(repos), err)
	}
	members, err := source.LoadProviderMembers(ctx, org, asOf)
	if err != nil || len(members) != 1 {
		t.Errorf("LoadProviderMembers after a re-open: %d facts, err %v; want 1", len(members), err)
	}
}
