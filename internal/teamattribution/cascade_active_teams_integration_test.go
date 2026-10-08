//go:build integration

package teamattribution

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Only an ACTIVE team takes part in attribution, and the rule is the same for
// every provider and every resolution path: a team whose newest `teams` row is
// inactive takes no work item by a project key, by its own id, by a native team
// key, by repo ownership or by membership. An inactive team stays KNOWN to the
// cascade (so the null-carrying rule treats it as on any team), it is only
// never the result. The newest row decides, so a team set inactive and then
// active again takes its items.
//
// The store is built from the real migration chain. Each team is written as an
// active row and, where the case says so, a newer row under the same sort key.
// Merges are stopped so that the rows stay physical: a loader that reads one
// of the older rows is seen. The rows are read through the real loader and
// resolved by the real context.
func TestAnInactiveTeamTakesNoWorkItemForEveryProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES teams"); err != nil {
		t.Fatalf("stop merges on teams: %v", err)
	}
	const org = "org-active-teams"
	first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// states is the is_active value of each row of a team, oldest first.
	teams := []struct {
		name   string
		states []uint8
		taken  bool
	}{
		{"active", []uint8{1}, true},
		{"inactive", []uint8{1, 0}, false},
		{"neveractive", []uint8{0}, false},
		{"activeagain", []uint8{1, 0, 1}, true},
	}
	physical := 0
	insertTeam := func(provider, id string, states []uint8, keys []string) {
		teamUUID := uuid.New()
		for version, isActive := range states {
			at := first.Add(time.Duration(version) * time.Hour)
			if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], ?, [], ?, ?, ?, ?, ?, ?)`,
				id, teamUUID, "Team "+id, keys, isActive, at, at, org, provider, id); err != nil {
				t.Fatalf("insert team %s: %v", id, err)
			}
			physical++
		}
	}
	for _, provider := range retractionProviders {
		for _, team := range teams {
			id := team.name + "-" + provider
			insertTeam(provider, id, team.states, []string{"KEY" + id})
			// The membership arm: a team with no project key and no ownership
			// (null-carrying) that has one open member. The ownership arm: a
			// team with no project key that owns one repository.
			insertTeam(provider, "m-"+id, team.states, nil)
			insertTeam(provider, "o-"+id, team.states, nil)
			member := "member-" + id
			if err := conn.Exec(ctx, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, identity_facets, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, ?, ?, ?, [?], 'native', 1, 100, 10, ?, ?)`,
				org, provider, "m-"+id, member, member, first, first); err != nil {
				t.Fatalf("insert membership %s: %v", id, err)
			}
			// The membership team owns ANOTHER repository, so it is not
			// null-carrying: the subject's repository has no ownership row and
			// the membership passes the gate. Only the active-team rule can
			// keep an inactive team from taking the item.
			if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, ?, ?, ?, 'exact', 'inferred', 1, 100, 10, ?, NULL, ?)`,
				org, provider, "m-"+id, uuid.NewSHA1(uuid.NameSpaceURL, []byte("other-"+id)), "acme/other-"+id, first, first); err != nil {
				t.Fatalf("insert other ownership %s: %v", id, err)
			}
			if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, ?, ?, ?, 'exact', 'inferred', 1, 100, 10, ?, NULL, ?)`,
				org, provider, "o-"+id, uuid.NewSHA1(uuid.NameSpaceURL, []byte(id)), "acme/"+id, first, first); err != nil {
				t.Fatalf("insert ownership %s: %v", id, err)
			}
		}
	}
	var stored uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM teams WHERE org_id = ?`, org).Scan(&stored); err != nil {
		t.Fatalf("count teams: %v", err)
	}
	if stored != uint64(physical) {
		t.Fatalf("teams holds %d physical rows, want %d (every version unmerged)", stored, physical)
	}

	facts, err := ClickHouseFactSource{Conn: conn}.LoadTeams(ctx, org)
	if err != nil {
		t.Fatalf("LoadTeams: %v", err)
	}
	inactive := map[string]bool{}
	for _, fact := range facts {
		inactive[fact.Provider+"/"+fact.TeamID] = fact.Inactive
	}
	asOf := first.Add(48 * time.Hour)
	members, err := ClickHouseFactSource{Conn: conn}.LoadProviderMembers(ctx, org, asOf)
	if err != nil {
		t.Fatalf("LoadProviderMembers: %v", err)
	}
	repos, err := ClickHouseFactSource{Conn: conn}.LoadRepos(ctx, org, asOf)
	if err != nil {
		t.Fatalf("LoadRepos: %v", err)
	}
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: facts, ProviderMembers: members, Repos: repos})

	for _, provider := range retractionProviders {
		for _, team := range teams {
			id := team.name + "-" + provider
			if flag, ok := inactive[provider+"/"+id]; !ok || flag == team.taken {
				t.Errorf("%s: LoadTeams known = %v, Inactive = %v, want Inactive = %v", id, ok, flag, !team.taken)
			}
			projectKey, teamKey := "KEY"+id, id
			// The membership and ownership arms resolve to their own team id.
			repoID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(id)).String()
			memberSubject := GithubWorkItemDerivationSubject{WorkItemID: provider + ":4", Provider: provider, Type: "issue", Assignees: []string{"member-" + id}}
			if provider == "github" || provider == "gitlab" {
				reporter := "member-" + id
				memberSubject.Type = map[string]string{"github": "pr", "gitlab": "merge_request"}[provider]
				memberSubject.Reporter = &reporter
			}
			for name, arm := range map[string]struct {
				subject GithubWorkItemDerivationSubject
				want    string
			}{
				"by membership": {memberSubject, "m-" + id},
				"by repo ownership": {GithubWorkItemDerivationSubject{
					WorkItemID: provider + ":5", Provider: provider, Type: "issue", RepoID: &repoID,
				}, "o-" + id},
			} {
				gotTeam, _, candidates := derived.Resolve(arm.subject)
				switch {
				case team.taken && (gotTeam == nil || *gotTeam != arm.want):
					t.Errorf("%s %s: resolved team = %v, want %s (candidates %+v)", id, name, gotTeam, arm.want, candidates)
				case !team.taken && gotTeam != nil:
					t.Errorf("%s %s: resolved team = %v, want none: the team is inactive (candidates %+v)", id, name, gotTeam, candidates)
				}
			}
			for name, subject := range map[string]GithubWorkItemDerivationSubject{
				"by the project key":     {WorkItemID: provider + ":1", Provider: provider, Type: "issue", ProjectKey: &projectKey},
				"by the team id as key":  {WorkItemID: provider + ":2", Provider: provider, Type: "issue", ProjectKey: &teamKey},
				"by the native team key": {WorkItemID: provider + ":3", Provider: provider, Type: "issue", NativeTeamKey: &teamKey},
			} {
				gotTeam, _, candidates := derived.Resolve(subject)
				// A subject no team takes still gets the one candidate that
				// says so; it names no team.
				named := 0
				for _, candidate := range candidates {
					if candidate.TeamID != nil && *candidate.TeamID != "" {
						named++
					}
				}
				switch {
				case team.taken && (gotTeam == nil || *gotTeam != id):
					t.Errorf("%s %s: resolved team = %v, want %s (candidates %+v)", id, name, gotTeam, id, candidates)
				case !team.taken && (gotTeam != nil || named != 0):
					t.Errorf("%s %s: resolved team = %v with %d candidates that name a team, want no team: the team is inactive (candidates %+v)",
						id, name, gotTeam, named, candidates)
				}
			}
		}
	}
}

// An INACTIVE team with no project key and no ownership (null-carrying) and an
// open membership: the cascade must treat it as it treats any null-carrying
// team (reason team_null_carrying), never as a team that takes the item. The
// inactive team stays KNOWN to the cascade; a loader that dropped it would
// turn the reason into the R74 pass-through and hand the item to it.
func TestAnInactiveNullCarryingTeamStaysKnownAndTakesNothingThroughMembership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	const org = "org-inactive-null-carrying"
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for _, provider := range retractionProviders {
		id := "old-" + provider
		teamUUID := uuid.New()
		for version, isActive := range []uint8{1, 0} {
			stamp := at.Add(time.Duration(version) * time.Hour)
			if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], [], [], ?, ?, ?, ?, ?, ?)`,
				id, teamUUID, "Old "+provider, isActive, stamp, stamp, org, provider, id); err != nil {
				t.Fatalf("insert team %s: %v", id, err)
			}
		}
		if err := conn.Exec(ctx, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, identity_facets, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, ?, ?, 'someone', ['someone'], 'native', 1, 100, 10, ?, ?)`,
			org, provider, id, at, at); err != nil {
			t.Fatalf("insert membership %s: %v", id, err)
		}
	}
	source := ClickHouseFactSource{Conn: conn}
	teams, err := source.LoadTeams(ctx, org)
	if err != nil {
		t.Fatalf("LoadTeams: %v", err)
	}
	members, err := source.LoadProviderMembers(ctx, org, at.Add(48*time.Hour))
	if err != nil {
		t.Fatalf("LoadProviderMembers: %v", err)
	}
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: teams, ProviderMembers: members})
	repoID := "d41d8cd9-8f00-3204-a980-0998ecf8427e"
	reporter := "someone"
	for _, provider := range retractionProviders {
		typ := map[string]string{"github": "pr", "gitlab": "merge_request", "linear": "issue", "jira": "issue"}[provider]
		subject := GithubWorkItemDerivationSubject{WorkItemID: provider + ":x#1", Provider: provider, Type: typ, RepoID: &repoID, Reporter: &reporter, Assignees: []string{"someone"}, OrgID: org}
		team, _, candidates := derived.Resolve(subject)
		if team != nil {
			t.Errorf("%s: resolved team = %q, want none: the team is inactive (candidates %+v)", provider, *team, candidates)
		}
		if len(candidates) != 1 || candidates[0].Evidence != "no_candidate:team_null_carrying" {
			t.Errorf("%s: candidates = %+v, want the one unassigned candidate with no_candidate:team_null_carrying", provider, candidates)
		}
	}
}
