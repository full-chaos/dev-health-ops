//go:build integration

package teamattribution

import (
	"context"
	"slices"
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
// open membership: the membership gate drops it before it counts teams (reason
// no_membership), and it never takes the item. The inactive team stays KNOWN
// to the cascade; a loader that dropped it would make the team unknown, not
// inactive, and hand the item to it.
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
		if len(candidates) != 1 || candidates[0].Evidence != "no_candidate:no_membership" {
			t.Errorf("%s: candidates = %+v, want the one unassigned candidate with no_candidate:no_membership", provider, candidates)
		}
	}
}

// A retired project-as-team row has id = the project key ("SEC"); a real
// Atlassian team has an ARI id and holds the same key. By byte order "S" < "a",
// so the inactive team is first by id on that key. The key must go to the
// ACTIVE team: the first-by-id map is built from active teams only. The same
// holds for every provider, and for a native team key that two teams hold.
func TestAnInactiveTeamFirstByIDDoesNotShadowTheActiveTeamOnTheSameKey(t *testing.T) {
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
	const org = "org-shadow"
	first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// The key maps are not scoped by provider: one key per provider.
	keyOf := func(provider string) string { return "SEC-" + provider }
	activeID := func(provider string) string { return "ari:cloud:identity::team/" + provider }
	for _, provider := range retractionProviders {
		// The retired pseudo-team: id = key, newest row inactive.
		key := keyOf(provider)
		pseudo, real := uuid.New(), uuid.New()
		for version, isActive := range []uint8{1, 0} {
			at := first.Add(time.Duration(version) * time.Hour)
			if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], ?, [], ?, ?, ?, ?, ?, ?)`,
				key, pseudo, "Project "+key, []string{key}, isActive, at, at, org, provider, key); err != nil {
				t.Fatalf("insert pseudo team: %v", err)
			}
		}
		if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], ?, [], 1, ?, ?, ?, ?, ?)`,
			activeID(provider), real, "Real "+provider, []string{key}, first, first, org, provider, activeID(provider)); err != nil {
			t.Fatalf("insert active team: %v", err)
		}
	}
	facts, err := ClickHouseFactSource{Conn: conn}.LoadTeams(ctx, org)
	if err != nil {
		t.Fatalf("LoadTeams: %v", err)
	}
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: facts})
	for _, provider := range retractionProviders {
		key := keyOf(provider)
		projectKey, nativeKey := key, key
		for name, subject := range map[string]GithubWorkItemDerivationSubject{
			"by the project key":     {WorkItemID: provider + ":1", Provider: provider, Type: "issue", ProjectKey: &projectKey},
			"by the native team key": {WorkItemID: provider + ":2", Provider: provider, Type: "issue", NativeTeamKey: &nativeKey},
		} {
			gotTeam, _, candidates := derived.Resolve(subject)
			if gotTeam == nil || *gotTeam != activeID(provider) {
				t.Errorf("%s %s: resolved team = %q, want %s: the inactive team %q must not shadow it (candidates %+v)",
					provider, name, GithubWorkItemDerivationStringValue(gotTeam), activeID(provider), key, candidates)
			}
		}
	}
}

// Teams are (provider, id) for the active-team rule, through the real loader.
// A retired Jira project-as-team row has id = the project key ("ENG") and a
// Linear team id is its team key ("ENG"): one id, two teams. For every pair of
// providers and both roles (the item's own team active and the other
// provider's team inactive, and the reverse), the inactive team of one
// provider does not drop the active team of the other, and the inactive team
// of the item's own provider drops it. The teams sorting key is (org_id, id),
// so a merge keeps one of the two rows; merges are stopped so that both teams
// stay physical, as a loader sees them between merges.
func TestAnInactiveTeamDropsOnlyTheTeamOfItsOwnProviderWithTheSameID(t *testing.T) {
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
	first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, provider := range retractionProviders {
		for _, other := range retractionProviders {
			if other == provider {
				continue
			}
			org := "org-same-id-" + provider + "-" + other
			// The item's provider is active; the other provider's team of the
			// same id was set inactive by a newer row.
			for _, row := range []struct {
				provider string
				states   []uint8
			}{{provider, []uint8{1}}, {other, []uint8{1, 0}}} {
				teamUUID := uuid.New()
				for version, isActive := range row.states {
					at := first.Add(time.Duration(version) * time.Hour)
					if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], ?, [], ?, ?, ?, ?, ?, ?)`,
						"ENG", teamUUID, "Eng "+row.provider, []string{"ENG"}, isActive, at, at, org, row.provider, "ENG"); err != nil {
						t.Fatalf("insert team %s/ENG: %v", row.provider, err)
					}
				}
			}
			facts, err := ClickHouseFactSource{Conn: conn}.LoadTeams(ctx, org)
			if err != nil {
				t.Fatalf("LoadTeams: %v", err)
			}
			if len(facts) != 2 {
				t.Fatalf("LoadTeams = %+v, want the two teams %s/ENG and %s/ENG", facts, provider, other)
			}
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: facts})
			key := "ENG"
			for _, item := range []struct {
				provider string
				want     string
			}{{provider, "ENG"}, {other, ""}} {
				for name, subject := range map[string]GithubWorkItemDerivationSubject{
					"by the project key":     {WorkItemID: item.provider + ":ENG-1", Provider: item.provider, Type: "issue", ProjectKey: &key},
					"by the native team key": {WorkItemID: item.provider + ":ENG-2", Provider: item.provider, Type: "issue", NativeTeamKey: &key},
				} {
					gotTeam, _, candidates := derived.Resolve(subject)
					if got := GithubWorkItemDerivationStringValue(gotTeam); got != item.want {
						t.Errorf("active %s, inactive %s: %s item %s: resolved team = %q, want %q (candidates %+v)",
							provider, other, item.provider, name, got, item.want, candidates)
					}
				}
			}
		}
	}
}

// A person in an inactive team and an active team is attributed to the active
// team: the exactly-one-team gate of the membership layers counts the active
// teams only. Real rows, real loaders, all providers, the provider layer
// (team_memberships) and the admin layer (teams.manual_members).
func TestAMemberOfAnInactiveAndAnActiveTeamResolvesToTheActiveTeam(t *testing.T) {
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
	const org = "org-inactive-membership-gate"
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for _, provider := range retractionProviders {
		old, current := "old-"+provider, "cur-"+provider
		oldUUID, currentUUID := uuid.New(), uuid.New()
		for version, isActive := range []uint8{1, 0} {
			stamp := at.Add(time.Duration(version) * time.Hour)
			if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], ['K'], [], ?, ?, ?, ?, ?, ?)`,
				old, oldUUID, "Old "+provider, isActive, stamp, stamp, org, provider, old); err != nil {
				t.Fatalf("insert team %s: %v", old, err)
			}
		}
		if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], ['K'], [], 1, ?, ?, ?, ?, ?)`,
			current, currentUUID, "Current "+provider, at, at, org, provider, current); err != nil {
			t.Fatalf("insert team %s: %v", current, err)
		}
		for _, id := range []string{old, current} {
			if err := conn.Exec(ctx, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, identity_facets, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, ?, ?, 'someone', ['someone'], 'native', 1, 100, 10, ?, ?)`,
				org, provider, id, at, at); err != nil {
				t.Fatalf("insert membership %s: %v", id, err)
			}
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
	for _, provider := range retractionProviders {
		candidates, reason := derived.ResolveMembership(provider, "someone")
		if reason != "" || candidateTeams(candidates) != "cur-"+provider {
			t.Errorf("%s: membership teams %q reason %q, want cur-%s and no reason", provider, candidateTeams(candidates), reason, provider)
		}
		typ := map[string]string{"github": "pr", "gitlab": "merge_request", "linear": "issue", "jira": "issue"}[provider]
		subject := GithubWorkItemDerivationSubject{WorkItemID: provider + ":x#1", Provider: provider, Type: typ, Assignees: []string{"someone"}, OrgID: org}
		team, _, resolved := derived.Resolve(subject)
		if got := GithubWorkItemDerivationStringValue(team); got != "cur-"+provider {
			t.Errorf("%s: resolved team = %q, want cur-%s (candidates %+v)", provider, got, provider, resolved)
		}
	}
}

// The admin layer (teams.manual_members, read by the real LoadMembers). The
// roster facet carries no provider, so team id "shr" held by an ACTIVE team of
// another provider names the item's own team "shr" too, which is inactive. The
// inactive team is not counted: with an active admin team beside it the person
// goes to the active one; with none the person falls through to the provider
// layer.
func TestAnInactiveAdminTeamIsNotCountedByTheMembershipGate(t *testing.T) {
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
	// Merges stop so that the rows stay physical, as in the other cases here.
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES teams`); err != nil {
		t.Fatalf("stop merges: %v", err)
	}
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	insertTeam := func(org, provider, id string, active uint8, stamp time.Time, manual []string) {
		if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], ?, ['K'], [], ?, ?, ?, ?, ?, ?)`,
			id, uuid.New(), id, manual, active, stamp, stamp, org, provider, id); err != nil {
			t.Fatalf("insert team %s: %v", id, err)
		}
	}
	for _, provider := range retractionProviders {
		for _, withActiveAdmin := range []bool{false, true} {
			org := "org-inactive-admin-" + provider
			want := "prov-" + provider
			if withActiveAdmin {
				org += "-active"
				want = "adm-" + provider
			}
			other := retractionProviders[(slices.Index(retractionProviders, provider)+1)%len(retractionProviders)]
			own, adm := "prov-"+provider, "adm-"+provider
			insertTeam(org, provider, "shr", 1, at, nil)
			insertTeam(org, provider, "shr", 0, at.Add(time.Hour), nil)
			insertTeam(org, other, "shr", 1, at.Add(2*time.Hour), []string{"someone"})
			insertTeam(org, provider, own, 1, at, nil)
			if withActiveAdmin {
				insertTeam(org, provider, adm, 1, at, []string{"someone"})
			}
			if err := conn.Exec(ctx, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, identity_facets, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, ?, ?, 'someone', ['someone'], 'native', 1, 100, 10, ?, ?)`,
				org, provider, own, at, at); err != nil {
				t.Fatalf("insert membership: %v", err)
			}
			source := ClickHouseFactSource{Conn: conn}
			var facts GithubWorkItemDerivationFacts
			var tagged []GithubWorkItemDerivationMemberFact
			if facts.Teams, err = source.LoadTeams(ctx, org); err != nil {
				t.Fatalf("LoadTeams: %v", err)
			}
			if facts.Members, facts.UntypedMembers, facts.ProviderUntypedMembers, tagged, err = source.LoadMembers(ctx, org, at.Add(48*time.Hour)); err != nil {
				t.Fatalf("LoadMembers: %v", err)
			}
			if facts.ProviderMembers, err = source.LoadProviderMembers(ctx, org, at.Add(48*time.Hour)); err != nil {
				t.Fatalf("LoadProviderMembers: %v", err)
			}
			facts.ProviderMembers = append(facts.ProviderMembers, tagged...)
			derived := NewGitHubWorkItemDerivationContext(facts)
			candidates, reason := derived.ResolveMembership(provider, "someone")
			if reason != "" || candidateTeams(candidates) != want {
				t.Errorf("%s activeAdmin=%v: membership teams %q reason %q, want %s and no reason (admin untyped %d)",
					provider, withActiveAdmin, candidateTeams(candidates), reason, want, len(facts.UntypedMembers))
			}
			typ := map[string]string{"github": "pr", "gitlab": "merge_request", "linear": "issue", "jira": "issue"}[provider]
			team, _, resolved := derived.Resolve(GithubWorkItemDerivationSubject{WorkItemID: provider + ":x#1", Provider: provider, Type: typ, Assignees: []string{"someone"}, OrgID: org})
			if got := GithubWorkItemDerivationStringValue(team); got != want {
				t.Errorf("%s activeAdmin=%v: resolved team = %q, want %s (candidates %+v)", provider, withActiveAdmin, got, want, resolved)
			}
		}
	}
}
