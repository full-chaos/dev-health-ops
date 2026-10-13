//go:build integration

package teamattribution

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// CHAOS-9548: a sync that repeats an open ownership row no longer writes it
// again. Two orgs hold the same ownership: one with every row written again at
// three later syncs (what a writer did before), one with each row once. The
// fact loaders must return the same facts for both, except for UpdatedAt,
// which is now the time of the last real change of the fact and not the time
// of the last sync.
func TestOwnershipFactsAreTheSameWhenARepeatedRowIsNotWrittenAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := openTeamAttributionSchema(ctx, t)
	const repeated, once = "org-rows-written-again", "org-rows-written-once"
	first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	changed := first.AddDate(0, 1, 0)
	asOf := first.AddDate(0, 6, 0)
	repoA, repoB := "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"
	key := "PLAT"

	type project struct {
		team, id string
		primary  uint8
		priority int32
		from     time.Time
		to       *time.Time
		changeAt time.Time
	}
	closedAt := changed
	projects := []project{
		{"team-a", "proj-1", 1, 0, first, nil, first},
		{"team-b", "proj-1", 2, 0, first, nil, first},
		{"team-a", "proj-2", 1, 0, first, &closedAt, changed},
		{"team-b", "proj-2", 1, 0, changed, nil, changed},
	}
	type repo struct {
		team, name, id string
		primary        uint8
		from           time.Time
		to             *time.Time
		changeAt       time.Time
	}
	repos := []repo{
		{"team-a", "acme/a", repoA, 1, first, nil, first},
		{"team-b", "acme/a", repoA, 2, first, nil, first},
		{"team-a", "acme/b", repoB, 1, first, &closedAt, changed},
		{"team-b", "acme/b", repoB, 1, changed, nil, changed},
	}
	for _, org := range []string{repeated, once} {
		syncs := 1
		if org == repeated {
			syncs = 4
		}
		for sync := 0; sync < syncs; sync++ {
			for _, p := range projects {
				updatedAt := p.changeAt.Add(time.Duration(sync) * time.Hour)
				if err := conn.Exec(ctx, `INSERT INTO team_project_ownership
					(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
					VALUES (?, 'jira', ?, ?, ?, 'native', ?, 100, ?, ?, ?, ?)`,
					org, p.team, p.id, &key, p.primary, p.priority, p.from, p.to, updatedAt); err != nil {
					t.Fatal(err)
				}
			}
			for _, r := range repos {
				updatedAt := r.changeAt.Add(time.Duration(sync) * time.Hour)
				if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
					(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
					VALUES (?, 'github', ?, toUUID(?), ?, 'exact', 'inferred', ?, 150, 0, ?, ?, ?)`,
					org, r.team, r.id, r.name, r.primary, r.from, r.to, updatedAt); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	source := ClickHouseFactSource{Conn: conn}
	loadProjects := func(org string) []GithubWorkItemDerivationProjectFact {
		t.Helper()
		facts, err := source.LoadProjects(ctx, org, asOf)
		if err != nil {
			t.Fatal(err)
		}
		for i := range facts {
			facts[i].UpdatedAt = time.Time{}
		}
		return facts
	}
	loadRepos := func(org string) []GithubWorkItemDerivationRepoFact {
		t.Helper()
		facts, err := source.LoadRepos(ctx, org, asOf)
		if err != nil {
			t.Fatal(err)
		}
		for i := range facts {
			facts[i].UpdatedAt = time.Time{}
		}
		return facts
	}
	if got, want := loadProjects(once), loadProjects(repeated); !reflect.DeepEqual(got, want) || len(got) != 3 {
		t.Fatalf("project facts differ: written once %+v, written again %+v, want the same 3", got, want)
	}
	if got, want := loadRepos(once), loadRepos(repeated); !reflect.DeepEqual(got, want) || len(got) != 3 {
		t.Fatalf("repo facts differ: written once %+v, written again %+v, want the same 3", got, want)
	}

	facts, err := source.LoadProjects(ctx, once, asOf)
	if err != nil {
		t.Fatal(err)
	}
	for _, fact := range facts {
		want := first
		if fact.ProjectID == "proj-2" {
			want = changed
		}
		if !fact.UpdatedAt.Equal(want) {
			t.Fatalf("fact %s/%s UpdatedAt = %s, want the time of its last change %s", fact.ProjectID, fact.TeamID, fact.UpdatedAt, want)
		}
	}
}
