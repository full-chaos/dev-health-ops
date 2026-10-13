//go:build integration

package providersync

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

// CHAOS-9548: the readers of the derivation (the project links it derives repo
// ownership from, and the open inferred rows it compares a new derivation with)
// give the same answer when a sync does not write an unchanged row again. One
// org has every row written again at three later syncs (what a writer did
// before); the other has each row once.
func TestDerivationReadersAnswerTheSameWhenARepeatedRowIsNotWrittenAgain(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const repeated, once = "org-rows-written-again", "org-rows-written-once"
	first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	changed := first.AddDate(0, 1, 0)
	asOf := first.AddDate(0, 6, 0)
	key := "PLAT"
	closedAt := changed

	for _, org := range []string{repeated, once} {
		syncs := 1
		if org == repeated {
			syncs = 4
		}
		for sync := 0; sync < syncs; sync++ {
			offset := time.Duration(sync) * time.Hour
			for _, p := range []struct {
				team, project string
				primary       uint8
				specificity   uint16
				from          time.Time
				to            *time.Time
				at            time.Time
			}{
				{"team-a", "proj-1", 1, 100, first, nil, first},
				{"team-b", "proj-1", 1, 100, first, nil, first},
				{"team-a", "proj-2", 1, 100, first, &closedAt, changed},
				{"team-b", "proj-2", 1, 100, changed, nil, changed},
				{"team-c", "proj-3", 0, 40, first, nil, first},
			} {
				if err := conn.Exec(ctx, `INSERT INTO team_project_ownership
					(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
					VALUES (?, 'jira', ?, ?, ?, 'native', ?, ?, 0, ?, ?, ?)`,
					org, p.team, p.project, &key, p.primary, p.specificity, p.from, p.to, p.at.Add(offset)); err != nil {
					t.Fatal(err)
				}
			}
			for _, r := range []struct {
				team, repo, id string
				primary        uint8
				share          float64
				from           time.Time
				to             *time.Time
				at             time.Time
			}{
				{"team-a", "acme/a", "11111111-1111-4111-8111-111111111111", 1, 0.75, first, nil, first},
				{"team-b", "acme/a", "11111111-1111-4111-8111-111111111111", 2, 0.25, first, nil, first},
				{"team-a", "acme/b", "22222222-2222-4222-8222-222222222222", 1, 1, first, &closedAt, changed},
				{"team-b", "acme/b", "22222222-2222-4222-8222-222222222222", 1, 1, changed, nil, changed},
			} {
				if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
					(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share)
					VALUES (?, 'github', ?, toUUID(?), ?, 'exact', 'inferred', ?, 150, 10, ?, ?, ?, ?)`,
					org, r.team, r.id, r.repo, r.primary, r.from, r.to, r.at.Add(offset), r.share); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	links := func(org string) []TeamRepoOwnershipProjectLink {
		t.Helper()
		got, err := loadTeamRepoOwnershipProjectLinks(ctx, conn, org, asOf)
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(got, func(i, j int) bool {
			return got[i].ProjectID+"/"+got[i].TeamID < got[j].ProjectID+"/"+got[j].TeamID
		})
		return got
	}
	if got, want := links(once), links(repeated); !reflect.DeepEqual(got, want) || len(got) != 4 {
		t.Fatalf("project links differ: written once %+v, written again %+v, want the same 4", got, want)
	}
	active := func(org string) []teamRepoOwnershipActiveRow {
		t.Helper()
		got, err := loadTeamRepoOwnershipActiveInferredRows(ctx, conn, org)
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(got, func(i, j int) bool {
			return got[i].RepoFullName+"/"+got[i].TeamID < got[j].RepoFullName+"/"+got[j].TeamID
		})
		return got
	}
	if got, want := active(once), active(repeated); !reflect.DeepEqual(got, want) || len(got) != 3 {
		t.Fatalf("open inferred rows differ: written once %+v, written again %+v, want the same 3", got, want)
	}
}
