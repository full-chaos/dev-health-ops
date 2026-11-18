package icfinalize

import (
	"testing"

	"github.com/google/uuid"
)

// CHAOS-14105 (acp form B'): one row per git row of a person, each with its own git values, the person's label on
// every row, and the person's work items on the no-repository row only.

var (
	repoLow  = uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	repoHigh = uuid.MustParse("00000000-0000-4000-8000-0000000000b2")
)

func rowsOf(merged []ICUserMetric, identity string) (git map[uuid.UUID]ICUserMetric, synthesized []ICUserMetric) {
	git = map[uuid.UUID]ICUserMetric{}
	for _, m := range merged {
		if m.IdentityID != identity {
			continue
		}
		if m.SynthesizedRepoID {
			synthesized = append(synthesized, m)
		} else {
			git[m.RepoID] = m
		}
	}
	return git, synthesized
}

func TestEveryGitRowOfAPersonIsARowWithItsOwnValues(t *testing.T) {
	resolve := TeamResolver(func(string) (string, bool) { return "github:member-team", true })
	merged := MergeICUserMetrics([]GitUserMetric{
		{AuthorEmail: "p", TeamID: "github:old", RepoID: repoHigh, LOCAdded: 7, LOCDeleted: 3, PRsAuthored: 2, PRsMerged: 2, MedianPRCycleHours: 4, PRCycleP90Hours: 9},
		{AuthorEmail: "p", TeamID: "unassigned", RepoID: repoLow, LOCAdded: 40, LOCDeleted: 10, PRsAuthored: 1, PRsMerged: 1, MedianPRCycleHours: 2, PRCycleP90Hours: 3},
	}, nil, resolve)
	git, synthesized := rowsOf(merged, "p")
	if len(git) != 2 || len(synthesized) != 0 {
		t.Fatalf("rows: %d git, %d synthesized, want 2 and 0", len(git), len(synthesized))
	}
	for repo, want := range map[uuid.UUID]ICUserMetric{
		repoLow:  {LOCTouched: 50, PRsOpened: 1, DeliveryUnits: 1, CycleP50Hours: 2, CycleP90Hours: 3},
		repoHigh: {LOCTouched: 10, PRsOpened: 2, DeliveryUnits: 2, CycleP50Hours: 4, CycleP90Hours: 9},
	} {
		got := git[repo]
		if got.TeamID != "github:member-team" || got.LOCTouched != want.LOCTouched || got.PRsOpened != want.PRsOpened ||
			got.DeliveryUnits != want.DeliveryUnits || got.CycleP50Hours != want.CycleP50Hours || got.CycleP90Hours != want.CycleP90Hours ||
			got.WorkItemsComplete != 0 || got.WorkItemsActive != 0 || got.PassThrough.RepoID != repo {
			t.Errorf("row of %s = %+v, want the label github:member-team, its own values %+v and no work item", repo, got, want)
		}
	}
	if merged[0].RepoID != repoLow || merged[1].RepoID != repoHigh {
		t.Errorf("order = %s, %s, want the git rows of a person by repository id", merged[0].RepoID, merged[1].RepoID)
	}
}

func TestWorkItemsSitOnTheNoRepositoryRowOnlyInEitherInputOrder(t *testing.T) {
	a := GitUserMetric{AuthorEmail: "p", RepoID: repoLow, PRsMerged: 1}
	b := GitUserMetric{AuthorEmail: "p", RepoID: repoHigh, PRsMerged: 2}
	items := []WorkItemUserMetric{{UserIdentity: "p", ItemsCompleted: 4, WIPCountEndOfDay: 2}}
	for name, order := range map[string][]GitUserMetric{"low first": {a, b}, "high first": {b, a}} {
		git, synthesized := rowsOf(MergeICUserMetrics(order, items, nil), "p")
		if len(git) != 2 || len(synthesized) != 1 {
			t.Fatalf("%s: rows %d git, %d synthesized, want 2 and 1", name, len(git), len(synthesized))
		}
		for repo, row := range git {
			if row.WorkItemsComplete != 0 || row.WorkItemsActive != 0 {
				t.Errorf("%s: git row %s holds work items %d/%d, want 0/0", name, repo, row.WorkItemsComplete, row.WorkItemsActive)
			}
		}
		if s := synthesized[0]; s.WorkItemsComplete != 4 || s.WorkItemsActive != 2 || s.DeliveryUnits != 4 || s.LOCTouched != 0 {
			t.Errorf("%s: no-repository row = %+v, want 4 completed, 2 open, delivery 4, no git value", name, s)
		}
		if git[repoLow].DeliveryUnits+git[repoHigh].DeliveryUnits+synthesized[0].DeliveryUnits != 7 {
			t.Errorf("%s: delivery over the person's rows != 1 + 2 + 4", name)
		}
	}
}

func TestAWorkItemOnlyPersonIsOneNoRepositoryRow(t *testing.T) {
	git, synthesized := rowsOf(MergeICUserMetrics(nil, []WorkItemUserMetric{{UserIdentity: "w"}}, nil), "w")
	if len(git) != 0 || len(synthesized) != 1 {
		t.Fatalf("rows: %d git, %d synthesized, want 0 and 1 (a work-item-only person keeps its row, also with zero counts)", len(git), len(synthesized))
	}
}

func TestAPersonWithGitRowsAndZeroWorkItemsGetsNoNoRepositoryRow(t *testing.T) {
	git, synthesized := rowsOf(MergeICUserMetrics(
		[]GitUserMetric{{AuthorEmail: "p", RepoID: repoLow}},
		[]WorkItemUserMetric{{UserIdentity: "p", ItemsCompleted: 0, WIPCountEndOfDay: 0}}, nil), "p")
	if len(git) != 1 || len(synthesized) != 0 {
		t.Fatalf("rows: %d git, %d synthesized, want 1 and 0", len(git), len(synthesized))
	}
}
