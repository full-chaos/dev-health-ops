package workitemengine

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Two areas of one (repository, team) used to be two rows with a p50 each; they are one row with one p50 of all
// the key's cycle hours, and the counts and delivery units are the sums.
func TestComputeWorkItemCompletionDailyIsOneRowPerRepositoryAndTeam(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	end := day.AddDate(0, 0, 1)
	repo := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	at := func(hour int) *time.Time { v := day.Add(time.Duration(hour) * time.Hour); return &v }
	started := func(hours int) *time.Time { v := day.Add(-time.Duration(hours) * time.Hour); return &v }
	points := 3.0
	items := []Item{
		{WorkItemID: "a", Provider: "github", RepoID: &repo, Labels: []string{"feature"}, CreatedAt: day.AddDate(0, 0, -5), StartedAt: started(10), CompletedAt: at(3)},
		{WorkItemID: "b", Provider: "github", RepoID: &repo, Labels: []string{"bug"}, CreatedAt: day.AddDate(0, 0, -5), StartedAt: started(30), CompletedAt: at(5), StoryPoints: &points},
		{WorkItemID: "c", Provider: "github", RepoID: &repo, Labels: []string{"chore"}, CreatedAt: day.AddDate(0, 0, -5), StartedAt: started(50), CompletedAt: at(7)},
		{WorkItemID: "d", Provider: "github", RepoID: &repo, Labels: []string{"feature"}, CreatedAt: day.AddDate(0, 0, -5), CompletedAt: at(9)},
		{WorkItemID: "e", Provider: "github", RepoID: nil, Labels: []string{"feature"}, CreatedAt: day.AddDate(0, 0, -5), CompletedAt: at(9)},
		{WorkItemID: "open", Provider: "github", RepoID: &repo, Labels: []string{"feature"}, CreatedAt: day.AddDate(0, 0, -1)},
		{WorkItemID: "early", Provider: "github", RepoID: &repo, Labels: []string{"feature"}, CreatedAt: day.AddDate(0, 0, -9), CompletedAt: started(2)},
	}
	rows, err := ComputeWorkItemCompletionDaily(items, day, end, teams("alpha", "alpha", "beta", "", "", "alpha", "alpha"))
	if err != nil {
		t.Fatal(err)
	}
	// alpha in the repository: a (13 h) and b (35 h, 3 units) -> 2 completed, 4 units, cycle hours [13, 35], p50 =
	// element len/2 = 35. beta: c (53 h). The two items of no team: d (no start: no cycle hour) in the repository,
	// e with no repository.
	want := []WorkItemCompletionDailyRow{
		{RepoID: &repo, TeamID: "alpha", DeliveryUnits: 4, WorkItemsCompleted: 2, CycleP50Hours: 35},
		{RepoID: &repo, TeamID: "beta", DeliveryUnits: 1, WorkItemsCompleted: 1, CycleP50Hours: 57},
		{RepoID: &repo, TeamID: "", DeliveryUnits: 1, WorkItemsCompleted: 1, CycleP50Hours: 0},
		{RepoID: nil, TeamID: "", DeliveryUnits: 1, WorkItemsCompleted: 1, CycleP50Hours: 0},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("completion rows:\n got %+v\nwant %+v", rows, want)
	}
}

func TestComputeWorkItemCompletionDailyRefusesWhatItCannotCount(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	end := day.AddDate(0, 0, 1)
	done := day.Add(time.Hour)
	bad := math.Inf(1)
	items := []Item{{WorkItemID: "x", Provider: "github", CreatedAt: day.AddDate(0, 0, -1), CompletedAt: &done, StoryPoints: &bad}}
	if _, err := ComputeWorkItemCompletionDaily(items, day, end, teams("")); !errors.Is(err, ErrUnrepresentableDeliveryUnits) {
		t.Fatalf("err = %v, want ErrUnrepresentableDeliveryUnits", err)
	}
}

func linkedPR(repo uuid.UUID, number uint32, tier string, additions, deletions *uint32) LinkedPR {
	return LinkedPR{RepoID: repo, Number: number, Provenance: tier, Additions: additions, Deletions: deletions}
}

func lines(value uint32) *uint32 { return &value }

// churn_loc is the additions plus deletions of the pull requests linked to the items completed on the day, each
// pull request once per (repository, team) row. Python wrote 0, so the expected values are worked by hand.
func TestComputeWorkItemCompletionDailyChurnIsTheSizeOfTheLinkedPullRequests(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	end := day.AddDate(0, 0, 1)
	repo := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	prRepo := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	for _, provider := range []string{"jira", "gitlab", "github", "linear"} {
		t.Run(provider, func(t *testing.T) {
			complete := func(id string, prs ...LinkedPR) Item {
				return Item{WorkItemID: id, Provider: provider, RepoID: &repo,
					CreatedAt: day.AddDate(0, 0, -2), CompletedAt: timePointer(day.Add(10 * time.Hour)), LinkedPRs: prs}
			}
			items := []Item{
				// PR 1 (100+20) by a native link and PR 2 (30+10) by a text link: 160.
				complete("1", linkedPR(prRepo, 1, LinkTierNative, lines(100), lines(20)),
					linkedPR(prRepo, 2, LinkTierExplicitText, lines(30), lines(10))),
				// The same PR 1 again (counted once for the row), and a heuristic PR with no size: adds 0.
				complete("2", linkedPR(prRepo, 1, LinkTierNative, lines(100), lines(20)),
					linkedPR(prRepo, 3, LinkTierHeuristic, nil, nil)),
				// No linked pull request: adds nothing.
				complete("3"),
				// Same number in another repository is another pull request: 5+5.
				complete("4", linkedPR(repo, 1, LinkTierNative, lines(5), lines(5))),
				// Open item with a huge linked PR: not completed, so not in the day's set.
				{WorkItemID: "5", Provider: provider, RepoID: &repo, CreatedAt: day.AddDate(0, 0, -2),
					LinkedPRs: []LinkedPR{linkedPR(prRepo, 9, LinkTierNative, lines(100000), lines(100000))}},
			}
			rows, err := ComputeWorkItemCompletionDaily(items, day, end, teams("", "", "", "", ""))
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || rows[0].WorkItemsCompleted != 4 || rows[0].ChurnLOC != 170 {
				t.Fatalf("rows = %+v, want one row with 4 completed items and churn_loc 170 (160 + 10)", rows)
			}
		})
	}
}

func TestComputeWorkItemCompletionDailyChurnIsPerTeamRow(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	repo := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	prRepo := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	complete := func(id string, pr LinkedPR) Item {
		return Item{WorkItemID: id, Provider: "github", RepoID: &repo,
			CreatedAt: day.AddDate(0, 0, -1), CompletedAt: timePointer(day.Add(time.Hour)), LinkedPRs: []LinkedPR{pr}}
	}
	items := []Item{
		complete("1", linkedPR(prRepo, 1, LinkTierNative, lines(10), lines(1))),
		complete("2", linkedPR(prRepo, 2, LinkTierNative, lines(20), lines(2))),
	}
	rows, err := ComputeWorkItemCompletionDaily(items, day, day.AddDate(0, 0, 1), teams("team-a", "team-b"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].TeamID != "team-a" || rows[0].ChurnLOC != 11 || rows[1].TeamID != "team-b" || rows[1].ChurnLOC != 22 {
		t.Fatalf("rows = %+v, want team-a 11 and team-b 22", rows)
	}
}

func TestComputeWorkItemCompletionDailyChurnIsZeroWithNoLinkedPullRequest(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	repo := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	items := []Item{{WorkItemID: "1", Provider: "github", RepoID: &repo,
		CreatedAt: day.AddDate(0, 0, -1), CompletedAt: timePointer(day.Add(time.Hour))}}
	rows, err := ComputeWorkItemCompletionDaily(items, day, day.AddDate(0, 0, 1), teams(""))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].WorkItemsCompleted != 1 || rows[0].ChurnLOC != 0 {
		t.Fatalf("rows = %+v, want one row with 1 completed item and churn_loc 0", rows)
	}
}
