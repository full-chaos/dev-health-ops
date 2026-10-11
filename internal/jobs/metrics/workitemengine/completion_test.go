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
