package workitemengine

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The completion rows are the rows of ComputeInvestmentDaily with the classification left out: summed over the
// areas of one (repository, team), count and delivery units are equal; the cycle p50 is the p50 of the whole key.
func TestComputeWorkItemCompletionDailyEqualsTheAreaRowsSummed(t *testing.T) {
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
	resolve := teams("alpha", "alpha", "beta", "", "", "alpha", "alpha")

	_, areaRows, err := ComputeInvestmentDaily(items, day, end, resolve, labelClassifier{})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := ComputeWorkItemCompletionDaily(items, day, end, resolve)
	if err != nil {
		t.Fatal(err)
	}

	type key struct {
		repo string
		team string
	}
	summed := map[key][2]int{}
	for _, r := range areaRows {
		k := key{team: r.TeamID}
		if r.RepoID != nil {
			k.repo = r.RepoID.String()
		}
		v := summed[k]
		v[0] += r.WorkItemsCompleted
		v[1] += r.DeliveryUnits
		summed[k] = v
	}
	got := map[key][2]int{}
	for _, r := range rows {
		k := key{team: r.TeamID}
		if r.RepoID != nil {
			k.repo = r.RepoID.String()
		}
		if _, dup := got[k]; dup {
			t.Fatalf("two rows for one (repository, team) key %+v", k)
		}
		got[k] = [2]int{r.WorkItemsCompleted, r.DeliveryUnits}
	}
	if len(got) != len(summed) {
		t.Fatalf("keys = %v, want the area rows' keys %v", got, summed)
	}
	for k, want := range summed {
		if got[k] != want {
			t.Errorf("key %+v: (completed, units) = %v, want %v (the area rows summed)", k, got[k], want)
		}
	}
	// alpha in the repository: a (13 h: started 10 h before the day, done at 03:00) and b (35 h); open and early items do not
	// count -> completed 2, cycle hours [13, 35], p50 = element len/2 = 35. One value for both areas.
	for _, r := range rows {
		if r.TeamID == "alpha" && r.RepoID != nil {
			if r.WorkItemsCompleted != 2 || r.DeliveryUnits != 4 || math.Abs(r.CycleP50Hours-35) > 1e-9 {
				t.Errorf("alpha row = %+v, want 2 completed, 4 units, p50 35 h", r)
			}
		}
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
