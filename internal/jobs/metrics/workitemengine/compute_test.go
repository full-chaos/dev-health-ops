package workitemengine

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

// These tests pin the rules of the two computations at the package that now
// holds them. The frozen-production cases and the engine tests of
// internal/providersync run the same functions through the sync deriver's
// adapter; they are the regression proof that the move changed nothing.

type labelNormalizer struct{}

func (labelNormalizer) NormalizeType(provider, typeRaw string, labels []string) string {
	if len(labels) > 0 {
		return provider + ":" + labels[0]
	}
	return provider + ":" + typeRaw
}

func timePointer(value time.Time) *time.Time { return &value }

func teams(values ...string) TeamResolver {
	return func(index int) *string {
		if values[index] == "" {
			return nil
		}
		value := values[index]
		return &value
	}
}

func TestComputeIssueTypeMetricsDailyOpensABucketForEveryItem(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	end := day.AddDate(0, 0, 1)
	repo := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	nilRepo := uuid.Nil
	items := []Item{
		// Completed on the day, 26 hours after its start and 82 hours after its creation.
		{WorkItemID: "1", Provider: "github", Type: "issue", Labels: []string{"bug"}, RepoID: &repo,
			CreatedAt: day.AddDate(0, 0, -3), StartedAt: timePointer(day.Add(-16 * time.Hour)),
			CompletedAt: timePointer(day.Add(10 * time.Hour))},
		// Created on the day, open: the same key as item 1.
		{WorkItemID: "2", Provider: "github", Type: "issue", Labels: []string{"bug"}, RepoID: &repo,
			CreatedAt: day.Add(8 * time.Hour)},
		// Created AFTER the day: its key still gets a row, all zeros.
		{WorkItemID: "3", Provider: "github", Type: "task", RepoID: &repo,
			CreatedAt: end.Add(time.Hour)},
		// Completed before the day, no repository, a team with spaces around it.
		{WorkItemID: "4", Provider: "linear", Type: "task", RepoID: &nilRepo,
			CreatedAt: day.AddDate(0, 0, -9), CompletedAt: timePointer(day.AddDate(0, 0, -2))},
	}
	got := ComputeIssueTypeMetricsDaily(items, day, end, teams("", "", "", " team-a "), labelNormalizer{})
	want := []IssueTypeMetricsDailyRow{
		{RepoID: &repo, Provider: "github", TeamID: "unassigned", IssueTypeNorm: "github:bug",
			CreatedCount: 1, CompletedCount: 1, ActiveCount: 2, CycleP50Hours: 26, CycleP90Hours: 26,
			LeadP50Hours: 82},
		{RepoID: &repo, Provider: "github", TeamID: "unassigned", IssueTypeNorm: "github:task"},
		{RepoID: nil, Provider: "linear", TeamID: "team-a", IssueTypeNorm: "linear:task"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("issue type rows:\n got %+v\nwant %+v", got, want)
	}
}

func TestComputeWorkItemCompletionDailyCountsTheDaysCompletions(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	end := day.AddDate(0, 0, 1)
	repo := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	points := func(value float64) *float64 { return &value }
	items := []Item{
		// Completed on the day: 2.9 points count as 2 units.
		{WorkItemID: "1", Provider: "github", Labels: []string{"quality"}, RepoID: &repo,
			CreatedAt: day.AddDate(0, 0, -3), StartedAt: timePointer(day.Add(-14 * time.Hour)),
			CompletedAt: timePointer(day.Add(10 * time.Hour)), StoryPoints: points(2.9)},
		// Completed on the day, zero points count as one unit; no team.
		{WorkItemID: "2", Provider: "github", Labels: []string{"quality"}, RepoID: &repo,
			CreatedAt: day.AddDate(0, 0, -1), CompletedAt: timePointer(day.Add(12 * time.Hour)), StoryPoints: points(0)},
		// Open on the day: not counted.
		{WorkItemID: "3", Provider: "github", Labels: []string{"security"}, RepoID: &repo,
			CreatedAt: day.Add(time.Hour)},
		// Completed before the day and created after it: neither is active.
		{WorkItemID: "4", Provider: "github", RepoID: &repo,
			CreatedAt: day.AddDate(0, 0, -9), CompletedAt: timePointer(day.Add(-time.Second))},
		{WorkItemID: "5", Provider: "github", RepoID: &repo, CreatedAt: end},
	}
	rows, err := ComputeWorkItemCompletionDaily(items, day, end, teams("unassigned", "", "team-a", "", ""))
	if err != nil {
		t.Fatal(err)
	}
	want := []WorkItemCompletionDailyRow{{
		RepoID: &repo, TeamID: "", DeliveryUnits: 3, WorkItemsCompleted: 2, CycleP50Hours: 24,
	}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("completion rows:\n got %+v\nwant %+v", rows, want)
	}
}

// Lead time is completed_at minus created_at, over the items completed on the day: the upper median, like the cycle
// p50. Python never computed it (it wrote 0.0), so there is no oracle: the expected values below are worked by hand
// from the timestamps. The rule does not look at the provider, so every provider runs the same case.
func TestComputeIssueTypeMetricsDailyLeadTimeIsTheUpperMedianOfCompletedMinusCreated(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	end := day.AddDate(0, 0, 1)
	for _, provider := range []string{"jira", "gitlab", "github", "linear"} {
		t.Run(provider, func(t *testing.T) {
			completedAt := func(hour int) *time.Time { return timePointer(day.Add(time.Duration(hour) * time.Hour)) }
			items := []Item{
				// 10h, 34h and 106h: the upper median of three is 34h.
				{WorkItemID: "1", Provider: provider, Type: "bug", CreatedAt: day, CompletedAt: completedAt(10)},
				{WorkItemID: "2", Provider: provider, Type: "bug", CreatedAt: day.Add(-24 * time.Hour), CompletedAt: completedAt(10)},
				{WorkItemID: "3", Provider: provider, Type: "bug", CreatedAt: day.Add(-96 * time.Hour), CompletedAt: completedAt(10)},
				// Completed on the day but created later: no lead time, left out of the p50 (not taken as 0).
				{WorkItemID: "4", Provider: provider, Type: "bug", CreatedAt: day.Add(20 * time.Hour), CompletedAt: completedAt(10)},
				// No creation time at all, and the Unix epoch the store holds for it: left out as well.
				{WorkItemID: "5b", Provider: provider, Type: "bug", CreatedAt: time.Unix(0, 0).UTC(), CompletedAt: completedAt(10)},
				{WorkItemID: "5", Provider: provider, Type: "bug", CompletedAt: completedAt(10)},
				// Completed on another day: its lead time belongs to that day.
				{WorkItemID: "6", Provider: provider, Type: "bug", CreatedAt: day.Add(-500 * time.Hour),
					CompletedAt: timePointer(day.Add(-time.Hour))},
				// Created and completed at the same instant: no lead time, so a stored 0 always means none.
				{WorkItemID: "6b", Provider: provider, Type: "bug", CreatedAt: day.Add(3 * time.Hour), CompletedAt: completedAt(3)},
				{WorkItemID: "6c", Provider: provider, Type: "bug", CreatedAt: day.Add(4 * time.Hour), CompletedAt: completedAt(4)},
				// Open: no lead time.
				{WorkItemID: "7", Provider: provider, Type: "bug", CreatedAt: day.Add(-300 * time.Hour)},
			}
			rows := ComputeIssueTypeMetricsDaily(items, day, end, teams("", "", "", "", "", "", "", "", "", ""), labelNormalizer{})
			if len(rows) != 1 {
				t.Fatalf("rows = %+v, want one", rows)
			}
			if rows[0].LeadP50Hours != 34 {
				t.Fatalf("lead_p50_hours = %v, want 34 (upper median of 10, 34, 106)", rows[0].LeadP50Hours)
			}
		})
	}
}

func TestComputeIssueTypeMetricsDailyLeadTimeIsZeroWhenNothingCompletedHasOne(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	items := []Item{
		{WorkItemID: "1", Provider: "github", Type: "bug", CreatedAt: day.Add(-time.Hour)},
		{WorkItemID: "2", Provider: "github", Type: "bug", CreatedAt: day.Add(5 * time.Hour), CompletedAt: timePointer(day.Add(time.Hour))},
	}
	rows := ComputeIssueTypeMetricsDaily(items, day, day.AddDate(0, 0, 1), teams("", ""), labelNormalizer{})
	if len(rows) != 1 || rows[0].CompletedCount != 1 || rows[0].LeadP50Hours != 0 {
		t.Fatalf("rows = %+v, want one row that counts 1 completed item and has no lead time", rows)
	}
}

func TestLeadTimeInputsCountsCompletedAndExcludedItems(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	items := []Item{
		{WorkItemID: "1", CreatedAt: day.Add(-time.Hour), CompletedAt: timePointer(day.Add(time.Hour))},
		{WorkItemID: "2", CompletedAt: timePointer(day.Add(time.Hour))},
		{WorkItemID: "3", CreatedAt: day.Add(5 * time.Hour), CompletedAt: timePointer(day.Add(time.Hour))},
		{WorkItemID: "4", CreatedAt: day.Add(-time.Hour)},
		{WorkItemID: "5", CreatedAt: day.Add(-48 * time.Hour), CompletedAt: timePointer(day.Add(-time.Hour))},
		{WorkItemID: "6", CreatedAt: time.Unix(0, 0).UTC(), CompletedAt: timePointer(day.Add(time.Hour))},
		{WorkItemID: "7", CreatedAt: day.Add(time.Hour), CompletedAt: timePointer(day.Add(time.Hour))},
	}
	if completed, excluded := LeadTimeInputs(items, day, day.AddDate(0, 0, 1)); completed != 5 || excluded != 4 {
		t.Fatalf("completed=%d excluded=%d, want 5 and 4", completed, excluded)
	}
}
