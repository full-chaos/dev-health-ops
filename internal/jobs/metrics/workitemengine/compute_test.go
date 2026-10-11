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
		// Completed on the day, 26 hours after its start.
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
			CreatedCount: 1, CompletedCount: 1, ActiveCount: 2, CycleP50Hours: 26, CycleP90Hours: 26},
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
