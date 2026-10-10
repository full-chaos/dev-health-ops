package workitemmetrics

import (
	"testing"
	"time"
)

func TestOncePerProviderAndIDKeepsTheNewestVersionAndTheLowerRepoOnATie(t *testing.T) {
	t0 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	versions := []ItemVersion{
		{Provider: "github", WorkItemID: "x", RepoID: "b", LastSynced: t0},
		{Provider: "jira", WorkItemID: "x", RepoID: "b", LastSynced: t0},   // another provider: its own item
		{Provider: "github", WorkItemID: "x", RepoID: "a", LastSynced: t0}, // tie: the lower repository id wins
		{Provider: "github", WorkItemID: "y", RepoID: "a", LastSynced: t0},
		{Provider: "github", WorkItemID: "y", RepoID: "b", LastSynced: t0.Add(1)}, // newer wins
	}
	kept, duplicates := OncePerProviderAndID(versions)
	if duplicates != 2 || len(kept) != 3 || kept[0] != 2 || kept[1] != 1 || kept[2] != 4 {
		t.Fatalf("kept = %v duplicates = %d, want [2 1 4] and 2", kept, duplicates)
	}
}

func TestPassesDayPredicate(t *testing.T) {
	day := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	end := day.AddDate(0, 0, 1)
	before := day.Add(-time.Hour)
	within := day.Add(time.Hour)
	for _, tc := range []struct {
		name      string
		status    string
		created   time.Time
		completed *time.Time
		want      bool
	}{
		{"open, created before the end", "in_progress", before, nil, true},
		{"created at the end of the day", "in_progress", end, nil, false},
		{"done within the day", "done", before, &within, true},
		{"done before the day", "done", before, &before, false},
		{"done with no completion time", "done", before, nil, false},
	} {
		if got := PassesDayPredicate(tc.status, tc.created, tc.completed, day, end); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}
