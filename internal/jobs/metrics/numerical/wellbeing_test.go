package numerical

import (
	"testing"
	"time"
)

type stubRepoResolver map[string][2]string

func (resolver stubRepoResolver) ResolveRepo(repoName string) (string, string) {
	if repoName == "" {
		return "", ""
	}
	pair, ok := resolver[repoName]
	if !ok {
		return "", ""
	}
	return pair[0], pair[1]
}

type stubMemberResolver map[string][2]string

func (resolver stubMemberResolver) ResolveMember(identity string) (string, string) {
	if identity == "" {
		return "", ""
	}
	pair, ok := resolver[identity]
	if !ok {
		return "", ""
	}
	return pair[0], pair[1]
}

func mustLoadLocation(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("load location %q: %v", name, err)
	}
	return loc
}

func TestComputeTeamWellbeingRepoPatternWinsOverMembership(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC) // Monday
	commits := []Commit{
		{
			RepoID:        "repo-1",
			AuthorEmail:   "dev@example.com",
			CommitterWhen: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC), // business hours, weekday
		},
	}
	repoNames := map[string]string{"repo-1": "org/service-a"}
	repoResolver := stubRepoResolver{"org/service-a": {"team-repo", "Repo Team"}}
	memberResolver := stubMemberResolver{"dev@example.com": {"team-member", "Member Team"}}

	got := ComputeTeamWellbeing(day, commits, repoNames, repoResolver, memberResolver, time.UTC, 9, 17)

	if len(got) != 1 || got[0].TeamID != "team-repo" {
		t.Fatalf("expected repo-pattern resolution to win, got %#v", got)
	}
	if got[0].CommitsCount != 1 || got[0].AfterHoursCommitsCount != 0 || got[0].WeekendCommitsCount != 0 {
		t.Fatalf("unexpected bucket counts: %#v", got[0])
	}
}

func TestComputeTeamWellbeingFallsBackToMembership(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	commits := []Commit{
		{
			RepoID:        "repo-unmapped",
			AuthorEmail:   "dev@example.com",
			CommitterWhen: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC),
		},
	}
	repoResolver := stubRepoResolver{}
	memberResolver := stubMemberResolver{"dev@example.com": {"team-member", "Member Team"}}

	got := ComputeTeamWellbeing(day, commits, nil, repoResolver, memberResolver, time.UTC, 9, 17)

	if len(got) != 1 || got[0].TeamID != "team-member" || got[0].TeamName != "Member Team" {
		t.Fatalf("expected membership fallback, got %#v", got)
	}
}

func TestComputeTeamWellbeingUnknownBucket(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	commits := []Commit{
		{RepoID: "repo-x", AuthorEmail: "nobody@example.com", CommitterWhen: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)},
	}
	got := ComputeTeamWellbeing(day, commits, nil, stubRepoResolver{}, stubMemberResolver{}, time.UTC, 9, 17)
	if len(got) != 1 || got[0].TeamID != UnknownTeamID || got[0].TeamName != UnknownTeamName {
		t.Fatalf("expected unassigned bucket, got %#v", got)
	}
}

func TestComputeTeamWellbeingWeekendAndAfterHoursAreMutuallyExclusive(t *testing.T) {
	day := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC) // Saturday
	commits := []Commit{
		// Saturday, 2am local -- weekend AND would otherwise be after-hours.
		// Must count only as weekend.
		{RepoID: "repo-x", AuthorEmail: "a@example.com", CommitterWhen: time.Date(2026, 8, 22, 2, 0, 0, 0, time.UTC)},
		// Sunday 23:00 -- still weekend, not after-hours.
		{RepoID: "repo-x", AuthorEmail: "a@example.com", CommitterWhen: time.Date(2026, 8, 22, 23, 0, 0, 0, time.UTC)},
	}
	got := ComputeTeamWellbeing(day, commits, nil, stubRepoResolver{}, stubMemberResolver{}, time.UTC, 9, 17)
	if len(got) != 1 {
		t.Fatalf("expected one team bucket, got %#v", got)
	}
	if got[0].WeekendCommitsCount != 2 || got[0].AfterHoursCommitsCount != 0 {
		t.Fatalf("expected both commits bucketed as weekend, got %#v", got[0])
	}
	if got[0].WeekendCommitRatio != 1.0 || got[0].AfterHoursCommitRatio != 0.0 {
		t.Fatalf("unexpected ratios: %#v", got[0])
	}
}

func TestComputeTeamWellbeingAfterHoursWeekday(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC) // Monday
	commits := []Commit{
		// 7am local, before businessHoursStart=9 -- after-hours.
		{RepoID: "repo-x", AuthorEmail: "a@example.com", CommitterWhen: time.Date(2026, 8, 24, 7, 0, 0, 0, time.UTC)},
		// 18:00 local, at/after businessHoursEnd=17 -- after-hours.
		{RepoID: "repo-x", AuthorEmail: "a@example.com", CommitterWhen: time.Date(2026, 8, 24, 18, 0, 0, 0, time.UTC)},
		// noon local -- business hours, neither bucket.
		{RepoID: "repo-x", AuthorEmail: "a@example.com", CommitterWhen: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)},
	}
	got := ComputeTeamWellbeing(day, commits, nil, stubRepoResolver{}, stubMemberResolver{}, time.UTC, 9, 17)
	if len(got) != 1 {
		t.Fatalf("expected one team bucket, got %#v", got)
	}
	if got[0].CommitsCount != 3 || got[0].AfterHoursCommitsCount != 2 || got[0].WeekendCommitsCount != 0 {
		t.Fatalf("unexpected bucket counts: %#v", got[0])
	}
}

func TestComputeTeamWellbeingFiltersOutsideDayWindow(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	commits := []Commit{
		{RepoID: "repo-x", AuthorEmail: "a@example.com", CommitterWhen: time.Date(2026, 8, 23, 23, 59, 59, 0, time.UTC)},
		{RepoID: "repo-x", AuthorEmail: "a@example.com", CommitterWhen: time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)},
	}
	got := ComputeTeamWellbeing(day, commits, nil, stubRepoResolver{}, stubMemberResolver{}, time.UTC, 9, 17)
	if len(got) != 0 {
		t.Fatalf("expected no rows for commits outside the day window, got %#v", got)
	}
}

func TestComputeTeamWellbeingSortedByTeamID(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	commits := []Commit{
		{RepoID: "repo-z", AuthorEmail: "z@example.com", CommitterWhen: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)},
		{RepoID: "repo-a", AuthorEmail: "a@example.com", CommitterWhen: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)},
	}
	repoResolver := stubRepoResolver{}
	memberResolver := stubMemberResolver{
		"z@example.com": {"team-z", "Team Z"},
		"a@example.com": {"team-a", "Team A"},
	}
	got := ComputeTeamWellbeing(day, commits, nil, repoResolver, memberResolver, time.UTC, 9, 17)
	if len(got) != 2 || got[0].TeamID != "team-a" || got[1].TeamID != "team-z" {
		t.Fatalf("expected rows sorted by team_id, got %#v", got)
	}
}

func TestComputeTeamWellbeingBusinessTimezoneConversion(t *testing.T) {
	ny := mustLoadLocation(t, "America/New_York")
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC) // Monday UTC
	// 2026-08-24 03:00 UTC == 2026-08-23 23:00 America/New_York (EDT, UTC-4)
	// -- Sunday locally, so this must bucket as weekend even though the UTC
	// calendar day it falls in (used for the window filter) is Monday.
	commits := []Commit{
		{RepoID: "repo-x", AuthorEmail: "a@example.com", CommitterWhen: time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)},
	}
	got := ComputeTeamWellbeing(day, commits, nil, stubRepoResolver{}, stubMemberResolver{}, ny, 9, 17)
	if len(got) != 1 || got[0].WeekendCommitsCount != 1 {
		t.Fatalf("expected local-timezone weekend bucketing, got %#v", got)
	}
}

func TestNormalizeGitIdentityFallback(t *testing.T) {
	cases := []struct {
		email, name, want string
	}{
		{"dev@example.com", "Dev Name", "dev@example.com"},
		{"", "Dev Name", "Dev Name"},
		{"  ", "Dev Name", "Dev Name"},
		{"", "", "unknown"},
		{"  ", "  ", "unknown"},
	}
	for _, tc := range cases {
		if got := normalizeGitIdentity(tc.email, tc.name); got != tc.want {
			t.Errorf("normalizeGitIdentity(%q, %q) = %q, want %q", tc.email, tc.name, got, tc.want)
		}
	}
}
