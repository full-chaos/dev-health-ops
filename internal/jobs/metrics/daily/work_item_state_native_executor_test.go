package daily

import (
	"errors"
	"testing"
	"time"
)

func TestNewWorkItemStateExecutorRejectsNilConn(t *testing.T) {
	if _, err := NewWorkItemStateExecutor(nil); !errors.Is(err, errWorkItemStateUnavailable) {
		t.Fatalf("err=%v, want errWorkItemStateUnavailable", err)
	}
}

func mustParseUTC(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse("2006-01-02T15:04:05Z", value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

// TestComputeWorkItemStateDurationsGolden ports
// test_time_in_state_is_bucketed_to_day
// (ops/tests/test_work_item_state_durations_compute.py) byte-for-byte: same
// item, same two transitions, same day/computed_at -- the Python test's own
// documented expectation (00:00-02:00 todo=2h; 02:00-10:00 in_progress=8h;
// 10:00-24:00 done=14h) is the oracle. This is CHAOS-4278's row-identity
// proof for the segment/overlap algorithm itself (the part of this port NOT
// substituted by a table read -- see WorkItemStateExecutor's doc comment for
// the team-attribution substitution and its own, separately measured,
// equivalence evidence).
func TestComputeWorkItemStateDurationsGolden(t *testing.T) {
	day := mustParseUTC(t, "2025-12-18T00:00:00Z")
	start := day
	end := start.Add(24 * time.Hour)
	created := mustParseUTC(t, "2025-12-17T20:00:00Z")
	computedAt := mustParseUTC(t, "2025-12-19T00:00:00Z")

	item := workItemStateWorkItem{
		WorkItemID: "jira:ABC-1",
		Provider:   "jira",
		Status:     "done",
		ProjectKey: "ABC",
		ProjectID:  "1",
		CreatedAt:  created,
	}
	transitions := []workItemStateTransition{
		{
			WorkItemID: "jira:ABC-1",
			OccurredAt: mustParseUTC(t, "2025-12-18T02:00:00Z"),
			FromStatus: "todo",
			ToStatus:   "in_progress",
		},
		{
			WorkItemID: "jira:ABC-1",
			OccurredAt: mustParseUTC(t, "2025-12-18T10:00:00Z"),
			FromStatus: "in_progress",
			ToStatus:   "done",
		},
	}

	rows, missingAttribution := computeWorkItemStateDurationsForRepo(
		day, start, end,
		[]workItemStateWorkItem{item}, transitions,
		map[string]workItemPrimaryAttribution{}, // no attribution row -> unassigned, matches team_resolver=None in the Python test
		computedAt,
	)
	if missingAttribution != 1 {
		t.Fatalf("missingAttribution=%d, want 1 (the one item has no attribution row)", missingAttribution)
	}

	byStatus := make(map[string]workItemStateDailyRow, len(rows))
	for _, row := range rows {
		byStatus[row.Status] = row
	}

	cases := []struct {
		status string
		hours  float64
	}{
		{"todo", 2.0},
		{"in_progress", 8.0},
		{"done", 14.0},
	}
	if len(rows) != len(cases) {
		t.Fatalf("got %d rows, want %d: %#v", len(rows), len(cases), rows)
	}
	for _, tc := range cases {
		row, ok := byStatus[tc.status]
		if !ok {
			t.Fatalf("missing status %q in %#v", tc.status, rows)
		}
		if row.DurationHours != tc.hours {
			t.Fatalf("status %q duration_hours=%v, want %v", tc.status, row.DurationHours, tc.hours)
		}
		if row.TeamID != unassignedTeamID || row.TeamName != unassignedTeamName {
			t.Fatalf("status %q team=(%q,%q), want unassigned", tc.status, row.TeamID, row.TeamName)
		}
		if row.WorkScopeID != "ABC" { // jira + project_key set -> work_scope_id is the project_key
			t.Fatalf("status %q work_scope_id=%q, want ABC", tc.status, row.WorkScopeID)
		}
		if row.ItemsTouched != 1 {
			t.Fatalf("status %q items_touched=%d, want 1", tc.status, row.ItemsTouched)
		}
		if row.AvgWIP != tc.hours/24.0 {
			t.Fatalf("status %q avg_wip=%v, want %v", tc.status, row.AvgWIP, tc.hours/24.0)
		}
	}
}

func TestWorkScopeIDFallbackChain(t *testing.T) {
	cases := []struct {
		name string
		item workItemStateWorkItem
		want string
	}{
		{"jira uses project_key", workItemStateWorkItem{Provider: "jira", ProjectKey: "ABC", ProjectID: "1"}, "ABC"},
		{"jira falls back to project_id when no project_key", workItemStateWorkItem{Provider: "jira", ProjectID: "1"}, "1"},
		{"github uses project_id", workItemStateWorkItem{Provider: "github", ProjectID: "owner/repo", ProjectKey: "ignored"}, "owner/repo"},
		{"linear prefers project_id over project_name/native_team_key", workItemStateWorkItem{Provider: "linear", ProjectID: "proj-1", ProjectName: "Proj", NativeTeamKey: "ENG"}, "proj-1"},
		{"linear falls back to project_name", workItemStateWorkItem{Provider: "linear", ProjectName: "Proj", NativeTeamKey: "ENG"}, "Proj"},
		{"linear falls back to native_team_key", workItemStateWorkItem{Provider: "linear", NativeTeamKey: "ENG"}, "ENG"},
		{"non-jira falls back to project_key last", workItemStateWorkItem{Provider: "github", ProjectKey: "fallback"}, "fallback"},
		{"empty when nothing set", workItemStateWorkItem{Provider: "github"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.item.workScopeID(); got != tc.want {
				t.Fatalf("workScopeID()=%q, want %q", got, tc.want)
			}
		})
	}
}

func TestSegmentWorkItemStatusesNoTransitionsReturnsNil(t *testing.T) {
	created := mustParseUTC(t, "2025-12-17T00:00:00Z")
	computedAt := mustParseUTC(t, "2025-12-19T00:00:00Z")
	if got := segmentWorkItemStatuses(created, nil, "in_progress", nil, computedAt); got != nil {
		t.Fatalf("got %#v, want nil", got)
	}
}

// TestSegmentWorkItemStatusesFromStatusFallback ports the "first.from_status
// is empty -> use item.status" branch (compute_work_item_state_durations.py:85-87).
func TestSegmentWorkItemStatusesFromStatusFallback(t *testing.T) {
	created := mustParseUTC(t, "2025-12-17T00:00:00Z")
	occurredAt := mustParseUTC(t, "2025-12-18T00:00:00Z")
	computedAt := mustParseUTC(t, "2025-12-19T00:00:00Z")
	segments := segmentWorkItemStatuses(created, nil, "backlog", []workItemStateTransition{
		{OccurredAt: occurredAt, FromStatus: "", ToStatus: "todo"},
	}, computedAt)
	if len(segments) != 2 {
		t.Fatalf("got %d segments, want 2: %#v", len(segments), segments)
	}
	if segments[0].status != "backlog" {
		t.Fatalf("segments[0].status=%q, want backlog (item.status fallback)", segments[0].status)
	}
	if segments[1].status != "todo" || !segments[1].end.Equal(computedAt) {
		t.Fatalf("segments[1]=%#v, want status=todo end=%v (open item -> computed_at)", segments[1], computedAt)
	}
}

// TestSegmentWorkItemStatusesCompletedAtWinsOverComputedAt ports the
// "end_of_item = completed_at if present else computed_at" rule.
func TestSegmentWorkItemStatusesCompletedAtWinsOverComputedAt(t *testing.T) {
	created := mustParseUTC(t, "2025-12-17T00:00:00Z")
	completed := mustParseUTC(t, "2025-12-18T06:00:00Z")
	computedAt := mustParseUTC(t, "2025-12-19T00:00:00Z")
	segments := segmentWorkItemStatuses(created, &completed, "todo", []workItemStateTransition{
		{OccurredAt: mustParseUTC(t, "2025-12-18T00:00:00Z"), FromStatus: "backlog", ToStatus: "in_progress"},
	}, computedAt)
	if len(segments) != 2 {
		t.Fatalf("got %d segments, want 2: %#v", len(segments), segments)
	}
	if !segments[1].end.Equal(completed) {
		t.Fatalf("segments[1].end=%v, want completed_at %v", segments[1].end, completed)
	}
}

// TestResolveWorkItemPrimaryTeamDefaultsToUnassigned ports
// normalize_team_id/normalize_team_name's None/empty -> "unassigned"/
// "Unassigned" default for a work item with no primary attribution row.
func TestResolveWorkItemPrimaryTeamDefaultsToUnassigned(t *testing.T) {
	teamID, teamName := resolveWorkItemPrimaryTeam(workItemPrimaryAttribution{})
	if teamID != unassignedTeamID || teamName != unassignedTeamName {
		t.Fatalf("got (%q,%q), want (%q,%q)", teamID, teamName, unassignedTeamID, unassignedTeamName)
	}
}

func TestResolveWorkItemPrimaryTeamPassesThroughAttribution(t *testing.T) {
	teamID, teamName := resolveWorkItemPrimaryTeam(workItemPrimaryAttribution{TeamID: "team-1", TeamName: "Team One"})
	if teamID != "team-1" || teamName != "Team One" {
		t.Fatalf("got (%q,%q), want (team-1, Team One)", teamID, teamName)
	}
}

// TestResolveWorkItemPrimaryTeamStripsWhitespaceLikePython pins the half of
// normalize_team_id/normalize_team_name (providers/teams.py:37-48) that the
// "" check alone does not reach. Python is
//
//	if not team_id or not team_id.strip(): return UNASSIGNED_TEAM_ID
//	return team_id.strip()
//
// so it BOTH treats a whitespace-only value as unassigned AND strips a padded
// one. team_id is a grouping key and part of the sorting key, so an untrimmed
// value splits a team's rows in two -- " team-a " and "team-a" aggregate
// separately and neither total is right.
//
// This is asserted here rather than only through the golden because the frozen
// corpus carries no whitespace-bearing attribution string: a test that never
// receives a padded value cannot tell the two normalizers apart.
func TestResolveWorkItemPrimaryTeamStripsWhitespaceLikePython(t *testing.T) {
	for _, testCase := range []struct {
		name             string
		attribution      workItemPrimaryAttribution
		wantID, wantName string
	}{
		{"padded", workItemPrimaryAttribution{TeamID: " team-a ", TeamName: " Core "}, "team-a", "Core"},
		{"whitespace only", workItemPrimaryAttribution{TeamID: "   ", TeamName: "\t\n"}, unassignedTeamID, unassignedTeamName},
		{"tab padded", workItemPrimaryAttribution{TeamID: "\tteam-b\t", TeamName: "\tPlatform\t"}, "team-b", "Platform"},
		// U+001C-U+001F are whitespace to CPython's str.strip() but NOT to Go's
		// unicode.IsSpace (Unicode White_Space excludes them), so
		// strings.TrimSpace leaves them in place. Without these two cases the
		// delegation looks Python-equivalent while a separator-only team_id
		// stays a live grouping key instead of becoming "unassigned".
		{"separator padded", workItemPrimaryAttribution{TeamID: "\x1cteam-c\x1f", TeamName: "\x1eData\x1d"}, "team-c", "Data"},
		{"separator only", workItemPrimaryAttribution{TeamID: "\x1c\x1d\x1e\x1f", TeamName: "\x1c"}, unassignedTeamID, unassignedTeamName},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			teamID, teamName := resolveWorkItemPrimaryTeam(testCase.attribution)
			if teamID != testCase.wantID || teamName != testCase.wantName {
				t.Fatalf("resolveWorkItemPrimaryTeam(%#v) = (%q,%q), want (%q,%q) -- Python strips and treats whitespace-only as unassigned",
					testCase.attribution, teamID, teamName, testCase.wantID, testCase.wantName)
			}
		})
	}
}

// TestComputeWorkItemStateDurationsItemsTouchedDedupesRepeatVisits ports the
// items_touched set semantics: an item revisiting the SAME status twice in
// one day (e.g. in_progress -> blocked -> in_progress) counts once as
// touched for that status, even though it contributes two separate segments'
// worth of hours.
func TestComputeWorkItemStateDurationsItemsTouchedDedupesRepeatVisits(t *testing.T) {
	day := mustParseUTC(t, "2025-12-18T00:00:00Z")
	start := day
	end := start.Add(24 * time.Hour)
	created := mustParseUTC(t, "2025-12-18T00:00:00Z")
	computedAt := mustParseUTC(t, "2025-12-19T00:00:00Z")

	item := workItemStateWorkItem{WorkItemID: "gh:o/r#1", Provider: "github", Status: "in_progress", ProjectID: "o/r", CreatedAt: created}
	transitions := []workItemStateTransition{
		{WorkItemID: item.WorkItemID, OccurredAt: mustParseUTC(t, "2025-12-18T04:00:00Z"), FromStatus: "in_progress", ToStatus: "blocked"},
		{WorkItemID: item.WorkItemID, OccurredAt: mustParseUTC(t, "2025-12-18T06:00:00Z"), FromStatus: "blocked", ToStatus: "in_progress"},
	}

	rows, missingAttribution := computeWorkItemStateDurationsForRepo(day, start, end, []workItemStateWorkItem{item}, transitions, nil, computedAt)
	if missingAttribution != 1 {
		t.Fatalf("missingAttribution=%d, want 1", missingAttribution)
	}

	var inProgress *workItemStateDailyRow
	for index := range rows {
		if rows[index].Status == "in_progress" {
			inProgress = &rows[index]
		}
	}
	if inProgress == nil {
		t.Fatalf("no in_progress row in %#v", rows)
	}
	if inProgress.ItemsTouched != 1 {
		t.Fatalf("items_touched=%d, want 1 (one item, two in_progress segments)", inProgress.ItemsTouched)
	}
	// 00:00-04:00 (4h) + 06:00-19:00(next day 00:00) (18h) = 22h.
	if inProgress.DurationHours != 22.0 {
		t.Fatalf("duration_hours=%v, want 22", inProgress.DurationHours)
	}
}

// TestComputeWorkItemStateDurationsSkipsItemsWithNoTransitions ports "if
// not item_transitions: continue" -- an item with zero transitions
// contributes no rows at all, even though it was loaded for the partition.
func TestComputeWorkItemStateDurationsSkipsItemsWithNoTransitions(t *testing.T) {
	day := mustParseUTC(t, "2025-12-18T00:00:00Z")
	item := workItemStateWorkItem{WorkItemID: "gh:o/r#2", Provider: "github", Status: "todo", ProjectID: "o/r", CreatedAt: day}
	rows, missingAttribution := computeWorkItemStateDurationsForRepo(day, day, day.Add(24*time.Hour), []workItemStateWorkItem{item}, nil, nil, day)
	if len(rows) != 0 {
		t.Fatalf("got %#v, want no rows", rows)
	}
	if missingAttribution != 0 {
		t.Fatalf("missingAttribution=%d, want 0 (item was skipped for having no transitions, never reached)", missingAttribution)
	}
}

// TestComputeWorkItemStateDurationsMissingAttributionCountsOnlyProcessedItems
// ports the CHAOS-4278 guard counter's own contract: an item with NO
// transitions is skipped before attribution is even looked up (matches
// Python's "if not item_transitions: continue"), so it must NOT inflate the
// missing-attribution count; an item WITH transitions but no attribution row
// must.
func TestComputeWorkItemStateDurationsMissingAttributionCountsOnlyProcessedItems(t *testing.T) {
	day := mustParseUTC(t, "2025-12-18T00:00:00Z")
	start := day
	end := start.Add(24 * time.Hour)
	computedAt := mustParseUTC(t, "2025-12-19T00:00:00Z")

	withTransitions := workItemStateWorkItem{WorkItemID: "gh:o/r#1", Provider: "github", Status: "todo", ProjectID: "o/r", CreatedAt: day}
	withoutTransitions := workItemStateWorkItem{WorkItemID: "gh:o/r#2", Provider: "github", Status: "todo", ProjectID: "o/r", CreatedAt: day}
	transitions := []workItemStateTransition{
		{WorkItemID: withTransitions.WorkItemID, OccurredAt: mustParseUTC(t, "2025-12-18T02:00:00Z"), FromStatus: "todo", ToStatus: "in_progress"},
	}

	_, missingAttribution := computeWorkItemStateDurationsForRepo(
		day, start, end,
		[]workItemStateWorkItem{withTransitions, withoutTransitions},
		transitions,
		map[string]workItemPrimaryAttribution{}, // neither item has an attribution row
		computedAt,
	)
	if missingAttribution != 1 {
		t.Fatalf("missingAttribution=%d, want 1 (only withTransitions was processed)", missingAttribution)
	}
}
