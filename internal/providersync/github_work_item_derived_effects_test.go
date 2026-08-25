package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type inertGitHubDerivedConn struct{ driver.Conn }

func TestGitHubEstimateCoverageWrite_rejectsForeignTenantBeforeLease(t *testing.T) {
	// Given: a foreign-tenant effect and a connection/lease that would expose a
	// write if row tenancy validation were bypassed.
	row := githubDerivedEffectCoverageRow()
	row.OrgID = "org-other"
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch(
		githubEstimateCoverageDestination, EffectReadbackRequired,
		[]json.RawMessage{raw},
	)
	if err != nil {
		t.Fatal(err)
	}
	identity := GitHubWorkItemEffectIdentity{
		OrgID: "org-acme", Provider: "github", Destination: githubEstimateCoverageDestination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	leaseCalls := 0
	sink := GitHubEstimateCoverageClickHouseEffects{
		Conn: &inertGitHubDerivedConn{},
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error {
			leaseCalls++
			return errors.New("lease reached")
		}),
	}

	// When: the public write entrypoint receives the effect.
	err = sink.WriteGitHubWorkItemEffect(context.Background(), identity, effect)

	// Then: it rejects the payload before using the lease or connection.
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("write error = %v, want ErrInvalidConfiguration", err)
	}
	if leaseCalls != 0 {
		t.Fatalf("lease assertions = %d, want 0", leaseCalls)
	}
}

func TestValidateGitHubWorkItemDerivedEffect_allowsForeignTenantForReadback(t *testing.T) {
	// Given: a persisted effect row from another tenant and an identity for the
	// tenant whose ClickHouse fence will be queried.
	row := githubDerivedEffectCoverageRow()
	row.OrgID = "org-other"
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch(
		githubEstimateCoverageDestination, EffectReadbackRequired,
		[]json.RawMessage{raw},
	)
	if err != nil {
		t.Fatal(err)
	}
	identity := GitHubWorkItemEffectIdentity{
		OrgID: "org-acme", Provider: "github", Destination: githubEstimateCoverageDestination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}

	// When: the effect is decoded for a readback.
	_, err = validateGitHubWorkItemDerivedEffect[githubEstimateCoverageMetricsDailyRow](
		identity, effect, githubEstimateCoverageDestination,
	)

	// Then: the SQL tenant fence gets the chance to return EffectAbsent.
	if err != nil {
		t.Fatalf("readback validation rejected a foreign tenant row: %v", err)
	}
}

func TestGitHubWorkItemDerivedWriteRows_rejectsForeignTenant(t *testing.T) {
	// Given: a decoded effect row whose tenant does not match the write identity.
	row := githubDerivedEffectCoverageRow()
	row.OrgID = "org-other"
	identity := GitHubWorkItemEffectIdentity{OrgID: "org-acme", Provider: "github"}

	// When: rows are checked before a write.
	valid := validGitHubWorkItemDerivedWriteRows([]githubEstimateCoverageMetricsDailyRow{row}, identity)

	// Then: the write path rejects the cross-tenant payload.
	if valid {
		t.Fatal("foreign tenant row passed write validation")
	}
}

// These are fast, synthetic-data unit tests of the three comparators' own
// logic. Each clause gets a case that exercises ONLY that clause, so a
// mutation to any one of them is caught by a case isolating it rather than by
// coincidence from a case exercising several at once.
//
// The ordering property matters most: `found != 1` must be tested BEFORE
// anything reads the scanned row. If a stale-version branch ran first, a table
// holding duplicates PLUS a stale row would answer "absent", the committer
// would rewrite, and the rewrite would add another duplicate -- an infinite
// loop, and the second blocking finding on PR #1535.

var (
	githubDerivedEffectComputedAt = time.Date(2026, 8, 5, 0, 30, 0, 0, time.UTC)
	githubDerivedEffectStaleAt    = time.Date(2026, 8, 4, 0, 30, 0, 0, time.UTC)
	githubDerivedEffectNewerAt    = time.Date(2026, 8, 6, 0, 30, 0, 0, time.UTC)
)

func githubDerivedEffectCoverageRow() githubEstimateCoverageMetricsDailyRow {
	teamID, teamName := "t1", "Team One"
	ratio := 0.5
	return githubEstimateCoverageMetricsDailyRow{
		Day: "2026-08-04", Provider: "github", WorkScopeID: "acme/api",
		TeamID: &teamID, TeamName: &teamName, EstimatedCount: 1,
		UnestimatedCount: 1, BacklogSize: 2, Ratio: &ratio,
		ComputedAt: githubDerivedEffectComputedAt, OrgID: "org-acme",
	}
}

func TestCompareGitHubEstimateCoverageVersionClauseCoverage(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*githubEstimateCoverageMetricsDailyRow)
		found  int
		want   EffectInspection
	}{
		{"identical single row is exact", nil, 1, EffectExact},
		{"absent when nothing read back", nil, 0, EffectAbsent},
		{
			// THE WEDGE CASE. A byte-identical recompute where only computed_at
			// moved -- the steady state for a producer re-run per backfill day.
			// Values all agree and the persisted stamp is NEWER. A two-way
			// `!Equal -> Absent` answers Absent here, the committer rewrites the
			// OLDER generation, the store keeps the newer one, and the readback
			// answers Absent forever. It must be Conflict.
			name:   "newer persisted version with identical values conflicts, never absent",
			mutate: func(row *githubEstimateCoverageMetricsDailyRow) { row.ComputedAt = githubDerivedEffectNewerAt },
			found:  1, want: EffectConflict,
		},
		{
			// The load-bearing ordering case: duplicates AND a stale version.
			// A comparator that checked staleness first would answer Absent
			// and rewrite forever.
			name:   "duplicates with a stale version conflict, never absent",
			mutate: func(row *githubEstimateCoverageMetricsDailyRow) { row.ComputedAt = githubDerivedEffectStaleAt },
			found:  2, want: EffectConflict,
		},
		{"duplicates alone conflict", nil, 2, EffectConflict},
		{
			name:   "older version reads as absent so the writer replaces it",
			mutate: func(row *githubEstimateCoverageMetricsDailyRow) { row.ComputedAt = githubDerivedEffectStaleAt },
			found:  1, want: EffectAbsent,
		},
		{
			name:   "different estimated_count conflicts",
			mutate: func(row *githubEstimateCoverageMetricsDailyRow) { row.EstimatedCount = 9 },
			found:  1, want: EffectConflict,
		},
		{
			name:   "different unestimated_count conflicts",
			mutate: func(row *githubEstimateCoverageMetricsDailyRow) { row.UnestimatedCount = 9 },
			found:  1, want: EffectConflict,
		},
		{
			name:   "different backlog_size conflicts",
			mutate: func(row *githubEstimateCoverageMetricsDailyRow) { row.BacklogSize = 9 },
			found:  1, want: EffectConflict,
		},
		{
			name:   "different day conflicts",
			mutate: func(row *githubEstimateCoverageMetricsDailyRow) { row.Day = "2026-08-05" },
			found:  1, want: EffectConflict,
		},
		{
			name:   "different work_scope_id conflicts",
			mutate: func(row *githubEstimateCoverageMetricsDailyRow) { row.WorkScopeID = "other/repo" },
			found:  1, want: EffectConflict,
		},
		{
			name:   "different provider conflicts",
			mutate: func(row *githubEstimateCoverageMetricsDailyRow) { row.Provider = "gitlab" },
			found:  1, want: EffectConflict,
		},
		{
			// A NULL ratio and a 0.0 ratio are different persisted facts: the
			// null arm is what an all-zero backlog produces.
			name:   "null ratio against a present ratio conflicts",
			mutate: func(row *githubEstimateCoverageMetricsDailyRow) { row.Ratio = nil },
			found:  1, want: EffectConflict,
		},
		{
			name: "different ratio value conflicts",
			mutate: func(row *githubEstimateCoverageMetricsDailyRow) {
				other := 0.75
				row.Ratio = &other
			},
			found: 1, want: EffectConflict,
		},
		{
			name:   "null team_id against a present team_id conflicts",
			mutate: func(row *githubEstimateCoverageMetricsDailyRow) { row.TeamID = nil },
			found:  1, want: EffectConflict,
		},
		{
			name:   "different team_name conflicts",
			mutate: func(row *githubEstimateCoverageMetricsDailyRow) { row.TeamName = stringPointer("Team Uno") },
			found:  1, want: EffectConflict,
		},
		{
			name:   "foreign tenant conflicts",
			mutate: func(row *githubEstimateCoverageMetricsDailyRow) { row.OrgID = "org-other" },
			found:  1, want: EffectConflict,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expected := githubDerivedEffectCoverageRow()
			actual := githubDerivedEffectCoverageRow()
			if tt.mutate != nil {
				tt.mutate(&actual)
			}
			got := compareGitHubEstimateCoverageVersion(expected, actual, tt.found, "org-acme")
			if got != tt.want {
				t.Fatalf("compareGitHubEstimateCoverageVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func githubDerivedEffectStateDurationRow() githubWorkItemStateDurationDailyRow {
	return githubWorkItemStateDurationDailyRow{
		Day: "2026-08-04", Provider: "github", WorkScopeID: "acme/api",
		TeamID: "unassigned", TeamName: "Unassigned", Status: "in_progress",
		DurationHours: 18, ItemsTouched: 2,
		ComputedAt: githubDerivedEffectComputedAt, AvgWIP: 0.75, OrgID: "org-acme",
	}
}

func TestCompareGitHubWorkItemStateDurationVersionClauseCoverage(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*githubWorkItemStateDurationDailyRow)
		found  int
		want   EffectInspection
	}{
		{"identical grouped row is exact", nil, 1, EffectExact},
		{"absent when the group is empty", nil, 0, EffectAbsent},
		{
			// THE WEDGE CASE. A byte-identical recompute where only computed_at
			// moved -- the steady state for a producer re-run per backfill day.
			// Values all agree and the persisted stamp is NEWER. A two-way
			// `!Equal -> Absent` answers Absent here, the committer rewrites the
			// OLDER generation, the store keeps the newer one, and the readback
			// answers Absent forever. It must be Conflict.
			name:   "newer persisted version with identical values conflicts, never absent",
			mutate: func(row *githubWorkItemStateDurationDailyRow) { row.ComputedAt = githubDerivedEffectNewerAt },
			found:  1, want: EffectConflict,
		},
		{
			// GROUP BY the full natural key cannot return two rows, so this is
			// a wiring defect. It must never be answered with a rewrite.
			name:   "more than one group conflicts even when stale",
			mutate: func(row *githubWorkItemStateDurationDailyRow) { row.ComputedAt = githubDerivedEffectStaleAt },
			found:  2, want: EffectConflict,
		},
		{
			name:   "older argMax version reads as absent",
			mutate: func(row *githubWorkItemStateDurationDailyRow) { row.ComputedAt = githubDerivedEffectStaleAt },
			found:  1, want: EffectAbsent,
		},
		{
			name:   "different duration_hours conflicts",
			mutate: func(row *githubWorkItemStateDurationDailyRow) { row.DurationHours = 17 },
			found:  1, want: EffectConflict,
		},
		{
			name:   "different items_touched conflicts",
			mutate: func(row *githubWorkItemStateDurationDailyRow) { row.ItemsTouched = 1 },
			found:  1, want: EffectConflict,
		},
		{
			// avg_wip is a stored column (migration 002), not a view over
			// duration_hours, so it has to be compared in its own right.
			name:   "different avg_wip conflicts",
			mutate: func(row *githubWorkItemStateDurationDailyRow) { row.AvgWIP = 0.5 },
			found:  1, want: EffectConflict,
		},
		{
			name:   "different team_name conflicts",
			mutate: func(row *githubWorkItemStateDurationDailyRow) { row.TeamName = "Other" },
			found:  1, want: EffectConflict,
		},
		{
			name:   "foreign tenant conflicts",
			mutate: func(row *githubWorkItemStateDurationDailyRow) { row.OrgID = "org-other" },
			found:  1, want: EffectConflict,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expected := githubDerivedEffectStateDurationRow()
			actual := githubDerivedEffectStateDurationRow()
			if tt.mutate != nil {
				tt.mutate(&actual)
			}
			got := compareGitHubWorkItemStateDurationVersion(expected, actual, tt.found, "org-acme")
			if got != tt.want {
				t.Fatalf("compareGitHubWorkItemStateDurationVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func githubDerivedEffectAttributionRow() githubWorkItemTeamAttributionRow {
	repoID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	teamID, teamName := "t1", "Team One"
	return githubWorkItemTeamAttributionRow{
		WorkItemID: "acme/api#1", Provider: "github", Source: "repo_ownership",
		IsPrimary: 1, Confidence: "high", Evidence: "repo:acme/api",
		ComputedAt: githubDerivedEffectComputedAt, RepoID: &repoID,
		TeamID: &teamID, TeamName: &teamName, OrgID: "org-acme",
	}
}

func TestCompareGitHubWorkItemTeamAttributionVersionClauseCoverage(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*githubWorkItemTeamAttributionRow)
		found  int
		want   EffectInspection
	}{
		{"identical single row is exact", nil, 1, EffectExact},
		{"absent when nothing read back", nil, 0, EffectAbsent},
		{
			// THE WEDGE CASE. A byte-identical recompute where only computed_at
			// moved -- the steady state for a producer re-run per backfill day.
			// Values all agree and the persisted stamp is NEWER. A two-way
			// `!Equal -> Absent` answers Absent here, the committer rewrites the
			// OLDER generation, the store keeps the newer one, and the readback
			// answers Absent forever. It must be Conflict.
			name:   "newer persisted version with identical values conflicts, never absent",
			mutate: func(row *githubWorkItemTeamAttributionRow) { row.ComputedAt = githubDerivedEffectNewerAt },
			found:  1, want: EffectConflict,
		},
		{
			// This table has no PARTITION BY, so FINAL deduplicates globally
			// and two survivors mean the sorting-key fence is wrong.
			name:   "duplicates with a stale version conflict, never absent",
			mutate: func(row *githubWorkItemTeamAttributionRow) { row.ComputedAt = githubDerivedEffectStaleAt },
			found:  2, want: EffectConflict,
		},
		{
			name:   "older version reads as absent",
			mutate: func(row *githubWorkItemTeamAttributionRow) { row.ComputedAt = githubDerivedEffectStaleAt },
			found:  1, want: EffectAbsent,
		},
		{
			name:   "different source conflicts",
			mutate: func(row *githubWorkItemTeamAttributionRow) { row.Source = "assignee_membership" },
			found:  1, want: EffectConflict,
		},
		{
			name:   "different is_primary conflicts",
			mutate: func(row *githubWorkItemTeamAttributionRow) { row.IsPrimary = 0 },
			found:  1, want: EffectConflict,
		},
		{
			name:   "different confidence conflicts",
			mutate: func(row *githubWorkItemTeamAttributionRow) { row.Confidence = "low" },
			found:  1, want: EffectConflict,
		},
		{
			name:   "different evidence conflicts",
			mutate: func(row *githubWorkItemTeamAttributionRow) { row.Evidence = "other" },
			found:  1, want: EffectConflict,
		},
		{
			// A null team_id is the persisted shape for an unassigned
			// candidate on THIS table, unlike the coverage rollup which
			// normalises it to "unassigned". Confusing the two must conflict.
			name:   "null team_id against a present team_id conflicts",
			mutate: func(row *githubWorkItemTeamAttributionRow) { row.TeamID = nil },
			found:  1, want: EffectConflict,
		},
		{
			name:   "different team_name conflicts",
			mutate: func(row *githubWorkItemTeamAttributionRow) { row.TeamName = stringPointer("Team Uno") },
			found:  1, want: EffectConflict,
		},
		{
			// repo_id is a NON-nullable UUID column, so a missing repo
			// persists as the nil UUID -- which must not read as equal to a
			// real repo id.
			name:   "nil repo_id against a real repo_id conflicts",
			mutate: func(row *githubWorkItemTeamAttributionRow) { row.RepoID = nil },
			found:  1, want: EffectConflict,
		},
		{
			name:   "different work_item_id conflicts",
			mutate: func(row *githubWorkItemTeamAttributionRow) { row.WorkItemID = "acme/api#2" },
			found:  1, want: EffectConflict,
		},
		{
			name:   "foreign tenant conflicts",
			mutate: func(row *githubWorkItemTeamAttributionRow) { row.OrgID = "org-other" },
			found:  1, want: EffectConflict,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expected := githubDerivedEffectAttributionRow()
			actual := githubDerivedEffectAttributionRow()
			if tt.mutate != nil {
				tt.mutate(&actual)
			}
			got := compareGitHubWorkItemTeamAttributionVersion(expected, actual, tt.found, "org-acme")
			if got != tt.want {
				t.Fatalf("compareGitHubWorkItemTeamAttributionVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestGitHubWorkItemTeamAttributionMetricSourceSplitsAuthorFromAssignee
// (CHAOS-4244) pins the label mapping WriteGitHubWorkItemEffect uses to feed
// dev_health_work_item_team_attributions_written_total: author_membership and
// assignee_membership are separate stored sources (chris's 2026-08-24
// precedence ruling gave the author its own rank 6, below linked_issue) that
// map onto the "author"/"assignee" metric labels directly -- no Evidence
// inspection needed any more.
func TestGitHubWorkItemTeamAttributionMetricSourceSplitsAuthorFromAssignee(t *testing.T) {
	cases := []struct {
		name   string
		row    githubWorkItemTeamAttributionRow
		wantMS string
	}{
		{"author_membership", githubWorkItemTeamAttributionRow{Source: "author_membership", Evidence: "reporter=alice"}, "author"},
		{"assignee_membership", githubWorkItemTeamAttributionRow{Source: "assignee_membership", Evidence: "assignee=carol"}, "assignee"},
		{"project_ownership", githubWorkItemTeamAttributionRow{Source: "project_ownership"}, "project"},
		{"issue_project", githubWorkItemTeamAttributionRow{Source: "issue_project"}, "project"},
		{"repo_ownership", githubWorkItemTeamAttributionRow{Source: "repo_ownership"}, "repo"},
		{"linked_issue passthrough", githubWorkItemTeamAttributionRow{Source: "linked_issue"}, "linked_issue"},
		{"unassigned passthrough", githubWorkItemTeamAttributionRow{Source: "unassigned"}, "unassigned"},
		{"native_team passthrough, bounded downstream", githubWorkItemTeamAttributionRow{Source: "native_team"}, "native_team"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := githubWorkItemTeamAttributionMetricSource(tt.row); got != tt.wantMS {
				t.Fatalf("githubWorkItemTeamAttributionMetricSource(%+v) = %q, want %q", tt.row, got, tt.wantMS)
			}
		})
	}
}

// TestGitHubWorkItemDerivedAdaptersSatisfyTheCompositeInterface keeps the three
// adapters wired to the dispatcher's contract. Without it they would compile
// as unreferenced exported types and could drift out of the interface silently.
func TestGitHubWorkItemDerivedAdaptersSatisfyTheCompositeInterface(t *testing.T) {
	var adapters = []GitHubWorkItemEffectAdapter{
		GitHubEstimateCoverageClickHouseEffects{},
		GitHubWorkItemTeamAttributionsClickHouseEffects{},
		GitHubWorkItemStateDurationsClickHouseEffects{},
	}
	if len(adapters) != 3 {
		t.Fatalf("expected three derived adapters, got %d", len(adapters))
	}
}
