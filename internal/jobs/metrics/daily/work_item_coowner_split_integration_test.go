//go:build integration

package daily

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// readWorkItemScopeWithCoOwners is the scope read of the work_item family: the
// read of readWorkItemScope plus the co-owner teams of each item, which only
// the team split of a person's row reads (CHAOS-12535).
func readWorkItemScopeWithCoOwners(t *testing.T, ctx context.Context, conn driver.Conn, org string, repos ...uuid.UUID) workItemScopeRead {
	t.Helper()
	run := Run{OrganizationID: org, TargetDay: scopeReadDay}
	partition := scopeReadPartition(repos...)
	scope, err := newWorkItemPartitionScope(run, partition, "work_item")
	if err != nil {
		t.Fatal(err)
	}
	read, err := loadWorkItemScopeRead(ctx, conn, "work_item", run, partition, scope, true, true, true)
	if err != nil {
		t.Fatalf("read the work scopes of %v with the co-owners: %v", repos, err)
	}
	return read
}

// The work_item family reads the co-owner rows (is_primary = 2) for the team
// split of a person's row only. With the store of
// TestTheDailyAttributionReadersIgnoreACoOwnerRow and one provenance row
// (is_primary = 0): the primary team of each
// item is the same with and without the co-owner rows, the co-owner teams are
// exactly the co-owner rows, and every rollup row the family computes
// (work_item_metrics_daily, work_item_user_metrics_daily's team and counts,
// work_item_cycle_times) is the same with and without them; only the team
// split of the person's row holds the co-owners.
func TestTheWorkItemFamilyReadsACoOwnerRowForTheTeamSplitOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	at := scopeReadDay.Add(12 * time.Hour)
	for _, org := range []string{scopeReadOrgA, scopeReadOrgB} {
		seedScopeReadItems(t, ctx, conn,
			scopeReadItem{org: org, repo: scopeReadRepoA, id: "it-1", projectID: "shared", storyPoints: 1},
			scopeReadItem{org: org, repo: scopeReadRepoA, id: "it-2", projectID: "shared", storyPoints: 1},
		)
	}
	seedScopeReadAttribution(t, ctx, conn, scopeReadOrgB, scopeReadRepoA, "it-1", "team-a")
	seedScopeReadAttribution(t, ctx, conn, scopeReadOrgB, scopeReadRepoA, "it-2", "team-a")
	if err := conn.Exec(ctx, `
INSERT INTO work_item_team_attributions
    (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at)
VALUES (?, ?, 'it-1', 'github', 'team-a', 'team-a', 'native_team', 1, 'high', 'test', ?),
       (?, ?, 'it-1', 'github', 'team-0', 'team-0', 'issue_project', 2, 'high', 'issue_project_key=KEY', ?),
       (?, ?, 'it-2', 'github', 'team-a', 'team-a', 'native_team', 1, 'high', 'test', ?),
       (?, ?, 'it-2', 'github', 'team-z', 'team-z', 'issue_project', 2, 'high', 'issue_project_key=KEY', ?),
       (?, ?, 'it-2', 'github', 'team-p', 'team-p', 'assignee_membership', 0, 'low', 'test', ?)`,
		scopeReadOrgA, scopeReadRepoA, at, scopeReadOrgA, scopeReadRepoA, at,
		scopeReadOrgA, scopeReadRepoA, at, scopeReadOrgA, scopeReadRepoA, at,
		scopeReadOrgA, scopeReadRepoA, at); err != nil {
		t.Fatal(err)
	}

	with := readWorkItemScopeWithCoOwners(t, ctx, conn, scopeReadOrgA, scopeReadRepoA)
	without := readWorkItemScopeWithCoOwners(t, ctx, conn, scopeReadOrgB, scopeReadRepoA)

	t.Run("the primary team of each item is the same", func(t *testing.T) {
		if len(with.Attributions) != 2 || len(without.Attributions) != 2 {
			t.Fatalf("attributions with = %+v, without = %+v, want it-1 and it-2 in each", with.Attributions, without.Attributions)
		}
		for _, id := range []string{"it-1", "it-2"} {
			got, want := with.Attributions[id], without.Attributions[id]
			if got.TeamID != "team-a" || got.TeamID != want.TeamID || got.TeamName != want.TeamName {
				t.Errorf("%s: primary team with co-owner rows = %q/%q, without = %q/%q, want team-a in both",
					id, got.TeamID, got.TeamName, want.TeamID, want.TeamName)
			}
		}
	})

	t.Run("the co-owner teams are exactly the co-owner rows", func(t *testing.T) {
		for id, want := range map[string][]string{"it-1": {"team-0"}, "it-2": {"team-z"}} {
			if got := with.Attributions[id].CoOwnerTeamIDs; !reflect.DeepEqual(got, want) {
				t.Errorf("%s: co-owner teams = %v, want %v (a provenance row is no co-owner)", id, got, want)
			}
			if got := without.Attributions[id].CoOwnerTeamIDs; len(got) != 0 {
				t.Errorf("%s: co-owner teams without co-owner rows = %v, want none", id, got)
			}
		}
	})

	t.Run("the rollup rows are the same, only the team split differs", func(t *testing.T) {
		withTriplet := tripletFromWorkItemRows(scopeReadDay, with.Items, with.Transitions, with.Attributions)
		withoutTriplet := tripletFromWorkItemRows(scopeReadDay, without.Items, without.Transitions, without.Attributions)
		if len(withTriplet.MetricsDaily) == 0 || !reflect.DeepEqual(withTriplet.MetricsDaily, withoutTriplet.MetricsDaily) {
			t.Errorf("work_item_metrics_daily rows with co-owner rows = %+v, without = %+v", withTriplet.MetricsDaily, withoutTriplet.MetricsDaily)
		}
		if len(withTriplet.CycleTimes) == 0 || !reflect.DeepEqual(withTriplet.CycleTimes, withoutTriplet.CycleTimes) {
			t.Errorf("work_item_cycle_times rows with co-owner rows = %+v, without = %+v", withTriplet.CycleTimes, withoutTriplet.CycleTimes)
		}
		splitless := func(rows []workitemmetrics.UserMetricsDailyRow) []workitemmetrics.UserMetricsDailyRow {
			out := make([]workitemmetrics.UserMetricsDailyRow, 0, len(rows))
			for _, row := range rows {
				row.TeamSplit = nil
				out = append(out, row)
			}
			return out
		}
		if len(withTriplet.UserMetricsDaily) != 1 || !reflect.DeepEqual(splitless(withTriplet.UserMetricsDaily), splitless(withoutTriplet.UserMetricsDaily)) {
			t.Fatalf("work_item_user_metrics_daily rows (team split set aside) with co-owner rows = %+v, without = %+v",
				withTriplet.UserMetricsDaily, withoutTriplet.UserMetricsDaily)
		}
		if row := withTriplet.UserMetricsDaily[0]; row.TeamID != "team-a" || row.ItemsCompleted != 2 {
			t.Errorf("the person's row = %+v, want team-a with the 2 done items", row)
		}
		wantWith := []workitemmetrics.UserTeamCount{
			{TeamID: "team-0", IsPrimary: 2, ItemsCompleted: 1},
			{TeamID: "team-a", IsPrimary: 1, ItemsCompleted: 2},
			{TeamID: "team-z", IsPrimary: 2, ItemsCompleted: 1},
		}
		if got := withTriplet.UserMetricsDaily[0].TeamSplit; !reflect.DeepEqual(got, wantWith) {
			t.Errorf("team split with co-owner rows = %+v, want %+v", got, wantWith)
		}
		wantWithout := []workitemmetrics.UserTeamCount{{TeamID: "team-a", IsPrimary: 1, ItemsCompleted: 2}}
		if got := withoutTriplet.UserMetricsDaily[0].TeamSplit; !reflect.DeepEqual(got, wantWithout) {
			t.Errorf("team split without co-owner rows = %+v, want %+v", got, wantWithout)
		}
	})
}
