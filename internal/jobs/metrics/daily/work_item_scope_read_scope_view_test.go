package daily

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// CHAOS-10797: the scope view counts each work item once per work scope (key
// (provider, work_scope_id, id): the newest row of the scope, tie lower
// repository id; no other scope competes). The org view counts it once in the
// organization (CHAOS-10796). The sum over scopes may be more than the org
// total (D5204). A run of listed repositories equals a full run, per view.

// scopeViewConn answers the reads of loadWorkItemScopeRead from a fake store
// of work_items rows. Every row is created in the day (so it is a new item of
// the day), has one transition (todo -> in_progress) and a primary attribution
// of its own row: team "team-<scope>". The type column of a row is its
// repository id.
type scopeViewConn struct {
	rows []orgWinnerStoredRow
}

func (conn *scopeViewConn) Query(_ context.Context, query string, args ...any) (chdriver.Rows, error) {
	created := orgWinnerDay.Add(time.Hour)
	switch {
	case strings.Contains(query, "is_primary = 1"):
		var out [][]any
		for _, row := range conn.rows {
			out = append(out, []any{row.repo, row.id, "team-" + row.projectID, "Team " + row.projectID})
		}
		return &orgWinnerRows{rows: out}, nil
	case strings.Contains(query, "FROM work_item_transitions"):
		seen := map[string]bool{}
		var out [][]any
		for _, row := range conn.rows {
			if seen[row.id] {
				continue
			}
			seen[row.id] = true
			out = append(out, []any{row.id, orgWinnerDay.Add(2 * time.Hour), "todo", "in_progress"})
		}
		return &orgWinnerRows{rows: out}, nil
	case strings.Contains(query, "SELECT DISTINCT provider"):
		var listed map[uuid.UUID]bool
		if strings.Contains(query, "repo_id IN ?") {
			listed = map[uuid.UUID]bool{}
			for _, repo := range args[1].([]uuid.UUID) {
				listed[repo] = true
			}
		}
		seen := map[string]bool{}
		var out [][]any
		for _, row := range conn.rows {
			if (listed != nil && !listed[row.repo]) || seen[row.projectID] {
				continue
			}
			seen[row.projectID] = true
			out = append(out, []any{"github", "", row.projectID, "", ""})
		}
		return &orgWinnerRows{rows: out}, nil
	case query == workItemOrgWinnerReadSQL:
		wanted := map[string]bool{}
		for _, id := range args[1].([]string) {
			wanted[id] = true
		}
		var out [][]any
		for _, row := range conn.rows {
			if wanted[row.id] {
				out = append(out, []any{row.repo, row.lastSynced, row.id, "github", "", row.projectID, "", ""})
			}
		}
		return &orgWinnerRows{rows: out}, nil
	case strings.Contains(query, "FROM work_items FINAL") && strings.Contains(query, "WITH ? AS scopes"):
		filtered := strings.Contains(query, "has(scopes")
		values := map[string]bool{}
		for _, value := range args[0].([]string) {
			values[value] = true
		}
		var out [][]any
		for _, row := range conn.rows {
			if filtered && !values[row.projectID] {
				continue
			}
			out = append(out, []any{
				row.repo, row.lastSynced, row.id, "github", "todo", "", row.projectID, "", "",
				created, (*time.Time)(nil), row.repo.String(), []string(nil), (*time.Time)(nil), (*time.Time)(nil), (*float64)(nil),
			})
		}
		return &orgWinnerRows{rows: out}, nil
	default:
		return &orgWinnerRows{}, nil
	}
}

func scopeViewStore() *scopeViewConn {
	return &scopeViewConn{rows: orgWinnerStore().rows}
}

func scopeViewPartition(t *testing.T, every bool, repos ...uuid.UUID) (Run, Partition, workItemPartitionScope) {
	t.Helper()
	run := Run{OrganizationID: "00000000-0000-4000-8000-00000000a001", TargetDay: orgWinnerDay}
	partition := Partition{ID: "00000000-0000-4000-8000-00000000a0c1", RunID: "00000000-0000-4000-8000-00000000a0c0"}
	for _, repo := range repos {
		partition.RepoIDs = append(partition.RepoIDs, RepositoryID(repo.String()))
	}
	scope, err := newWorkItemPartitionScope(run, partition, "work_item")
	if err != nil {
		t.Fatal(err)
	}
	scope.everyRepository = every
	return run, partition, scope
}

func readScopeView(t *testing.T, every bool, repos ...uuid.UUID) workItemScopeRead {
	t.Helper()
	run, partition, scope := scopeViewPartition(t, every, repos...)
	read, err := loadWorkItemScopeRead(context.Background(), scopeViewStore(), "work_item", run, partition, scope, true, true, true)
	if err != nil {
		t.Fatal(err)
	}
	return read
}

// countedAt is rows as "id@scope/repo", sorted.
func countedAt(rows []workItemMetricsRow) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.WorkItemID+"@"+row.workScopeID()+"/"+row.Type[len(row.Type)-2:])
	}
	sort.Strings(out)
	return out
}

func TestWorkItemScopeReadCountsAnItemOncePerScopeInTheScopeView(t *testing.T) {
	for _, test := range []struct {
		name  string
		every bool
		repos []uuid.UUID
		scope []string
		org   []string
	}{
		{name: "listed {old}: A in old; the org winner is B in new",
			repos: []uuid.UUID{orgWinnerRepoA}, scope: []string{"moved@old/a1"}, org: []string{}},
		{name: "listed {new}: B in new",
			repos: []uuid.UUID{orgWinnerRepoB}, scope: []string{"moved@new/b1"}, org: []string{"moved@new/b1"}},
		{name: "listed {kept}: same scope twice, newest, tie lower repository id",
			repos: []uuid.UUID{orgWinnerRepoC},
			scope: []string{"same-newest@kept/d1", "same-tie@kept/c1"}, org: []string{"same-newest@kept/d1", "same-tie@kept/c1"}},
		{name: "full run: A in old and B in new; the org view B only", every: true,
			scope: []string{"moved@new/b1", "moved@old/a1", "same-newest@kept/d1", "same-tie@kept/c1"},
			org:   []string{"moved@new/b1", "same-newest@kept/d1", "same-tie@kept/c1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			read := readScopeView(t, test.every, test.repos...)
			if got := countedAt(scopeViewItems(read)); !reflect.DeepEqual(got, test.scope) {
				t.Errorf("scope view = %v, want %v", got, test.scope)
			}
			if got := countedAt(read.Items); !reflect.DeepEqual(got, test.org) {
				t.Errorf("org view = %v, want %v", got, test.org)
			}
		})
	}

	// listed == full, per view: the scope-disjoint partitions of one run
	// count, in each view, what one run of the whole organization counts.
	var listedScope, listedOrg []string
	for _, repos := range [][]uuid.UUID{{orgWinnerRepoA}, {orgWinnerRepoB}, {orgWinnerRepoC}} {
		read := readScopeView(t, false, repos...)
		listedScope = append(listedScope, countedAt(scopeViewItems(read))...)
		listedOrg = append(listedOrg, countedAt(read.Items)...)
	}
	sort.Strings(listedScope)
	sort.Strings(listedOrg)
	full := readScopeView(t, true)
	if want := countedAt(scopeViewItems(full)); !reflect.DeepEqual(listedScope, want) {
		t.Errorf("scope view: listed %v, full %v", listedScope, want)
	}
	if want := countedAt(full.Items); !reflect.DeepEqual(listedOrg, want) {
		t.Errorf("org view: listed %v, full %v", listedOrg, want)
	}
}

func TestWorkItemScopeViewAttributionIsPerScopeAndID(t *testing.T) {
	read := readScopeView(t, true)
	for scope, want := range map[string]string{"old": "team-old", "new": "team-new"} {
		got, found := scopeViewAttribution(read, scope, "moved")
		if !found || got.TeamID != want {
			t.Errorf("scope view attribution of moved in %s = %+v (found %v), want %s", scope, got, found, want)
		}
	}
	if got := read.Attributions["moved"]; got.TeamID != "team-new" {
		t.Errorf("org view attribution of moved = %+v, want the org winner's team-new", got)
	}
}

// newItemsByKey is the new items of the day by "scope/team".
func newItemsByKey(rows []workitemmetrics.MetricsDailyRow) map[string]int {
	out := map[string]int{}
	for _, row := range rows {
		out[row.WorkScopeID+"/"+row.TeamID] += row.NewItemsCount
	}
	return out
}

func TestWorkItemTripletViewsAreComputedFromEachViewsItems(t *testing.T) {
	for _, test := range []struct {
		name       string
		every      bool
		repos      []uuid.UUID
		org, scope map[string]int
	}{
		{name: "listed {old}", repos: []uuid.UUID{orgWinnerRepoA},
			org: map[string]int{}, scope: map[string]int{"old/team-old": 1}},
		{name: "listed {new}", repos: []uuid.UUID{orgWinnerRepoB},
			org: map[string]int{"new/team-new": 1}, scope: map[string]int{"new/team-new": 1}},
		{name: "full run: the sum over scopes (4) is more than the org total (3)", every: true,
			org:   map[string]int{"new/team-new": 1, "kept/team-kept": 2},
			scope: map[string]int{"old/team-old": 1, "new/team-new": 1, "kept/team-kept": 2}},
	} {
		t.Run(test.name, func(t *testing.T) {
			run, partition, scope := scopeViewPartition(t, test.every, test.repos...)
			_, org, scoped, err := computeTripletViews(context.Background(), scopeViewStore(), run, partition, scope)
			if err != nil {
				t.Fatal(err)
			}
			if got := newItemsByKey(org.MetricsDaily); !reflect.DeepEqual(got, test.org) {
				t.Errorf("org view rows = %v, want %v", got, test.org)
			}
			if got := newItemsByKey(scoped.MetricsDaily); !reflect.DeepEqual(got, test.scope) {
				t.Errorf("scope view rows = %v, want %v", got, test.scope)
			}
		})
	}
}

func TestWorkItemStateViewsCountAnItemOncePerScope(t *testing.T) {
	read := readScopeView(t, true)
	_, _, scope := scopeViewPartition(t, true)
	org, scoped := stateViews(scope, read, orgWinnerDay.Add(4*time.Hour))
	touched := func(rows []workItemStateDailyRow) map[string]uint32 {
		out := map[string]uint32{}
		for _, row := range rows {
			out[row.WorkScopeID+"/"+row.TeamID+"/"+row.Status] += row.ItemsTouched
		}
		return out
	}
	wantOrg := map[string]uint32{
		"new/team-new/todo": 1, "new/team-new/in_progress": 1,
		"kept/team-kept/todo": 2, "kept/team-kept/in_progress": 2,
	}
	wantScope := map[string]uint32{
		"old/team-old/todo": 1, "old/team-old/in_progress": 1,
		"new/team-new/todo": 1, "new/team-new/in_progress": 1,
		"kept/team-kept/todo": 2, "kept/team-kept/in_progress": 2,
	}
	if got := touched(org); !reflect.DeepEqual(got, wantOrg) {
		t.Errorf("org view state rows = %v, want %v", got, wantOrg)
	}
	if got := touched(scoped); !reflect.DeepEqual(got, wantScope) {
		t.Errorf("scope view state rows = %v, want %v", got, wantScope)
	}
}

// appendedByView is the appended rows of a batch as "view:scope/team", for the
// row_view column at len-viewAt and the scope and team at the given columns.
func appendedByView(appended [][]any, viewAt, scopeAt, teamAt int) []string {
	out := make([]string, 0, len(appended))
	for _, row := range appended {
		out = append(out, row[len(row)-viewAt].(string)+":"+row[scopeAt].(string)+"/"+row[teamAt].(string))
	}
	sort.Strings(out)
	return out
}

// Each view's rows are written under that view only: the org rows under
// 'org', the scope rows under 'scope'. A key of one view only has no row in
// the other. The legacy row of zeros is written once for a key of either view.
func TestWorkItemWritersAppendEachViewsOwnRows(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name                  string
		viewAt, scopeAt, idAt int
		live                  [][]any
		write                 func(conn workItemBatchConn) (int, error)
	}{
		{name: "work_item_metrics_daily", viewAt: 1, scopeAt: 2, idAt: 3,
			live: [][]any{{"github", "old", "t-old", "", rowViewsStored}},
			write: func(conn workItemBatchConn) (int, error) {
				return writeMetricsViews(ctx, conn,
					[]workitemmetrics.MetricsDailyRow{{Day: rowViewsDay, Provider: "github", WorkScopeID: "new", TeamID: "t-new", NewItemsCount: 1}},
					[]workitemmetrics.MetricsDailyRow{
						{Day: rowViewsDay, Provider: "github", WorkScopeID: "new", TeamID: "t-new", NewItemsCount: 1},
						{Day: rowViewsDay, Provider: "github", WorkScopeID: "old", TeamID: "t-old", NewItemsCount: 1},
					})
			}},
		{name: "work_item_user_metrics_daily", viewAt: 2, scopeAt: 2, idAt: 3,
			live: [][]any{{"github", "old", "u", "", rowViewsStored}},
			write: func(conn workItemBatchConn) (int, error) {
				return writeUserViews(ctx, conn,
					[]workitemmetrics.UserMetricsDailyRow{{Day: rowViewsDay, Provider: "github", WorkScopeID: "new", UserIdentity: "u", TeamID: "t-new"}},
					[]workitemmetrics.UserMetricsDailyRow{
						{Day: rowViewsDay, Provider: "github", WorkScopeID: "new", UserIdentity: "u", TeamID: "t-new"},
						{Day: rowViewsDay, Provider: "github", WorkScopeID: "old", UserIdentity: "u", TeamID: "t-old"},
					})
			}},
		{name: "work_item_state_durations_daily", viewAt: 1, scopeAt: 2, idAt: 3,
			live: [][]any{{"github", "old", "t-old", "open", "", rowViewsStored}},
			write: func(conn workItemBatchConn) (int, error) {
				return writeStateViews(ctx, conn,
					[]workItemStateDailyRow{{Provider: "github", WorkScopeID: "new", TeamID: "t-new", Status: "open", ItemsTouched: 1}},
					[]workItemStateDailyRow{
						{Provider: "github", WorkScopeID: "new", TeamID: "t-new", Status: "open", ItemsTouched: 1},
						{Provider: "github", WorkScopeID: "old", TeamID: "t-old", Status: "open", ItemsTouched: 1},
					})
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn := &liveLegacyConn{batch: &countingBatch{}, live: test.live}
			written, err := test.write(conn)
			if err != nil {
				t.Fatal(err)
			}
			got := appendedByView(conn.batch.appended, test.viewAt, test.scopeAt, test.idAt)
			newID, oldID := "t-new", "t-old"
			if test.name == "work_item_user_metrics_daily" {
				newID, oldID = "u", "u"
			}
			want := []string{":old/" + oldID, "org:new/" + newID, "scope:new/" + newID, "scope:old/" + oldID}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("appended rows by view = %v, want %v", got, want)
			}
			if written != 3 {
				t.Fatalf("written = %d, want the two keys and the one row of zeros", written)
			}
			if conn.batch.sends != 1 {
				t.Fatalf("sends = %d, want one block", conn.batch.sends)
			}
		})
	}
}

// The run-level retraction decides keys per (key, view): a key of one view
// only is not a key of the other, so the other view's live row of the key is
// stale and gets the row of zeros.
func TestWorkItemRetractorKeysCarryTheView(t *testing.T) {
	keyTexts := func(keys []staleKey) []string {
		out := make([]string, 0, len(keys))
		for _, key := range keys {
			out = append(out, strings.Join(key, "|"))
		}
		sort.Strings(out)
		return out
	}
	org, scope := teamkeytables.OrgRowView, teamkeytables.ScopeRowView
	metrics := metricsViewKeys(
		[]workitemmetrics.MetricsDailyRow{{Provider: "github", WorkScopeID: "new", TeamID: "t"}},
		[]workitemmetrics.MetricsDailyRow{{Provider: "github", WorkScopeID: "new", TeamID: "t"}, {Provider: "github", WorkScopeID: "old", TeamID: "t"}},
	)
	if got, want := keyTexts(metrics), []string{"github|new|t|" + org, "github|new|t|" + scope, "github|old|t|" + scope}; !reflect.DeepEqual(got, want) {
		t.Errorf("metrics keys = %v, want %v", got, want)
	}
	users := userViewKeys(
		[]workitemmetrics.UserMetricsDailyRow{{Provider: "github", WorkScopeID: "new", UserIdentity: "u"}},
		[]workitemmetrics.UserMetricsDailyRow{{Provider: "github", WorkScopeID: "old", UserIdentity: "u"}},
	)
	if got, want := keyTexts(users), []string{"github|new|u|" + org, "github|old|u|" + scope}; !reflect.DeepEqual(got, want) {
		t.Errorf("user keys = %v, want %v", got, want)
	}
	states := stateViewKeys(
		nil,
		[]workItemStateDailyRow{{Provider: "github", WorkScopeID: "old", TeamID: "t", Status: "open"}},
	)
	if got, want := keyTexts(states), []string{"github|old|t|open|" + scope}; !reflect.DeepEqual(got, want) {
		t.Errorf("state keys = %v, want %v", got, want)
	}
}
