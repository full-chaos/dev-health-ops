package linkeditems

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

const stateRepo = "11111111-1111-1111-1111-111111111111"

func at(d, hour int) time.Time { return day(d).Add(time.Duration(hour) * time.Hour) }

func stateVersion(id, provider, status string, created time.Time, completed *time.Time, repo string, synced time.Time) Version {
	return Version{
		Item:   workitemmetrics.StoredItem{WorkItemID: id, Provider: provider, Status: status, ProjectKey: "OPS", CreatedAt: created, CompletedAt: completed},
		RepoID: repo, Synced: synced,
	}
}

func statusesOn(rows []workitemmetrics.StateDurationRow) map[string]float64 {
	out := map[string]float64{}
	for _, row := range rows {
		out[row.Status] += row.DurationHours
	}
	return out
}

// The daily job computes a day from the transitions before the day's end. An item whose first transition is later has
// no transition that day and is skipped: the recompute of the window reads every transition of the window once, and
// must hold each day to the transitions of that day.
func TestStateRowsByDayHoldsEachDayToItsOwnTransitions(t *testing.T) {
	versions := []Version{stateVersion("jira:OPS-1", "jira", "in_progress", at(1, 9), nil, stateRepo, day(9))}
	transitions := []workitemmetrics.StateTransition{{WorkItemID: "jira:OPS-1", OccurredAt: at(3, 12), FromStatus: "todo", ToStatus: "in_progress"}}
	byDay := StateRowsByDay(day(1), day(5), versions, nil, transitions, day(9), nil)
	for _, d := range []int{1, 2} {
		if rows, found := byDay[day(d)]; found {
			t.Fatalf("day %d has rows %+v: the item has no transition before that day's end", d, rows)
		}
	}
	if got := statusesOn(byDay[day(3)]); got["todo"] != 12 || got["in_progress"] != 12 {
		t.Fatalf("day 3 = %v, want 12 h todo and 12 h in_progress", got)
	}
	if got := statusesOn(byDay[day(4)]); got["in_progress"] != 24 || len(got) != 1 {
		t.Fatalf("day 4 = %v, want 24 h in_progress", got)
	}
}

// Each day is the shared compute of that day over the day's items: the versions that pass the day's predicate, each
// item once (the newest stored row), with the attribution of the row it is.
func TestStateRowsByDayIsTheSharedComputeOfEachDay(t *testing.T) {
	done := at(3, 6)
	versions := []Version{
		stateVersion("jira:OPS-1", "jira", "in_progress", at(1, 9), nil, stateRepo, day(8)),
		// The same item stored under a second repository, synced later: it is the item.
		stateVersion("jira:OPS-1", "jira", "in_progress", at(1, 9), nil, "22222222-2222-2222-2222-222222222222", day(9)),
		stateVersion("jira:OPS-2", "jira", "done", at(1, 0), &done, stateRepo, day(9)),
	}
	attributions := map[Address]workitemmetrics.PrimaryAttribution{
		{Provider: "jira", WorkItemID: "jira:OPS-1", RepoID: stateRepo}:                              {TeamID: "old", TeamName: "Old"},
		{Provider: "jira", WorkItemID: "jira:OPS-1", RepoID: "22222222-2222-2222-2222-222222222222"}: {TeamID: "new", TeamName: "New"},
	}
	transitions := []workitemmetrics.StateTransition{
		{WorkItemID: "jira:OPS-1", OccurredAt: at(1, 12), FromStatus: "todo", ToStatus: "in_progress"},
		{WorkItemID: "jira:OPS-2", OccurredAt: at(2, 0), FromStatus: "todo", ToStatus: "in_progress"},
		{WorkItemID: "jira:OPS-2", OccurredAt: done, FromStatus: "in_progress", ToStatus: "done"},
	}
	computedAt := day(9)
	byDay := StateRowsByDay(day(1), day(5), versions, attributions, transitions, computedAt, nil)
	for d := 1; d < 5; d++ {
		start, end := day(d), day(d+1)
		var items []workitemmetrics.StateItem
		dayAttributions := map[string]workitemmetrics.PrimaryAttribution{}
		for _, index := range []int{1, 2} {
			v := versions[index]
			if workitemmetrics.PassesDayPredicate(v.Item.Status, v.Item.CreatedAt, v.Item.CompletedAt, start, end) {
				items = append(items, stateItemOf(v.Item))
				if a, ok := attributions[Address{v.Item.Provider, v.Item.WorkItemID, v.RepoID}]; ok {
					dayAttributions[v.Item.WorkItemID] = a
				}
			}
		}
		var dayTransitions []workitemmetrics.StateTransition
		for _, transition := range transitions {
			if transition.OccurredAt.Before(end) {
				dayTransitions = append(dayTransitions, transition)
			}
		}
		want, _, _ := workitemmetrics.ComputeStateDurationsDay(start, end, items, dayTransitions, dayAttributions, computedAt, nil)
		if len(want) == 0 {
			want = nil
		}
		if got := byDay[start]; !reflect.DeepEqual(got, want) {
			t.Errorf("day %d:\n got %+v\nwant %+v", d, got, want)
		}
	}
	for _, row := range byDay[day(2)] {
		if row.TeamID == "old" {
			t.Fatalf("the older stored row's team was used: %+v", row)
		}
	}
}

// CHAOS-8493: the parts of a non-terminal segment in which the item has an open blocker are "blocked", as the daily job
// writes them.
func TestStateRowsByDayOverlaysTheBlockedSpans(t *testing.T) {
	versions := []Version{stateVersion("jira:OPS-1", "jira", "in_progress", at(1, 0), nil, stateRepo, day(9))}
	transitions := []workitemmetrics.StateTransition{{WorkItemID: "jira:OPS-1", OccurredAt: at(1, 0), FromStatus: "todo", ToStatus: "in_progress"}}
	unblocked := at(2, 18)
	blocked := map[string][]workitemmetrics.BlockedInterval{"jira:OPS-1": {{Start: at(2, 6), End: &unblocked}}}
	byDay := StateRowsByDay(day(1), day(4), versions, nil, transitions, day(9), blocked)
	if got := statusesOn(byDay[day(2)]); got["blocked"] != 12 || got["in_progress"] != 12 {
		t.Fatalf("day 2 = %v, want 12 h blocked and 12 h in_progress", got)
	}
	if got := statusesOn(byDay[day(3)]); got["blocked"] != 0 || got["in_progress"] != 24 {
		t.Fatalf("day 3 = %v, want 24 h in_progress", got)
	}
}

func TestBlockingNamesCarryTheExternalKeyOfAnIssue(t *testing.T) {
	versions := []Version{
		stateVersion("jira:OPS-1", "jira", "todo", day(1), nil, stateRepo, day(2)),
		stateVersion("jira:OPS-1", "jira", "todo", day(1), nil, "r2", day(3)),
		stateVersion("gh:a/r#4", "github", "todo", day(1), nil, stateRepo, day(2)),
		stateVersion("linear:PLAT-9", "linear", "todo", day(1), nil, stateRepo, day(2)),
	}
	want := []string{"extkey:OPS-1", "extkey:PLAT-9", "gh:a/r#4", "jira:OPS-1", "linear:PLAT-9"}
	if got := blockingNames(versions); !reflect.DeepEqual(got, want) {
		t.Fatalf("names = %v, want %v", got, want)
	}
}

// --- a recording client answering each read by the table it names ---

type stateStatement struct {
	query    string
	bindings map[string]any
}

type stateClient struct {
	statements []stateStatement
	failOn     string
	answers    map[string][][]any
}

func (c *stateClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	bound := map[string]any{}
	for _, binding := range bindings {
		bound[binding.Name] = binding.Value
	}
	c.statements = append(c.statements, stateStatement{query, bound})
	if c.failOn != "" && strings.Contains(query, c.failOn) {
		return nil, errors.New("max_result_rows exceeded")
	}
	for _, marker := range []string{"FROM work_item_transitions", "FROM work_item_team_attributions", "FROM work_item_dependency_first_seen", "FROM work_item_dependencies", "FROM work_item_relations_read", "FROM work_items FINAL"} {
		if strings.Contains(query, marker) {
			return &valueRows{rows: c.answers[marker]}, nil
		}
	}
	return &valueRows{}, nil
}

type valueRows struct {
	rows  [][]any
	index int
}

func (r *valueRows) Next() bool   { r.index++; return r.index <= len(r.rows) }
func (r *valueRows) Err() error   { return nil }
func (r *valueRows) Close() error { return nil }
func (r *valueRows) Scan(dest ...any) error {
	row := r.rows[r.index-1]
	if len(row) != len(dest) {
		return fmt.Errorf("value rows: %d values for %d destinations", len(row), len(dest))
	}
	for i, value := range row {
		target := reflect.ValueOf(dest[i]).Elem()
		if value == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		target.Set(reflect.ValueOf(value))
	}
	return nil
}

func stateWorld() map[string][][]any {
	created := at(1, 0)
	return map[string][][]any{
		"FROM work_items FINAL": {
			{stateRepo, day(9), "jira:OPS-1", "jira", "in_progress", "OPS", "", "", "", created, (*time.Time)(nil), "task", []string(nil), (*time.Time)(nil), (*time.Time)(nil), (*float64)(nil)},
		},
		"FROM work_item_transitions": {
			{"jira:OPS-1", at(1, 0), "todo", "in_progress"},
		},
	}
}

// The recompute reads the items, their attributions and transitions through the link of an allowed repository, and the
// blocking relations of those items by name (their ids and the external key of an issue), in the organization only.
func TestStateDailyReadsTheLinkedItemsAndTheirBlockersOnly(t *testing.T) {
	client := &stateClient{answers: stateWorld()}
	ids := []string{stateRepo}
	daily := NewStateDaily(Loader{Client: client, OrgID: "org", RepoIDs: ids, WindowStart: day(1), WindowEnd: day(3), Budget: StateBudget}, day(9))
	rows, err := daily.Rows(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := statusesOn(rows[day(2)]); got["in_progress"] != 24 {
		t.Fatalf("day 2 = %v, want 24 h in_progress", got)
	}
	var tables []string
	for _, statement := range client.statements {
		org, named := statement.bindings["org_id"], false
		for name, value := range statement.bindings {
			if value == "org" && name != "org_id" {
				named = true
			}
		}
		if org != "org" && !named {
			t.Errorf("a statement does not bind the organization:\n%s", statement.query)
		}
		switch {
		case strings.Contains(statement.query, "FROM work_item_dependencies"):
			tables = append(tables, "relations")
			var naming []string
			for _, value := range statement.bindings {
				if list, ok := value.([]string); ok {
					naming = list
				}
			}
			if want := []string{"extkey:OPS-1", "jira:OPS-1"}; !reflect.DeepEqual(naming, want) {
				t.Errorf("relations named %v, want %v", naming, want)
			}
		case strings.Contains(statement.query, "work_item_id IN "+Subquery):
			if got := statement.bindings[bindingLinkRepoIDs]; !reflect.DeepEqual(got, ids) {
				t.Errorf("link repositories bound %v, want %v:\n%s", got, ids, statement.query)
			}
			if !strings.HasSuffix(statement.query, StateBudget) {
				t.Errorf("a linked read carries no bound:\n%s", statement.query)
			}
			tables = append(tables, "linked")
		default:
			t.Errorf("an unexpected read:\n%s", statement.query)
		}
	}
	sort.Strings(tables)
	if want := []string{"linked", "linked", "linked", "relations"}; !reflect.DeepEqual(tables, want) {
		t.Fatalf("reads = %v, want the three linked reads and the relation read", tables)
	}
}

const bindingLinkRepoIDs = "link_repo_ids"

// C13: a bound hit of any read -- the items, the transitions, the blocking relations -- is an error of the request,
// never rows computed without it, and the read is not run again.
func TestStateDailyBoundHitIsAnErrorEveryTime(t *testing.T) {
	for _, failOn := range []string{"FROM work_items FINAL", "FROM work_item_transitions", "FROM work_item_dependencies"} {
		client := &stateClient{answers: stateWorld(), failOn: failOn}
		daily := NewStateDaily(Loader{Client: client, OrgID: "org", RepoIDs: []string{stateRepo}, WindowStart: day(1), WindowEnd: day(3), Budget: StateBudget}, day(9))
		for i := 0; i < 2; i++ {
			if rows, err := daily.Rows(context.Background()); err == nil || rows != nil {
				t.Fatalf("%s: rows %v err %v, want the bound error and no rows", failOn, rows, err)
			}
		}
		calls := len(client.statements)
		if _, _ = daily.Rows(context.Background()); len(client.statements) != calls {
			t.Fatalf("%s: the failed read ran again", failOn)
		}
	}
}

// C6: no repository reads nothing.
func TestStateDailyWithNoRepositoryReadsNothing(t *testing.T) {
	client := &stateClient{answers: stateWorld()}
	daily := NewStateDaily(Loader{Client: client, OrgID: "org", WindowStart: day(1), WindowEnd: day(3), Budget: StateBudget}, day(9))
	rows, err := daily.Rows(context.Background())
	if err != nil || len(rows) != 0 || len(client.statements) != 0 {
		t.Fatalf("rows %v err %v statements %d, want none", rows, err, len(client.statements))
	}
}
