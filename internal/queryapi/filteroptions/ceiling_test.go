package filteroptions

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-13088: the options of a caller held to a repository ceiling come from rows that name an allowed repository.

const (
	allowedRepo = "11111111-1111-1111-1111-111111111111"
	otherRepo   = "22222222-2222-2222-2222-222222222222"
)

func sp(s string) *string { return &s }

type optionRows struct {
	rows  [][]any
	index int
}

func (r *optionRows) Next() bool   { r.index++; return r.index <= len(r.rows) }
func (r *optionRows) Err() error   { return nil }
func (r *optionRows) Close() error { return nil }
func (r *optionRows) Scan(dest ...any) error {
	row := r.rows[r.index-1]
	if len(row) != len(dest) {
		return fmt.Errorf("optionRows: %d values for %d destinations", len(row), len(dest))
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

// optionWorld answers each read of a restricted request. The linked reads return the item linked to the bound
// repositories only: jira:OPS-1 (todo, then in_progress) is linked to the allowed repository, jira:OPS-2 (todo, then
// in_review) only to the other one.
type optionWorld struct {
	t          *testing.T
	fail       string
	statements []string
	bindings   [][]dhclickhouse.Binding
}

func bound(bindings []dhclickhouse.Binding, name string) []string {
	for _, b := range bindings {
		if b.Name == name {
			values, _ := b.Value.([]string)
			return values
		}
	}
	return nil
}

func (w *optionWorld) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	w.statements = append(w.statements, query)
	w.bindings = append(w.bindings, bindings)
	if w.fail != "" && strings.Contains(query, w.fail) {
		return nil, errors.New("max_result_rows exceeded")
	}
	items := map[string]string{allowedRepo: "jira:OPS-1", otherRepo: "jira:OPS-2"}
	second := map[string]string{"jira:OPS-1": "in_progress", "jira:OPS-2": "in_review"}
	var linked []string
	for _, repo := range bound(bindings, "link_repo_ids") {
		linked = append(linked, items[repo])
	}
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var rows [][]any
	switch {
	case strings.Contains(query, "work_item_user_metrics_daily") || strings.Contains(query, "work_item_state_durations_daily") ||
		strings.Contains(query, "issue_type_metrics_daily"):
		w.t.Fatalf("a restricted request read a table that names no allowed repository:\n%s", query)
	case strings.Contains(query, "FROM work_item_transitions"):
		for _, id := range linked {
			rows = append(rows, []any{id, created.AddDate(0, 0, 1), "todo", second[id]})
		}
	case strings.Contains(query, "FROM work_item_team_attributions"), strings.Contains(query, "FROM work_item_dependenc"):
	case strings.Contains(query, "FROM work_items FINAL"):
		for _, id := range linked {
			rows = append(rows, []any{allowedRepo, created.AddDate(0, 0, 9), id, "jira", second[id], "OPS", "", "", "", created,
				(*time.Time)(nil), "task", []string(nil), (*time.Time)(nil), (*time.Time)(nil), (*float64)(nil)})
		}
	case strings.Contains(query, "author_email AS value"):
		rows = [][]any{{sp("dev@example.com")}, {sp("not-an-email")}}
	case strings.Contains(query, "SELECT DISTINCT value") && strings.Contains(query, "user_metrics_daily"):
		rows = [][]any{{sp("T1")}}
	case strings.Contains(query, "trim(name) AS team_name"):
		rows = [][]any{{sp("T1"), sp("Team One")}, {sp("T2"), sp("Team Two")}}
	case strings.Contains(query, "SELECT DISTINCT repo AS value"):
		rows = [][]any{{sp("acme/a")}}
	case strings.Contains(query, "AS repo_name"):
		rows = [][]any{{sp(allowedRepo), sp("acme/a")}}
	case strings.Contains(query, "FROM identities"):
		rows = [][]any{{sp("dev@example.com"), sp("Dev")}, {sp("other@example.com"), sp("Other")}}
	}
	return &optionRows{rows: rows}, nil
}

type matcherScan struct {
	ids   []string
	index int
}

func (s *matcherScan) Next() bool   { s.index++; return s.index <= len(s.ids) }
func (s *matcherScan) Close() error { return nil }
func (s *matcherScan) Err() error   { return nil }
func (s *matcherScan) Scan(dest ...any) error {
	*dest[0].(*string) = fmt.Sprintf("acme/r%d", s.index)
	*dest[1].(*[]string) = []string{s.ids[s.index-1]}
	return nil
}

type matcherClient struct{ ids []string }

func (c matcherClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return &matcherScan{ids: c.ids}, nil
}

func ceilingOf(t *testing.T, ids ...string) teamscope.Ceiling {
	t.Helper()
	keys := make([]string, len(ids))
	for i := range ids {
		keys[i] = fmt.Sprintf("acme/r%d", i+1)
	}
	ceiling, err := teamscope.ResolveCeiling(context.Background(), matcherClient{ids: ids}, pepgate.Allowlist("0b6a1c9e-55d4-4a43-9e34-6f4c1d2b3a10", keys))
	if err != nil || !ceiling.Restricted() {
		t.Fatalf("ceiling %v restricted %v err %v", ceiling, ceiling.Restricted(), err)
	}
	return ceiling
}

var now = time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)

// C1, C2: every list of a restricted request is held to the allowed repository: the statuses are those of the item
// linked to it (in_review, a status of the other repository's item only, is not listed); the names are those of the
// listed teams and developers only; the catalog and issue-type lists are not served and say why.
func TestRestrictedOptionsComeFromTheAllowedRepository(t *testing.T) {
	world := &optionWorld{t: t}
	resp, err := BuildCeilingResponse(context.Background(), world, "org-1", ceilingOf(t, allowedRepo), now)
	if err != nil {
		t.Fatal(err)
	}
	want := Response{
		Teams: []string{"T1"}, Repos: []string{"acme/a"}, Services: []string{}, Developers: []string{"dev@example.com"},
		WorkCategory: workCategory(), IssueType: []string{}, FlowStage: []string{"in_progress", "todo"},
		TeamNames: map[string]string{"T1": "Team One"}, RepoNames: map[string]string{allowedRepo: "acme/a"},
		DeveloperNames: resp.DeveloperNames,
		OmittedSignals: resp.OmittedSignals, NarrowedCounts: resp.NarrowedCounts,
	}
	if !reflect.DeepEqual(resp, want) {
		t.Fatalf("response =\n%+v\nwant\n%+v", resp, want)
	}
	if !reflect.DeepEqual(resp.DeveloperNames, map[string]string{"dev@example.com": "Dev"}) {
		t.Fatalf("developer names = %v, want the listed developer's only", resp.DeveloperNames)
	}
	if fmt.Sprint(resp.OmittedSignals) != "[{issue_type not scoped to repositories} {team_catalog not scoped to repositories}]" ||
		fmt.Sprint(resp.NarrowedCounts) != "[{filter_options repository ceiling}]" {
		t.Fatalf("omitted %v narrowed %v", resp.OmittedSignals, resp.NarrowedCounts)
	}
	for i, statement := range world.statements {
		if strings.Contains(statement, "SELECT id AS value") {
			t.Fatalf("the teams catalog is listed:\n%s", statement)
		}
		repoKeyed := strings.Contains(statement, "FROM repos FINAL") || strings.Contains(statement, "FROM user_metrics_daily")
		if repoKeyed && !reflect.DeepEqual(bound(world.bindings[i], teamscope.BindingCeilingIDs), []string{allowedRepo}) {
			t.Fatalf("a repository-keyed list has no ceiling over [%s]:\n%s", allowedRepo, statement)
		}
		itemKeyed := strings.Contains(statement, "FROM work_item")
		if itemKeyed && !strings.Contains(statement, "work_dependencies") && !strings.Contains(statement, "work_item_depend") &&
			!reflect.DeepEqual(bound(world.bindings[i], "link_repo_ids"), []string{allowedRepo}) {
			t.Fatalf("an item read is not held to the link of [%s]:\n%s", allowedRepo, statement)
		}
	}
}

// C6: a ceiling with no repository reads nothing at all.
func TestRestrictedOptionsWithNoRepositoryReadNothing(t *testing.T) {
	world := &optionWorld{t: t}
	ceiling, err := teamscope.ResolveCeiling(context.Background(), matcherClient{}, pepgate.Allowlist("0b6a1c9e-55d4-4a43-9e34-6f4c1d2b3a10", nil))
	if err != nil || !ceiling.Restricted() {
		t.Fatal(err)
	}
	resp, err := BuildCeilingResponse(context.Background(), world, "org-1", ceiling, now)
	if err != nil || len(world.statements) != 0 {
		t.Fatalf("err %v, %d statements; want none", err, len(world.statements))
	}
	if len(resp.Teams)+len(resp.Repos)+len(resp.Developers)+len(resp.FlowStage)+len(resp.IssueType) != 0 || resp.Teams == nil || resp.FlowStage == nil {
		t.Fatalf("response %+v, want empty lists (never null)", resp)
	}
}

// C13: a bound hit of the status history is an error of the request; the state table is never the fallback (the
// world fails the test if it is read).
func TestRestrictedOptionsBoundHitIsAnError(t *testing.T) {
	for _, fail := range []string{"FROM work_items FINAL", "FROM work_item_transitions", "FROM work_item_dependencies"} {
		world := &optionWorld{t: t, fail: fail}
		if resp, err := BuildCeilingResponse(context.Background(), world, "org-1", ceilingOf(t, allowedRepo), now); err == nil {
			t.Fatalf("%s: a bound hit answered %+v", fail, resp)
		}
	}
}

// C4: with no ceiling the answer is BuildResponse's, statement for statement.
func TestUnrestrictedOptionsAreTodaysStatements(t *testing.T) {
	record := func(build func(QueryClient) (Response, error)) ([]string, Response) {
		client := &recordingByQuery{byQueryClient: byQueryClient{teams: []string{"ENG"}, developers: []string{"dev@example.com"}}}
		resp, err := build(client)
		if err != nil {
			t.Fatal(err)
		}
		return client.statements, resp
	}
	wantStatements, wantResp := record(func(c QueryClient) (Response, error) { return BuildResponse(context.Background(), c, "org-1") })
	gotStatements, gotResp := record(func(c QueryClient) (Response, error) {
		return BuildCeilingResponse(context.Background(), c, "org-1", teamscope.NoCeiling(), now)
	})
	if !reflect.DeepEqual(gotStatements, wantStatements) || !reflect.DeepEqual(gotResp, wantResp) {
		t.Fatalf("an unrestricted request differs from today's:\n%v\n%v", gotStatements, wantStatements)
	}
	if gotResp.OmittedSignals != nil || gotResp.NarrowedCounts != nil {
		t.Fatalf("an unrestricted answer carries %v / %v", gotResp.OmittedSignals, gotResp.NarrowedCounts)
	}
}

type recordingByQuery struct {
	byQueryClient
	statements []string
}

func (c *recordingByQuery) Query(ctx context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.statements = append(c.statements, query)
	return c.byQueryClient.Query(ctx, query, bindings)
}
