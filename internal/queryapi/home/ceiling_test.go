package home

import (
	"context"
	"reflect"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

const (
	ceilingOrgID = "org-13086"
	ceilingRepoA = "11111111-1111-1111-1111-111111111111"
	ceilingRepoB = "22222222-2222-2222-2222-222222222222"
)

type ceilingKeyRows struct {
	paths []string
	ids   [][]string
	i     int
}

func (r *ceilingKeyRows) Next() bool { r.i++; return r.i <= len(r.paths) }
func (r *ceilingKeyRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.paths[r.i-1]
	*(dest[1].(*[]string)) = r.ids[r.i-1]
	return nil
}
func (r *ceilingKeyRows) Err() error   { return nil }
func (r *ceilingKeyRows) Close() error { return nil }

type ceilingKeyQuerier map[string][]string

func (q ceilingKeyQuerier) Query(_ context.Context, _ string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	out := &ceilingKeyRows{}
	for _, b := range bindings {
		if b.Name != teamscope.BindingCeilingKeys {
			continue
		}
		for _, key := range b.Value.([]string) {
			out.paths, out.ids = append(out.paths, key), append(out.ids, q[key])
		}
	}
	return out, nil
}

func restrictedCeiling(t *testing.T, ids ...string) teamscope.Ceiling {
	t.Helper()
	keys, rows := make([]string, 0, len(ids)), ceilingKeyQuerier{}
	for i, id := range ids {
		key := "acme/r" + string(rune('a'+i))
		keys, rows[key] = append(keys, key), []string{id}
	}
	ceiling, err := teamscope.ResolveCeiling(context.Background(), rows, pepgate.Allowlist(ceilingOrgID, keys))
	if err != nil || !ceiling.Restricted() {
		t.Fatalf("ceiling: restricted=%t err=%v", ceiling.Restricted(), err)
	}
	return ceiling
}

type recordedStatement struct {
	statement string
	bindings  []dhclickhouse.Binding
}

// recordingClient records every statement and answers it with rows chosen by the caller (none by default).
type recordingClient struct {
	seen   []recordedStatement
	answer func(statement string) [][]any
}

func (c *recordingClient) Query(_ context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.seen = append(c.seen, recordedStatement{statement, bindings})
	var rows [][]any
	if c.answer != nil {
		rows = c.answer(statement)
	}
	return &fixtureRowScanner{rows: rows}, nil
}

func bindingStrings(bindings []dhclickhouse.Binding, name string) []string {
	for _, b := range bindings {
		if b.Name == name {
			values, _ := b.Value.([]string)
			return values
		}
	}
	return nil
}

func ceilingFilters(level string, ceiling teamscope.Ceiling, ids ...string) Filters {
	f, _ := signalReadFilters(level, ids...)
	f.Ceiling = ceiling
	return f
}

// acp (b): compounding risk serves rows of scope repo whose id is in the ceiling; a team request, and an empty
// ceiling, read nothing.
func TestCeilingRiskSignalsReadRepoRowsOfTheCeiling(t *testing.T) {
	_, now := signalReadFilters("org")
	_ = now
	ceiling := restrictedCeiling(t, ceilingRepoA)
	for _, level := range []string{"org", "repo"} {
		client := &recordingClient{}
		f := ceilingFilters(level, ceiling)
		if level == "repo" {
			f = ceilingFilters(level, ceiling, ceilingRepoA)
		}
		if _, err := fetchRiskSignals(context.Background(), client, f, day(2024, 1, 1), day(2024, 1, 8), ceilingOrgID); err != nil {
			t.Fatalf("%s: %v", level, err)
		}
		if len(client.seen) == 0 {
			t.Fatalf("%s: no risk statement ran", level)
		}
		for _, s := range client.seen {
			if strings.Count(s.statement, "scope = 'repo' AND scope_id IN {ceiling_ids:Array(String)}") != 2 ||
				!reflect.DeepEqual(bindingStrings(s.bindings, teamscope.BindingCeilingIDs), []string{ceilingRepoA}) {
				t.Fatalf("%s: the risk read is not held to repository rows of the ceiling (both reads of the statement):\n%s", level, s.statement)
			}
		}
	}
	for name, f := range map[string]Filters{
		"team request": ceilingFilters("team", ceiling, "T1"),
		"empty":        ceilingFilters("org", restrictedCeiling(t)),
	} {
		client := &recordingClient{}
		rows, err := fetchRiskSignals(context.Background(), client, f, day(2024, 1, 1), day(2024, 1, 8), ceilingOrgID)
		if err != nil || len(rows) != 0 || len(client.seen) != 0 {
			t.Fatalf("%s: rows %v err %v statements %d, want none", name, rows, err, len(client.seen))
		}
	}
}

// Without a ceiling the risk read is today's statement.
func TestCeilingRiskSignalsWithoutACeilingAreTodays(t *testing.T) {
	client := &recordingClient{}
	f := ceilingFilters("org", teamscope.NoCeiling())
	if _, err := fetchRiskSignals(context.Background(), client, f, day(2024, 1, 1), day(2024, 1, 8), ceilingOrgID); err != nil {
		t.Fatal(err)
	}
	for _, s := range client.seen {
		if strings.Contains(s.statement, "ceiling_") || strings.Contains(s.statement, "scope = 'repo'") {
			t.Fatalf("an unrestricted risk read carries a ceiling:\n%s", s.statement)
		}
	}
}

// acp (a): recommendations are not read for an allowlist caller.
func TestCeilingRecommendationsAreNotRead(t *testing.T) {
	client := &recordingClient{}
	rows, err := fetchRecommendationSignals(context.Background(), client, ceilingFilters("team", restrictedCeiling(t, ceilingRepoA), "T1"), day(2024, 1, 1), day(2024, 1, 8), ceilingOrgID)
	if err != nil || rows != nil || len(client.seen) != 0 {
		t.Fatalf("rows %v err %v statements %d, want none", rows, err, len(client.seen))
	}
}

func TestCeilingOmittedSignalsNameTheReason(t *testing.T) {
	if got := omittedSignals(ceilingFilters("org", teamscope.NoCeiling())); got != nil {
		t.Fatalf("an unrestricted caller omits %v", got)
	}
	got := omittedSignals(ceilingFilters("org", restrictedCeiling(t, ceilingRepoA)))
	want := []OmittedSignal{{"recommendations", "not scoped to repositories"}, {"compounding_risk_team", "not scoped to repositories"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("omitted = %v, want %v", got, want)
	}
}

// The attribution of signals counts items linked to the repositories of the view, never wi.repo_id.
func TestCeilingSignalAttributionIsReadThroughTheLink(t *testing.T) {
	client := &recordingClient{}
	f := ceilingFilters("org", restrictedCeiling(t, ceilingRepoA))
	if _, err := fetchSignalAttribution(context.Background(), client, f, day(2024, 1, 1), day(2024, 1, 8), ceilingOrgID, day(2024, 1, 8)); err != nil {
		t.Fatal(err)
	}
	if len(client.seen) != 1 {
		t.Fatalf("%d statements, want 1", len(client.seen))
	}
	s := client.seen[0]
	if strings.Contains(s.statement, "wi.repo_id") || !strings.Contains(s.statement, "wct.work_item_id IN (SELECT work_item_id FROM work_graph_issue_pr FINAL") ||
		!reflect.DeepEqual(bindingStrings(s.bindings, "link_repo_ids"), []string{ceilingRepoA}) {
		t.Fatalf("the attribution read is not held to linked items:\n%s", s.statement)
	}
}

// The repositories of the work-item view are the named ones the ceiling holds; a named repository outside it is never
// linked, and a team never widens the ceiling.
func TestCeilingLinkedRepoIDs(t *testing.T) {
	ceiling := restrictedCeiling(t, ceilingRepoA)
	client := &recordingClient{}
	ids, err := linkedRepoIDs(context.Background(), client, ceilingFilters("org", ceiling), ceilingOrgID, day(2024, 1, 8))
	if err != nil || !reflect.DeepEqual(ids, []string{ceilingRepoA}) || len(client.seen) != 0 {
		t.Fatalf("no named repository: ids %v err %v statements %d, want the ceiling and no statement", ids, err, len(client.seen))
	}
	for name, resolves := range map[string]string{"named repository outside the ceiling": ceilingRepoB, "named repository inside": ceilingRepoA} {
		client := &recordingClient{answer: func(statement string) [][]any {
			if strings.Contains(statement, "FROM repos") {
				return [][]any{{resolves}}
			}
			return nil
		}}
		ids, err := linkedRepoIDs(context.Background(), client, ceilingFilters("repo", ceiling, resolves), ceilingOrgID, day(2024, 1, 8))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if resolves == ceilingRepoB && len(ids) != 0 || resolves == ceilingRepoA && !reflect.DeepEqual(ids, []string{ceilingRepoA}) {
			t.Fatalf("%s: ids %v", name, ids)
		}
	}
	if ids, _ := linkedRepoIDs(context.Background(), &recordingClient{}, ceilingFilters("org", teamscope.NoCeiling()), ceilingOrgID, day(2024, 1, 8)); len(ids) != 0 {
		t.Fatalf("no ceiling and no named repository: ids %v, want none (today's answer)", ids)
	}
}

// A repository outside the ceiling is never named in the prose of the answer.
func TestCeilingScopeNamesNeverNameAHiddenRepository(t *testing.T) {
	client := &recordingClient{}
	f := ceilingFilters("repo", restrictedCeiling(t, ceilingRepoA), ceilingRepoA, ceilingRepoB)
	scopeNames(context.Background(), client, ceilingOrgID, f)
	for _, s := range client.seen {
		for _, b := range s.bindings {
			if values, ok := b.Value.([]string); ok {
				for _, v := range values {
					if v == ceilingRepoB {
						t.Fatalf("the hidden repository reached a name lookup (%s):\n%s", b.Name, s.statement)
					}
				}
			}
		}
	}
}

// The coverage and the basis of the repository view hold their counts to the view: an organization-wide count would
// tell a caller how many items live in repositories it may not read.
func TestCeilingCoverageAndBasisAreHeldToTheView(t *testing.T) {
	ceiling := restrictedCeiling(t, ceilingRepoA)
	for name, c := range map[string]teamscope.Ceiling{"ceiling": ceiling, "none": teamscope.NoCeiling()} {
		client := &recordingClient{}
		loader := &repoLinkedLoader{client: client, f: ceilingFilters("org", c), orgID: ceilingOrgID, repoIDs: []string{ceilingRepoA},
			compareStart: day(2024, 1, 1), endDay: day(2024, 1, 8)}
		view := &repoLinkedView{}
		if err := loader.readCoverage(context.Background(), "", view); err != nil {
			t.Fatal(err)
		}
		if err := loader.readBasis(context.Background(), "", view); err != nil {
			t.Fatal(err)
		}
		coverage, basis := client.seen[0].statement, client.seen[1].statement
		scoped := strings.Contains(coverage, "AND work_item_id IN (SELECT work_item_id FROM work_graph_issue_pr FINAL") && strings.Contains(basis, "uniqExactIf(repo_id, repo_id IN {link_repo_ids:Array(UUID)})")
		if (name == "ceiling") != scoped {
			t.Fatalf("%s: coverage and basis held to the view = %t\n%s\n%s", name, scoped, coverage, basis)
		}
	}
}
