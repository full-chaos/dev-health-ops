package investmentexplain

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-13090: the work-unit reads, the quote text of a unit that touches other repositories, the quote-title lookup
// and the explanation cache of #9 under a repository ceiling.

const (
	ceilingOrg  = "org-9555"
	ceilingRepo = "11111111-1111-1111-1111-111111111111"
)

type keyRows struct {
	paths []string
	ids   [][]string
	i     int
}

func (r *keyRows) Next() bool { r.i++; return r.i <= len(r.paths) }
func (r *keyRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.paths[r.i-1]
	*(dest[1].(*[]string)) = r.ids[r.i-1]
	return nil
}
func (r *keyRows) Err() error   { return nil }
func (r *keyRows) Close() error { return nil }

type keyQuerier map[string][]string

func (q keyQuerier) Query(_ context.Context, _ string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	out := &keyRows{}
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
	keys, rows := make([]string, 0, len(ids)), keyQuerier{}
	for i, id := range ids {
		key := "acme/r" + string(rune('a'+i))
		keys, rows[key] = append(keys, key), []string{id}
	}
	ceiling, err := teamscope.ResolveCeiling(context.Background(), rows, pepgate.Allowlist(ceilingOrg, keys))
	if err != nil || !ceiling.Restricted() {
		t.Fatalf("ceiling: restricted=%t err=%v", ceiling.Restricted(), err)
	}
	return ceiling
}

// recordingClient serves the golden fixtures, records every statement, answers the "units that touch other
// repositories" statement with mixed, and may replace the quote rows.
type recordingClient struct {
	mixed  []string
	quotes [][]any
	mu     sync.Mutex
	stmts  []string
	binds  [][]dhclickhouse.Binding
}

func (c *recordingClient) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.mu.Lock()
	c.stmts, c.binds = append(c.stmts, statement), append(c.binds, bindings)
	c.mu.Unlock()
	switch {
	case strings.Contains(statement, "touching_other_repos"):
		rows := make([][]any, 0, len(c.mixed))
		for _, id := range c.mixed {
			rows = append(rows, []any{id})
		}
		return &fixtureRowScanner{rows: rows}, nil
	case strings.Contains(statement, "FROM git_pull_requests"):
		return &fixtureRowScanner{}, nil
	case c.quotes != nil && strings.Contains(statement, "FROM work_unit_investment_quotes"):
		return &fixtureRowScanner{rows: c.quotes}, nil
	}
	return explainFixtureClient{}.Query(ctx, statement, bindings)
}

func (c *recordingClient) statementsContaining(part string) []int {
	var out []int
	for i, statement := range c.stmts {
		if strings.Contains(statement, part) {
			out = append(out, i)
		}
	}
	return out
}

func bindingIDs(bindings []dhclickhouse.Binding, name string) []string {
	for _, b := range bindings {
		if b.Name == name {
			ids, _ := b.Value.([]string)
			return ids
		}
	}
	return nil
}

func ceilingUnitOpts(ceiling teamscope.Ceiling) BuildWorkUnitInvestmentsOptions {
	return BuildWorkUnitInvestmentsOptions{
		OrgID: ceilingOrg, StartTS: time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC), EndTS: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Limit: 50, IncludeText: true, Ceiling: ceiling,
	}
}

// F3, Q3: under a ceiling the work-unit read takes the repository-allocation path (one row per unit and allowed
// repository, with the allocated effort), the ceiling is ANDed in as bound ids, and no unit-wide scope widens it.
func TestWorkUnits_AllocationPath_OnlyAllowedRepoEffort(t *testing.T) {
	client := &recordingClient{}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.BuildWorkUnitInvestments(context.Background(), ceilingUnitOpts(restrictedCeiling(t, ceilingRepo))); err != nil {
		t.Fatal(err)
	}
	units := client.statementsContaining("ORDER BY work_unit_count DESC")
	if len(units) != 1 {
		t.Fatalf("%d work-unit statements, want 1", len(units))
	}
	statement := client.stmts[units[0]]
	if !strings.Contains(statement, "allocation_weight") || strings.Contains(statement, "latest_repo_rows") {
		t.Fatalf("a ceiling read must use the allocation source, not the unit-wide scope:\n%s", statement)
	}
	if !strings.Contains(statement, "repo_id IN {ceiling_ids:Array(String)}") || !reflect.DeepEqual(bindingIDs(client.binds[units[0]], teamscope.BindingCeilingIDs), []string{ceilingRepo}) {
		t.Fatalf("ceiling term missing or wrong ids:\n%s", statement)
	}
}

func TestWorkUnits_NoCeiling_ReadsTheWholeUnit(t *testing.T) {
	client := &recordingClient{}
	reader, _ := NewReader(client)
	if _, err := reader.BuildWorkUnitInvestments(context.Background(), ceilingUnitOpts(teamscope.NoCeiling())); err != nil {
		t.Fatal(err)
	}
	for _, statement := range client.stmts {
		if strings.Contains(statement, "ceiling_") || strings.Contains(statement, "touching_other_repos") {
			t.Fatalf("an unrestricted read carries a ceiling:\n%s", statement)
		}
	}
}

// Q3 (acp, fail closed): the quote text of a work unit that touches ANY repository outside the ceiling is omitted;
// the unit itself (its allowed-repository allocation) is still served.
func TestWorkUnits_MixedUnit_QuoteTextOmitted(t *testing.T) {
	client := &recordingClient{mixed: []string{"unit-1"}}
	reader, _ := NewReader(client)
	got, err := reader.BuildWorkUnitInvestments(context.Background(), ceilingUnitOpts(restrictedCeiling(t, ceilingRepo)))
	if err != nil {
		t.Fatal(err)
	}
	var unit1 *WorkUnitInvestment
	for i := range got {
		if got[i].WorkUnitID == "unit-1" {
			unit1 = &got[i]
		}
	}
	if unit1 == nil {
		t.Fatalf("the mixed unit is not served at all: %d units", len(got))
	}
	if len(unit1.Evidence.Textual) != 0 {
		t.Fatalf("the quote text of a mixed unit is served: %v", unit1.Evidence.Textual)
	}
	mixed := client.statementsContaining("touching_other_repos")
	if len(mixed) != 1 || !reflect.DeepEqual(bindingIDs(client.binds[mixed[0]], teamscope.BindingCeilingIDs), []string{ceilingRepo}) {
		t.Fatalf("the mixed-unit read ran %d times or without the ceiling ids", len(mixed))
	}
}

// A unit whose every repository is allowed keeps its quote text.
func TestWorkUnits_UnitInsideTheCeiling_KeepsQuoteText(t *testing.T) {
	client := &recordingClient{}
	reader, _ := NewReader(client)
	got, err := reader.BuildWorkUnitInvestments(context.Background(), ceilingUnitOpts(restrictedCeiling(t, ceilingRepo)))
	if err != nil {
		t.Fatal(err)
	}
	for _, unit := range got {
		if unit.WorkUnitID == "unit-1" && len(unit.Evidence.Textual) != 1 {
			t.Fatalf("textual evidence = %v, want the unit's one quote", unit.Evidence.Textual)
		}
	}
}

// The pull-request title of a quote source is read inside the ceiling too.
func TestWorkUnits_QuoteTitleLookupCarriesTheCeiling(t *testing.T) {
	client := &recordingClient{quotes: [][]any{{"unit-1", "a quote", "pr", ceilingRepo + "#pr12", "run-1"}}}
	reader, _ := NewReader(client)
	if _, err := reader.BuildWorkUnitInvestments(context.Background(), ceilingUnitOpts(restrictedCeiling(t, ceilingRepo))); err != nil {
		t.Fatal(err)
	}
	titles := client.statementsContaining("FROM git_pull_requests")
	if len(titles) != 1 {
		t.Fatalf("%d title statements, want 1", len(titles))
	}
	if !strings.Contains(client.stmts[titles[0]], "{ceiling_ids:Array(String)}") || !reflect.DeepEqual(bindingIDs(client.binds[titles[0]], teamscope.BindingCeilingIDs), []string{ceilingRepo}) {
		t.Fatalf("the title lookup has no ceiling:\n%s", client.stmts[titles[0]])
	}
}

// --- #9 explanation cache ---

type recordingConn struct {
	mu      sync.Mutex
	queries []string
}

func (c *recordingConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.mu.Lock()
	c.queries = append(c.queries, query)
	c.mu.Unlock()
	return nil, errors.New("not persisted")
}

func (c *recordingConn) wrote(table string) bool {
	for _, query := range c.queries {
		if strings.Contains(query, "INSERT INTO "+table) {
			return true
		}
	}
	return false
}

type cacheRun struct {
	client mixRecorder
	conn   *recordingConn
	reads  int
	calls  int
}

type mixRecorder struct {
	mixAgreementClient
	mu    *sync.Mutex
	stmts *[]string
}

func (m mixRecorder) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	m.mu.Lock()
	*m.stmts = append(*m.stmts, statement)
	m.mu.Unlock()
	return m.mixAgreementClient.Query(ctx, statement, bindings)
}

func runExplain(t *testing.T, bypass bool) (cacheRun, []string) {
	t.Helper()
	var run cacheRun
	var stmts []string
	run.client = mixRecorder{mixAgreementClient: mixAgreementClient{breakdown: mixRows(), cachedJSON: cachedExplanationJSON(t, 10.0), cachedAt: explainFixtureNow, cacheReadCount: &run.reads}, mu: &sync.Mutex{}, stmts: &stmts}
	run.conn = &recordingConn{}
	writer, err := NewCacheWriter(run.conn)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(run.client)
	if err != nil {
		t.Fatal(err)
	}
	opts := explainOpts()
	opts.ForceRefresh = false
	opts.BypassCache = bypass
	if _, err := reader.ExplainInvestmentMix(context.Background(), writer, alwaysAvailable, countingComplete(completionWithKeys(10), &run.calls), opts); err != nil {
		t.Fatal(err)
	}
	return run, stmts
}

func hasVersionRead(stmts []string) bool {
	for _, statement := range stmts {
		if strings.Contains(statement, "toString(max(computed_at))") {
			return true
		}
	}
	return false
}

func TestCache_OrgScope_ReadsVersionReadsCacheAndWrites(t *testing.T) {
	run, stmts := runExplain(t, false)
	if !hasVersionRead(stmts) || run.reads != 1 {
		t.Fatalf("data-version read=%t cache reads=%d: the unrestricted path must stay as it was", hasVersionRead(stmts), run.reads)
	}
}

func TestCache_Allowlist_NoCacheRead(t *testing.T) {
	run, _ := runExplain(t, true)
	if run.reads != 0 || run.calls != 1 {
		t.Fatalf("cache reads=%d model calls=%d, want 0 reads and a live answer", run.reads, run.calls)
	}
}

func TestCache_Allowlist_NoCacheWrite_TokenUsageStillWritten(t *testing.T) {
	run, _ := runExplain(t, true)
	if run.conn.wrote("investment_explanations") {
		t.Fatalf("an allowlist answer was cached: %v", run.conn.queries)
	}
	if !run.conn.wrote("llm_token_usage") {
		t.Fatalf("the token-usage accounting write is gone: %v", run.conn.queries)
	}
}

func TestCache_Allowlist_NoOrgWideDataVersionRead(t *testing.T) {
	_, stmts := runExplain(t, true)
	if hasVersionRead(stmts) {
		t.Fatalf("an allowlist caller read the org-wide data version")
	}
}

// R1 (acp): under a ceiling the structural evidence of a unit that touches ANY repository outside it is OMITTED (the
// field is absent, not an empty list that reads as "no evidence"); a unit inside the ceiling keeps it; no ceiling
// serves every unit's, as before. The omission holds with and without quote text.
func TestWorkUnits_MixedUnit_StructuralEvidenceOmitted(t *testing.T) {
	for _, includeText := range []bool{true, false} {
		client := &recordingClient{mixed: []string{"unit-1"}}
		reader, _ := NewReader(client)
		opts := ceilingUnitOpts(restrictedCeiling(t, ceilingRepo))
		opts.IncludeText = includeText
		got, err := reader.BuildWorkUnitInvestments(context.Background(), opts)
		if err != nil {
			t.Fatal(err)
		}
		served := map[string]WorkUnitInvestment{}
		for _, unit := range got {
			served[unit.WorkUnitID] = unit
		}
		mixed, inside := served["unit-1"], served["unit-2"]
		if mixed.WorkUnitID == "" || inside.WorkUnitID == "" {
			t.Fatalf("includeText=%t: units served: %d", includeText, len(got))
		}
		if !mixed.Evidence.StructuralOmitted || len(mixed.Evidence.Structural) != 0 || len(mixed.Evidence.StructuralOrdered) != 0 {
			t.Fatalf("includeText=%t: the structural evidence of a mixed unit is served: %+v", includeText, mixed.Evidence)
		}
		if inside.Evidence.StructuralOmitted || len(inside.Evidence.Structural) != 1 || len(inside.Evidence.StructuralOrdered) != 1 {
			t.Fatalf("includeText=%t: a unit inside the ceiling lost its structural evidence: %+v", includeText, inside.Evidence)
		}
	}
}

func TestWorkUnits_NoCeiling_StructuralEvidenceServedForEveryUnit(t *testing.T) {
	client := &recordingClient{mixed: []string{"unit-1"}}
	reader, _ := NewReader(client)
	got, err := reader.BuildWorkUnitInvestments(context.Background(), ceilingUnitOpts(teamscope.NoCeiling()))
	if err != nil {
		t.Fatal(err)
	}
	for _, unit := range got {
		if unit.Evidence.StructuralOmitted || len(unit.Evidence.Structural) != 1 {
			t.Fatalf("%s: unrestricted read changed the structural evidence: %+v", unit.WorkUnitID, unit.Evidence)
		}
	}
}

// R1: the issue and pull-request references of the omitted structural evidence of a mixed unit are not read either.
func TestWorkUnits_MixedUnit_StructuralReferencesNotRead(t *testing.T) {
	client := &recordingClient{mixed: []string{"unit-1"}}
	reader, _ := NewReader(client)
	if _, err := reader.BuildWorkUnitInvestments(context.Background(), ceilingUnitOpts(restrictedCeiling(t, ceilingRepo))); err != nil {
		t.Fatal(err)
	}
	reads := client.statementsContaining("FROM work_item_team_attributions")
	if len(reads) == 0 {
		t.Fatal("no work-item lookup ran: the cell proves nothing")
	}
	var ids []string
	for _, i := range reads {
		ids = append(ids, bindingIDs(client.binds[i], "work_item_ids")...)
	}
	joined := strings.Join(ids, ",")
	if strings.Contains(joined, "issue-a") || !strings.Contains(joined, "issue-b") {
		t.Fatalf("work_item_ids = %v: the mixed unit's reference must be absent, the inside unit's present", ids)
	}
}
