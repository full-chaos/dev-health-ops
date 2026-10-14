package aianalytics

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/scopelabel"
)

type oracleDCase struct {
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	Limit  int    `json:"limit"`
	Window int    `json:"window"`
	Scope  *struct {
		RepoID *string `json:"repoId"`
		TeamID *string `json:"teamId"`
	} `json:"scope"`
	Impact   []map[string]any `json:"impact"`
	Rep      []map[string]any `json:"rep"`
	Dep      []map[string]any `json:"dep"`
	Mig      []map[string]any `json:"mig"`
	Drift    []map[string]any `json:"drift"`
	Flaky    []map[string]any `json:"flaky"`
	Repo     []map[string]any `json:"repo"`
	Team     []map[string]any `json:"team"`
	Fail     string           `json:"fail"`
	SlugRows []string         `json:"slugRows"`
	Calls    []map[string]any `json:"calls"`
	Expected map[string]any   `json:"expected"`
}

type fixtureClientD struct {
	c          oracleDCase
	statements []string
	bindings   [][]clickhouse.Binding

	// The two catalogue name reads of aiOpportunities (CHAOS-8114) are Go-only: the Python detector never
	// made them. They are answered and recorded APART from the detector reads, so the read-by-read
	// comparison with the Python calls (checkCallsD) and the statement pins still see exactly the detector
	// reads. With no rows set the catalogues are empty and every name is null, which is what a recorded
	// Python case compares with. opportunity_names_test.go sets them.
	catalogueTeams      [][]any // id, name, repo_patterns
	catalogueRepos      [][]any // repo_id, full_name
	catalogueTeamsErr   error
	catalogueReposErr   error
	catalogueStatements []string
	catalogueBindings   [][]clickhouse.Binding
}

// catalogueNameRead reports which catalogue a statement reads for display names: "teams", "repos" or "".
// The repository read is told from the slug lookup (also FROM repos) by its id-list binding. The teams read is
// the statement whose own source is the teams table; the team flow read only names it in the subquery of the
// active-team rule (FROM teams FINAL).
func catalogueNameRead(st string) string {
	switch {
	case strings.Contains(st, "FROM teams\n"):
		return "teams"
	case strings.Contains(st, "FROM repos") && strings.Contains(st, "{repo_ids:Array(String)}"):
		return "repos"
	}
	return ""
}

func (f *fixtureClientD) catalogueQuery(which, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	f.catalogueStatements = append(f.catalogueStatements, st)
	f.catalogueBindings = append(f.catalogueBindings, b)
	if which == "teams" {
		if f.catalogueTeamsErr != nil {
			return nil, f.catalogueTeamsErr
		}
		return &scriptedRows{rows: f.catalogueTeams}, nil
	}
	if f.catalogueReposErr != nil {
		return nil, f.catalogueReposErr
	}
	// Like the statement's IN predicate: only the repositories asked for.
	asked := map[string]bool{}
	for _, x := range b {
		if ids, ok := x.Value.([]string); ok && x.Name == "repo_ids" {
			for _, id := range ids {
				asked[id] = true
			}
		}
	}
	var rows [][]any
	for _, row := range f.catalogueRepos {
		if asked[row[0].(string)] {
			rows = append(rows, row)
		}
	}
	return &scriptedRows{rows: rows}, nil
}

func optF(m map[string]any, k string) any {
	if v, ok := m[k]; ok && v != nil {
		return v.(float64)
	}
	return nil
}

func strList(v any) []string {
	out := []string{}
	if l, ok := v.([]any); ok {
		for _, x := range l {
			out = append(out, x.(string))
		}
	}
	return out
}

func (f *fixtureClientD) Query(_ context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if which := catalogueNameRead(st); which != "" {
		return f.catalogueQuery(which, st, b)
	}
	f.statements = append(f.statements, st)
	f.bindings = append(f.bindings, b)
	c := f.c
	switch {
	case strings.Contains(st, "{slug:String}"):
		var rows [][]any
		for _, id := range c.SlugRows {
			rows = append(rows, []any{id})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "FROM ai_impact_metrics_daily"):
		var rows [][]any
		for _, m := range c.Impact {
			rows = append(rows, []any{m["team_id"].(string), m["repo_id"].(string), m["attribution_bucket"].(string), m["prs_total"].(float64),
				optF(m, "reviews_per_pr"), optF(m, "cycle_time_avg_hours"), m["rework_prs"].(float64), m["test_gap_prs"].(float64)})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "title_prefix"):
		var rows [][]any
		for _, m := range c.Rep {
			rows = append(rows, []any{m["repo_id"].(string), m["title_prefix"].(string), uint64(m["prs_total"].(float64)), strList(m["pr_refs"])})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "LEFT ANTI JOIN"):
		src := c.Mig
		if strings.Contains(st, "bump") {
			src = c.Dep
		}
		var rows [][]any
		for _, m := range src {
			rows = append(rows, []any{m["repo_id"].(string), uint64(m["prs_total"].(float64)), strList(m["pr_refs"])})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "FROM git_commits"):
		var rows [][]any
		for _, m := range c.Drift {
			rows = append(rows, []any{m["repo_id"].(string), uint64(m["code_commits"].(float64)), uint64(m["doc_changes"].(float64))})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "testops_test_metrics_daily"):
		var rows [][]any
		for _, m := range c.Flaky {
			rows = append(rows, []any{m["repo_id"].(string), uint64(m["cases_total"].(float64)), m["weighted_flake_rate"].(float64)})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "FROM repo_metrics_daily") || strings.Contains(st, "AS repo_metrics_daily"):
		if c.Fail == "repo" {
			return nil, errors.New("boom")
		}
		var rows [][]any
		for _, m := range c.Repo {
			rows = append(rows, []any{m["entity_id"].(string), uint64(m["data_days"].(float64)), optF(m, "pr_first_review_p50_hours"), optF(m, "pr_rework_ratio"), optF(m, "rework_churn_ratio_30d"), optF(m, "change_failure_rate")})
		}
		return &scriptedRows{rows: rows}, nil
	case strings.Contains(st, "work_item_metrics_daily"):
		if c.Fail == "team" {
			return nil, errors.New("boom")
		}
		var rows [][]any
		for _, m := range c.Team {
			rows = append(rows, []any{m["entity_id"].(string), uint64(m["data_days"].(float64)), optF(m, "cycle_time_p50_hours"), optF(m, "wip_congestion_ratio"), optF(m, "items_completed")})
		}
		return &scriptedRows{rows: rows}, nil
	}
	return nil, errors.New("fixtureClientD: unscripted statement: " + st)
}

// flowGoOnlyKeys are the only response keys the Go flow detector returns that the Python detector does
// not (CHAOS-7626): the measured value, the rule's threshold, their unit and the side of the threshold
// that fires. They are a declared Go-only extension of ImproveOpportunity (the Go plane owns the SDL;
// Python must only be a subset of it). Everything else is still compared whole against Python. The list
// is pinned by TestFlowGoOnlyKeysAreExactlyTheDeclaredFour: it cannot grow unnoticed.
var flowGoOnlyKeys = []string{"threshold", "thresholdDirection", "unit", "value"}

// checkAndStripFlowGoOnly requires every flow opportunity to carry the Go-only keys, typed, and removes
// them so the rest of the response can be compared with the Python one.
func checkAndStripFlowGoOnly(t *testing.T, got map[string]any) {
	t.Helper()
	list, _ := got["opportunities"].([]any)
	for i, item := range list {
		o, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("opportunities[%d] is not an object", i)
		}
		for _, key := range flowGoOnlyKeys {
			if _, present := o[key]; !present {
				t.Errorf("opportunities[%d] lacks the Go-only key %q", i, key)
			}
		}
		if _, ok := o["value"].(float64); !ok {
			t.Errorf("opportunities[%d].value = %T, want a number", i, o["value"])
		}
		if th, ok := o["threshold"].(float64); !ok || th <= 0 {
			t.Errorf("opportunities[%d].threshold = %v, want a positive number", i, o["threshold"])
		}
		switch o["unit"] {
		case "HOURS", "RATIO", "ITEMS":
		default:
			t.Errorf("opportunities[%d].unit = %v, want HOURS, RATIO or ITEMS", i, o["unit"])
		}
		switch o["thresholdDirection"] {
		case "ABOVE", "BELOW":
		default:
			t.Errorf("opportunities[%d].thresholdDirection = %v, want ABOVE or BELOW", i, o["thresholdDirection"])
		}
		for _, key := range flowGoOnlyKeys {
			delete(o, key)
		}
	}
}

// TestFlowGoOnlyKeysAreExactlyTheDeclaredFour fails if the strip list ever holds a key that is not one
// of the four CHAOS-7626 fields, so the Go-only allowance cannot grow quietly.
func TestFlowGoOnlyKeysAreExactlyTheDeclaredFour(t *testing.T) {
	want := []string{"threshold", "thresholdDirection", "unit", "value"}
	if !reflect.DeepEqual(flowGoOnlyKeys, want) {
		t.Fatalf("flowGoOnlyKeys = %v, want exactly %v", flowGoOnlyKeys, want)
	}
}

// TestOpportunityDetectors_MatchPythonDetectors replays captured Python
// cases of the AI opportunity detector and the flow detector -- every rule
// threshold, formatted rationale, score, ordering and clamp -- and requires
// the same response and the same scope and window arguments reaching each
// read.
func TestOpportunityDetectors_MatchPythonDetectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/oracle_d_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []oracleDCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Cases) == 0 {
		t.Fatal("no captured cases")
	}
	renamedChangeFailureEvidence := 0
	defer func() {
		if renamedChangeFailureEvidence == 0 {
			t.Error("no captured flow case carries change-failure evidence: the evidence rename below is untested")
		}
	}()
	for _, c := range doc.Cases {
		t.Run(c.Name, func(t *testing.T) {
			client := &fixtureClientD{c: c}
			var in *model.AIScopeInput
			if c.Scope != nil {
				in = &model.AIScopeInput{RepoID: c.Scope.RepoID, TeamID: c.Scope.TeamID}
			}
			var got any
			if c.Kind == "ai" {
				got, err = AiOpportunities(context.Background(), client, "org-1", in, c.Limit)
			} else {
				got, err = FlowOpportunities(context.Background(), client, "org-1", in, c.Limit, c.Window)
			}
			if err != nil {
				t.Fatal(err)
			}
			enc, _ := json.Marshal(got)
			var gotMap map[string]any
			_ = json.Unmarshal(enc, &gotMap)
			if c.Kind != "ai" {
				checkAndStripFlowGoOnly(t, gotMap)
				renamedChangeFailureEvidence += renameChangeFailureEvidence(c.Expected)
			}
			declareTitleNaming(t, gotMap, c.Expected, c.Kind)
			declareDrilldownRoots(t, gotMap, c.Expected)
			g, w := normalizeD(gotMap), normalizeD(c.Expected)
			if !reflect.DeepEqual(g, w) {
				gj, _ := json.MarshalIndent(g, "", " ")
				wj, _ := json.MarshalIndent(w, "", " ")
				t.Fatalf("go response differs from the python response\n--- go\n%s\n--- python\n%s", gj, wj)
			}
			checkCallsD(t, c, client)
		})
	}
}

func bindingNumber(v any) any {
	switch t := v.(type) {
	case uint32:
		return float64(t)
	case uint64:
		return float64(t)
	case string:
		if f, err := strconv.ParseFloat(t, 64); err == nil {
			return f
		}
	}
	return v
}

// checkCallsD compares the arguments Python's reads received with the
// bindings the Go statements carry, read by read, in the same order.
func checkCallsD(t *testing.T, c oracleDCase, f *fixtureClientD) {
	t.Helper()
	marker := map[string]string{
		"impact": "FROM ai_impact_metrics_daily", "repetitive": "title_prefix", "drift": "FROM git_commits",
		"flaky": "testops_test_metrics_daily", "repo": "metrics_daily", "team": "work_item_metrics_daily",
	}
	var reads []int
	for i, st := range f.statements {
		if strings.Contains(st, "{slug:String}") {
			continue
		}
		reads = append(reads, i)
	}
	if len(reads) != len(c.Calls) {
		t.Fatalf("go issued %d reads, python %d", len(reads), len(c.Calls))
	}
	for n, call := range c.Calls {
		st := f.statements[reads[n]]
		q := call["q"].(string)
		switch q {
		case "dep":
			if !strings.Contains(st, "bump") {
				t.Errorf("read %d is not the dependency rule", n)
			}
		case "mig":
			if !strings.Contains(st, "migrat") {
				t.Errorf("read %d is not the migration rule", n)
			}
		default:
			if m := marker[q]; m != "" && !strings.Contains(st, m) {
				t.Errorf("read %d (%s) does not match its statement", n, q)
			}
		}
		for k, want := range call {
			if k == "q" {
				continue
			}
			var got any
			for _, b := range f.bindings[reads[n]] {
				if b.Name == k {
					got = bindingNumber(b.Value)
				}
			}
			if list, ok := want.([]any); ok {
				_ = list
				continue
			}
			if got != want {
				t.Errorf("read %d (%s): %s = %v, python %v", n, q, k, got, want)
			}
		}
	}
}

// normalizeD drops null entries so an omitted field equals an explicit null.
func normalizeD(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, x := range t {
			if x == nil {
				continue
			}
			out[k] = normalizeD(x)
		}
		return out
	case []any:
		out := make([]any, 0, len(t))
		for _, x := range t {
			out = append(out, normalizeD(x))
		}
		return out
	}
	return v
}

// renameChangeFailureEvidence rewrites the Python capture's change-failure
// evidence reference to the table Go reads it from. CHAOS-8981 moved change
// failure rate to repo_change_failure_daily (incident-based, window counts);
// the rule, threshold, rationale and score are still compared with Python
// whole. It returns the number of references rewritten.
func renameChangeFailureEvidence(value any) int {
	const legacy = "repo_metrics_daily:change_failure_rate:"
	count := 0
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if text, ok := child.(string); ok && strings.HasPrefix(text, legacy) {
				typed[key] = changefailure.Table + ":change_failure_rate:" + strings.TrimPrefix(text, legacy)
				count++
				continue
			}
			count += renameChangeFailureEvidence(child)
		}
	case []any:
		for i, child := range typed {
			if text, ok := child.(string); ok && strings.HasPrefix(text, legacy) {
				typed[i] = changefailure.Table + ":change_failure_rate:" + strings.TrimPrefix(text, legacy)
				count++
				continue
			}
			count += renameChangeFailureEvidence(child)
		}
	}
	return count
}

// declareTitleNaming is the DECLARED divergence of D5945 (CHAOS-9205 class D5428: the product never shows a raw
// id): the recorded Python title ends in the entity's id ("High rework churn in <repo id>"); Go serves the entity
// NAME, or, with an empty catalogue as in these recorded cases, the unnamed wording (scopelabel.Unnamed*). The
// declaration is path-scoped: ONLY the title field of an opportunity, ONLY its trailing id. For each opportunity
// it asserts (1) the Python title ends in the id, (2) the Go title is the same words + the unnamed wording and
// holds the id nowhere; then it sets the recorded title to the Go one, so every other field is still compared
// whole. The frozen file is not edited.
func declareTitleNaming(t *testing.T, got, expected map[string]any, kind string) {
	t.Helper()
	key, idKey := "opportunities", "entityId"
	if kind == "ai" {
		key, idKey = "recommendations", "repoId"
	}
	gotList, _ := got[key].([]any)
	wantList, _ := expected[key].([]any)
	if len(gotList) != len(wantList) {
		return // the whole-response comparison reports the length difference
	}
	for i := range wantList {
		g, _ := gotList[i].(map[string]any)
		w, _ := wantList[i].(map[string]any)
		id, _ := w[idKey].(string)
		pyTitle, _ := w["title"].(string)
		goTitle, _ := g["title"].(string)
		if id == "" || !strings.HasSuffix(pyTitle, id) {
			t.Errorf("%s[%d]: the recorded title %q does not end in its entity id %q: the declaration does not apply", key, i, pyTitle, id)
			continue
		}
		unnamed := scopelabel.UnnamedRepository
		if et, _ := w["entityType"].(string); et == "team" {
			unnamed = scopelabel.UnnamedTeam
		}
		if want := strings.TrimSuffix(pyTitle, id) + unnamed; goTitle != want {
			t.Errorf("%s[%d]: title = %q, want %q (the words of the recorded title, then the unnamed wording)", key, i, goTitle, want)
		}
		if strings.Contains(goTitle, id) {
			t.Errorf("%s[%d]: the served title %q carries the id %q", key, i, goTitle, id)
		}
		w["title"] = goTitle
	}
}

var (
	recordedPRRoot     = regexp.MustCompile(`^([0-9a-f-]{36})#([0-9]+)$`)
	recordedMetricRoot = regexp.MustCompile(`^([a-z_]+)#([0-9a-f-]{36})$`)
)

// declareDrilldownRoots is the DECLARED divergence of CHAOS-9225 (class D5428 / D5945): the recorded Python
// builds every git_pull_requests:<a>:<b> evidence ref into a drilldown with the root "<a>#<b>". Two defects
// follow, and Go serves neither:
//   - a pull request chip's root id is "<repository uuid>#<number>", but the edge tables store
//     "<repository uuid>:<number>" and the drilldown walk matches it verbatim (0 edges on prod): Go serves
//     the stored form;
//   - a toil rule's METRIC ref (git_pull_requests:migration_prs:<repository uuid>) became a chip named
//     "PR <uuid>" with the root "migration_prs#<uuid>": Go serves no chip for it (the ref stays a key of
//     evidenceRefs).
//
// The declaration is path-scoped: ONLY the workGraphDrilldowns entries of the opportunities. Each recorded
// entry must be one of the two shapes above, else it is a failure (the declaration does not apply); the
// recorded list is then rewritten to what Go serves, so every other field is still compared whole. The
// frozen file is not edited.
func declareDrilldownRoots(t *testing.T, got, expected map[string]any) {
	t.Helper()
	for _, key := range []string{"opportunities", "recommendations"} {
		list, _ := expected[key].([]any)
		for i, item := range list {
			opportunity, _ := item.(map[string]any)
			recorded, _ := opportunity["workGraphDrilldowns"].([]any)
			if len(recorded) == 0 {
				continue
			}
			served := []any{}
			for j, entry := range recorded {
				drill, _ := entry.(map[string]any)
				root, _ := drill["rootId"].(string)
				label, _ := drill["label"].(string)
				if m := recordedPRRoot.FindStringSubmatch(root); m != nil && label == "PR "+m[2] {
					served = append(served, map[string]any{"rootType": drill["rootType"], "rootId": m[1] + ":" + m[2], "label": label})
					continue
				}
				if m := recordedMetricRoot.FindStringSubmatch(root); m != nil && label == "PR "+m[2] {
					continue // a metric ref: no chip
				}
				t.Errorf("%s[%d].workGraphDrilldowns[%d] = %v: neither a pull request root nor a metric ref root, the declaration does not apply", key, i, j, drill)
			}
			opportunity["workGraphDrilldowns"] = served
		}
	}
}
