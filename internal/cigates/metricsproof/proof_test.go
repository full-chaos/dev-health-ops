package metricsproof_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cigates/metricsproof"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/icfinalize"
)

const (
	orgA     = "org-11111111-aaaa"
	orgB     = "org-22222222-bbbb"
	repo     = "11111111-1111-1111-1111-111111111111"
	deadRepo = "99999999-9999-9999-9999-999999999999"
	team     = "team-alpha"
)

var (
	runStart = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	late     = runStart.Add(5 * time.Minute)
	early    = runStart.Add(-5 * time.Minute)
)

const timeLayout = "2006-01-02 15:04:05.000000"

type rec struct {
	org, repo, scope, scopeID, team, scopeKey, scopeType, identity string
	at                                                             time.Time
}

type fakeRows struct {
	driver.Rows
	rows [][]any
	idx  int
}

func (r *fakeRows) Next() bool   { r.idx++; return r.idx < len(r.rows) }
func (r *fakeRows) Close() error { return nil }
func (r *fakeRows) Err() error   { return nil }
func (r *fakeRows) Scan(dest ...any) error {
	row := r.rows[r.idx]
	if len(dest) != len(row) {
		return errors.New("column count mismatch")
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = row[i].(string)
		case *uint64:
			*p = row[i].(uint64)
		case *time.Time:
			*p = row[i].(time.Time)
		default:
			return errors.New("unsupported scan destination")
		}
	}
	return nil
}

// honestConn applies a predicate only when the SQL text literally names it, so a query that drops a predicate
// returns the rows real ClickHouse would return for it.
type honestConn struct {
	repos   map[string]string // repo id -> org id
	tables  map[string][]rec
	queries []string
}

var tableRe = regexp.MustCompile(`FROM (\w+)\b`)

func (c *honestConn) Query(_ context.Context, sql string, args ...any) (driver.Rows, error) {
	c.queries = append(c.queries, sql)
	params := map[string]any{}
	for _, a := range args {
		nv, ok := a.(driver.NamedValue)
		if !ok {
			return nil, errors.New("positional argument: every parameter must be named")
		}
		params[nv.Name] = nv.Value
	}
	if strings.Contains(sql, " FROM (") && strings.Contains(sql, "repos") {
		var ids []string
		for id, org := range c.repos {
			if org == params["org_id"] {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		var out [][]any
		for _, id := range ids {
			out = append(out, []any{id})
		}
		return &fakeRows{rows: out, idx: -1}, nil
	}
	m := tableRe.FindStringSubmatch(sql)
	if m == nil {
		return nil, errors.New("unrecognized query, no source table: " + sql)
	}
	rows := append([]rec(nil), c.tables[m[1]]...)
	filter := func(keep func(rec) bool) {
		var out []rec
		for _, r := range rows {
			if keep(r) {
				out = append(out, r)
			}
		}
		rows = out
	}
	if strings.Contains(sql, "org_id = {org_id") {
		filter(func(r rec) bool { return r.org == params["org_id"] })
	}
	if strings.Contains(sql, "repo_id IN {repo_ids") {
		allowed := toSet(params["repo_ids"])
		filter(func(r rec) bool { return allowed[r.repo] })
	}
	if strings.Contains(sql, "scope_id IN {repo_ids") {
		allowed := toSet(params["repo_ids"])
		filter(func(r rec) bool { return allowed[r.scopeID] })
	}
	if strings.Contains(sql, "scope_key IN {repo_ids") {
		allowed := toSet(params["repo_ids"])
		filter(func(r rec) bool { return allowed[r.scopeKey] })
	}
	if strings.Contains(sql, "scope = 'repo'") {
		filter(func(r rec) bool { return r.scope == "repo" })
	}
	if strings.Contains(sql, "scope = 'team'") {
		filter(func(r rec) bool { return r.scope == "team" })
	}
	if strings.Contains(sql, "scope_type = 'repo'") {
		filter(func(r rec) bool { return r.scopeType == "repo" })
	}
	if strings.Contains(sql, "computed_at >= {run_start") {
		lit, ok := params["run_start"].(string)
		if !ok {
			return nil, errors.New("run_start must be bound as a DateTime64 literal string")
		}
		at, err := time.ParseInLocation(timeLayout, lit, time.UTC)
		if err != nil {
			return nil, err
		}
		filter(func(r rec) bool { return !r.at.Before(at) })
	}
	switch {
	case strings.Contains(sql, "DISTINCT repo_id"):
		return distinct(rows, func(r rec) string { return r.repo }), nil
	case strings.Contains(sql, "DISTINCT scope_id"):
		return distinct(rows, func(r rec) string { return r.scopeID }), nil
	case strings.Contains(sql, "GROUP BY repo_id, "):
		type key struct{ repo, identity string }
		counts := map[key]uint64{}
		for _, r := range rows {
			counts[key{r.repo, r.identity}]++
		}
		var keys []key
		for k := range counts {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].repo+keys[i].identity < keys[j].repo+keys[j].identity })
		var out [][]any
		for _, k := range keys {
			out = append(out, []any{k.repo, k.identity, counts[k]})
		}
		return &fakeRows{rows: out, idx: -1}, nil
	case strings.Contains(sql, "GROUP BY repo_id"):
		return grouped(rows, func(r rec) string { return r.repo }), nil
	case strings.Contains(sql, "GROUP BY scope_id"):
		return grouped(rows, func(r rec) string { return r.scopeID }), nil
	case strings.Contains(sql, "GROUP BY scope_key"):
		return grouped(rows, func(r rec) string { return r.scopeKey }), nil
	case strings.Contains(sql, "GROUP BY team_id"):
		return grouped(rows, func(r rec) string { return r.team }), nil
	}
	return nil, errors.New("unexpected query shape: " + sql)
}

func toSet(v any) map[string]bool {
	out := map[string]bool{}
	for _, s := range v.([]string) {
		out[s] = true
	}
	return out
}

func distinct(rows []rec, key func(rec) string) driver.Rows {
	seen := map[string]bool{}
	var ids []string
	for _, r := range rows {
		if k := key(r); !seen[k] {
			seen[k] = true
			ids = append(ids, k)
		}
	}
	sort.Strings(ids)
	var out [][]any
	for _, id := range ids {
		out = append(out, []any{id})
	}
	return &fakeRows{rows: out, idx: -1}
}

func grouped(rows []rec, key func(rec) string) driver.Rows {
	counts := map[string]uint64{}
	latest := map[string]time.Time{}
	for _, r := range rows {
		k := key(r)
		counts[k]++
		if r.at.After(latest[k]) {
			latest[k] = r.at
		}
	}
	var keys []string
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out [][]any
	for _, k := range keys {
		out = append(out, []any{k, counts[k], latest[k]})
	}
	return &fakeRows{rows: out, idx: -1}
}

func ctx() context.Context { return context.Background() }

func TestCrossOrgRowsDoNotSatisfyReadback(t *testing.T) {
	c := &honestConn{repos: map[string]string{repo: orgA}, tables: map[string][]rec{
		"cicd_metrics_daily": {{org: orgB, repo: repo, at: late}},
	}}
	ids, err := metricsproof.LiveRepoIDs(ctx(), c, orgA)
	if err != nil || len(ids) != 1 {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
	got, err := metricsproof.FamilyReadback(ctx(), c, "cicd_metrics_daily", orgA, ids, runStart)
	if err != nil || len(got) != 0 {
		t.Fatalf("another org's rows for a repo the target org owns must not count: %v err=%v", got, err)
	}
}

func TestCorrectOrgRowsPass(t *testing.T) {
	c := &honestConn{repos: map[string]string{repo: orgA}, tables: map[string][]rec{
		"cicd_metrics_daily": {{org: orgA, repo: repo, at: late}},
	}}
	ids, _ := metricsproof.LiveRepoIDs(ctx(), c, orgA)
	got, err := metricsproof.FamilyReadback(ctx(), c, "cicd_metrics_daily", orgA, ids, runStart)
	want := map[string]metricsproof.FamilyRowCount{repo: {Rows: 1, LatestComputedAt: late}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v err=%v", got, want, err)
	}
}

func TestStaleRowsBeforeRunStartDoNotSatisfyReadback(t *testing.T) {
	c := &honestConn{repos: map[string]string{repo: orgA}, tables: map[string][]rec{
		"cicd_metrics_daily": {{org: orgA, repo: repo, at: early}},
	}}
	ids, _ := metricsproof.LiveRepoIDs(ctx(), c, orgA)
	got, err := metricsproof.FamilyReadback(ctx(), c, "cicd_metrics_daily", orgA, ids, runStart)
	if err != nil || len(got) != 0 {
		t.Fatalf("a row from before the run is not evidence: %v err=%v", got, err)
	}
}

func TestEmptyRepoSetReadsNothing(t *testing.T) {
	c := &honestConn{}
	got, err := metricsproof.FamilyReadback(ctx(), c, "cicd_metrics_daily", orgA, metricsproof.IDSet{}, runStart)
	if err != nil || len(got) != 0 || len(c.queries) != 0 {
		t.Fatalf("an empty repo set must return {} without a query: %v err=%v queries=%d", got, err, len(c.queries))
	}
}

func TestScopeIDTeamReadbackExcludesRepoScopeRows(t *testing.T) {
	c := &honestConn{tables: map[string][]rec{"compounding_risk_daily": {
		{org: orgA, scope: "repo", scopeID: "some-repo", at: late},
		{org: orgA, scope: "team", scopeID: team, at: late},
	}}}
	got, err := metricsproof.ScopeIDTeamReadback(ctx(), c, "compounding_risk_daily", orgA, runStart)
	want := map[string]metricsproof.FamilyRowCount{team: {Rows: 1, LatestComputedAt: late}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v err=%v", got, want, err)
	}
}

func TestScopeIDTeamReadbackIsOrgScopedAndRunGated(t *testing.T) {
	c := &honestConn{tables: map[string][]rec{"compounding_risk_daily": {
		{org: orgB, scope: "team", scopeID: team, at: late},
		{org: orgA, scope: "team", scopeID: team, at: early},
	}}}
	got, err := metricsproof.ScopeIDTeamReadback(ctx(), c, "compounding_risk_daily", orgA, runStart)
	if err != nil || len(got) != 0 {
		t.Fatalf("foreign-org and stale rows must not satisfy: %v err=%v", got, err)
	}
}

func TestScopeIDRepoReadbackPinsRepoScope(t *testing.T) {
	c := &honestConn{tables: map[string][]rec{"compounding_risk_daily": {
		{org: orgA, scope: "team", scopeID: repo, at: late},
		{org: orgA, scope: "repo", scopeID: repo, at: late},
	}}}
	got, err := metricsproof.ScopeIDRepoReadback(ctx(), c, "compounding_risk_daily", orgA, metricsproof.IDSet{repo: {}}, runStart)
	if err != nil || got[repo].Rows != 1 {
		t.Fatalf("a team-scope row must not stand in for the repo path: %v err=%v", got, err)
	}
	stray, err := metricsproof.UnscopedScopeIDs(ctx(), c, "compounding_risk_daily", runStart)
	if err != nil || len(stray) != 1 {
		t.Fatalf("stray ids %v err=%v", stray, err)
	}
}

func TestScopeKeyRepoReadbackPinsRepoScopeType(t *testing.T) {
	c := &honestConn{tables: map[string][]rec{"testops_metric_baselines": {
		{org: orgA, scopeType: "team", scopeKey: repo, at: late},
		{org: orgA, scopeType: "repo", scopeKey: repo, at: late},
	}}}
	got, err := metricsproof.ScopeKeyRepoReadback(ctx(), c, "testops_metric_baselines", orgA, metricsproof.IDSet{repo: {}}, runStart)
	if err != nil || got[repo].Rows != 1 {
		t.Fatalf("got %v err=%v", got, err)
	}
}

func TestTeamReadbackIsOrgScoped(t *testing.T) {
	c := &honestConn{tables: map[string][]rec{"team_metrics_daily": {
		{org: orgB, team: "unassigned", at: late},
		{org: orgA, team: "unassigned", at: late},
	}}}
	got, err := metricsproof.TeamReadback(ctx(), c, "team_metrics_daily", orgA, runStart)
	if err != nil || got["unassigned"].Rows != 1 {
		t.Fatalf("got %v err=%v", got, err)
	}
}

func TestUnscopedRepoIDsIgnoreOrgButHonourRunStart(t *testing.T) {
	c := &honestConn{tables: map[string][]rec{"cicd_metrics_daily": {
		{org: orgB, repo: deadRepo, at: late},
		{org: orgA, repo: repo, at: early},
	}}}
	got, err := metricsproof.UnscopedRepoIDs(ctx(), c, "cicd_metrics_daily", runStart)
	if err != nil || len(got) != 1 {
		t.Fatalf("got %v err=%v", got, err)
	}
	if _, ok := got[deadRepo]; !ok {
		t.Fatalf("got %v", got)
	}
}

func TestSynthesizedRepoIDIsDeterministicAndScoped(t *testing.T) {
	first := metricsproof.SynthesizedRepoID(orgA, "wi-only@example.com")
	if first != metricsproof.SynthesizedRepoID(orgA, "wi-only@example.com") {
		t.Fatal("must be stable across calls")
	}
	if first == metricsproof.SynthesizedRepoID("org-other", "wi-only@example.com") {
		t.Fatal("must separate orgs")
	}
	if first == metricsproof.SynthesizedRepoID(orgA, "someone-else@example.com") {
		t.Fatal("must separate identities")
	}
	for _, pair := range [][2]string{{orgA, "wi-only@example.com"}, {"o", "i"}, {"", ""}, {orgB, "ünï@example.com"}} {
		if got, want := metricsproof.SynthesizedRepoID(pair[0], pair[1]), icfinalize.SynthesizedRepoID(pair[0], pair[1]).String(); got != want {
			t.Fatalf("drift from the ic_finalize writer for %q: got %s want %s", pair, got, want)
		}
	}
}

func TestSynthesizedRowAloneIsCountedNotStray(t *testing.T) {
	identity := "wi-only@example.com"
	syn := metricsproof.SynthesizedRepoID(orgA, identity)
	c := &honestConn{tables: map[string][]rec{"user_metrics_daily": {
		{org: orgA, repo: syn, identity: identity, at: late},
		{org: orgA, repo: syn, identity: identity, at: late},
		{org: orgA, repo: syn, identity: identity, at: late},
	}}}
	total, stray, err := metricsproof.SynthesizedRepoReadback(ctx(), c, "user_metrics_daily", "author_email", orgA, metricsproof.IDSet{repo: {}}, runStart)
	if err != nil || total != 3 || len(stray) != 0 {
		t.Fatalf("total=%d stray=%v err=%v", total, stray, err)
	}
}

func TestGitBackedRowCountsViaLiveSet(t *testing.T) {
	c := &honestConn{tables: map[string][]rec{"user_metrics_daily": {
		{org: orgA, repo: repo, identity: "git-user@example.com", at: late},
		{org: orgA, repo: repo, identity: "git-user@example.com", at: late},
	}}}
	total, stray, err := metricsproof.SynthesizedRepoReadback(ctx(), c, "user_metrics_daily", "author_email", orgA, metricsproof.IDSet{repo: {}}, runStart)
	if err != nil || total != 2 || len(stray) != 0 {
		t.Fatalf("total=%d stray=%v err=%v", total, stray, err)
	}
}

func TestTrulyDeadRepoIDStaysStray(t *testing.T) {
	c := &honestConn{tables: map[string][]rec{"user_metrics_daily": {
		{org: orgA, repo: deadRepo, identity: "someone@example.com", at: late},
	}}}
	total, stray, err := metricsproof.SynthesizedRepoReadback(ctx(), c, "user_metrics_daily", "author_email", orgA, metricsproof.IDSet{repo: {}}, runStart)
	if _, ok := stray[deadRepo]; err != nil || total != 1 || !ok || len(stray) != 1 {
		t.Fatalf("total=%d stray=%v err=%v", total, stray, err)
	}
}

func TestFamilyShapesAreDisjoint(t *testing.T) {
	shapes := map[string]map[string]bool{}
	add := func(shape string, names []string) {
		for _, n := range names {
			if shapes[n] == nil {
				shapes[n] = map[string]bool{}
			}
			shapes[n][shape] = true
		}
	}
	keys := func(m map[string]string) []string {
		var out []string
		for k := range m {
			out = append(out, k)
		}
		return out
	}
	add("repo_day", keys(metricsproof.RepoDayFamilies))
	add("team_day", keys(metricsproof.TeamDayFamilies))
	add("scope_id_repo", keys(metricsproof.ScopeIDRepoFamilies))
	add("scope_id_team", keys(metricsproof.ScopeIDTeamFamilies))
	add("scope_key", keys(metricsproof.ScopeKeyFamilies))
	var synth []string
	for k := range metricsproof.SynthesizedRepoIDFamilies {
		synth = append(synth, k)
	}
	add("synth", synth)
	for name, in := range shapes {
		if len(in) != 1 {
			t.Errorf("family %q is registered in more than one shape: %v", name, in)
		}
	}
	if _, ok := metricsproof.SynthesizedRepoIDFamilies["ic_finalize"]; !ok {
		t.Error("ic_finalize must be a synthesized-repo-id family")
	}
	if _, ok := metricsproof.ScopeIDTeamFamilies["compounding_risk_team"]; !ok {
		t.Error("compounding_risk_team must be scope-id-team shaped")
	}
	if metricsproof.RepoDayFamilies["complexity"] != "repo_complexity_daily" || metricsproof.RepoDayFamilies["file_hotspots"] != "file_metrics_daily" {
		t.Error("complexity and file_hotspots must each check the table their own compute writes")
	}
}

func opener(c metricsproof.Conn, gotURI *string) metricsproof.Opener {
	return func(_ context.Context, uri string) (metricsproof.Conn, func() error, error) {
		if gotURI != nil {
			*gotURI = uri
		}
		return c, func() error { return nil }, nil
	}
}

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func runCLI(t *testing.T, c metricsproof.Conn, environ map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	rc := metricsproof.Run(args, env(environ), opener(c, nil), &out, &errb)
	return rc, out.String(), errb.String()
}

func baseArgs(families ...string) []string {
	return append([]string{"--clickhouse-uri", "fake://unused", "--org-id", orgA, "--run-start", runStart.Format(time.RFC3339), "--families"}, families...)
}

func TestCLIZeroRowsForTargetOrgFails(t *testing.T) {
	c := &honestConn{repos: map[string]string{repo: orgA}, tables: map[string][]rec{"cicd_metrics_daily": nil}}
	rc, _, stderr := runCLI(t, c, nil, baseArgs("cicd")...)
	if rc != 1 || !strings.Contains(stderr, "zero_rows_with_source_data") {
		t.Fatalf("rc=%d stderr=%q", rc, stderr)
	}
}

func TestCLICrossOrgOnlyRowsFail(t *testing.T) {
	c := &honestConn{repos: map[string]string{repo: orgA}, tables: map[string][]rec{
		"cicd_metrics_daily": {{org: orgB, repo: repo, at: late}},
	}}
	rc, _, stderr := runCLI(t, c, nil, baseArgs("cicd")...)
	if rc != 1 || !strings.Contains(stderr, "zero_rows_with_source_data") {
		t.Fatalf("rc=%d stderr=%q", rc, stderr)
	}
}

// Parity pair: the stdout below is the exact output of ci/assert_metrics_executed_proof.py (json.dumps indent=2,
// then the OK line) on the same recorded input, as run on the base commit.
func TestCLIOutputParityRecordedPair(t *testing.T) {
	c := &honestConn{repos: map[string]string{repo: orgA}, tables: map[string][]rec{
		"cicd_metrics_daily": {{org: orgA, repo: repo, at: late}},
	}}
	rc, stdout, stderr := runCLI(t, c, nil, baseArgs("cicd")...)
	want := `{
  "cicd": {
    "table": "cicd_metrics_daily",
    "org_id": "org-11111111-aaaa",
    "rows_written": 1,
    "repos_with_rows": [
      "11111111-1111-1111-1111-111111111111"
    ],
    "repo_ids_outside_org": []
  }
}

OK: 1 family(ies) produced rows for org org-11111111-aaaa, all within its 1 live repo id(s).
`
	if rc != 0 || stdout != want || stderr != "" {
		t.Fatalf("rc=%d stderr=%q\n got: %q\nwant: %q", rc, stderr, stdout, want)
	}
}

func TestCLISummaryJSONFileMatchesStdoutJSON(t *testing.T) {
	c := &honestConn{repos: map[string]string{repo: orgA}, tables: map[string][]rec{
		"cicd_metrics_daily": {{org: orgA, repo: repo, at: late}},
	}}
	path := filepath.Join(t.TempDir(), "summary.json")
	rc, stdout, stderr := runCLI(t, c, nil, append(baseArgs("cicd"), "--summary-json", path)...)
	raw, err := os.ReadFile(path)
	if rc != 0 || err != nil || stderr != "" {
		t.Fatalf("rc=%d err=%v stderr=%q", rc, err, stderr)
	}
	if !strings.HasPrefix(stdout, string(raw)+"\n") || strings.HasSuffix(string(raw), "\n") {
		t.Fatalf("file %q is not the stdout JSON without a trailing newline", raw)
	}
}

func TestCLIFamilyOrderFollowsArguments(t *testing.T) {
	c := &honestConn{repos: map[string]string{repo: orgA}, tables: map[string][]rec{
		"cicd_metrics_daily":   {{org: orgA, repo: repo, at: late}},
		"deploy_metrics_daily": {{org: orgA, repo: repo, at: late}},
	}}
	rc, stdout, _ := runCLI(t, c, nil, baseArgs("deploy", "cicd")...)
	if rc != 0 || strings.Index(stdout, `"deploy"`) > strings.Index(stdout, `"cicd"`) {
		t.Fatalf("rc=%d stdout=%q", rc, stdout)
	}
}

func TestCLIAcceptsIcFinalize(t *testing.T) {
	identity := "wi-only@example.com"
	syn := metricsproof.SynthesizedRepoID(orgA, identity)
	c := &honestConn{repos: map[string]string{repo: orgA}, tables: map[string][]rec{
		"user_metrics_daily": {{org: orgA, repo: syn, identity: identity, at: late}},
	}}
	rc, stdout, stderr := runCLI(t, c, nil, baseArgs("ic_finalize")...)
	if rc != 0 || !strings.Contains(stdout, `"ic_finalize"`) {
		t.Fatalf("rc=%d stderr=%q", rc, stderr)
	}
}

func TestCLIIcFinalizeDeadIDFails(t *testing.T) {
	c := &honestConn{repos: map[string]string{repo: orgA}, tables: map[string][]rec{
		"user_metrics_daily": {{org: orgA, repo: deadRepo, identity: "x@example.com", at: late}},
	}}
	rc, _, stderr := runCLI(t, c, nil, baseArgs("ic_finalize")...)
	if rc != 1 || !strings.Contains(stderr, "CHAOS-4263 dead-id shape") {
		t.Fatalf("rc=%d stderr=%q", rc, stderr)
	}
}

func TestCLIAcceptsCompoundingRiskTeam(t *testing.T) {
	c := &honestConn{repos: map[string]string{repo: orgA}, tables: map[string][]rec{
		"compounding_risk_daily": {{org: orgA, scope: "team", scopeID: team, at: late}},
	}}
	rc, stdout, stderr := runCLI(t, c, nil, baseArgs("compounding_risk_team")...)
	if rc != 0 || !strings.Contains(stdout, `"compounding_risk_team"`) {
		t.Fatalf("rc=%d stderr=%q", rc, stderr)
	}
}

func TestCLICompoundingRiskTeamZeroRowsFails(t *testing.T) {
	c := &honestConn{repos: map[string]string{repo: orgA}}
	rc, _, stderr := runCLI(t, c, nil, baseArgs("compounding_risk_team")...)
	if rc != 1 || !strings.Contains(stderr, "zero_rows_with_source_data") {
		t.Fatalf("rc=%d stderr=%q", rc, stderr)
	}
}

func TestCLIOrgWithNoReposFails(t *testing.T) {
	c := &honestConn{}
	rc, _, stderr := runCLI(t, c, nil, baseArgs("cicd")...)
	if rc != 1 || !strings.Contains(stderr, "has no rows in ClickHouse repos") {
		t.Fatalf("rc=%d stderr=%q", rc, stderr)
	}
}

func TestCLIStrayRepoIDFails(t *testing.T) {
	c := &honestConn{repos: map[string]string{repo: orgA}, tables: map[string][]rec{
		"cicd_metrics_daily": {{org: orgA, repo: repo, at: late}, {org: orgB, repo: deadRepo, at: late}},
	}}
	rc, _, stderr := runCLI(t, c, nil, baseArgs("cicd")...)
	if rc != 1 || !strings.Contains(stderr, "CHAOS-4263 dead-id shape") {
		t.Fatalf("rc=%d stderr=%q", rc, stderr)
	}
}

func TestCLIDSNFromEnvironmentKeepsCredentialOutOfOutput(t *testing.T) {
	const secret = "s3cret-pw"
	dsn := "clickhouse://u:" + secret + "@host:9000/db"
	c := &honestConn{repos: map[string]string{repo: orgA}, tables: map[string][]rec{
		"cicd_metrics_daily": {{org: orgA, repo: repo, at: late}},
	}}
	var got string
	var out, errb bytes.Buffer
	args := []string{"--org-id", orgA, "--run-start", runStart.Format(time.RFC3339), "--families", "cicd"}
	rc := metricsproof.Run(args, env(map[string]string{"CLICKHOUSE_URI": dsn}), opener(c, &got), &out, &errb)
	if rc != 0 || got != dsn {
		t.Fatalf("rc=%d the DSN from CLICKHOUSE_URI did not reach the opener: %q", rc, got)
	}
	if strings.Contains(out.String()+errb.String(), secret) {
		t.Fatal("the credential leaked into output")
	}
}

func TestCLIRequiresDSNFromFlagOrEnvironment(t *testing.T) {
	rc, _, stderr := runCLI(t, &honestConn{}, nil, "--org-id", orgA, "--run-start", runStart.Format(time.RFC3339))
	if rc != 2 || !strings.Contains(stderr, "--clickhouse-uri or CLICKHOUSE_URI is required") {
		t.Fatalf("rc=%d stderr=%q", rc, stderr)
	}
}

func TestCLIUsageErrorsExitTwo(t *testing.T) {
	for name, args := range map[string][]string{
		"unknown family":    {"--clickhouse-uri", "x", "--org-id", orgA, "--run-start", "2026-09-01T00:00:00+00:00", "--families", "nope"},
		"missing org":       {"--clickhouse-uri", "x", "--run-start", "2026-09-01T00:00:00+00:00"},
		"missing run-start": {"--clickhouse-uri", "x", "--org-id", orgA},
		"bad run-start":     {"--clickhouse-uri", "x", "--org-id", orgA, "--run-start", "yesterday"},
		"unknown flag":      {"--clickhouse-uri", "x", "--org-id", orgA, "--run-start", "2026-09-01T00:00:00+00:00", "--bogus"},
		"empty families":    {"--clickhouse-uri", "x", "--org-id", orgA, "--run-start", "2026-09-01T00:00:00+00:00", "--families"},
	} {
		if rc, _, stderr := runCLI(t, &honestConn{}, nil, args...); rc != 2 || stderr == "" {
			t.Errorf("%s: rc=%d stderr=%q", name, rc, stderr)
		}
	}
}

func TestRunStartAcceptsTheFormsTheCallersEmit(t *testing.T) {
	c := &honestConn{repos: map[string]string{repo: orgA}, tables: map[string][]rec{
		"cicd_metrics_daily": {{org: orgA, repo: repo, at: runStart.Add(500 * time.Microsecond)}},
	}}
	for _, stamp := range []string{
		"2026-09-01T00:00:00.000000+00:00",
		"2026-09-01T00:00:00.000500+00:00",
		"2026-09-01T00:00:00Z",
		"2026-09-01T00:00:00",
		"2026-09-01 00:00:00+00:00",
	} {
		args := []string{"--clickhouse-uri", "x", "--org-id", orgA, "--run-start", stamp, "--families", "cicd"}
		if rc, _, stderr := runCLI(t, c, nil, args...); rc != 0 {
			t.Errorf("%s: rc=%d stderr=%q", stamp, rc, stderr)
		}
	}
	args := []string{"--clickhouse-uri", "x", "--org-id", orgA, "--run-start", "2026-09-01T00:00:00.001000+00:00", "--families", "cicd"}
	if rc, _, _ := runCLI(t, c, nil, args...); rc != 1 {
		t.Errorf("a run start after the row must fail, rc=%d", rc)
	}
}

func TestRunnerCallsTheGoGateWithTheDSNInTheEnvironment(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "ci", "run_metrics_executed_proof.sh"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, "metrics-proof") || strings.Contains(text, "assert_metrics_executed_proof.py") {
		t.Fatal("run_metrics_executed_proof.sh must call `cigate metrics-proof`, not the deleted script")
	}
	if strings.Contains(text, "python3") {
		t.Fatal("run_metrics_executed_proof.sh must not call python3")
	}
	re := regexp.MustCompile(`(?s)assert_readback\(\) \{.*?\n\}`)
	body := re.FindString(text)
	if body == "" {
		t.Fatal("assert_readback function not found")
	}
	if !strings.Contains(body, `CLICKHOUSE_URI="${CLICKHOUSE_URI_NATIVE}"`) {
		t.Fatal("the DSN must reach the gate by the CLICKHOUSE_URI environment variable (native DSN)")
	}
	if strings.Contains(body, "--clickhouse-uri") {
		t.Fatal("the DSN can hold a password: never pass it as an argument")
	}
}

// knownUncovered pins native golden-required families the executed-proof gate does not name yet. Each is a gap, not
// an exemption: a change to this set must be deliberate. Reasons: the E2E seed drives only the cicd, deployments,
// incidents and tests sync targets plus `fixtures generate`; families whose inputs (ai_attribution, git_pull_requests,
// repo_complexity_daily, work_items for the measure family) it does not seed cannot produce rows there.
var knownUncovered = map[string]bool{
	"file_hotspots": true, "file_risk_hotspots": true, "work_item_state": true, "incident": true, "testops_risk": true,
	"work_item": true, "work_item_estimate": true, "work_item_attribution": true, "ai_governance": true,
	"review_edges": true, "benchmarking": true, "work_graph_edges": true, "ai_workflow": true, "ai_impact": true,
	"team_complexity": true, "work_item_measure_capability": true,
}

var familiesFlag = regexp.MustCompile(`--families[ \t]+((?:[^\n\\]|\\\n)*)`)

func TestNativeGoldenRequiredFamiliesAreNamedInTheGate(t *testing.T) {
	rawFamilies, err := os.ReadFile(filepath.Join("..", "..", "jobs", "metrics", "daily", "families.json"))
	if err != nil {
		t.Fatal(err)
	}
	nativeRequired := map[string]bool{}
	for _, f := range parseFamilies(t, rawFamilies) {
		if f.Golden == "required" && f.Port == "go" {
			nativeRequired[f.Name] = true
		}
	}
	if len(nativeRequired) == 0 {
		t.Fatal("found NO native golden-required families -- the enumeration is broken")
	}
	script, err := os.ReadFile(filepath.Join("..", "..", "..", "ci", "run_metrics_executed_proof.sh"))
	if err != nil {
		t.Fatal(err)
	}
	m := familiesFlag.FindStringSubmatch(string(script))
	if m == nil {
		t.Fatal("no --families flag found in run_metrics_executed_proof.sh")
	}
	proof := map[string]bool{}
	for _, tok := range strings.Fields(strings.ReplaceAll(m[1], "\\\n", " ")) {
		if strings.HasPrefix(tok, "-") {
			break
		}
		proof[tok] = true
	}
	if len(proof) == 0 {
		t.Fatal("parsed an EMPTY --families list from run_metrics_executed_proof.sh")
	}
	var newlyUncovered, nowCovered []string
	for name := range nativeRequired {
		if !proof[name] && !knownUncovered[name] {
			newlyUncovered = append(newlyUncovered, name)
		}
		if proof[name] && knownUncovered[name] {
			nowCovered = append(nowCovered, name)
		}
	}
	sort.Strings(newlyUncovered)
	sort.Strings(nowCovered)
	if len(newlyUncovered) > 0 {
		t.Errorf("native golden-required families %v are not named in the executed-proof gate: add them to the --families list in ci/run_metrics_executed_proof.sh", newlyUncovered)
	}
	if len(nowCovered) > 0 {
		t.Errorf("%v are native and named in the gate: remove them from knownUncovered", nowCovered)
	}
	for name := range proof {
		if !metricsproof.Known(name) {
			t.Errorf("the gate names family %q but the readback does not know it", name)
		}
	}
}

type familyEntry struct {
	Name   string `json:"name"`
	Port   string `json:"port"`
	Golden string `json:"golden"`
}

func parseFamilies(t *testing.T, raw []byte) []familyEntry {
	t.Helper()
	var wrapped struct {
		Families []familyEntry `json:"families"`
	}
	if err := json.Unmarshal(raw, &wrapped); err != nil {
		t.Fatal(err)
	}
	return wrapped.Families
}
