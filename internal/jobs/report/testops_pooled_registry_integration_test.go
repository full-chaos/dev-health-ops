//go:build integration

package report

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// The AI report charts the TestOps duration and queue metrics over the raw samples of their runs, the definition the
// TestOps tiles read, never as the mean of the stored per-repository-day values. Every cell seeds the raw rows through
// the REAL GitHub route (GitHubTestsRouteHandler.Collect over a fake HTTP doer) and the real sink
// (TestOpsClickHouseEffects.WriteEffect), runs the real daily writers, and charts through the real adapter. The Go
// reference below only computes the expected values from the seed.

type pooledEnv struct {
	t    *testing.T
	ctx  context.Context
	conn driver.Conn
	uri  string
	n    int
}

func startPooledEnv(t *testing.T) *pooledEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &pooledEnv{t: t, ctx: ctx, conn: conn, uri: instance.URI}
}

func (e *pooledEnv) exec(q string, args ...any) {
	e.t.Helper()
	if err := e.conn.Exec(e.ctx, q, args...); err != nil {
		e.t.Fatalf("exec: %v\n%s", err, q)
	}
}

func (e *pooledEnv) newOrg() string {
	e.n++
	return fmt.Sprintf("org-pooled-%d", e.n)
}

// seedRun is one workflow run as the provider lists it. duration and queue are whole seconds; a negative value leaves
// the provider timestamp out, so the route stores NULL.
type seedRun struct {
	id         int
	day        string
	startMin   int
	durationS  int
	queueS     int
	suiteTimes []string
}

func (r seedRun) payload() string {
	day, err := time.Parse(time.DateOnly, r.day)
	if err != nil {
		panic(err)
	}
	started := day.Add(10*time.Hour + time.Duration(r.startMin)*time.Minute)
	parts := []string{
		fmt.Sprintf(`"id":%d`, r.id), `"name":"CI"`, `"status":"completed"`, `"conclusion":"success"`,
		`"run_started_at":"` + started.Format(time.RFC3339) + `"`,
		`"run_attempt":1`, `"event":"push"`, `"head_sha":"abc"`, `"head_branch":"main"`,
		fmt.Sprintf(`"html_url":"https://github.com/acme/api/actions/runs/%d"`, r.id), `"pull_requests":[]`,
	}
	if r.queueS >= 0 {
		parts = append(parts, `"created_at":"`+started.Add(-time.Duration(r.queueS)*time.Second).Format(time.RFC3339)+`"`)
	}
	if r.durationS >= 0 {
		parts = append(parts, `"updated_at":"`+started.Add(time.Duration(r.durationS)*time.Second).Format(time.RFC3339)+`"`)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func (r seedRun) junit() []byte {
	var body strings.Builder
	body.WriteString("<testsuites>")
	for i, value := range r.suiteTimes {
		attribute := ""
		if value != "" {
			attribute = ` time="` + value + `"`
		}
		fmt.Fprintf(&body, `<testsuite name="s%d"%s><testcase name="c" classname="x" time="1"/></testsuite>`, i, attribute)
	}
	body.WriteString("</testsuites>")
	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)
	member, _ := writer.Create("junit.xml")
	_, _ = member.Write([]byte(body.String()))
	_ = writer.Close()
	return archive.Bytes()
}

type routeDoer struct {
	t     *testing.T
	slug  string
	repo  int
	runs  []seedRun
	byID  map[string]seedRun
	paths []string
}

func (d *routeDoer) Do(request *http.Request) (*http.Response, error) {
	status, header, body := http.StatusOK, http.Header{"Content-Type": {"application/json"}}, "{}"
	path := request.URL.Path
	root := "/repos/" + d.slug
	switch {
	case path == root:
		body = fmt.Sprintf(`{"id":%d,"name":"api","full_name":"%s","html_url":"https://github.com/%s","default_branch":"main","archived":false,"updated_at":"2026-01-20T10:00:00Z","pushed_at":"2026-01-21T10:00:00Z"}`, d.repo, d.slug, d.slug)
	case path == root+"/actions/runs":
		items := make([]string, 0, len(d.runs))
		for _, run := range d.runs {
			items = append(items, run.payload())
		}
		body = `{"workflow_runs":[` + strings.Join(items, ",") + `]}`
	case strings.HasPrefix(path, root+"/actions/runs/") && strings.HasSuffix(path, "/jobs"):
		body = `{"jobs":[]}`
	case strings.HasPrefix(path, root+"/actions/runs/") && strings.HasSuffix(path, "/artifacts"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, root+"/actions/runs/"), "/artifacts")
		if run, ok := d.byID[id]; ok && len(run.suiteTimes) > 0 {
			body = `{"artifacts":[{"id":` + id + `,"expired":false}]}`
		} else {
			body = `{"artifacts":[]}`
		}
	case strings.HasPrefix(path, root+"/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, root+"/actions/artifacts/"), "/zip")
		status = http.StatusFound
		header.Set("Location", "https://blob.example/report-"+id+".zip")
		body = ""
	case strings.HasPrefix(path, "/report-"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/report-"), ".zip")
		header.Set("Content-Type", "application/zip")
		body = string(d.byID[id].junit())
	default:
		d.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func pooledClaim(org string, slug string) providersync.Claim {
	capability, _ := providersync.Capability("github", "tests")
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 1, 31, 23, 59, 59, 0, time.UTC)
	return providersync.Claim{
		Unit: providersync.Unit{
			ID: uuid.NewString(), SyncRunID: uuid.NewString(), OrgID: org,
			IntegrationID: uuid.NewString(), SourceID: uuid.NewString(),
			SourceExternalID: slug, SourceName: slug,
			Provider: "github", Dataset: "tests", CostClass: capability.CostClass,
			Mode: "incremental", SinceAt: &since, BeforeAt: &before,
			CredentialID: uuid.NewString(), AuthSource: "integration_credential",
		},
		Owner: uuid.NewString(), Attempt: 1,
		LeaseExpiresAt: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
	}
}

// seedViaRoute writes the runs and suites of one repository through the route and the sink and returns the repository id
// the route derived.
func (e *pooledEnv) seedViaRoute(org, slug string, repoNumber int, runs []seedRun) string {
	e.t.Helper()
	doer := &routeDoer{t: e.t, slug: slug, repo: repoNumber, runs: runs, byID: map[string]seedRun{}}
	for _, run := range runs {
		doer.byID[fmt.Sprint(run.id)] = run
	}
	client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		e.t.Fatal(err)
	}
	claim := pooledClaim(org, slug)
	batch, err := (providersync.GitHubTestsRouteHandler{}).Collect(e.ctx, claim, providerfoundation.Credential{}, client, time.Date(2026, 1, 9, 0, 0, 0, 0, time.UTC))
	if err != nil {
		e.t.Fatalf("collect: %v", err)
	}
	sink := providersync.TestOpsClickHouseEffects{Conn: e.conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	repo := ""
	for _, effect := range batch.Effects {
		if err := sink.WriteEffect(e.ctx, claim, effect); err != nil {
			e.t.Fatalf("write %s: %v", effect.Destination, err)
		}
		if effect.Destination == "ci_pipeline_runs" && len(effect.Rows) > 0 {
			var row struct {
				RepoID string `json:"repo_id"`
			}
			if err := json.Unmarshal(effect.Rows[0], &row); err != nil {
				e.t.Fatal(err)
			}
			repo = row.RepoID
		}
	}
	if repo == "" {
		e.t.Fatal("the route wrote no pipeline row")
	}
	return repo
}

// writeEffectOnly writes pipeline rows the route cannot produce through the real sink. Each caller names why.
func (e *pooledEnv) writeEffectOnly(org, slug, repo string, rows []map[string]any) {
	e.t.Helper()
	encoded := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		row["org_id"], row["repo_id"] = org, repo
		raw, err := json.Marshal(row)
		if err != nil {
			e.t.Fatal(err)
		}
		encoded = append(encoded, raw)
	}
	effect, err := providersync.BuildEffectBatch("ci_pipeline_runs", providersync.EffectReadbackRequired, encoded)
	if err != nil {
		e.t.Fatal(err)
	}
	sink := providersync.TestOpsClickHouseEffects{Conn: e.conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	if err := sink.WriteEffect(e.ctx, pooledClaim(org, slug), effect); err != nil {
		e.t.Fatal(err)
	}
}

func (e *pooledEnv) own(org, team, repo string) {
	e.t.Helper()
	e.exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, now(), now())`, repo, "acme/"+repo[len(repo)-6:], org)
	e.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 1, 0, 0, '2025-01-01 00:00:00', NULL, '2025-01-01 00:00:00')`,
		org, team, repo, "acme/"+repo[len(repo)-6:])
}

// computeDaily runs the real daily writers of both TestOps families for the days.
func (e *pooledEnv) computeDaily(org string, repos []string, days ...string) {
	e.t.Helper()
	pipeline, err := daily.NewTestopsPipelineExecutor(e.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	tests, err := daily.NewTestopsTestExecutor(e.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	ids := make([]daily.RepositoryID, 0, len(repos))
	for _, repo := range repos {
		ids = append(ids, daily.RepositoryID(repo))
	}
	for i, day := range days {
		target, _ := time.Parse(time.DateOnly, day)
		run := daily.Run{ID: fmt.Sprintf("run-%s-%d", org, i), OrganizationID: org, TargetDay: target}
		part := daily.Partition{ID: fmt.Sprintf("part-%s-%d", org, i), RunID: run.ID, RepoIDs: ids}
		if _, err := pipeline.ComputeFamily(e.ctx, run, part); err != nil {
			e.t.Fatal(err)
		}
		if _, err := tests.ComputeFamily(e.ctx, run, part); err != nil {
			e.t.Fatal(err)
		}
	}
}

func (e *pooledEnv) chart(org, metric, groupBy, chartType, start, end string, teams ...string) ChartResult {
	e.t.Helper()
	adapter := &ClickHouseQueryAdapter{conn: e.conn, now: func() time.Time { return time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) }}
	result, err := adapter.executeChart(e.ctx, ChartSpec{ChartType: chartType, Metric: metric, GroupBy: groupBy, OrganizationID: org,
		TimeRangeStart: start, TimeRangeEnd: end, FilterTeams: teams})
	if err != nil {
		e.t.Fatalf("%s chart: %v", metric, err)
	}
	return result
}

func (e *pooledEnv) value(org, metric, groupBy, start, end string) map[string]float64 {
	e.t.Helper()
	out := map[string]float64{}
	for _, point := range e.chart(org, metric, groupBy, "bar", start, end).DataPoints {
		out[point.X] = point.Y
	}
	return out
}

// The Go reference: the writer's percentile (linear between closest ranks) and median, and the nearest-rank rule a wrong
// reader would use.
func inclusive(values []float64, p float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	rank := float64(len(sorted)-1) * p
	lo := int(math.Floor(rank))
	hi := lo + 1
	if hi > len(sorted)-1 {
		hi = len(sorted) - 1
	}
	frac := rank - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

func nearestRank(values []float64, p float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	index := int(math.Ceil(p*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	return sorted[index]
}

func mean(values []float64) float64 {
	total := 0.0
	for _, v := range values {
		total += v
	}
	return total / float64(len(values))
}

func floats(values ...int) []float64 {
	out := make([]float64, len(values))
	for i, v := range values {
		out[i] = float64(v)
	}
	return out
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func mustEqual(t *testing.T, what string, got map[string]float64, key string, want float64) {
	t.Helper()
	value, ok := got[key]
	if !ok {
		t.Errorf("%s: no point %q, want %v (got %v)", what, key, want, got)
		return
	}
	if !near(value, want) {
		t.Errorf("%s: %q = %v, want %v", what, key, value, want)
	}
}

func mustHaveNoPoint(t *testing.T, what string, got map[string]float64) {
	t.Helper()
	if len(got) != 0 {
		t.Errorf("%s: want no point (NULL, never 0), got %v", what, got)
	}
}

// The numbers of the original report: team-a owns A (3 runs 10/20/30 s queued 600 s) and D (17 runs 10..170 s queued 60 s,
// plus a run of 10 s with no queue sample). A mean of the stored repository-day values gave 95.25 s and 330 s where the
// tiles give 160 s and 141 s.
func TestRegistryTestopsChartsEqualThePooledTiles(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	var runsA, runsD []seedRun
	for i, v := range []int{10, 20, 30} {
		runsA = append(runsA, seedRun{id: 100 + i, day: "2026-01-02", startMin: i, durationS: v, queueS: 600, suiteTimes: []string{fmt.Sprint(v)}})
	}
	var durations, queues []float64
	durations, queues = append(durations, 10, 20, 30), append(queues, 600, 600, 600)
	for i := 0; i < 17; i++ {
		v := 10 * (i + 1)
		runsD = append(runsD, seedRun{id: 200 + i, day: "2026-01-02", startMin: i, durationS: v, queueS: 60, suiteTimes: []string{fmt.Sprint(v)}})
		durations, queues = append(durations, float64(v)), append(queues, 60)
	}
	runsD = append(runsD, seedRun{id: 300, day: "2026-01-02", startMin: 40, durationS: 10, queueS: -1})
	durations = append(durations, 10)
	suites := append(floats(10, 20, 30), floats(10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170)...)

	repoA := e.seedViaRoute(org, "acme/a", 4001, runsA)
	repoD := e.seedViaRoute(org, "acme/d", 4002, runsD)
	e.own(org, "team-a", repoA)
	e.own(org, "team-a", repoD)
	e.computeDaily(org, []string{repoA, repoD}, "2026-01-02")

	want := map[string]float64{
		"p95_duration_seconds":       inclusive(durations, 0.95),
		"median_duration_seconds":    inclusive(durations, 0.5),
		"avg_queue_seconds":          mean(queues),
		"p95_queue_seconds":          inclusive(queues, 0.95),
		"suite_duration_p95_seconds": inclusive(suites, 0.95),
		"suite_duration_p50_seconds": inclusive(suites, 0.5),
	}
	if !near(want["p95_duration_seconds"], 160) || !near(want["avg_queue_seconds"], 141) {
		t.Fatalf("the seed no longer holds the original numbers: p95 %v queue %v", want["p95_duration_seconds"], want["avg_queue_seconds"])
	}
	for metric, expected := range want {
		for _, group := range []string{"", "team"} {
			got := e.value(org, metric, group, "2026-01-01", "2026-01-06")
			key := "total"
			if group == "team" {
				key = "team-a"
			}
			mustEqual(t, metric+" group_by="+group, got, key, expected)
		}
	}

	// The tiles: the chart and the tile of one window and scope are one number.
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: e.uri})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	start, _ := graphqldate.Parse("2026-01-01")
	end, _ := graphqldate.Parse("2026-01-06")
	tiles := map[string]model.MeasureInput{
		"p95_duration_seconds":       model.MeasureInputPipelineDurationP95,
		"avg_queue_seconds":          model.MeasureInputPipelineQueueTime,
		"suite_duration_p95_seconds": model.MeasureInputTestSuiteDurationP95,
	}
	for metric, measure := range tiles {
		request := model.AnalyticsRequestInput{Timeseries: []model.TimeseriesRequestInput{{
			Dimension: model.DimensionInputTeam, Measure: measure, Interval: model.BucketIntervalInputDay,
			DateRange: &model.DateRangeInput{StartDate: start, EndDate: end}}}}
		result, err := analytics.Resolve(e.ctx, client, org, request)
		if err != nil {
			t.Fatalf("%s tile: %v", metric, err)
		}
		seen := 0
		for _, series := range result.Timeseries {
			for _, bucket := range series.Buckets {
				if bucket.Value == nil {
					continue
				}
				seen++
				if !near(*bucket.Value, want[metric]) {
					t.Errorf("%s: tile %s %s = %v, chart %v", metric, series.Scope, series.DimensionValue, *bucket.Value, want[metric])
				}
			}
		}
		if seen == 0 {
			t.Errorf("%s: the tile returned no value", metric)
		}
	}
}

// The writer and the chart interpolate (quantileExactInclusive): an even count, one sample and a tie are each red under
// nearest-rank.
func TestRegistryTestopsPercentilesInterpolate(t *testing.T) {
	e := startPooledEnv(t)
	cases := []struct {
		name      string
		durations []int
	}{
		{"even count", []int{10, 20, 30, 40}},
		{"one sample", []int{70}},
		{"tie", []int{10, 20, 20, 20, 40}},
	}
	for i, c := range cases {
		org := e.newOrg()
		var runs []seedRun
		for j, v := range c.durations {
			runs = append(runs, seedRun{id: 1000 + j, day: "2026-01-02", startMin: j, durationS: v, queueS: v, suiteTimes: []string{fmt.Sprint(v)}})
		}
		repo := e.seedViaRoute(org, "acme/p", 5000+i, runs)
		e.own(org, "team-p", repo)
		e.computeDaily(org, []string{repo}, "2026-01-02")
		values := floats(c.durations...)
		for metric, p := range map[string]float64{
			"p95_duration_seconds": 0.95, "median_duration_seconds": 0.5, "p95_queue_seconds": 0.95,
			"suite_duration_p95_seconds": 0.95, "suite_duration_p50_seconds": 0.5,
		} {
			got := e.value(org, metric, "", "2026-01-01", "2026-01-06")
			mustEqual(t, c.name+" "+metric, got, "total", inclusive(values, p))
			if len(values) == 1 || (c.name == "tie" && p == 0.5) {
				// One sample is its own percentile under every rule; the median of the tie is a tied value.
				continue
			}
			if near(nearestRank(values, p), inclusive(values, p)) {
				t.Errorf("%s %s: the cell does not tell interpolation from nearest-rank", c.name, metric)
			}
		}
	}
}

// A group with no sample has no point: NULL, never 0. A window with no run, a run with no finished time (no duration) and
// a run with no created time (no queue sample) are the cases the route produces.
func TestRegistryTestopsNoSampleIsNoPoint(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	repo := e.seedViaRoute(org, "acme/n", 6001, []seedRun{
		{id: 2001, day: "2026-01-02", startMin: 0, durationS: -1, queueS: -1, suiteTimes: []string{""}},
		{id: 2002, day: "2026-01-02", startMin: 5, durationS: -1, queueS: -1},
	})
	e.own(org, "team-n", repo)
	e.computeDaily(org, []string{repo}, "2026-01-02")
	for _, metric := range []string{"p95_duration_seconds", "median_duration_seconds", "avg_queue_seconds", "p95_queue_seconds",
		"suite_duration_p95_seconds", "suite_duration_p50_seconds"} {
		mustHaveNoPoint(t, metric+" with no sample", e.value(org, metric, "", "2026-01-01", "2026-01-06"))
		mustHaveNoPoint(t, metric+" by team with no sample", e.value(org, metric, "team", "2026-01-01", "2026-01-06"))
		mustHaveNoPoint(t, metric+" in a window with no run", e.value(org, metric, "", "2026-01-10", "2026-01-12"))
		mustHaveNoPoint(t, metric+" scorecard with no sample", pointsOf(e.chart(org, metric, "", "scorecard", "2026-01-01", "2026-01-06")))
	}
}

func pointsOf(chart ChartResult) map[string]float64 {
	out := map[string]float64{}
	for _, point := range chart.DataPoints {
		out[point.X] = point.Y
	}
	return out
}

// A suite of negative duration (a JUnit time of -3, which the route stores) is no sample, as in the writer.
func TestRegistryTestopsNegativeSuiteDurationIsNoSample(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	repo := e.seedViaRoute(org, "acme/s", 7001, []seedRun{
		{id: 3001, day: "2026-01-02", startMin: 0, durationS: 10, queueS: 1, suiteTimes: []string{"-3", "1", "2", "4", ""}},
	})
	e.own(org, "team-s", repo)
	e.computeDaily(org, []string{repo}, "2026-01-02")
	values := []float64{1, 2, 4}
	mustEqual(t, "suite p95", e.value(org, "suite_duration_p95_seconds", "", "2026-01-01", "2026-01-06"), "total", inclusive(values, 0.95))
	mustEqual(t, "suite p50", e.value(org, "suite_duration_p50_seconds", "", "2026-01-01", "2026-01-06"), "total", inclusive(values, 0.5))
}

// A run of stored status deleted is no sample, as in the writer. NOT producible by the GitHub route (it maps a provider
// status, never "deleted"): the row goes through the real sink with BuildEffectBatch. Covers testops.ExcludedFromPipelineCounts
// and the shared source's counted-run predicate.
func TestRegistryTestopsDeletedRunIsNoSample(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	repo := e.seedViaRoute(org, "acme/x", 8001, []seedRun{
		{id: 4001, day: "2026-01-02", startMin: 0, durationS: 10, queueS: 5},
		{id: 4002, day: "2026-01-02", startMin: 5, durationS: 30, queueS: 15},
	})
	started := "2026-01-02T11:00:00Z"
	e.writeEffectOnly(org, "acme/x", repo, []map[string]any{{
		"run_id": "deleted-run", "provider": "github_actions", "status": "deleted", "started_at": started,
		"finished_at": "2026-01-02T11:16:40Z", "duration_seconds": 1000.0, "queue_seconds": 900.0, "retry_count": 0,
		"last_synced": "2026-01-09T00:00:00Z",
	}})
	e.own(org, "team-x", repo)
	e.computeDaily(org, []string{repo}, "2026-01-02")
	mustEqual(t, "p95 without the deleted run", e.value(org, "p95_duration_seconds", "", "2026-01-01", "2026-01-06"), "total", inclusive(floats(10, 30), 0.95))
	mustEqual(t, "queue without the deleted run", e.value(org, "avg_queue_seconds", "", "2026-01-01", "2026-01-06"), "total", 10)
}

// The writer's own fallbacks (a duration from finished_at - started_at when duration_seconds is negative; a queue from
// started_at - queued_at when queue_seconds is negative) cannot be produced by the GitHub route: secondsBetween clamps at 0
// (github_tests_route.go) and a duration is NULL only when finished_at is NULL. The rows go through the real sink; they
// guard rows written before the route only.
func TestRegistryTestopsFallbackSamplesOfLegacyRows(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	repo := e.seedViaRoute(org, "acme/f", 9001, []seedRun{{id: 5001, day: "2026-01-02", startMin: 0, durationS: 10, queueS: 4}})
	e.writeEffectOnly(org, "acme/f", repo, []map[string]any{{
		"run_id": "legacy-1", "provider": "github_actions", "status": "success", "queued_at": "2026-01-02T10:59:00Z",
		"started_at": "2026-01-02T11:00:00Z", "finished_at": "2026-01-02T11:00:30Z", "duration_seconds": -1.0, "queue_seconds": -1.0,
		"retry_count": 0, "last_synced": "2026-01-09T00:00:00Z",
	}})
	e.own(org, "team-f", repo)
	e.computeDaily(org, []string{repo}, "2026-01-02")
	mustEqual(t, "p95 with a fallback duration", e.value(org, "p95_duration_seconds", "", "2026-01-01", "2026-01-06"), "total", inclusive(floats(10, 30), 0.95))
	mustEqual(t, "queue with a fallback sample", e.value(org, "avg_queue_seconds", "", "2026-01-01", "2026-01-06"), "total", mean(floats(4, 60)))
}

// A team is the owner of the repository as of the chart, a co-owned repository whole for each owner; the organization
// value pools each run once; a bucket pools the runs of the bucket, not the mean of its days; the range end is inclusive.
func TestRegistryTestopsTeamsBucketsAndRange(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	var runs []seedRun
	var all []float64
	for i, v := range []int{10, 20, 30, 40} {
		runs = append(runs, seedRun{id: 6000 + i, day: "2026-01-02", startMin: i, durationS: v, queueS: v})
		all = append(all, float64(v))
	}
	var day2 []float64
	for i := 0; i < 8; i++ {
		runs = append(runs, seedRun{id: 6100 + i, day: "2026-01-03", startMin: i, durationS: 100, queueS: 100})
		all, day2 = append(all, 100), append(day2, 100)
	}
	runs = append(runs, seedRun{id: 6200, day: "2026-01-04", startMin: 0, durationS: 9999, queueS: 9999})
	repo := e.seedViaRoute(org, "acme/t", 4100, runs)
	// The owners are set AFTER the daily writer: the stored team of the repository-day is not read.
	e.computeDaily(org, []string{repo}, "2026-01-02", "2026-01-03", "2026-01-04")
	e.exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, 'acme/t', 'github', ?, now(), now())`, repo, org)
	for _, team := range []string{"team-b", "team-c"} {
		e.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', ?, ?, 'acme/t', 'exact', 'inferred', 1, 0, 0, '2025-01-01 00:00:00', NULL, '2025-01-01 00:00:00')`, org, team, repo)
	}
	// An owner whose ownership ended before the chart is not an owner at the chart.
	e.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-ended', ?, 'acme/t', 'exact', 'inferred', 1, 0, 0, '2025-01-01 00:00:00', '2026-02-01 00:00:00', '2025-01-01 00:00:00')`, org, repo)
	want := inclusive(all, 0.95)
	got := e.value(org, "p95_duration_seconds", "team", "2026-01-01", "2026-01-03")
	mustEqual(t, "co-owner team-b", got, "team-b", want)
	mustEqual(t, "co-owner team-c", got, "team-c", want)
	if len(got) != 2 {
		t.Errorf("teams = %v, want only the two owners", got)
	}
	mustEqual(t, "organization pools each run once", e.value(org, "p95_duration_seconds", "", "2026-01-01", "2026-01-03"), "total", want)
	mustEqual(t, "team filter, each run once", e.value(org, "p95_duration_seconds", "", "2026-01-01", "2026-01-03"), "total", want)
	filtered := pointsOf(e.chart(org, "p95_duration_seconds", "", "bar", "2026-01-01", "2026-01-03", "team-b", "team-c"))
	mustEqual(t, "two teams filtered, a shared repository once", filtered, "total", want)
	mustHaveNoPoint(t, "an unknown team", pointsOf(e.chart(org, "p95_duration_seconds", "", "bar", "2026-01-01", "2026-01-03", "team-none")))
	mustEqual(t, "month bucket pools the runs", e.value(org, "p95_duration_seconds", "month", "2026-01-01", "2026-01-03"), "2026-01-01", want)
	days := e.value(org, "p95_duration_seconds", "day", "2026-01-01", "2026-01-03")
	mustEqual(t, "day 1", days, "2026-01-02", inclusive(floats(10, 20, 30, 40), 0.95))
	mustEqual(t, "day 2", days, "2026-01-03", inclusive(day2, 0.95))
	if _, leaked := days["2026-01-04"]; leaked {
		t.Errorf("the range end 2026-01-03 is inclusive and the day after is out: %v", days)
	}
	meanOfDays := (inclusive(floats(10, 20, 30, 40), 0.95) + inclusive(day2, 0.95)) / 2
	if near(meanOfDays, want) {
		t.Errorf("the cell does not tell a pooled bucket from the mean of its days")
	}
	_ = org
}

// Only the repository-days the daily writer computed are read, as the tiles do: a raw run of a day the writer has not
// computed yet is no sample.
func TestRegistryTestopsOnlyComputedRepositoryDays(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	first := []seedRun{
		{id: 7001, day: "2026-01-02", startMin: 0, durationS: 10, queueS: 10, suiteTimes: []string{"10"}},
		{id: 7002, day: "2026-01-02", startMin: 1, durationS: 30, queueS: 30, suiteTimes: []string{"30"}},
	}
	repo := e.seedViaRoute(org, "acme/c", 4200, first)
	e.own(org, "team-c", repo)
	e.computeDaily(org, []string{repo}, "2026-01-02")
	// Later runs of 2026-01-03, written through the route; the writer has not computed that day.
	e.seedViaRoute(org, "acme/c", 4200, []seedRun{{id: 7101, day: "2026-01-03", startMin: 0, durationS: 5000, queueS: 5000, suiteTimes: []string{"5000"}}})
	for metric, want := range map[string]float64{
		"p95_duration_seconds":       inclusive(floats(10, 30), 0.95),
		"avg_queue_seconds":          20,
		"suite_duration_p95_seconds": inclusive(floats(10, 30), 0.95),
	} {
		mustEqual(t, metric+" over a day the writer did not compute", e.value(org, metric, "", "2026-01-01", "2026-01-06"), "total", want)
	}
}

// A re-run attempt's report replaces the earlier attempt's: the suite totals of a run are the newest attempt of each
// report slot, a legacy row (no slot: written before the slot columns existed, by either provider route) counts unless its run also has slotted
// rows. This is the daily writer's own rule (testopsNewestAttemptSuites), so the chart, the tile and the stored value agree.
type attemptDoer struct {
	t         *testing.T
	runID     int
	attempt   int
	started   string
	artifacts []string
	zips      map[string][]string
	names     map[string][]string // optional suite names per artifact id (default s0, s1, ...)
	lcov      bool                // add a coverage report to every artifact
}

// archive is the artifact zip: a JUnit report (suite names and times) and, when asked, an LCOV coverage report.
func (d *attemptDoer) archive(id string) []byte {
	times := d.zips[id]
	var body strings.Builder
	body.WriteString("<testsuites>")
	for i, value := range times {
		name := fmt.Sprintf("s%d", i)
		if names := d.names[id]; i < len(names) {
			name = names[i]
		}
		attribute := ""
		if value != "" {
			attribute = ` time="` + value + `"`
		}
		fmt.Fprintf(&body, `<testsuite name=%q%s><testcase name="c" classname="x" time="1"/></testsuite>`, name, attribute)
	}
	body.WriteString("</testsuites>")
	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)
	member, _ := writer.Create("junit.xml")
	_, _ = member.Write([]byte(body.String()))
	if d.lcov {
		cov, _ := writer.Create("coverage.info")
		_, _ = cov.Write([]byte("TN:\nSF:services/api/main.go\nDA:1,1\nDA:2,0\nend_of_record\n"))
	}
	_ = writer.Close()
	return archive.Bytes()
}

func (d *attemptDoer) Do(request *http.Request) (*http.Response, error) {
	status, header, body := http.StatusOK, http.Header{"Content-Type": {"application/json"}}, "{}"
	path := request.URL.Path
	const root = "/repos/acme/r"
	switch {
	case path == root:
		body = `{"id":9100,"name":"r","full_name":"acme/r","html_url":"https://github.com/acme/r","default_branch":"main","archived":false,"updated_at":"2026-01-20T10:00:00Z","pushed_at":"2026-01-21T10:00:00Z"}`
	case path == root+"/actions/runs":
		body = fmt.Sprintf(`{"workflow_runs":[{"id":%d,"name":"CI","status":"completed","conclusion":"success","created_at":"2026-01-02T09:59:00Z","run_started_at":%q,"updated_at":"2026-01-02T12:30:00Z","run_attempt":%d,"event":"push","head_sha":"abc","head_branch":"main","html_url":"https://github.com/acme/r/actions/runs/%d","pull_requests":[]}]}`, d.runID, d.started, d.attempt, d.runID)
	case strings.HasSuffix(path, "/jobs"):
		body = `{"jobs":[]}`
	case strings.HasSuffix(path, "/artifacts"):
		body = `{"artifacts":[` + strings.Join(d.artifacts, ",") + `]}`
	case strings.HasSuffix(path, "/zip"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, root+"/actions/artifacts/"), "/zip")
		status = http.StatusFound
		header.Set("Location", "https://blob.example/report-"+id+".zip")
		body = ""
	case strings.HasPrefix(path, "/report-"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/report-"), ".zip")
		header.Set("Content-Type", "application/zip")
		body = string(d.archive(id))
	default:
		d.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func (e *pooledEnv) collectAttempt(org string, doer *attemptDoer, at time.Time) string {
	e.t.Helper()
	client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		e.t.Fatal(err)
	}
	claim := pooledClaim(org, "acme/r")
	batch, err := (providersync.GitHubTestsRouteHandler{}).Collect(e.ctx, claim, providerfoundation.Credential{}, client, at)
	if err != nil {
		e.t.Fatalf("collect: %v", err)
	}
	sink := providersync.TestOpsClickHouseEffects{Conn: e.conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	repo := ""
	for _, effect := range batch.Effects {
		if err := sink.WriteEffect(e.ctx, claim, effect); err != nil {
			e.t.Fatalf("write %s: %v", effect.Destination, err)
		}
		if effect.Destination == "ci_pipeline_runs" && len(effect.Rows) > 0 {
			var row struct {
				RepoID string `json:"repo_id"`
			}
			if err := json.Unmarshal(effect.Rows[0], &row); err != nil {
				e.t.Fatal(err)
			}
			repo = row.RepoID
		}
	}
	return repo
}

func attemptArtifact(id int, created string) string {
	return attemptNamedArtifact(id, "report", created)
}

func attemptNamedArtifact(id int, name, created string) string {
	return fmt.Sprintf(`{"id":%d,"name":%q,"expired":false,"created_at":%q}`, id, name, created)
}

func (e *pooledEnv) storedSuiteQuantiles(org string) (p50, p95 float64) {
	e.t.Helper()
	var a, b *float64
	if err := e.conn.QueryRow(e.ctx, `SELECT suite_duration_p50_seconds, suite_duration_p95_seconds FROM testops_test_metrics_daily FINAL WHERE org_id = ?`, org).Scan(&a, &b); err != nil || a == nil || b == nil {
		e.t.Fatalf("stored suite quantiles: %v %v %v", a, b, err)
	}
	return *a, *b
}

func TestRegistryTestopsSuiteTotalsTakeTheNewestAttempt(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	// Attempt 1 uploads artifact 77 (two suites of 10 s and 20 s); the re-run attempt 2 uploads artifact 78 of the same
	// name (one suite of 100 s) and the listing still holds artifact 77.
	repo := e.collectAttempt(org, &attemptDoer{t: t, runID: 9001, attempt: 1, started: "2026-01-02T10:00:00Z",
		artifacts: []string{attemptArtifact(77, "2026-01-02T10:05:00Z")}, zips: map[string][]string{"77": {"10", "20"}}},
		time.Date(2026, 1, 9, 0, 0, 0, 0, time.UTC))
	e.collectAttempt(org, &attemptDoer{t: t, runID: 9001, attempt: 2, started: "2026-01-02T12:00:00Z",
		artifacts: []string{attemptArtifact(77, "2026-01-02T10:05:00Z"), attemptArtifact(78, "2026-01-02T12:05:00Z")},
		zips:      map[string][]string{"77": {"10", "20"}, "78": {"100"}}},
		time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC))
	e.own(org, "team-r", repo)
	e.computeDaily(org, []string{repo}, "2026-01-02")

	storedP50, storedP95 := e.storedSuiteQuantiles(org)
	if !near(storedP50, 100) || !near(storedP95, 100) {
		t.Fatalf("the daily writer's own value is the newest attempt (100), got p50 %v p95 %v", storedP50, storedP95)
	}
	mustEqual(t, "suite p50 takes the newest attempt", e.value(org, "suite_duration_p50_seconds", "", "2026-01-01", "2026-01-06"), "total", 100)
	mustEqual(t, "suite p95 takes the newest attempt", e.value(org, "suite_duration_p95_seconds", "", "2026-01-01", "2026-01-06"), "total", 100)
	mustEqual(t, "team group", e.value(org, "suite_duration_p95_seconds", "team", "2026-01-01", "2026-01-06"), "team-r", 100)
}
