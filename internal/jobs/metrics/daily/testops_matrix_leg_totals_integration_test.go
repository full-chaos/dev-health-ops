//go:build integration

package daily

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopsrisk"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopswindow"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-14045: two matrix legs run the same tests (cases a, b; leg A suites 1.0 s, leg B 3.0 s) and each leg uploads a total.
// Leg A's total fails case a (a mismatch); leg B's total is leg B exactly. The run is synced through the production chunked route
// and sink, and every served reader of its rows runs: the daily testops_test and testops_risk families, testopsTestWindow,
// testOpsRisk and the pooled suite durations. With leg B's total in the listing (BOTH) every served number equals the run
// without it (WITHOUT): the exact total is dropped, not counted a second time as its own report slot. Leg A's mismatch still
// keeps the run partial and the unit's watermark withheld.

type matrixTotalsCase struct{ name, status, dur string }

type matrixTotalsSuite struct {
	name, time string
	cases      []matrixTotalsCase
}

func matrixTotalsArchive(t *testing.T, suites ...matrixTotalsSuite) []byte {
	t.Helper()
	var body strings.Builder
	body.WriteString(`<testsuites>`)
	for _, suite := range suites {
		body.WriteString(`<testsuite name="` + suite.name + `" time="` + suite.time + `" timestamp="2026-07-22T10:02:00">`)
		for _, c := range suite.cases {
			if c.status == "failed" {
				body.WriteString(`<testcase name="` + c.name + `" classname="pkg" time="` + c.dur + `"><failure message="boom"/></testcase>`)
			} else {
				body.WriteString(`<testcase name="` + c.name + `" classname="pkg" time="` + c.dur + `"/>`)
			}
		}
		body.WriteString(`</testsuite>`)
	}
	body.WriteString(`</testsuites>`)
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	member, err := writer.Create("report.xml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := member.Write([]byte(body.String())); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// matrixTotalsServed is every stored and served number the cell compares.
type matrixTotalsServed struct {
	suites, cases, slots uint64
	watermarkWithheld    bool
	complete             uint8
	reasons              string

	totalCases, passed, failed, totalSuites uint32
	passRate, failureRate, flakeRate        float64
	p50, p95                                string
	pooledSamples                           uint64

	confidence, dragHours, stability float64
	confidenceFactors, dragFactors   string

	window, risk string

	pooledP95   float64
	pooledCount uint64
}

func matrixTotalsSync(t *testing.T, stack attemptsStack, org, runID string, since time.Time, artifacts []attemptsArtifact) *time.Time {
	t.Helper()
	before := since.Add(time.Hour)
	capability, _ := providersync.Capability("github", "tests")
	claim := providersync.Claim{
		Unit: providersync.Unit{
			ID: uuid.NewString(), SyncRunID: uuid.NewString(), OrgID: org,
			IntegrationID: uuid.NewString(), SourceID: uuid.NewString(),
			SourceExternalID: "acme/api", SourceName: "acme/api",
			Provider: "github", Dataset: "tests", CostClass: capability.CostClass,
			Mode: "incremental", SinceAt: &since, BeforeAt: &before,
			CredentialID: uuid.NewString(), AuthSource: "integration_credential",
		},
		Owner: uuid.NewString(), Attempt: 1, LeaseExpiresAt: before.Add(time.Hour),
	}
	doer := &attemptsDoer{t: t, runID: runID, state: attemptsRunState{
		attempt: 1, started: "2026-07-22T10:01:00Z", updated: "2026-07-22T10:30:00Z", artifacts: artifacts,
	}}
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", fakehttp.Client(doer),
		func(request *http.Request) error { request.Header.Set("Authorization", "Bearer secret"); return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}, lease)
	if err != nil {
		t.Fatal(err)
	}
	sink := providersync.TestOpsClickHouseEffects{Conn: stack.conn, Lease: lease}
	var watermark *time.Time
	finals := 0
	err = providersync.GitHubTestsRouteHandler{}.CollectChunks(stack.ctx, claim, providerfoundation.Credential{}, client, before.Add(5*time.Minute), "",
		func(emission providersync.ChunkRouteEmission) error {
			if emission.Final {
				watermark = emission.Batch.Watermark
				finals++
			}
			for _, effect := range emission.Batch.Effects {
				if len(effect.Rows) == 0 {
					continue
				}
				if err := sink.WriteEffect(stack.ctx, claim, effect); err != nil {
					return fmt.Errorf("write %s: %w", effect.Destination, err)
				}
			}
			return nil
		})
	if err != nil {
		t.Fatalf("CollectChunks: %v", err)
	}
	if finals != 1 {
		t.Fatalf("%d final emissions, want 1", finals)
	}
	return watermark
}

func matrixTotalsRead(t *testing.T, stack attemptsStack, client *dhclickhouse.Client, org, runID string, day time.Time) matrixTotalsServed {
	t.Helper()
	ctx, conn := stack.ctx, stack.conn
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	var served matrixTotalsServed
	// Live rows only: a dropped report's zero-measure retraction rows (CHAOS-14259) are no suite of the run.
	must(conn.QueryRow(ctx, "SELECT countIf(retracted = 0), uniqExactIf(report_slot, retracted = 0) FROM test_suite_results FINAL WHERE org_id = ?", org).Scan(&served.suites, &served.slots))
	must(conn.QueryRow(ctx, "SELECT count() FROM test_case_results FINAL WHERE org_id = ?", org).Scan(&served.cases))
	var reasons []string
	must(conn.QueryRow(ctx, `SELECT complete, reasons FROM testops_run_report_completeness FINAL WHERE org_id = ? AND run_id = ?`, org, runID).Scan(&served.complete, &reasons))
	served.reasons = strings.Join(reasons, ",")

	repoID := stack.repoID(t, org)
	seedTestopsAuthoritativeOwner(ctx, t, conn, org, repoID, day)
	run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}
	partition := Partition{ID: uuid.NewString(), RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repoID.String())}}
	testExecutor, err := NewTestopsTestExecutor(conn)
	must(err)
	_, err = testExecutor.ComputeFamily(ctx, run, partition)
	must(err)
	pipelineExecutor, err := NewTestopsPipelineExecutor(conn)
	must(err)
	_, err = pipelineExecutor.ComputeFamily(ctx, run, partition)
	must(err)
	riskExecutor, err := NewTestopsRiskExecutor(conn)
	must(err)
	_, err = riskExecutor.ComputeFamily(ctx, run, partition)
	must(err)

	var p50, p95 *float64
	must(conn.QueryRow(ctx, `SELECT total_cases, passed_count, failed_count, total_suites, pass_rate, failure_rate, flake_rate,
suite_duration_p50_seconds, suite_duration_p95_seconds, length(pooled_durations)
FROM testops_test_metrics_daily WHERE org_id = ? AND repo_id = ? AND day = ? ORDER BY computed_at DESC LIMIT 1`, org, repoID, day).Scan(
		&served.totalCases, &served.passed, &served.failed, &served.totalSuites, &served.passRate, &served.failureRate, &served.flakeRate,
		&p50, &p95, &served.pooledSamples))
	format := func(value *float64) string {
		if value == nil {
			return "NULL"
		}
		return fmt.Sprint(*value)
	}
	served.p50, served.p95 = format(p50), format(p95)
	must(conn.QueryRow(ctx, `SELECT confidence_score, factors_json FROM testops_release_confidence WHERE org_id = ? ORDER BY computed_at DESC LIMIT 1`, org).
		Scan(&served.confidence, &served.confidenceFactors))
	must(conn.QueryRow(ctx, `SELECT drag_hours, factors_json FROM testops_quality_drag WHERE org_id = ? ORDER BY computed_at DESC LIMIT 1`, org).
		Scan(&served.dragHours, &served.dragFactors))
	must(conn.QueryRow(ctx, `SELECT stability_index FROM testops_pipeline_stability WHERE org_id = ? ORDER BY computed_at DESC LIMIT 1`, org).
		Scan(&served.stability))

	window, err := testopswindow.Resolve(ctx, client, org, graphqldate.New(day), graphqldate.New(day), testopswindow.Scope{})
	must(err)
	windowJSON, err := json.Marshal(window)
	must(err)
	served.window = string(windowJSON)
	risk, err := testopsrisk.Resolve(ctx, client, org, model.TestOpsRiskInput{StartDate: graphqldate.New(day), EndDate: graphqldate.New(day.Add(24 * time.Hour))})
	must(err)
	risk.OrgID = ""
	riskJSON, err := json.Marshal(risk)
	must(err)
	served.risk = string(riskJSON)

	must(conn.QueryRow(ctx, "SELECT quantileExactInclusive(0.95)(duration_sample), count() FROM "+testopspooled.SuiteRunsSource,
		clickhouse.Named("org_id", org), clickhouse.Named("start_date", day.Format(time.DateOnly)),
		clickhouse.Named("end_date", day.AddDate(0, 0, 1).Format(time.DateOnly))).Scan(&served.pooledP95, &served.pooledCount))
	return served
}

func TestTestopsMatrixLegExactTotalIsNotCountedTwice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	chschema.Apply(ctx, t, instance)
	stack := attemptsStack{ctx: ctx, conn: conn}
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	day := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	const runID, made = "43600014045", "2026-07-22T10:20:00Z"
	legA := []matrixTotalsSuite{{"left", "1.0", []matrixTotalsCase{{"a", "", "0.125"}}}, {"right", "1.0", []matrixTotalsCase{{"b", "", "0.125"}}}}
	legB := []matrixTotalsSuite{{"left", "3.0", []matrixTotalsCase{{"a", "", "0.25"}}}, {"right", "3.0", []matrixTotalsCase{{"b", "", "0.25"}}}}
	totalA := []matrixTotalsSuite{{"left", "1.0", []matrixTotalsCase{{"a", "failed", "0.125"}}}, legA[1]}
	artifact := func(id int, name string, suites ...matrixTotalsSuite) attemptsArtifact {
		return attemptsArtifact{id, name, made, false, matrixTotalsArchive(t, suites...)}
	}
	without := []attemptsArtifact{
		artifact(1, "leg-a-left", legA[0]), artifact(2, "leg-a-right", legA[1]),
		artifact(3, "leg-b-left", legB[0]), artifact(4, "leg-b-right", legB[1]), artifact(5, "total-a", totalA...),
	}
	both := append(slices.Clone(without), artifact(6, "total-b", legB...))

	arms := map[string]matrixTotalsServed{}
	for _, arm := range []struct {
		name      string
		artifacts []attemptsArtifact
	}{{"WITHOUT", without}, {"BOTH", both}} {
		org := "org-matrix-totals-" + strings.ToLower(arm.name) + "-" + uuid.NewString()[:8]
		watermark := matrixTotalsSync(t, stack, org, runID, day.Add(10*time.Hour), arm.artifacts)
		served := matrixTotalsRead(t, stack, client, org, runID, day)
		served.watermarkWithheld = watermark == nil
		t.Logf("%s: %+v", arm.name, served)
		arms[arm.name] = served
		// K1 (CHAOS-14259): BOTH also writes one zero-measure retraction row per suite of the dropped total, and the served
		// numbers above still equal WITHOUT's.
		var retracted uint64
		if err := conn.QueryRow(ctx, "SELECT countIf(retracted = 1) FROM test_suite_results FINAL WHERE org_id = ? AND report_slot = 'total-b'", org).Scan(&retracted); err != nil {
			t.Fatal(err)
		}
		if want := map[string]uint64{"WITHOUT": 0, "BOTH": 2}[arm.name]; retracted != want {
			t.Errorf("%s: %d retraction rows at total-b, want %d", arm.name, retracted, want)
		}
	}

	got, want := arms["BOTH"], arms["WITHOUT"]
	if got != want {
		t.Errorf("served numbers with leg B's exact total differ from the run without it:\n got  %+v\n want %+v", got, want)
	}
	near := func(value, target float64) bool { return math.Abs(value-target) < 1e-6 }
	if got.suites != 6 || got.cases != 6 || got.slots != 5 || got.totalCases != 6 || got.passed != 5 || got.totalSuites != 6 ||
		!near(got.passRate, 5.0/6) || got.p50 != "1" || got.pooledSamples != 6 || got.pooledCount != 6 ||
		// no coverage row: coverage is unknown, base (0.4+0.25+0.05)/0.8 = 0.875, flake penalty 0.1
		!near(got.confidence, 0.775) || !near(got.dragHours, 0.75) || !strings.Contains(got.risk, `"riskScore":0.22499999999999998}`) {
		t.Errorf("served numbers %+v, want suites 6, cases 6, slots 5, total_cases 6, passed 5, total_suites 6, pass_rate 5/6, "+
			"p50 1, 6 pooled samples, confidence 0.775, drag 0.75 h, riskScore 0.225", got)
	}
	// Leg A's mismatch keeps the run partial and the watermark withheld, with or without leg B's total.
	for name, served := range arms {
		if !served.watermarkWithheld || served.complete != 0 || served.reasons != "aggregate_mismatch" {
			t.Errorf("%s: watermark withheld %t, complete %d, reasons %q; want withheld, 0, aggregate_mismatch",
				name, served.watermarkWithheld, served.complete, served.reasons)
		}
	}
}
