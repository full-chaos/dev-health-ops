//go:build integration

package daily

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
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
)

// CHAOS-14259: a run synced before CHAOS-14045 stored leg B's exact total (total-b) as its own report slot. A later sync of the
// same run drops total-b as an aggregate of its parts and writes nothing for it, so the stale slot stays and every served number
// counts leg B twice. These cells sync such runs through the production chunked route and sink, read them through every served
// reader (matrixTotalsRead), and want every served number of the stale run equal to a run synced once (CONTROL).
//
// testdata/chaos14259_pre14045_both_emission.json is the emission of the BOTH listing below as the build before CHAOS-14045
// (7e816f311, the parent of 37b7d692a) wrote it: CollectChunks over the synthetic artifacts of this file for org
// retiredStaleAOrg, unit window [10:00, 11:00), every effect with rows, in order. total-b is admitted there (8 suites, 8 cases).
//
// testdata/chaos14259_base_suite_rule_{window,org}.txt are testopspooled.SuiteRowsNewestAttempt at 85378e468f25 rendered with
// the daily readers' scope and with no scope: the reader an old binary runs after a rollback.

const (
	retiredRunID     = "43600014045"
	retiredStaleAOrg = "org-chaos14259-stale-a"
)

// retiredDay is the day every run of these cells starts on (a function: a package-level var with a call makes a test file a
// hook file).
func retiredDay() time.Time { return time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC) }

// retiredArtifacts are the matrix fixture of CHAOS-14045 (testops_matrix_leg_totals_integration_test.go): legs A and B run cases
// a and b, total-a fails case a (a mismatch), total-b is leg B exactly.
func retiredArtifacts(t *testing.T) (both, withoutLegB, totalBOnly []attemptsArtifact) {
	t.Helper()
	const made = "2026-07-22T10:20:00Z"
	legA := []matrixTotalsSuite{{"left", "1.0", []matrixTotalsCase{{"a", "", "0.125"}}}, {"right", "1.0", []matrixTotalsCase{{"b", "", "0.125"}}}}
	legB := []matrixTotalsSuite{{"left", "3.0", []matrixTotalsCase{{"a", "", "0.25"}}}, {"right", "3.0", []matrixTotalsCase{{"b", "", "0.25"}}}}
	totalA := []matrixTotalsSuite{{"left", "1.0", []matrixTotalsCase{{"a", "failed", "0.125"}}}, legA[1]}
	artifact := func(id int, name string, suites ...matrixTotalsSuite) attemptsArtifact {
		return attemptsArtifact{id, name, made, false, matrixTotalsArchive(t, suites...)}
	}
	legALeft, legARight := artifact(1, "leg-a-left", legA[0]), artifact(2, "leg-a-right", legA[1])
	legBLeft, legBRight := artifact(3, "leg-b-left", legB[0]), artifact(4, "leg-b-right", legB[1])
	totalAArtifact, totalBArtifact := artifact(5, "total-a", totalA...), artifact(6, "total-b", legB...)
	both = []attemptsArtifact{legALeft, legARight, legBLeft, legBRight, totalAArtifact, totalBArtifact}
	withoutLegB = []attemptsArtifact{legALeft, legARight, totalAArtifact, totalBArtifact}
	return both, withoutLegB, []attemptsArtifact{totalBArtifact}
}

// retiredState is the run as the API shows it to a unit over [10:00, 11:00) (first) or [11:00, 12:00) (later): one attempt, the
// same start; only updated_at moves into the later window. Every arm's last sync is a later one, so the pipeline and job rows the
// served numbers read are the same in every arm.
func retiredState(later bool, artifacts []attemptsArtifact) attemptsRunState {
	updated := "2026-07-22T10:30:00Z"
	if later {
		updated = "2026-07-22T11:40:00Z"
	}
	return attemptsRunState{attempt: 1, started: "2026-07-22T10:01:00Z", updated: updated, artifacts: artifacts}
}

type retiredRecordedEffect struct {
	Destination string            `json:"destination"`
	Rows        []json.RawMessage `json:"rows"`
}

func retiredRecordedEmission(t *testing.T) []retiredRecordedEffect {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "chaos14259_pre14045_both_emission.json"))
	if err != nil {
		t.Fatal(err)
	}
	var effects []retiredRecordedEffect
	if err := json.Unmarshal(raw, &effects); err != nil {
		t.Fatal(err)
	}
	return effects
}

// retiredRecordedTotalBSuites are the suite ids and the last_synced of the recorded total-b rows.
func retiredRecordedTotalBSuites(t *testing.T) (ids []string, lastSynced time.Time) {
	t.Helper()
	for _, effect := range retiredRecordedEmission(t) {
		if effect.Destination != "test_suite_results" {
			continue
		}
		for _, raw := range effect.Rows {
			var row struct {
				SuiteID    string    `json:"suite_id"`
				ReportSlot string    `json:"report_slot"`
				LastSynced time.Time `json:"last_synced"`
			}
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			if row.ReportSlot == "total-b" {
				ids, lastSynced = append(ids, row.SuiteID), row.LastSynced
			}
		}
	}
	slices.Sort(ids)
	if len(ids) != 2 {
		t.Fatalf("the recorded emission holds %d total-b suites, want 2", len(ids))
	}
	return ids, lastSynced
}

// writeRetiredRecordedEmission writes the recorded pre-CHAOS-14045 emission through the production sink.
func writeRetiredRecordedEmission(t *testing.T, stack attemptsStack) {
	t.Helper()
	since := retiredDay().Add(10 * time.Hour)
	before := since.Add(time.Hour)
	capability, _ := providersync.Capability("github", "tests")
	claim := providersync.Claim{
		Unit: providersync.Unit{
			ID: uuid.NewString(), SyncRunID: uuid.NewString(), OrgID: retiredStaleAOrg,
			IntegrationID: uuid.NewString(), SourceID: uuid.NewString(),
			SourceExternalID: "acme/api", SourceName: "acme/api",
			Provider: "github", Dataset: "tests", CostClass: capability.CostClass,
			Mode: "incremental", SinceAt: &since, BeforeAt: &before,
			CredentialID: uuid.NewString(), AuthSource: "integration_credential",
		},
		Owner: uuid.NewString(), Attempt: 1, LeaseExpiresAt: before.Add(time.Hour),
	}
	sink := providersync.TestOpsClickHouseEffects{Conn: stack.conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	for _, effect := range retiredRecordedEmission(t) {
		if err := sink.WriteEffect(stack.ctx, claim, providersync.EffectBatch{Destination: effect.Destination, Rows: effect.Rows}); err != nil {
			t.Fatalf("write recorded %s: %v", effect.Destination, err)
		}
	}
}

// retiredServedOnly drops the stored row counts: a stale run keeps rows no reader serves.
func retiredServedOnly(served matrixTotalsServed) matrixTotalsServed {
	served.suites, served.cases, served.slots, served.watermarkWithheld = 0, 0, 0, false
	return served
}

func retiredBaseRule(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "chaos14259_base_suite_rule_"+name+".txt"))
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(string(raw), "\n")
}

// retiredBaseRead is the day's suite read and the pooled raw arm of an old binary (the base rule text): row count, the sums of
// the counts, the suites with a duration, their median.
type retiredBaseRead struct {
	rows, total, passed, failed, skipped, timed, pooledRows, pooledTimed uint64
	p50                                                                  float64
}

func readRetiredBase(t *testing.T, stack attemptsStack, org string) retiredBaseRead {
	t.Helper()
	repoID := stack.repoID(t, org)
	var read retiredBaseRead
	if err := stack.conn.QueryRow(stack.ctx, `
SELECT count(), sum(total_count), sum(passed_count), sum(failed_count), sum(skipped_count), count(duration_seconds),
       quantileExactInclusive(0.5)(duration_seconds)`+testopsSuiteResultsSource+"\n  AND "+retiredBaseRule(t, "window"),
		testopsSourceArgs(org, []uuid.UUID{repoID}, retiredDay(), retiredDay().Add(24*time.Hour))...).
		Scan(&read.rows, &read.total, &read.passed, &read.failed, &read.skipped, &read.timed, &read.p50); err != nil {
		t.Fatal(err)
	}
	if err := stack.conn.QueryRow(stack.ctx, `SELECT count(), count(duration_seconds) FROM test_suite_results FINAL
WHERE org_id = {org_id:String} AND `+retiredBaseRule(t, "org"), clickhouse.Named("org_id", org)).Scan(&read.pooledRows, &read.pooledTimed); err != nil {
		t.Fatal(err)
	}
	return read
}

type retiredStack struct {
	stack  attemptsStack
	client *dhclickhouse.Client
}

func startRetiredStack(t *testing.T, ctx context.Context) retiredStack {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	chschema.Apply(ctx, t, instance)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return retiredStack{stack: attemptsStack{ctx: ctx, conn: conn}, client: client}
}

func TestTestopsStaleTotalSlotServesTheSyncedOnceNumbers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env := startRetiredStack(t, ctx)
	stack := env.stack
	both, withoutLegB, _ := retiredArtifacts(t)
	near := func(value, target float64) bool { return math.Abs(value-target) < 1e-6 }

	// CONTROL: a fresh org, one sync of BOTH.
	controlOrg := "org-chaos14259-control-" + uuid.NewString()[:8]
	stack.pass(t, controlOrg, retiredRunID, retiredDay().Add(11*time.Hour), retiredState(true, both))
	control := matrixTotalsRead(t, stack, env.client, controlOrg, retiredRunID, retiredDay())
	t.Logf("CONTROL: %+v", control)
	if control.totalCases != 6 || control.passed != 5 || control.totalSuites != 6 || !near(control.passRate, 5.0/6) {
		t.Fatalf("CONTROL serves total_cases %d, passed %d, total_suites %d, pass_rate %v; want 6, 5, 6, 5/6",
			control.totalCases, control.passed, control.totalSuites, control.passRate)
	}

	// STALE-A: sync 1 is the recorded pre-CHAOS-14045 emission (total-b admitted), sync 2 the route on BOTH an hour later.
	writeRetiredRecordedEmission(t, stack)
	stack.pass(t, retiredStaleAOrg, retiredRunID, retiredDay().Add(11*time.Hour), retiredState(true, both))
	staleA := matrixTotalsRead(t, stack, env.client, retiredStaleAOrg, retiredRunID, retiredDay())

	// STALE-B: the listing grows. Sync 1 lists total-b without leg B's parts (admitted), sync 2 lists BOTH.
	staleBOrg := "org-chaos14259-stale-b-" + uuid.NewString()[:8]
	stack.pass(t, staleBOrg, retiredRunID, retiredDay().Add(10*time.Hour), retiredState(false, withoutLegB))
	stack.pass(t, staleBOrg, retiredRunID, retiredDay().Add(11*time.Hour), retiredState(true, both))
	staleB := matrixTotalsRead(t, stack, env.client, staleBOrg, retiredRunID, retiredDay())

	for name, served := range map[string]matrixTotalsServed{"STALE-A": staleA, "STALE-B": staleB} {
		t.Logf("%s: %+v", name, served)
		if got, want := retiredServedOnly(served), retiredServedOnly(control); got != want {
			t.Errorf("%s serves numbers other than the run synced once:\n got  %+v\n want %+v", name, got, want)
		}
	}

	// The total-b rows sync 2 writes for the stale slot are the recorded rows' keys (same suite id, newer last_synced): the
	// stale row is replaced in place, not joined by a row at another key.
	recordedIDs, recordedSynced := retiredRecordedTotalBSuites(t)
	rows, err := stack.conn.Query(ctx, `SELECT suite_id, last_synced FROM test_suite_results FINAL
WHERE org_id = ? AND report_slot = 'total-b' ORDER BY suite_id`, retiredStaleAOrg)
	if err != nil {
		t.Fatal(err)
	}
	var storedIDs []string
	for rows.Next() {
		var id string
		var synced time.Time
		if err := rows.Scan(&id, &synced); err != nil {
			t.Fatal(err)
		}
		storedIDs = append(storedIDs, id)
		if !synced.After(recordedSynced) {
			t.Errorf("STALE-A total-b suite %s keeps last_synced %s, the recorded sync's: sync 2 wrote nothing at its key", id, synced)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if !slices.Equal(storedIDs, recordedIDs) {
		t.Errorf("STALE-A total-b suite ids %v, want the recorded stale ids %v", storedIDs, recordedIDs)
	}

	// K0 (rollback): the rows a new build writes for a fresh run with a drop, read by the OLD rule. Every sum and the median are
	// CONTROL's; the suite count and the pooled raw sample count are CONTROL + 2, the two zero-measure total-b rows with no
	// duration (the named residual of a rollback).
	// K0b: the stale same-key arm (STALE-A) read by the old rule: the zero rows replaced the stale rows in place, so the sums are
	// CONTROL's too.
	for name, org := range map[string]string{"K0 CONTROL": controlOrg, "K0b STALE-A": retiredStaleAOrg} {
		read := readRetiredBase(t, stack, org)
		t.Logf("%s read by the base rule: %+v", name, read)
		if read.total != uint64(control.totalCases) || read.passed != uint64(control.passed) || read.failed != uint64(control.failed) ||
			read.skipped != 0 || read.timed != uint64(control.totalSuites) || fmt.Sprint(read.p50) != control.p50 {
			t.Errorf("%s: the base rule reads total %d, passed %d, failed %d, skipped %d, timed suites %d, p50 %v; want CONTROL's %d, %d, %d, 0, %d, %s",
				name, read.total, read.passed, read.failed, read.skipped, read.timed, read.p50,
				control.totalCases, control.passed, control.failed, control.totalSuites, control.p50)
		}
		if read.rows != uint64(control.totalSuites)+2 || read.pooledRows != control.pooledCount+2 || read.pooledTimed != control.pooledCount {
			t.Errorf("%s: the base rule reads %d suite rows, %d pooled rows (%d timed); want CONTROL + 2 = %d, %d (%d timed)",
				name, read.rows, read.pooledRows, read.pooledTimed, control.totalSuites+2, control.pooledCount+2, control.pooledCount)
		}
	}
}

// retiredArchive is matrixTotalsArchive with the suites' timestamp given.
func retiredArchive(t *testing.T, timestamp string, suites ...matrixTotalsSuite) []byte {
	t.Helper()
	var body strings.Builder
	body.WriteString(`<testsuites>`)
	for _, suite := range suites {
		body.WriteString(`<testsuite name="` + suite.name + `" time="` + suite.time + `" timestamp="` + timestamp + `">`)
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
	return attemptsZip(t, body.String())
}

func attemptsZip(t *testing.T, report string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	member, err := writer.Create("report.xml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := member.Write([]byte(report)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// retiredMetric is the part of a test day the cells compare.
type retiredMetric struct {
	total, passed, failed, suites, flake, distinct, recurrent, failedNames int
	flakeUnknown                                                           bool
}

func retiredMetricOf(t *testing.T, stack attemptsStack, org string, day time.Time) retiredMetric {
	t.Helper()
	m := stack.metric(t, org, stack.repoID(t, org), day)
	return retiredMetric{total: m.TotalCases, passed: m.PassedCount, failed: m.FailedCount, suites: m.TotalSuites, flake: m.FlakeCases,
		distinct: m.DistinctCases, recurrent: m.RecurrentFailedNames, failedNames: m.DistinctFailedNames, flakeUnknown: m.FlakeUnknown}
}

func retiredRows(t *testing.T, stack attemptsStack, org, runID, slot string) (live, retracted uint64) {
	t.Helper()
	if err := stack.conn.QueryRow(stack.ctx, `SELECT countIf(retracted = 0), countIf(retracted = 1) FROM test_suite_results FINAL
WHERE org_id = ? AND run_id = ? AND report_slot = ?`, org, runID, slot).Scan(&live, &retracted); err != nil {
		t.Fatal(err)
	}
	return live, retracted
}

// retiredTwoAttemptRun is a run whose slot total-b holds, at attempt 1, a report no part mirrors (case a failed) and, at attempt 2,
// the exact aggregate of the re-run leg's two parts (case a passed): attempt 2 drops it and retires the slot over both attempts.
// unit is a kept slot whose case c fails at attempt 1 and passes at attempt 2 (a flake). day is the run's day.
func retiredTwoAttemptRun(t *testing.T, stack attemptsStack, org, runID string, day time.Time, withUnit bool) {
	t.Helper()
	date := day.Format(time.DateOnly)
	stamp := func(clock string) string { return date + "T" + clock + "Z" }
	ts1, ts2 := date+"T10:02:00", date+"T11:12:00"
	attempt1Total := []matrixTotalsSuite{{"left", "3.0", []matrixTotalsCase{{"a", "failed", "0.25"}}}, {"right", "3.0", []matrixTotalsCase{{"b", "", "0.25"}}}}
	attempt2Leg := []matrixTotalsSuite{{"left", "3.0", []matrixTotalsCase{{"a", "", "0.25"}}}, {"right", "3.0", []matrixTotalsCase{{"b", "", "0.25"}}}}
	attempt1 := []attemptsArtifact{{6, "total-b", stamp("10:20:00"), false, retiredArchive(t, ts1, attempt1Total...)}}
	attempt2 := []attemptsArtifact{
		{13, "leg-b-left", stamp("11:30:00"), false, retiredArchive(t, ts2, attempt2Leg[0])},
		{14, "leg-b-right", stamp("11:30:00"), false, retiredArchive(t, ts2, attempt2Leg[1])},
		{16, "total-b", stamp("11:30:00"), false, retiredArchive(t, ts2, attempt2Leg...)},
	}
	if withUnit {
		attempt1 = append(attempt1, attemptsArtifact{7, "unit", stamp("10:20:00"), false,
			retiredArchive(t, ts1, matrixTotalsSuite{"u", "1.0", []matrixTotalsCase{{"c", "failed", "0.5"}}})})
		attempt2 = append(attempt2, attemptsArtifact{17, "unit", stamp("11:30:00"), false,
			retiredArchive(t, ts2, matrixTotalsSuite{"u", "1.0", []matrixTotalsCase{{"c", "", "0.5"}}})})
	}
	stack.pass(t, org, runID, day.Add(10*time.Hour), attemptsRunState{attempt: 1, started: stamp("10:01:00"), updated: stamp("10:30:00"), artifacts: attempt1})
	stack.pass(t, org, runID, day.Add(11*time.Hour), attemptsRunState{created: stamp("10:01:00"), attempt: 2, started: stamp("11:10:00"),
		updated: stamp("11:40:00"), artifacts: append(slices.Clone(attempt1), attempt2...)})
	if live, retracted := retiredRows(t, stack, org, runID, "total-b"); live != 2 || retracted != 2 {
		t.Fatalf("run %s slot total-b: %d live (attempt 1) and %d retracted (attempt 2) rows, want 2 and 2", runID, live, retracted)
	}
}

func TestTestopsRetiredSlotKeepCells(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	env := startRetiredStack(t, ctx)
	stack := env.stack
	both, withoutLegB, _ := retiredArtifacts(t)
	newOrg := func(name string) string { return "org-chaos14259-" + name + "-" + uuid.NewString()[:8] }

	controlOrg := newOrg("control")
	stack.pass(t, controlOrg, retiredRunID, retiredDay().Add(11*time.Hour), retiredState(true, both))
	control := retiredMetricOf(t, stack, controlOrg, retiredDay())
	if control.total != 6 || control.suites != 6 || control.flake != 1 || control.distinct != 2 {
		t.Fatalf("CONTROL %+v, want 6 cases, 6 suites, 1 flake (case a), 2 distinct", control)
	}

	// K2 revive: a later walk that admits total-b again (its parts left the listing) writes live rows over the retraction rows.
	t.Run("K2 revive", func(t *testing.T) {
		org := newOrg("revive")
		stack.pass(t, org, retiredRunID, retiredDay().Add(10*time.Hour), retiredState(false, withoutLegB))
		stack.pass(t, org, retiredRunID, retiredDay().Add(11*time.Hour), retiredState(true, both))
		if got := retiredMetricOf(t, stack, org, retiredDay()); got != control {
			t.Fatalf("retired %+v, want CONTROL %+v", got, control)
		}
		stack.pass(t, org, retiredRunID, retiredDay().Add(12*time.Hour), attemptsRunState{attempt: 1, started: "2026-07-22T10:01:00Z",
			updated: "2026-07-22T12:40:00Z", artifacts: withoutLegB})
		if live, retracted := retiredRows(t, stack, org, retiredRunID, "total-b"); live != 2 || retracted != 0 {
			t.Fatalf("revived total-b: %d live, %d retracted rows; want 2 and 0", live, retracted)
		}
		if got := retiredMetricOf(t, stack, org, retiredDay()); got.total != 8 || got.suites != 8 {
			t.Fatalf("revived %+v, want total-b counted again: 8 cases, 8 suites", got)
		}
	})

	// K3 re-run: attempt 2 re-runs leg A only. The other slots are carried over at attempt 1 and stay; total-b, still an
	// aggregate of leg B, stays retired; the newest-attempt rule is unchanged.
	t.Run("K3 re-run of one leg", func(t *testing.T) {
		org := newOrg("rerun")
		stack.pass(t, org, retiredRunID, retiredDay().Add(10*time.Hour), retiredState(false, both))
		rerun := slices.Clone(both)
		for _, index := range []int{0, 1} {
			again := both[index]
			again.id, again.created = again.id+10, "2026-07-22T11:30:00Z"
			rerun = append(rerun, again)
		}
		stack.pass(t, org, retiredRunID, retiredDay().Add(11*time.Hour), attemptsRunState{created: "2026-07-22T10:01:00Z", attempt: 2,
			started: "2026-07-22T11:10:00Z", updated: "2026-07-22T11:40:00Z", artifacts: rerun})
		if got := retiredMetricOf(t, stack, org, retiredDay()); got != control {
			t.Fatalf("re-run %+v, want CONTROL %+v", got, control)
		}
		var attempts uint64
		if err := stack.conn.QueryRow(ctx, `SELECT uniqExact(run_attempt) FROM test_suite_results FINAL WHERE org_id = ? AND report_slot = 'leg-a-left'`, org).Scan(&attempts); err != nil {
			t.Fatal(err)
		}
		if live, retracted := retiredRows(t, stack, org, retiredRunID, "total-b"); attempts != 2 || live != 0 || retracted != 2 {
			t.Fatalf("leg-a-left attempts %d, total-b %d live / %d retracted; want 2 attempts and total-b retired", attempts, live, retracted)
		}
	})

	// K4 flake: case rows of an older attempt of a kept slot count (unit: c failed, then passed = a flake); case rows of the
	// retired slot at any attempt do not (total-b's attempt-1 failure of case a is no flake of a).
	t.Run("K4 flake", func(t *testing.T) {
		org := newOrg("flake")
		retiredTwoAttemptRun(t, stack, org, "43600014261", retiredDay(), true)
		want := retiredMetric{total: 3, passed: 3, suites: 3, flake: 1, distinct: 3, failedNames: 1}
		if got := retiredMetricOf(t, stack, org, retiredDay()); got != want {
			t.Fatalf("flake day %+v, want %+v (case c the only flake and the only failed name)", got, want)
		}
	})

	// K5 history: the retired slot sits on a HISTORY day. Its attempt-1 failure of case a is no failure in history, so today's
	// failure of a does not recur. The history set is scoped by its own window: scoped to today, the retired slot would not be
	// found and a would recur.
	t.Run("K5 history", func(t *testing.T) {
		org := newOrg("history")
		retiredTwoAttemptRun(t, stack, org, "43600014262", retiredDay().AddDate(0, 0, -1), false)
		stack.pass(t, org, "43600014263", retiredDay().Add(10*time.Hour), retiredState(false, []attemptsArtifact{
			{21, "today", "2026-07-22T10:20:00Z", false, retiredArchive(t, "2026-07-22T10:02:00", matrixTotalsSuite{"t", "1.0", []matrixTotalsCase{{"a", "failed", "0.5"}}})},
		}))
		got := retiredMetricOf(t, stack, org, retiredDay())
		if got.failed != 1 || got.failedNames != 1 || got.recurrent != 0 {
			t.Fatalf("today %+v, want case a failed today (1 failed name) and not recurrent", got)
		}
	})

	// K6 legacy: the retraction rows still list total-b's pre-slot ids, so a pre-slot copy of total-b stays hidden; a legacy row
	// no kept or retracted row lists stays.
	t.Run("K6 legacy", func(t *testing.T) {
		org := newOrg("legacy")
		stack.pass(t, org, retiredRunID, retiredDay().Add(11*time.Hour), retiredState(true, both))
		repoID := stack.repoID(t, org)
		var listed string
		if err := stack.conn.QueryRow(ctx, `SELECT legacy_suite_ids[1] FROM test_suite_results FINAL
WHERE org_id = ? AND report_slot = 'total-b' AND retracted = 1 ORDER BY suite_id LIMIT 1`, org).Scan(&listed); err != nil || listed == "" {
			t.Fatalf("no listed legacy id on a total-b retraction row (%v)", err)
		}
		started := retiredDay().Add(10*time.Hour + time.Minute)
		for _, id := range []string{listed, "chaos14259-unlisted-legacy"} {
			if err := stack.conn.Exec(ctx, `INSERT INTO test_suite_results
(org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, error_count, quarantined_count,
 duration_seconds, started_at, finished_at, last_synced)
VALUES (?, ?, ?, ?, 'legacy', 1, 1, 0, 0, 0, 0, 3.0, ?, ?, ?)`, org, repoID, retiredRunID, id, started, started, retiredDay().Add(9*time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
		kept := retiredKeptSuites(t, stack, org)
		if kept[listed] || !kept["chaos14259-unlisted-legacy"] || len(kept) != 7 {
			t.Fatalf("kept %v: want the 6 live slotted suites and the unlisted legacy row, not the listed pre-slot copy %s", kept, listed)
		}
	})

	// K8, K9 ties at one last_synced. Same key: the engine keeps the last inserted row (no SQL tie-break acts: FINAL collapses
	// first). Drifted key (another suite id at the slot): the retraction row wins the slot.
	t.Run("K8 K9 ties", func(t *testing.T) {
		org := newOrg("ties")
		repoID := uuid.New()
		at := retiredDay().Add(12 * time.Hour)
		started := retiredDay().Add(10 * time.Hour)
		insert := func(run, suite string, synced time.Time, retracted uint8) {
			t.Helper()
			total := uint32(1 - retracted)
			if err := stack.conn.Exec(ctx, `INSERT INTO test_suite_results
(org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, error_count, quarantined_count,
 started_at, last_synced, report_slot, run_attempt, retracted)
VALUES (?, ?, ?, ?, 'tie', ?, ?, 0, 0, 0, 0, ?, ?, 'slot', 1, ?)`, org, repoID, run, suite, total, total, started, synced, retracted); err != nil {
				t.Fatal(err)
			}
		}
		insert("k8-live-first", "k8", at, 0)
		insert("k8-live-first", "k8", at, 1)
		insert("k8-retraction-first", "k8", at, 1)
		insert("k8-retraction-first", "k8", at, 0)
		insert("k9-drifted", "k9-stale", at, 0)
		insert("k9-drifted", "k9-retraction", at, 1)
		insert("k9-older-retraction", "k9-live", at, 0)
		insert("k9-older-retraction", "k9-retraction", at.Add(-time.Hour), 1)
		kept := map[string]bool{}
		rows, err := stack.conn.Query(ctx, `SELECT run_id, suite_id FROM test_suite_results AS s FINAL WHERE s.org_id = {org_id:String} AND `+
			testopspooled.SuiteRowsNewestAttempt("s", testopspooled.Scope{}), clickhouse.Named("org_id", org))
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var run, suite string
			if err := rows.Scan(&run, &suite); err != nil {
				t.Fatal(err)
			}
			kept[run+"/"+suite] = true
		}
		rows.Close()
		want := map[string]bool{"k8-retraction-first/k8": true, "k9-older-retraction/k9-live": true}
		if !maps.Equal(kept, want) {
			t.Fatalf("kept %v, want %v", kept, want)
		}
	})

	// K13: an off-default run (branch feature/x, default main) with a stale total-b slot serves what the run synced once serves,
	// the off-default mark and the unknown-test rows included.
	t.Run("K13 off-default", func(t *testing.T) {
		offState := func(later bool, artifacts []attemptsArtifact) attemptsRunState {
			state := retiredState(later, artifacts)
			state.branch = "feature/x"
			return state
		}
		controlOff, staleOff := newOrg("control-off"), newOrg("stale-off")
		stack.pass(t, controlOff, retiredRunID, retiredDay().Add(11*time.Hour), offState(true, both))
		stack.pass(t, staleOff, retiredRunID, retiredDay().Add(10*time.Hour), offState(false, withoutLegB))
		stack.pass(t, staleOff, retiredRunID, retiredDay().Add(11*time.Hour), offState(true, both))
		served := map[string]retiredOffDefaultServed{}
		for _, org := range []string{controlOff, staleOff} {
			if err := stack.conn.Exec(ctx, `INSERT INTO repos (id, repo, created_at, settings, last_synced, org_id) VALUES (?, 'acme/api', ?, ?, ?, ?)`,
				stack.repoID(t, org), retiredDay(), `{"default_branch":"main"}`, retiredDay().Add(23*time.Hour), org); err != nil {
				t.Fatal(err)
			}
			metric := retiredMetricOf(t, stack, org, retiredDay())
			if !metric.flakeUnknown || metric.total != 6 {
				t.Fatalf("%s %+v: want 6 cases and the off-default mark", org, metric)
			}
			served[org] = readRetiredOffDefault(t, stack, env.client, org, metric)
		}
		if served[staleOff] != served[controlOff] {
			t.Fatalf("off-default stale run serves\n %+v\nwant\n %+v", served[staleOff], served[controlOff])
		}
	})
}

// retiredOffDefaultServed is what an off-default day serves: the test day, and the window and risk answers the families write
// (the risk family takes the unknown-test rows). matrixTotalsRead scans the rates as numbers, and an off-default day stores them
// NULL.
type retiredOffDefaultServed struct {
	metric       retiredMetric
	window, risk string
}

func readRetiredOffDefault(t *testing.T, stack attemptsStack, client *dhclickhouse.Client, org string, metric retiredMetric) retiredOffDefaultServed {
	t.Helper()
	ctx, conn, day := stack.ctx, stack.conn, retiredDay()
	repoID := stack.repoID(t, org)
	seedTestopsAuthoritativeOwner(ctx, t, conn, org, repoID, day)
	run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}
	partition := Partition{ID: uuid.NewString(), RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repoID.String())}}
	for _, build := range []func(driver.Conn) (NativeFamilyExecutor, error){
		func(conn driver.Conn) (NativeFamilyExecutor, error) { return NewTestopsTestExecutor(conn) },
		func(conn driver.Conn) (NativeFamilyExecutor, error) { return NewTestopsPipelineExecutor(conn) },
		func(conn driver.Conn) (NativeFamilyExecutor, error) { return NewTestopsRiskExecutor(conn) },
	} {
		executor, err := build(conn)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
			t.Fatal(err)
		}
	}
	window, err := testopswindow.Resolve(ctx, client, org, graphqldate.New(day), graphqldate.New(day), testopswindow.Scope{})
	if err != nil {
		t.Fatal(err)
	}
	windowJSON, err := json.Marshal(window)
	if err != nil {
		t.Fatal(err)
	}
	risk, err := testopsrisk.Resolve(ctx, client, org, model.TestOpsRiskInput{StartDate: graphqldate.New(day), EndDate: graphqldate.New(day.Add(24 * time.Hour))})
	if err != nil {
		t.Fatal(err)
	}
	risk.OrgID = ""
	riskJSON, err := json.Marshal(risk)
	if err != nil {
		t.Fatal(err)
	}
	return retiredOffDefaultServed{metric: metric, window: string(windowJSON), risk: string(riskJSON)}
}

// retiredKeptSuites is the suite ids the readers keep (the pooled rule, the organization scope).
func retiredKeptSuites(t *testing.T, stack attemptsStack, org string) map[string]bool {
	t.Helper()
	rows, err := stack.conn.Query(stack.ctx, `SELECT suite_id FROM test_suite_results AS s FINAL WHERE s.org_id = {org_id:String} AND `+
		testopspooled.SuiteRowsNewestAttempt("s", testopspooled.Scope{}), clickhouse.Named("org_id", org))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	kept := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		kept[id] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return kept
}

// PROFILE: the reads that carry the retired-slot rule read at most their named scans of each table, +50 %. Every scan of an
// org-bounded select reads at most the table (one container, one org, merges stopped), so the bound per statement is
// sum(scans of a table x its rows) x 1.5. The new selects are all over test_suite_results: per use of SuiteRowsNewestAttempt the
// retired-slot select and the retired slots' legacy-id select with its own retired-slot select, each with its window run-set
// select (+6, +3 without a window); per use of SuiteRowsNotRetired the same (+6). No statement gains a test_case_results scan.
func TestTestopsRetiredSlotReadsStayInTheirScans(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	env := startRetiredStack(t, ctx)
	stack := env.stack
	for _, table := range []string{"test_suite_results", "test_case_results", "ci_pipeline_runs", "testops_run_report_completeness"} {
		if err := stack.conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatal(err)
		}
	}
	both, withoutLegB, _ := retiredArtifacts(t)
	org := "org-chaos14259-profile"
	stack.pass(t, org, retiredRunID, retiredDay().Add(10*time.Hour), retiredState(false, withoutLegB))
	stack.pass(t, org, retiredRunID, retiredDay().Add(11*time.Hour), retiredState(true, both))
	retiredTwoAttemptRun(t, stack, org, "43600014261", retiredDay(), true)
	repoID := stack.repoID(t, org)
	if err := stack.conn.Exec(ctx, `INSERT INTO repos (id, repo, created_at, settings, last_synced, org_id) VALUES (?, 'acme/api', ?, ?, ?, ?)`,
		repoID, retiredDay(), `{"default_branch":"main"}`, retiredDay().Add(23*time.Hour), org); err != nil {
		t.Fatal(err)
	}
	rowsOf := func(table string) uint64 {
		var count uint64
		if err := stack.conn.QueryRow(ctx, "SELECT count() FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	suites, cases, pipelines, marks, repos := rowsOf("test_suite_results"), rowsOf("test_case_results"), rowsOf("ci_pipeline_runs"),
		rowsOf("testops_run_report_completeness"), rowsOf("repos")
	t.Logf("raw rows: suites %d, cases %d, pipelines %d, marks %d, repos %d", suites, cases, pipelines, marks, repos)

	start, end := retiredDay(), retiredDay().Add(24*time.Hour)
	historyStart := retiredDay().AddDate(0, 0, -29)
	accumulator := testops.NewTestAccumulator(repoID, "", nil)
	// the window source + SuiteRowsNewestAttempt with a window: newest x2, legacy-id, retired, retired legacy-id + its retired,
	// each + its window run set
	const keptToday = 1 + 12
	const notRetired = 6 // retired, retired legacy-id + its retired; each + its window run set
	const offDefault = 2 // one reference to off_default: window_runs twice (and one ci_pipeline_runs scan)
	statements := []struct {
		name                                             string
		suiteScans, caseScans, pipeScans, mks, repoScans uint64
		run                                              func(context.Context) error
	}{
		{"suite read", keptToday, 0, 0, 0, 0, func(ctx context.Context) error {
			return loadNativeTestopsSuites(ctx, stack.conn, accumulator, org, repoID, start, end)
		}},
		// The case aggregate reads the default branch first (repos), under the same query id.
		{"case aggregate", keptToday + (1 + notRetired) + (1 + notRetired) + 2*offDefault, 1, 2, 0, 1, func(ctx context.Context) error {
			_, err := loadNativeTestopsCaseAggregate(ctx, stack.conn, accumulator, org, repoID, historyStart, start, end)
			return err
		}},
		{"off-default check", offDefault + keptToday + (1 + notRetired), 1, 1, 0, 0, func(ctx context.Context) error {
			_, err := hasNativeTestopsOffDefaultCaseToday(ctx, stack.conn, org, repoID, "main", historyStart, start, end)
			return err
		}},
		{"completeness read", 2 * keptToday, 0, 0, 1, 0, func(ctx context.Context) error {
			rows, err := stack.conn.Query(ctx, testopsReportCompletenessStatement, testopsSourceArgs(org, []uuid.UUID{repoID}, start, end)...)
			if err == nil {
				rows.Close()
			}
			return err
		}},
		{"unknown tests", keptToday, 0, 0, 0, 0, func(ctx context.Context) error {
			rows, err := stack.conn.Query(ctx, "SELECT DISTINCT repo_id"+testopsKeptSuitesToday, testopsSourceArgs(org, []uuid.UUID{repoID}, historyStart, start)...)
			if err == nil {
				rows.Close()
			}
			return err
		}},
		{"pooled", 1 + 6, 0, 0, 0, 0, func(ctx context.Context) error {
			var count uint64
			return stack.conn.QueryRow(ctx, "SELECT count() FROM "+testopspooled.SuiteRunsSource, clickhouse.Named("org_id", org),
				clickhouse.Named("start_date", start.Format(time.DateOnly)), clickhouse.Named("end_date", end.Format(time.DateOnly))).Scan(&count)
		}},
	}
	ids := make([]string, len(statements))
	for index, statement := range statements {
		ids[index] = "chaos14259-profile-" + uuid.NewString()
		if err := statement.run(clickhouse.Context(ctx, clickhouse.WithQueryID(ids[index]))); err != nil {
			t.Fatalf("%s: %v", statement.name, err)
		}
	}
	if err := stack.conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	for index, statement := range statements {
		var read, queries uint64
		if err := stack.conn.QueryRow(ctx, `SELECT sum(read_rows), count() FROM system.query_log WHERE query_id = ? AND type = 'QueryFinish'`, ids[index]).Scan(&read, &queries); err != nil {
			t.Fatalf("%s: query_log: %v", statement.name, err)
		}
		scans := statement.suiteScans*suites + statement.caseScans*cases + statement.pipeScans*pipelines + statement.mks*marks + statement.repoScans*repos
		bound := scans + scans/2
		t.Logf("%s: %d queries read %d rows, named scans %d rows, bound %d", statement.name, queries, read, scans, bound)
		if read == 0 || read > bound {
			t.Errorf("%s read %d rows, want 1..%d (its named scans + 50 %%)", statement.name, read, bound)
		}
	}
}

// The base reader's case read, taken from the base source, not copied: testdata/chaos14259_base_testops_native_clickhouse.go.txt
// is `git show 85378e468f250a75e3dcd61250bc489eba6fee79:internal/jobs/metrics/daily/testops_native_clickhouse.go` (the blob
// 14d8a684a379aa42d7a099d81463c0df73979c03), pinned by its sha256. The SQL is the second argument of the conn.Query call in
// loadNativeTestopsCaseAggregate, its concatenation evaluated with the base's kept-suite text (the window source and the base
// rule) and no default branch.
const (
	retiredBaseReaderFile   = "chaos14259_base_testops_native_clickhouse.go.txt"
	retiredBaseReaderSHA256 = "34c048123f0872bed807e2e1c2115afc8525d322846247a851f2dad7ecb72794"
)

func retiredBaseCaseAggregateSQL(t *testing.T) string {
	t.Helper()
	query := retiredBaseReaderSQL(t, "loadNativeTestopsCaseAggregate")
	if !strings.Contains(query, "FROM test_case_results FINAL") {
		t.Fatalf("no case read found in the base loadNativeTestopsCaseAggregate:\n%s", query)
	}
	return query
}

// retiredBaseReaderSQL is the SQL of the first conn.Query call of function in the base reader source.
func retiredBaseReaderSQL(t *testing.T, function string) string {
	t.Helper()
	source, err := os.ReadFile(filepath.Join("testdata", retiredBaseReaderFile))
	if err != nil {
		t.Fatal(err)
	}
	if sum := sha256.Sum256(source); hex.EncodeToString(sum[:]) != retiredBaseReaderSHA256 {
		t.Fatalf("%s sha256 %x, pinned %s: the base reader source was edited", retiredBaseReaderFile, sum, retiredBaseReaderSHA256)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "testops_native_clickhouse.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{
		"offDefaultCTE":          "",
		"offDefaultExclusion":    "",
		"testopsKeptSuitesToday": testopsSuiteResultsSource + "\n  AND " + retiredBaseRule(t, "window"),
	}
	var evaluate func(ast.Expr) string
	evaluate = func(expression ast.Expr) string {
		switch typed := expression.(type) {
		case *ast.BinaryExpr:
			if typed.Op != token.ADD {
				t.Fatalf("base SQL holds operator %s", typed.Op)
			}
			return evaluate(typed.X) + evaluate(typed.Y)
		case *ast.BasicLit:
			text, err := strconv.Unquote(typed.Value)
			if err != nil {
				t.Fatal(err)
			}
			return text
		case *ast.Ident:
			value, known := values[typed.Name]
			if !known {
				t.Fatalf("base SQL names %s, which the cell does not evaluate", typed.Name)
			}
			return value
		}
		t.Fatalf("base SQL holds a %T", expression)
		return ""
	}
	query := ""
	ast.Inspect(file, func(node ast.Node) bool {
		declaration, isFunction := node.(*ast.FuncDecl)
		if !isFunction || declaration.Name.Name != function {
			return true
		}
		ast.Inspect(declaration.Body, func(inner ast.Node) bool {
			call, isCall := inner.(*ast.CallExpr)
			if selector, isSelector := callSelector(call, isCall); isSelector && selector == "Query" && query == "" {
				query = evaluate(call.Args[1])
			}
			return true
		})
		return false
	})
	if query == "" {
		t.Fatalf("no conn.Query call in the base %s", function)
	}
	return query
}

func callSelector(call *ast.CallExpr, isCall bool) (string, bool) {
	if !isCall {
		return "", false
	}
	selector, isSelector := call.Fun.(*ast.SelectorExpr)
	if !isSelector {
		return "", false
	}
	return selector.Sel.Name, true
}

// retiredOldReaderCaseMeasures runs the base case read over the stored rows: today's flake and distinct names, today's failed
// names and those of them that failed in history (testops.TestAccumulator's rules).
func retiredOldReaderCaseMeasures(t *testing.T, stack attemptsStack, org string, day time.Time) (flake, distinct, failedToday, recurrent int) {
	t.Helper()
	args := append(testopsCaseArgs(org, stack.repoID(t, org), "", day.AddDate(0, 0, -29), day, day.Add(24*time.Hour)),
		clickhouse.Named("failure_statuses", testopsFailureStatuses))
	rows, err := stack.conn.Query(stack.ctx, retiredBaseCaseAggregateSQL(t), args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var statuses []string
		var maxRetry uint32
		var seenToday, failedBefore uint8
		if err := rows.Scan(&name, &statuses, &maxRetry, &seenToday, &failedBefore); err != nil {
			t.Fatal(err)
		}
		if seenToday == 0 {
			continue
		}
		passed, failed := false, false
		for _, status := range statuses {
			switch strings.ToLower(strings.TrimSpace(status)) {
			case "passed":
				passed = true
			case "failed":
				failed = true
			}
		}
		distinct++
		if passed && failed {
			flake++
		}
		if failed {
			failedToday++
			if failedBefore != 0 {
				recurrent++
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return flake, distinct, failedToday, recurrent
}

// A rollback to the base binary serves what the run synced once serves on the case side: the case retraction rows of every
// attempt label of the dropped slot hide its stale cases from the base case read (a row with no name). K4: the slot's
// attempt-1 failure of case a is no flake (1 flake, c, of 3 names). K5: the slot's attempt-1 failure on a history day is no
// recurrence of today's failure of a (0 of 1).
func TestTestopsRetiredSlotOldReaderServesTheSyncedOnceCaseMeasures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	env := startRetiredStack(t, ctx)
	stack := env.stack
	newOrg := func(name string) string { return "org-chaos14259-" + name + "-" + uuid.NewString()[:8] }

	flakeOrg := newOrg("old-reader-flake")
	retiredTwoAttemptRun(t, stack, flakeOrg, "43600014261", retiredDay(), true)
	if flake, distinct, _, _ := retiredOldReaderCaseMeasures(t, stack, flakeOrg, retiredDay()); flake != 1 || distinct != 3 {
		t.Errorf("K4 base reader: flake %d of %d names, want CONTROL 1 of 3", flake, distinct)
	}

	historyOrg := newOrg("old-reader-history")
	retiredTwoAttemptRun(t, stack, historyOrg, "43600014262", retiredDay().AddDate(0, 0, -1), false)
	stack.pass(t, historyOrg, "43600014263", retiredDay().Add(10*time.Hour), retiredState(false, []attemptsArtifact{
		{21, "today", "2026-07-22T10:20:00Z", false, retiredArchive(t, "2026-07-22T10:02:00", matrixTotalsSuite{"t", "1.0", []matrixTotalsCase{{"a", "failed", "0.5"}}})},
	}))
	if _, _, failedToday, recurrent := retiredOldReaderCaseMeasures(t, stack, historyOrg, retiredDay()); failedToday != 1 || recurrent != 0 {
		t.Errorf("K5 base reader: recurrent %d of %d failed names, want CONTROL 0 of 1", recurrent, failedToday)
	}
}

// retiredAttemptLegacyID is the pre-slot id total-b's attempt-1 left suite lists (artifact 6, no superseded copy).
func retiredAttemptLegacyID(t *testing.T, stack attemptsStack, org, runID string) string {
	t.Helper()
	var ids []string
	if err := stack.conn.QueryRow(stack.ctx, `SELECT legacy_suite_ids FROM test_suite_results FINAL
WHERE org_id = ? AND run_id = ? AND report_slot = 'total-b' AND run_attempt = 1 AND suite_name = 'left' AND retracted = 0`, org, runID).Scan(&ids); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] == "" {
		t.Fatalf("total-b attempt 1 lists legacy ids %v, want one", ids)
	}
	return ids[0]
}

// A pre-slot copy of a retired slot is retired with it, in the suite reads and the case reads: a copy of the slot's attempt-1
// failure (listed by every attempt), and a copy whose id only the slot's attempt-1 rows list (the attempt-1 artifact had left the
// listing when attempt 2 dropped the report).
func TestTestopsRetiredSlotRetiresItsPreSlotCopies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	env := startRetiredStack(t, ctx)
	stack := env.stack
	newOrg := func(name string) string { return "org-chaos14259-" + name + "-" + uuid.NewString()[:8] }
	insertLegacy := func(org, runID, id string, withFailedCase bool) {
		t.Helper()
		repoID := stack.repoID(t, org)
		started, synced := retiredDay().Add(10*time.Hour+time.Minute), retiredDay().Add(9*time.Hour)
		if err := stack.conn.Exec(ctx, `INSERT INTO test_suite_results
(org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, error_count, quarantined_count,
 duration_seconds, started_at, last_synced)
VALUES (?, ?, ?, ?, 'left', 1, 0, 1, 0, 0, 0, 3.0, ?, ?)`, org, repoID, runID, id, started, synced); err != nil {
			t.Fatal(err)
		}
		if withFailedCase {
			if err := stack.conn.Exec(ctx, `INSERT INTO test_case_results
(org_id, repo_id, run_id, suite_id, case_id, case_name, status, retry_attempt, last_synced)
VALUES (?, ?, ?, ?, 'legacy-a', 'a', 'failed', 0, ?)`, org, repoID, runID, id, synced); err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("pre-slot copy of a failed case", func(t *testing.T) {
		org := newOrg("legacy-copy")
		retiredTwoAttemptRun(t, stack, org, "43600014261", retiredDay(), true)
		insertLegacy(org, "43600014261", retiredAttemptLegacyID(t, stack, org, "43600014261"), true)
		want := retiredMetric{total: 3, passed: 3, suites: 3, flake: 1, distinct: 3, failedNames: 1}
		if got := retiredMetricOf(t, stack, org, retiredDay()); got != want {
			t.Errorf("served %+v, want CONTROL %+v", got, want)
		}
	})

	t.Run("legacy id of an older attempt only", func(t *testing.T) {
		org := newOrg("legacy-older")
		runID := "43600014264"
		date := retiredDay().Format(time.DateOnly)
		stamp := func(clock string) string { return date + "T" + clock + "Z" }
		ts1, ts2 := date+"T10:02:00", date+"T11:12:00"
		attempt1Total := []matrixTotalsSuite{{"left", "3.0", []matrixTotalsCase{{"a", "failed", "0.25"}}}, {"right", "3.0", []matrixTotalsCase{{"b", "", "0.25"}}}}
		attempt2Leg := []matrixTotalsSuite{{"left", "3.0", []matrixTotalsCase{{"a", "", "0.25"}}}, {"right", "3.0", []matrixTotalsCase{{"b", "", "0.25"}}}}
		stack.pass(t, org, runID, retiredDay().Add(10*time.Hour), attemptsRunState{attempt: 1, started: stamp("10:01:00"), updated: stamp("10:30:00"),
			artifacts: []attemptsArtifact{{6, "total-b", stamp("10:20:00"), false, retiredArchive(t, ts1, attempt1Total...)}}})
		listed := retiredAttemptLegacyID(t, stack, org, runID)
		stack.pass(t, org, runID, retiredDay().Add(11*time.Hour), attemptsRunState{created: stamp("10:01:00"), attempt: 2, started: stamp("11:10:00"),
			updated: stamp("11:40:00"), artifacts: []attemptsArtifact{
				{13, "leg-b-left", stamp("11:30:00"), false, retiredArchive(t, ts2, attempt2Leg[0])},
				{14, "leg-b-right", stamp("11:30:00"), false, retiredArchive(t, ts2, attempt2Leg[1])},
				{16, "total-b", stamp("11:30:00"), false, retiredArchive(t, ts2, attempt2Leg...)},
			}})
		insertLegacy(org, runID, listed, false)
		if got := retiredMetricOf(t, stack, org, retiredDay()); got.total != 2 || got.suites != 2 || got.failed != 0 {
			t.Errorf("served total %d, suites %d, failed %d; want CONTROL 2, 2, 0", got.total, got.suites, got.failed)
		}
		if retiredKeptSuites(t, stack, org)[listed] {
			t.Errorf("the pooled rule keeps the pre-slot copy %s of the retired slot", listed)
		}
	})
}

// ROLLBACK-ONLY RESIDUAL, pinned as measured (not CONTROL): a suite whose stored key drifted from the parsed key in the same
// attempt. The base suite read (taken from the base source, as above) admits every row of the slot's newest attempt, and no row
// the writer can key replaces the drifted one, so after a rollback the base reader counts the stale failure until that day is
// recomputed by the new build. CONTROL is total 1, passed 1, failed 0, timed 1; the base reader serves total 2, passed 1,
// failed 1, timed 2.
func TestTestopsRetiredSlotOldReaderDriftedKeyResidualIsPinned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := startRetiredStack(t, ctx)
	stack := env.stack
	org := "org-chaos14259-drifted-" + uuid.NewString()[:8]
	repoID := uuid.New()
	started := retiredDay().Add(10 * time.Hour)
	insert := func(suite, slot string, total, passed, failed uint32, duration *float64, synced time.Time, retracted uint8) {
		t.Helper()
		if err := stack.conn.Exec(ctx, `INSERT INTO test_suite_results
(org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, error_count, quarantined_count,
 duration_seconds, started_at, last_synced, report_slot, run_attempt, retracted)
VALUES (?, ?, 'drift-run', ?, 'drift', ?, ?, ?, 0, 0, 0, ?, ?, ?, ?, 1, ?)`, org, repoID, suite, total, passed, failed, duration, started, synced, slot, retracted); err != nil {
			t.Fatal(err)
		}
	}
	three := 3.0
	insert("kept-1", "kept", 1, 1, 0, &three, started.Add(time.Hour), 0)
	insert("drift-old", "total-b", 1, 0, 1, &three, started.Add(time.Hour), 0)
	insert("drift-new", "total-b", 0, 0, 0, nil, started.Add(2*time.Hour), 1)
	var total, passed, failed, timed uint64
	if err := stack.conn.QueryRow(ctx, "SELECT sum(total_count), sum(passed_count), sum(failed_count), count(duration_seconds) FROM ("+
		retiredBaseReaderSQL(t, "loadNativeTestopsSuites")+")", testopsSourceArgs(org, []uuid.UUID{repoID}, retiredDay(), retiredDay().Add(24*time.Hour))...).
		Scan(&total, &passed, &failed, &timed); err != nil {
		t.Fatal(err)
	}
	if total != 2 || passed != 1 || failed != 1 || timed != 2 {
		t.Errorf("base reader serves total %d, passed %d, failed %d, timed %d; the pinned residual is 2, 1, 1, 2 (CONTROL 1, 1, 0, 1)", total, passed, failed, timed)
	}
	if got := retiredMetricOf(t, stack, org, retiredDay()); got.total != 1 || got.passed != 1 || got.failed != 0 || got.suites != 1 {
		t.Errorf("new readers serve %+v, want CONTROL total 1, passed 1, failed 0, 1 suite", got)
	}
}

// The "suite_rows_only" fallback serves CONTROL through the new readers: a run whose dropped aggregate is large (two parts of
// 25,001 cases, their exact aggregate) gets the suite retraction rows and no case retraction row, and the stale run serves
// what the run synced once serves.
func TestTestopsRetiredSlotSuiteRowsOnlyServesTheSyncedOnceNumbers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env := startRetiredStack(t, ctx)
	stack := env.stack
	suite := func(name string) matrixTotalsSuite {
		out := matrixTotalsSuite{name: name, time: "3.0"}
		for index := 0; index < 25_001; index++ {
			out.cases = append(out.cases, matrixTotalsCase{name + "-" + strconv.Itoa(index), "", "0.01"})
		}
		return out
	}
	left, right := suite("left"), suite("right")
	const made, timestamp = "2026-07-22T10:20:00Z", "2026-07-22T10:02:00"
	total := attemptsArtifact{6, "total", made, false, retiredArchive(t, timestamp, left, right)}
	all := []attemptsArtifact{
		{1, "part-left", made, false, retiredArchive(t, timestamp, left)},
		{2, "part-right", made, false, retiredArchive(t, timestamp, right)},
		total,
	}
	controlOrg, staleOrg := "org-chaos14259-suite-only-control-"+uuid.NewString()[:8], "org-chaos14259-suite-only-stale-"+uuid.NewString()[:8]
	stack.pass(t, controlOrg, retiredRunID, retiredDay().Add(11*time.Hour), retiredState(true, all))
	stack.pass(t, staleOrg, retiredRunID, retiredDay().Add(10*time.Hour), retiredState(false, []attemptsArtifact{total}))
	stack.pass(t, staleOrg, retiredRunID, retiredDay().Add(11*time.Hour), retiredState(true, all))
	for _, org := range []string{controlOrg, staleOrg} {
		var suites, cases uint64
		if err := stack.conn.QueryRow(ctx, `SELECT (SELECT countIf(retracted = 1) FROM test_suite_results FINAL WHERE org_id = ?),
       (SELECT countIf(retracted = 1) FROM test_case_results FINAL WHERE org_id = ?)`, org, org).Scan(&suites, &cases); err != nil {
			t.Fatal(err)
		}
		if suites != 2 || cases != 0 {
			t.Fatalf("%s: %d suite and %d case retraction rows, want the suite_rows_only fallback: 2 and 0", org, suites, cases)
		}
	}
	control, stale := retiredMetricOf(t, stack, controlOrg, retiredDay()), retiredMetricOf(t, stack, staleOrg, retiredDay())
	t.Logf("CONTROL %+v, stale %+v", control, stale)
	if control.total != 50_002 || control.suites != 2 || stale != control {
		t.Errorf("stale run serves %+v, want CONTROL %+v (50,002 cases, 2 suites)", stale, control)
	}
}

// PINNED LIMIT: a revival does not restore what the retired-slot rule hid. Attempt 1 stores total-b alone with case a failing;
// attempt 2 passes a in leg B's parts and drops total-b, whose case retraction rows replace total-b's attempt-1 cases at their
// keys (no name); attempt 3 uploads a total-b of its own execution (other durations, so no lossless aggregate of the
// attempt-2 parts) and the planner keeps it. The slot is live again, and its attempt-1 failure stays hidden: flake 0 and no
// failed name. One sync at attempt 3 serves the same, since it parses only the newest artifact of each name and never reads
// total-b@1 (githubTestsKeepNewestArtifactPerName); K4 pins the same 0 for that failure before the revival.
func TestTestopsRetiredSlotRevivalDoesNotRestoreWhatTheRuleHidIsPinned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	reader := manualMeterProvider(t)
	warns := retiredCaptureRevivalWarns(t)
	env := startRetiredStack(t, ctx)
	stack := env.stack
	org := "org-chaos14259-revival-" + uuid.NewString()[:8]
	const runID = "43600014265"
	retiredTwoAttemptRun(t, stack, org, runID, retiredDay(), false)
	retiredMetricOf(t, stack, org, retiredDay())
	if revived, lines := retiredRevivedCount(t, reader), warns(); revived != 0 || lines != 0 {
		t.Fatalf("before the revival: revived counter %d, %d Warn lines; want 0 and 0", revived, lines)
	}
	stamp := func(clock string) string { return "2026-07-22T" + clock + "Z" }
	attempt3Total := []matrixTotalsSuite{{"left", "4.0", []matrixTotalsCase{{"a", "", "0.3"}}}, {"right", "4.0", []matrixTotalsCase{{"b", "", "0.3"}}}}
	attempt2Leg := []matrixTotalsSuite{{"left", "3.0", []matrixTotalsCase{{"a", "", "0.25"}}}, {"right", "3.0", []matrixTotalsCase{{"b", "", "0.25"}}}}
	ts2, ts3 := "2026-07-22T11:12:00", "2026-07-22T12:12:00"
	stack.pass(t, org, runID, retiredDay().Add(12*time.Hour), attemptsRunState{created: stamp("10:01:00"), attempt: 3, started: stamp("12:10:00"),
		updated: stamp("12:40:00"), artifacts: []attemptsArtifact{
			{13, "leg-b-left", stamp("11:30:00"), false, retiredArchive(t, ts2, attempt2Leg[0])},
			{14, "leg-b-right", stamp("11:30:00"), false, retiredArchive(t, ts2, attempt2Leg[1])},
			{16, "total-b", stamp("11:30:00"), false, retiredArchive(t, ts2, attempt2Leg...)},
			{26, "total-b", stamp("12:30:00"), false, retiredArchive(t, ts3, attempt3Total...)},
		}})
	var live, retracted uint64
	if err := stack.conn.QueryRow(ctx, `SELECT countIf(retracted = 0 AND run_attempt = 3), countIf(retracted = 1) FROM test_suite_results FINAL
WHERE org_id = ? AND run_id = ? AND report_slot = 'total-b'`, org, runID).Scan(&live, &retracted); err != nil {
		t.Fatal(err)
	}
	if live != 2 || retracted != 2 {
		t.Fatalf("slot total-b: %d live attempt-3 rows and %d retraction rows, want 2 and 2 (revived at attempt 3)", live, retracted)
	}
	var attempt1Cases, named uint64
	if err := stack.conn.QueryRow(ctx, `SELECT count(), countIf(case_name != '') FROM test_case_results FINAL
WHERE org_id = ? AND run_id = ? AND suite_id IN (
  SELECT suite_id FROM test_suite_results FINAL WHERE org_id = ? AND run_id = ? AND report_slot = 'total-b' AND run_attempt = 1)`,
		org, runID, org, runID).Scan(&attempt1Cases, &named); err != nil {
		t.Fatal(err)
	}
	if attempt1Cases != 2 || named != 0 {
		t.Errorf("total-b attempt-1 case rows: %d, %d with a name; want 2, all with no name", attempt1Cases, named)
	}
	got := retiredMetricOf(t, stack, org, retiredDay())
	t.Logf("revived run serves %+v", got)
	if got.flake != 0 || got.failedNames != 0 || got.distinct != 2 {
		t.Errorf("revived run serves flake %d, failed names %d of %d names; the pinned limit is flake 0, failed names 0 of 2", got.flake, got.failedNames, got.distinct)
	}
	// The revival is loud (CHAOS-14555): the test-day read counts the revived slot and warns, with counts only.
	if revived, lines := retiredRevivedCount(t, reader), warns(); revived != 1 || lines != 1 {
		t.Errorf("after the revival: revived counter %d, %d Warn lines with slots 1; want 1 and 1", revived, lines)
	}
	// No false revival: a re-run of one leg (K3's shape) keeps total-b retired at every attempt.
	rerunOrg := "org-chaos14555-rerun-" + uuid.NewString()[:8]
	both, _, _ := retiredArtifacts(t)
	stack.pass(t, rerunOrg, retiredRunID, retiredDay().Add(10*time.Hour), retiredState(false, both))
	rerun := slices.Clone(both)
	for _, index := range []int{0, 1} {
		again := both[index]
		again.id, again.created = again.id+10, "2026-07-22T11:30:00Z"
		rerun = append(rerun, again)
	}
	stack.pass(t, rerunOrg, retiredRunID, retiredDay().Add(11*time.Hour), attemptsRunState{created: "2026-07-22T10:01:00Z", attempt: 2,
		started: "2026-07-22T11:10:00Z", updated: "2026-07-22T11:40:00Z", artifacts: rerun})
	retiredMetricOf(t, stack, rerunOrg, retiredDay())
	if revived, lines := retiredRevivedCount(t, reader), warns(); revived != 1 || lines != 1 {
		t.Errorf("after a re-run of one leg: revived counter %d, %d Warn lines; want them unchanged at 1 and 1", revived, lines)
	}
}

// retiredRevivedCount is the sum of the revived-slot counter's points (CHAOS-14555).
func retiredRevivedCount(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "dev_health_testops_report_slots_revived_total" {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				total += point.Value
			}
		}
	}
	return total
}

// retiredCaptureRevivalWarns captures the default logger and returns a counter of the revival Warn lines that carry counts
// only (slots 1, no id field).
func retiredCaptureRevivalWarns(t *testing.T) func() int {
	t.Helper()
	var captured bytes.Buffer
	var mu sync.Mutex
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&lockedWriter{mu: &mu, w: &captured}, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return func() int {
		mu.Lock()
		defer mu.Unlock()
		lines := 0
		for _, line := range strings.Split(captured.String(), "\n") {
			if !strings.Contains(line, "report slots revived after their retirement") {
				continue
			}
			var record map[string]any
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				t.Fatal(err)
			}
			_, org := record["org_id"]
			_, repo := record["repo_id"]
			if record["level"] == "WARN" && record["slots"] == float64(1) && !org && !repo {
				lines++
			}
		}
		return lines
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (writer *lockedWriter) Write(p []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.w.Write(p)
}

// retiredOldReaderSuiteSums is the base suite read (from the base source) over the stored rows: the sums of the counts and
// the suites with a duration.
func retiredOldReaderSuiteSums(t *testing.T, stack attemptsStack, org string, day time.Time) (total, passed, failed, timed uint64) {
	t.Helper()
	if err := stack.conn.QueryRow(stack.ctx, "SELECT sum(total_count), sum(passed_count), sum(failed_count), count(duration_seconds) FROM ("+
		retiredBaseReaderSQL(t, "loadNativeTestopsSuites")+")", testopsSourceArgs(org, []uuid.UUID{stack.repoID(t, org)}, day, day.Add(24*time.Hour))...).
		Scan(&total, &passed, &failed, &timed); err != nil {
		t.Fatal(err)
	}
	return total, passed, failed, timed
}

// retiredInsertPreSlotCopy stores a pre-slot copy (report_slot = ”) of a suite under suiteID, with one case failedCase that
// failed when it is not empty.
func retiredInsertPreSlotCopy(t *testing.T, stack attemptsStack, org, runID, suiteID string, started time.Time, failedCase string) {
	t.Helper()
	repoID := stack.repoID(t, org)
	synced := started.Add(-time.Hour)
	if err := stack.conn.Exec(stack.ctx, `INSERT INTO test_suite_results
(org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, error_count, quarantined_count,
 duration_seconds, started_at, last_synced)
VALUES (?, ?, ?, ?, 'left', 1, 0, 1, 0, 0, 0, 3.0, ?, ?)`, org, repoID, runID, suiteID, started, synced); err != nil {
		t.Fatal(err)
	}
	if failedCase == "" {
		return
	}
	if err := stack.conn.Exec(stack.ctx, `INSERT INTO test_case_results
(org_id, repo_id, run_id, suite_id, case_id, case_name, status, retry_attempt, last_synced)
VALUES (?, ?, ?, ?, 'pre-slot-case', ?, 'failed', 0, ?)`, org, repoID, runID, suiteID, failedCase, synced); err != nil {
		t.Fatal(err)
	}
}

// retiredLargeTwoAttemptRun is K4's shape at the size that takes the "suite_rows_only" fallback: total-b alone at attempt 1
// (two suites of 25,001 cases, case left-0 failing), then at attempt 2 leg B's two parts and their exact aggregate, all passing.
// Admitted 50,002 cases plus 2 x 50,002 case retraction rows (labels 1 and 2) pass the 100,000-row case batch.
func retiredLargeTwoAttemptRun(t *testing.T, stack attemptsStack, org, runID string, day time.Time) {
	t.Helper()
	date := day.Format(time.DateOnly)
	stamp := func(clock string) string { return date + "T" + clock + "Z" }
	suite := func(name, failing string) matrixTotalsSuite {
		out := matrixTotalsSuite{name: name, time: "3.0"}
		for index := 0; index < 25_001; index++ {
			caseName, status := name+"-"+strconv.Itoa(index), ""
			if caseName == failing {
				status = "failed"
			}
			out.cases = append(out.cases, matrixTotalsCase{caseName, status, "0.01"})
		}
		return out
	}
	ts1, ts2 := date+"T10:02:00", date+"T11:12:00"
	attempt1 := []attemptsArtifact{{6, "total-b", stamp("10:20:00"), false, retiredArchive(t, ts1, suite("left", "left-0"), suite("right", ""))}}
	left, right := suite("left", ""), suite("right", "")
	attempt2 := []attemptsArtifact{
		{13, "leg-b-left", stamp("11:30:00"), false, retiredArchive(t, ts2, left)},
		{14, "leg-b-right", stamp("11:30:00"), false, retiredArchive(t, ts2, right)},
		{16, "total-b", stamp("11:30:00"), false, retiredArchive(t, ts2, left, right)},
	}
	stack.pass(t, org, runID, day.Add(10*time.Hour), attemptsRunState{attempt: 1, started: stamp("10:01:00"), updated: stamp("10:30:00"), artifacts: attempt1})
	stack.pass(t, org, runID, day.Add(11*time.Hour), attemptsRunState{created: stamp("10:01:00"), attempt: 2, started: stamp("11:10:00"),
		updated: stamp("11:40:00"), artifacts: append(slices.Clone(attempt1), attempt2...)})
	var suites, cases uint64
	if err := stack.conn.QueryRow(stack.ctx, `SELECT (SELECT countIf(retracted = 1) FROM test_suite_results FINAL WHERE org_id = ? AND run_id = ?),
       (SELECT countIf(retracted = 1) FROM test_case_results FINAL WHERE org_id = ? AND run_id = ?)`, org, runID, org, runID).Scan(&suites, &cases); err != nil {
		t.Fatal(err)
	}
	if suites != 2 || cases != 0 {
		t.Fatalf("run %s: %d suite and %d case retraction rows, want the suite_rows_only fallback: 2 and 0", runID, suites, cases)
	}
}

// PINNED LIMITS (rollback-only or pre-slot-only residuals, measured, not CONTROL). Each subtest asserts the numbers the named
// reader serves today; CONTROL is in the subtest's comment.
func TestTestopsRetiredSlotNamedResidualsArePinned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	env := startRetiredStack(t, ctx)
	stack := env.stack
	newOrg := func(name string) string { return "org-chaos14259-" + name + "-" + uuid.NewString()[:8] }

	// one_row_per_slot, old reader: the stale run (STALE-A's recorded pre-14045 emission) retired by one minimal row (the row
	// githubTestsMinimalRetractionRows writes: no measure, retracted, a retired-slot key). The old reader still counts the slot's
	// stale suites: total 8, passed 7, failed 1, timed 8. CONTROL (the new readers) is 6, 5, 1, 6 suites.
	t.Run("one_row_per_slot old reader", func(t *testing.T) {
		writeRetiredRecordedEmission(t, stack)
		repoID := stack.repoID(t, retiredStaleAOrg)
		started := retiredDay().Add(10*time.Hour + time.Minute)
		if err := stack.conn.Exec(ctx, `INSERT INTO test_suite_results
(org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, error_count, quarantined_count,
 started_at, last_synced, report_slot, run_attempt, retracted)
VALUES (?, ?, ?, '~retired-pin', '', 0, 0, 0, 0, 0, 0, ?, ?, 'total-b', 1, 1)`, retiredStaleAOrg, repoID, retiredRunID, started, retiredDay().Add(12*time.Hour)); err != nil {
			t.Fatal(err)
		}
		total, passed, failed, timed := retiredOldReaderSuiteSums(t, stack, retiredStaleAOrg, retiredDay())
		if total != 8 || passed != 7 || failed != 1 || timed != 8 {
			t.Errorf("old reader serves total %d, passed %d, failed %d, timed %d; pinned 8, 7, 1, 8 (CONTROL 6, 5, 1, 6)", total, passed, failed, timed)
		}
		if got := retiredMetricOf(t, stack, retiredStaleAOrg, retiredDay()); got.total != 6 || got.passed != 5 || got.suites != 6 {
			t.Errorf("new readers serve %+v, want CONTROL 6 cases, 5 passed, 6 suites", got)
		}
	})

	// suite_rows_only, old reader: K4 and K5 at the size that takes the fallback (no case retraction row). The old reader sees
	// the slot's stale attempt-1 failure of left-0: flake 1 of 50,002 names (CONTROL, the new readers: 0), and a recurrence of
	// today's failure of left-0, 1 of 1 (CONTROL: 0 of 1).
	t.Run("suite_rows_only old reader", func(t *testing.T) {
		flakeOrg := newOrg("suite-only-flake")
		retiredLargeTwoAttemptRun(t, stack, flakeOrg, "43600014271", retiredDay())
		flake, distinct, _, _ := retiredOldReaderCaseMeasures(t, stack, flakeOrg, retiredDay())
		if flake != 1 || distinct != 50_002 {
			t.Errorf("old reader serves flake %d of %d names; pinned 1 of 50,002 (CONTROL 0)", flake, distinct)
		}
		if got := retiredMetricOf(t, stack, flakeOrg, retiredDay()); got.flake != 0 || got.distinct != 50_002 {
			t.Errorf("new readers serve flake %d of %d names, want CONTROL 0 of 50,002", got.flake, got.distinct)
		}
		historyOrg := newOrg("suite-only-history")
		retiredLargeTwoAttemptRun(t, stack, historyOrg, "43600014272", retiredDay().AddDate(0, 0, -1))
		stack.pass(t, historyOrg, "43600014273", retiredDay().Add(10*time.Hour), retiredState(false, []attemptsArtifact{
			{21, "today", "2026-07-22T10:20:00Z", false, retiredArchive(t, "2026-07-22T10:02:00", matrixTotalsSuite{"t", "1.0", []matrixTotalsCase{{"left-0", "failed", "0.5"}}})},
		}))
		_, _, failedToday, recurrent := retiredOldReaderCaseMeasures(t, stack, historyOrg, retiredDay())
		if failedToday != 1 || recurrent != 1 {
			t.Errorf("old reader serves recurrent %d of %d failed names; pinned 1 of 1 (CONTROL 0 of 1)", recurrent, failedToday)
		}
		if got := retiredMetricOf(t, stack, historyOrg, retiredDay()); got.failedNames != 1 || got.recurrent != 0 {
			t.Errorf("new readers serve recurrent %d of %d, want CONTROL 0 of 1", got.recurrent, got.failedNames)
		}
	})

	// An earlier attempt whose suite names differ from the dropped report's: the case retraction keys of label 1 are derived from
	// attempt 2's names, so the stale attempt-1 failure keeps its row. The old reader serves flake 2 of 3 names (CONTROL 1 of 3).
	t.Run("earlier attempt with other names old reader", func(t *testing.T) {
		org := newOrg("renamed")
		runID := "43600014274"
		stamp := func(clock string) string { return "2026-07-22T" + clock + "Z" }
		ts1, ts2 := "2026-07-22T10:02:00", "2026-07-22T11:12:00"
		attempt1 := []attemptsArtifact{
			{6, "total-b", stamp("10:20:00"), false, retiredArchive(t, ts1, matrixTotalsSuite{"left-v1", "3.0", []matrixTotalsCase{{"a", "failed", "0.25"}}},
				matrixTotalsSuite{"right-v1", "3.0", []matrixTotalsCase{{"b", "", "0.25"}}})},
			{7, "unit", stamp("10:20:00"), false, retiredArchive(t, ts1, matrixTotalsSuite{"u", "1.0", []matrixTotalsCase{{"c", "failed", "0.5"}}})},
		}
		leg := []matrixTotalsSuite{{"left", "3.0", []matrixTotalsCase{{"a", "", "0.25"}}}, {"right", "3.0", []matrixTotalsCase{{"b", "", "0.25"}}}}
		attempt2 := []attemptsArtifact{
			{13, "leg-b-left", stamp("11:30:00"), false, retiredArchive(t, ts2, leg[0])},
			{14, "leg-b-right", stamp("11:30:00"), false, retiredArchive(t, ts2, leg[1])},
			{16, "total-b", stamp("11:30:00"), false, retiredArchive(t, ts2, leg...)},
			{17, "unit", stamp("11:30:00"), false, retiredArchive(t, ts2, matrixTotalsSuite{"u", "1.0", []matrixTotalsCase{{"c", "", "0.5"}}})},
		}
		stack.pass(t, org, runID, retiredDay().Add(10*time.Hour), attemptsRunState{attempt: 1, started: stamp("10:01:00"), updated: stamp("10:30:00"), artifacts: attempt1})
		stack.pass(t, org, runID, retiredDay().Add(11*time.Hour), attemptsRunState{created: stamp("10:01:00"), attempt: 2, started: stamp("11:10:00"),
			updated: stamp("11:40:00"), artifacts: append(slices.Clone(attempt1), attempt2...)})
		flake, distinct, _, _ := retiredOldReaderCaseMeasures(t, stack, org, retiredDay())
		if flake != 2 || distinct != 3 {
			t.Errorf("old reader serves flake %d of %d names; pinned 2 of 3 (CONTROL 1 of 3)", flake, distinct)
		}
		if got := retiredMetricOf(t, stack, org, retiredDay()); got.flake != 1 || got.distinct != 3 {
			t.Errorf("new readers serve flake %d of %d names, want CONTROL 1 of 3", got.flake, got.distinct)
		}
	})

	// 1b: a superseded artifact leaves the listing before a re-sync of the same attempt drops the slot. The full retraction
	// rows replace the kept rows at their keys and list only the current artifact's pre-slot id, so the pre-slot copy under the
	// superseded artifact's id is no longer hidden: the new readers serve 3 cases (CONTROL, leg B's parts: 2).
	t.Run("superseded id lost on a same-attempt re-sync", func(t *testing.T) {
		org := newOrg("lost-id")
		runID := "43600014275"
		stamp := func(clock string) string { return "2026-07-22T" + clock + "Z" }
		ts1, ts2 := "2026-07-22T10:02:00", "2026-07-22T11:12:00"
		leg := []matrixTotalsSuite{{"left", "3.0", []matrixTotalsCase{{"a", "", "0.25"}}}, {"right", "3.0", []matrixTotalsCase{{"b", "", "0.25"}}}}
		old := attemptsArtifact{6, "total-b", stamp("10:20:00"), false, retiredArchive(t, ts1, leg...)}
		current := attemptsArtifact{16, "total-b", stamp("11:30:00"), false, retiredArchive(t, ts2, leg...)}
		state := func(updated string, artifacts ...attemptsArtifact) attemptsRunState {
			return attemptsRunState{created: stamp("10:01:00"), attempt: 2, started: stamp("11:10:00"), updated: stamp(updated), artifacts: artifacts}
		}
		stack.pass(t, org, runID, retiredDay().Add(11*time.Hour), state("11:40:00", old, current))
		var superseded string
		if err := stack.conn.QueryRow(ctx, `SELECT legacy_suite_ids[2] FROM test_suite_results FINAL
WHERE org_id = ? AND run_id = ? AND report_slot = 'total-b' AND suite_name = 'left'`, org, runID).Scan(&superseded); err != nil || superseded == "" {
			t.Fatalf("total-b@2 lists no superseded pre-slot id (%v)", err)
		}
		retiredInsertPreSlotCopy(t, stack, org, runID, superseded, retiredDay().Add(11*time.Hour+11*time.Minute), "")
		stack.pass(t, org, runID, retiredDay().Add(12*time.Hour), state("12:40:00", current,
			attemptsArtifact{13, "leg-b-left", stamp("11:30:00"), false, retiredArchive(t, ts2, leg[0])},
			attemptsArtifact{14, "leg-b-right", stamp("11:30:00"), false, retiredArchive(t, ts2, leg[1])}))
		if got := retiredMetricOf(t, stack, org, retiredDay()); got.total != 3 || got.suites != 3 {
			t.Errorf("new readers serve %d cases, %d suites; pinned 3 and 3 (CONTROL 2 and 2)", got.total, got.suites)
		}
	})

}

// retiredPreSlotCaseKey is the case key the pre-slot writer gives case name in the suite suiteID: sha256 of "suiteID::name" in hex,
// computed here, apart from providersync, as the oracle of the key the retraction must hit.
func retiredPreSlotCaseKey(suiteID, name string) string {
	sum := sha256.Sum256([]byte(suiteID + "::" + name))
	return hex.EncodeToString(sum[:])
}

// retiredInsertPreSlotCaseCopy stores a pre-slot copy (report_slot = ”) of total-b's attempt-1 left suite under its pre-slot
// suite key, with its failed case a at the pre-slot case key.
func retiredInsertPreSlotCaseCopy(t *testing.T, stack attemptsStack, org, runID string, started time.Time) {
	t.Helper()
	suiteID := retiredAttemptLegacyID(t, stack, org, runID)
	retiredInsertPreSlotCopy(t, stack, org, runID, suiteID, started, "")
	if err := stack.conn.Exec(stack.ctx, `INSERT INTO test_case_results
(org_id, repo_id, run_id, suite_id, case_id, case_name, status, retry_attempt, last_synced)
VALUES (?, ?, ?, ?, ?, 'a', 'failed', 0, ?)`, org, stack.repoID(t, org), runID, suiteID, retiredPreSlotCaseKey(suiteID, "a"), started.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
}

// CHAOS-14555: the stale case also stored under its pre-slot key (the artifact-id scope of a build before slots) is neutralised by
// a case retraction row at that key, so a rollback to the base binary serves CONTROL: K4 flake 1 of 3, K5 recurrent 0 of 1. (The
// base reader takes every suite row in its case sets and drops only a case with no name.)
func TestTestopsRetiredSlotOldReaderHidesThePreSlotCaseKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	env := startRetiredStack(t, ctx)
	stack := env.stack
	newOrg := func(name string) string { return "org-chaos14555-" + name + "-" + uuid.NewString()[:8] }

	flakeOrg := newOrg("pre-slot-flake")
	retiredTwoAttemptRun(t, stack, flakeOrg, "43600014261", retiredDay(), true)
	retiredInsertPreSlotCaseCopy(t, stack, flakeOrg, "43600014261", retiredDay().Add(10*time.Hour+time.Minute))
	if flake, distinct, _, _ := retiredOldReaderCaseMeasures(t, stack, flakeOrg, retiredDay()); flake != 1 || distinct != 3 {
		t.Errorf("K4 old reader serves flake %d of %d names, want CONTROL 1 of 3", flake, distinct)
	}

	historyOrg := newOrg("pre-slot-history")
	historyDay := retiredDay().AddDate(0, 0, -1)
	retiredTwoAttemptRun(t, stack, historyOrg, "43600014262", historyDay, false)
	retiredInsertPreSlotCaseCopy(t, stack, historyOrg, "43600014262", historyDay.Add(10*time.Hour+time.Minute))
	stack.pass(t, historyOrg, "43600014263", retiredDay().Add(10*time.Hour), retiredState(false, []attemptsArtifact{
		{21, "today", "2026-07-22T10:20:00Z", false, retiredArchive(t, "2026-07-22T10:02:00", matrixTotalsSuite{"t", "1.0", []matrixTotalsCase{{"a", "failed", "0.5"}}})},
	}))
	if _, _, failedToday, recurrent := retiredOldReaderCaseMeasures(t, stack, historyOrg, retiredDay()); failedToday != 1 || recurrent != 0 {
		t.Errorf("K5 old reader serves recurrent %d of %d failed names, want CONTROL 0 of 1", recurrent, failedToday)
	}
}

// CHAOS-14555: above 33,333 admitted cases at attempt 1 the pre-slot case retraction rows do not fit the case batch beside the
// slot-label ones (the counter reason legacy_case_rows_skipped); the run keeps today's rows, and the new readers serve CONTROL.
// Two parts of 16,667 cases and their exact aggregate (33,334 cases); the stale run stored the aggregate first.
func TestTestopsRetiredSlotLegacyCaseRowsSkippedServesTheSyncedOnceNumbers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env := startRetiredStack(t, ctx)
	stack := env.stack
	suite := func(name string) matrixTotalsSuite {
		out := matrixTotalsSuite{name: name, time: "3.0"}
		for index := 0; index < 16_667; index++ {
			out.cases = append(out.cases, matrixTotalsCase{name + "-" + strconv.Itoa(index), "", "0.01"})
		}
		return out
	}
	left, right := suite("left"), suite("right")
	const made, timestamp = "2026-07-22T10:20:00Z", "2026-07-22T10:02:00"
	total := attemptsArtifact{6, "total", made, false, retiredArchive(t, timestamp, left, right)}
	all := []attemptsArtifact{
		{1, "part-left", made, false, retiredArchive(t, timestamp, left)},
		{2, "part-right", made, false, retiredArchive(t, timestamp, right)},
		total,
	}
	controlOrg, staleOrg := "org-chaos14555-legacy-skip-control-"+uuid.NewString()[:8], "org-chaos14555-legacy-skip-stale-"+uuid.NewString()[:8]
	stack.pass(t, controlOrg, retiredRunID, retiredDay().Add(11*time.Hour), retiredState(true, all))
	stack.pass(t, staleOrg, retiredRunID, retiredDay().Add(10*time.Hour), retiredState(false, []attemptsArtifact{total}))
	stack.pass(t, staleOrg, retiredRunID, retiredDay().Add(11*time.Hour), retiredState(true, all))
	for _, org := range []string{controlOrg, staleOrg} {
		var suites, slotCases, legacyCases uint64
		if err := stack.conn.QueryRow(ctx, `SELECT
  (SELECT countIf(retracted = 1) FROM test_suite_results FINAL WHERE org_id = ?),
  (SELECT countIf(retracted = 1) FROM test_case_results FINAL WHERE org_id = ? AND suite_id IN (
     SELECT suite_id FROM test_suite_results FINAL WHERE org_id = ? AND retracted = 1)),
  (SELECT countIf(retracted = 1) FROM test_case_results FINAL WHERE org_id = ? AND suite_id NOT IN (
     SELECT suite_id FROM test_suite_results FINAL WHERE org_id = ? AND retracted = 1))`, org, org, org, org, org).Scan(&suites, &slotCases, &legacyCases); err != nil {
			t.Fatal(err)
		}
		if suites != 2 || slotCases != 33_334 || legacyCases != 0 {
			t.Fatalf("%s: %d suite, %d slot-label case and %d pre-slot case retraction rows; want the legacy_case_rows_skipped arm: 2, 33,334, 0",
				org, suites, slotCases, legacyCases)
		}
	}
	control, stale := retiredMetricOf(t, stack, controlOrg, retiredDay()), retiredMetricOf(t, stack, staleOrg, retiredDay())
	t.Logf("CONTROL %+v, stale %+v", control, stale)
	if control.total != 33_334 || control.suites != 2 || stale != control {
		t.Errorf("stale run serves %+v, want CONTROL %+v (33,334 cases, 2 suites)", stale, control)
	}
}

// The revived-slot count adds one statement to the test-day read. Its cost, read from system.query_log on one fixture (one
// container, one org, merges stopped): the read's rows and peak memory without the statement (before) and with it (after). The
// statement reads the suite table twice at most (the slot select and its window run set), so after - before <= 2 x the table's
// rows x 1.5. A records bound, not wall time.
func TestTestopsRevivedSlotReadCost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	env := startRetiredStack(t, ctx)
	stack := env.stack
	for _, table := range []string{"test_suite_results", "test_case_results", "ci_pipeline_runs", "testops_run_report_completeness"} {
		if err := stack.conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatal(err)
		}
	}
	org := "org-chaos14555-read-cost"
	retiredTwoAttemptRun(t, stack, org, "43600014261", retiredDay(), true)
	var suites uint64
	if err := stack.conn.QueryRow(ctx, "SELECT count() FROM test_suite_results").Scan(&suites); err != nil {
		t.Fatal(err)
	}
	id := "chaos14555-read-cost-" + uuid.NewString()
	day := retiredDay()
	if _, err := computeTestopsTestMetricsWithoutTeams(clickhouse.Context(ctx, clickhouse.WithQueryID(id)), stack.conn, org, stack.repoID(t, org),
		day.AddDate(0, 0, -29), day, day.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := stack.conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	var beforeRows, afterRows, beforeMemory, afterMemory, statements, revived uint64
	if err := stack.conn.QueryRow(ctx, `SELECT
  sumIf(read_rows, NOT revived), sum(read_rows), toUInt64(maxIf(memory_usage, NOT revived)), toUInt64(max(memory_usage)), count(), countIf(revived)
FROM (SELECT read_rows, memory_usage, position(query, 'HAVING max(retracted) = 1') > 0 AS revived
      FROM system.query_log WHERE query_id = ? AND type = 'QueryFinish')`, id).
		Scan(&beforeRows, &afterRows, &beforeMemory, &afterMemory, &statements, &revived); err != nil {
		t.Fatal(err)
	}
	t.Logf("test-day read: %d statements; read rows before %d, after %d; peak memory before %d, after %d bytes; suite table %d rows",
		statements, beforeRows, afterRows, beforeMemory, afterMemory, suites)
	if revived != 1 || beforeRows == 0 {
		t.Fatalf("query_log holds %d revived-slot statements and %d rows before it, want 1 and some", revived, beforeRows)
	}
	if bound := 2 * suites * 3 / 2; afterRows-beforeRows > bound {
		t.Errorf("the revived-slot statement read %d rows, want at most %d (2 scans of %d rows + 50 %%)", afterRows-beforeRows, bound, suites)
	}
}
