//go:build integration

package daily

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
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

var retiredDay = time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)

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
	since := retiredDay.Add(10 * time.Hour)
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
		testopsSourceArgs(org, []uuid.UUID{repoID}, retiredDay, retiredDay.Add(24*time.Hour))...).
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
	stack.pass(t, controlOrg, retiredRunID, retiredDay.Add(11*time.Hour), retiredState(true, both))
	control := matrixTotalsRead(t, stack, env.client, controlOrg, retiredRunID, retiredDay)
	t.Logf("CONTROL: %+v", control)
	if control.totalCases != 6 || control.passed != 5 || control.totalSuites != 6 || !near(control.passRate, 5.0/6) {
		t.Fatalf("CONTROL serves total_cases %d, passed %d, total_suites %d, pass_rate %v; want 6, 5, 6, 5/6",
			control.totalCases, control.passed, control.totalSuites, control.passRate)
	}

	// STALE-A: sync 1 is the recorded pre-CHAOS-14045 emission (total-b admitted), sync 2 the route on BOTH an hour later.
	writeRetiredRecordedEmission(t, stack)
	stack.pass(t, retiredStaleAOrg, retiredRunID, retiredDay.Add(11*time.Hour), retiredState(true, both))
	staleA := matrixTotalsRead(t, stack, env.client, retiredStaleAOrg, retiredRunID, retiredDay)

	// STALE-B: the listing grows. Sync 1 lists total-b without leg B's parts (admitted), sync 2 lists BOTH.
	staleBOrg := "org-chaos14259-stale-b-" + uuid.NewString()[:8]
	stack.pass(t, staleBOrg, retiredRunID, retiredDay.Add(10*time.Hour), retiredState(false, withoutLegB))
	stack.pass(t, staleBOrg, retiredRunID, retiredDay.Add(11*time.Hour), retiredState(true, both))
	staleB := matrixTotalsRead(t, stack, env.client, staleBOrg, retiredRunID, retiredDay)

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
