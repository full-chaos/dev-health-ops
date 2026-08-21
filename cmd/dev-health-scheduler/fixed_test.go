package main

import (
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	schedulerfixed "github.com/full-chaos/dev-health-ops/internal/scheduler/fixed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testContractRoot is defaultContractRoot resolved from this package directory,
// which is where `go test` runs. defaultContractRoot is repo-relative because
// the binary is launched from the repo root.
const testContractRoot = "../../" + defaultContractRoot

// checkedInUnbuiltSchedules is the complete, reviewed set of fixed schedules
// this binary declares but cannot execute, with the blocker for each.
//
// It exists because ScheduleCoverage does not and cannot catch this. That test
// proves every legacy Beat entry maps to an owner with a matching cadence, zone
// and catch-up policy — an OWNERSHIP property. It never constructs a producer,
// so a schedule whose producer fails every invocation passes it unchanged. Two
// did, for long enough that their tickets were closed as done.
//
// Pinning the set here closes the loophole in both directions: adding a stub, or
// forgetting to remove one after building the real producer, both fail the
// build. It is deliberately data rather than a comment so the remaining gap is
// visible to anyone reading the test output, not only to someone reading
// buildFixedScheduleProducers. It is empty only when every declared schedule
// is executable.
var checkedInUnbuiltSchedules = map[string]string{}

// unconnectedPool is a constructed but never-dialled pool. The stores
// buildFixedScheduleProducers wires reject a nil pool at construction, and
// pgxpool opens no connection until first acquire with the default MinConns of
// zero, so this exercises the real construction path without a database. No test
// here issues a statement.
func unconnectedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig("postgres://unused:unused@127.0.0.1:1/unused")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestFixedScheduleProducersAreConstructedForEveryDeclaredSchedule(t *testing.T) {
	registry, err := jobruntime.Load(testContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	producers, err := buildFixedScheduleProducers(unconnectedPool(t), registry)
	if err != nil {
		t.Fatalf("buildFixedScheduleProducers(): %v", err)
	}
	schedules, err := schedulerfixed.Schedules()
	if err != nil {
		t.Fatal(err)
	}
	if missing := producers.Missing(schedules); len(missing) != 0 {
		t.Fatalf("schedules with no constructed producer: %v", missing)
	}
}

// CHAOS-3481: the rollout proof this contract's README requires was
// collected on 2026-08-21 against live fleet revision 4a39bcf0e (every
// go-worker-* / go-scheduler / go-reconciler container on prod, docker
// inspect) -- report-ops.json's v3 schema digest for system.retention_cleanup
// matches this tree's compiled digest exactly, and
// `worker-contractcheck rollout` against that live report passes with
// producer_version=3. migration-state.json now routes at v3.
//
// This test proves the composition root actually reached that route: the
// registered producer version is v3, and the schedule no longer refuses on
// version at all -- it now reaches past the version gate into Ask Dev
// admission (which needs a real transaction; a nil one here fails there
// instead, an entirely different, later failure mode than the version skip
// this test replaces).
func TestAskDevRetentionEmitsV3NowThatTheRouteIsActivated(t *testing.T) {
	registry, err := jobruntime.Load(testContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := registry.Descriptor(jobcontract.KindRetentionCleanup)
	if !ok {
		t.Fatal("retention contract is not registered")
	}
	if descriptor.CurrentVersion != jobcontract.ContractVersionV3 ||
		descriptor.ProducerVersion != jobcontract.ContractVersionV3 ||
		!slices.Contains(descriptor.SupportedVersions, jobcontract.ContractVersionV3) {
		t.Fatalf(
			"retention versions = current %d producer %d supported %v, want v3 supported and both current and producer routed to v3",
			descriptor.CurrentVersion,
			descriptor.ProducerVersion,
			descriptor.SupportedVersions,
		)
	}
	producers, err := buildFixedScheduleProducers(unconnectedPool(t), registry)
	if err != nil {
		t.Fatal(err)
	}
	schedules, err := schedulerfixed.Schedules()
	if err != nil {
		t.Fatal(err)
	}
	schedule := scheduleWithID(t, schedules, "prune_ask_dev_conversations")
	producer, ok := producers.Producer(schedule.ProducerID)
	if !ok {
		t.Fatal("Ask Dev retention producer is not constructed")
	}
	outcome, err := producer.Produce(
		t.Context(), nil, schedule,
		schedulerfixed.NewOccurrence(schedule, time.Date(2026, 7, 28, 5, 30, 0, 0, time.UTC), time.Date(2026, 7, 28, 5, 30, 0, 0, time.UTC)),
	)
	// A nil tx makes production's real Postgres admission reader fail (it
	// requires the engine's coordinator transaction); that is expected and is
	// exactly the proof this test needs -- the failure is no longer the
	// version skip, so Produce() got past the version gate to reach
	// admission at all.
	if err == nil {
		t.Fatalf("outcome = %+v, err = nil; want the nil-tx admission failure once the version gate is open", outcome)
	}
	if !errors.Is(err, schedulerfixed.ErrProducerUnavailable) {
		t.Fatalf("error = %v, want the admission read's nil-tx failure, not a version-gate skip", err)
	}
	if outcome.SkipReason == "consumer_version_incompatible" {
		t.Fatal("the schedule still refuses on version even though the route is now v3")
	}
}

// TestAskDevRetentionWiringHonorsWhateverProducerVersionTheRouteDeclares is
// the retargeted admission-boundary control from before the rollout: the
// composition root must still read producer_version from the registry
// rather than assuming it is always activated. It proves this with a
// synthetic lower pin -- a private copy of the contract tree with
// system.retention_cleanup pinned back to v2 -- and confirms the schedule
// goes back to refusing v3 without ever constructing an envelope, exactly as
// production did before 2026-08-21's rollout proof.
func TestAskDevRetentionWiringHonorsWhateverProducerVersionTheRouteDeclares(t *testing.T) {
	root := t.TempDir()
	copyContractTree(t, testContractRoot, root)
	pinRetentionProducerVersion(t, root, jobcontract.ContractVersionV2)

	registry, err := jobruntime.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := registry.Descriptor(jobcontract.KindRetentionCleanup)
	if !ok {
		t.Fatal("retention contract is not registered")
	}
	if descriptor.ProducerVersion != jobcontract.ContractVersionV2 {
		t.Fatalf("synthetic pin did not take: producer version = %d, want 2", descriptor.ProducerVersion)
	}
	producers, err := buildFixedScheduleProducers(unconnectedPool(t), registry)
	if err != nil {
		t.Fatal(err)
	}
	schedules, err := schedulerfixed.Schedules()
	if err != nil {
		t.Fatal(err)
	}
	schedule := scheduleWithID(t, schedules, "prune_ask_dev_conversations")
	producer, ok := producers.Producer(schedule.ProducerID)
	if !ok {
		t.Fatal("Ask Dev retention producer is not constructed")
	}
	outcome, err := producer.Produce(
		t.Context(), nil, schedule,
		schedulerfixed.NewOccurrence(schedule, time.Date(2026, 7, 28, 5, 30, 0, 0, time.UTC), time.Date(2026, 7, 28, 5, 30, 0, 0, time.UTC)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(outcome.Requests) != 0 || outcome.SkipReason != "consumer_version_incompatible" {
		t.Fatalf("outcome = %+v, want the same version-gate skip production had before the route was activated", outcome)
	}
}

// copyContractTree copies the contract v1 directory so a test can mutate its
// own private copy without touching the checked-in tree other tests read
// concurrently.
func copyContractTree(t *testing.T, source, destination string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// pinRetentionProducerVersion overwrites system.retention_cleanup's
// producer_version in a private copy of migration-state.json.
func pinRetentionProducerVersion(t *testing.T, root string, version int) {
	t.Helper()
	path := filepath.Join(root, "migration-state.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var state struct {
		SchemaVersion int              `json:"schema_version"`
		Jobs          []map[string]any `json:"jobs"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err := decoder.Decode(&state); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, job := range state.Jobs {
		if job["kind"] == jobcontract.KindRetentionCleanup {
			job["producer_version"] = version
			found = true
		}
	}
	if !found {
		t.Fatalf("migration-state.json has no %s entry to pin", jobcontract.KindRetentionCleanup)
	}
	encoded, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		t.Fatal(err)
	}
}

// The unbuilt set must equal the checked-in declaration exactly. An added stub
// is an undeclared regression; a removed one means this table is stale and is
// still advertising a gap that has been closed.
func TestUnbuiltFixedScheduleProducersMatchTheCheckedInDeclaration(t *testing.T) {
	registry, err := jobruntime.Load(testContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	producers, err := buildFixedScheduleProducers(unconnectedPool(t), registry)
	if err != nil {
		t.Fatal(err)
	}
	schedules, err := schedulerfixed.Schedules()
	if err != nil {
		t.Fatal(err)
	}
	unbuilt := producers.Unbuilt(schedules)
	got := slices.Sorted(maps.Keys(unbuilt))
	want := slices.Sorted(maps.Keys(checkedInUnbuiltSchedules))
	if !slices.Equal(got, want) {
		t.Fatalf("unbuilt schedules = %v, checked-in declaration = %v", got, want)
	}
	for id, reason := range checkedInUnbuiltSchedules {
		if unbuilt[id] != reason {
			t.Errorf("schedule %s unbuilt reason =\n  %q\nwant\n  %q", id, unbuilt[id], reason)
		}
	}
}

// Every unbuilt producer must still FAIL rather than quietly return no work.
// This is the property that keeps `fixed_scheduler_loop` readiness honest: a
// stub that returned an empty outcome would be indistinguishable from a healthy
// schedule with nothing to do.
func TestUnbuiltFixedScheduleProducersFailEveryInvocation(t *testing.T) {
	registry, err := jobruntime.Load(testContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	producers, err := buildFixedScheduleProducers(unconnectedPool(t), registry)
	if err != nil {
		t.Fatal(err)
	}
	schedules, err := schedulerfixed.Schedules()
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[string]schedulerfixed.Schedule, len(schedules))
	for _, schedule := range schedules {
		byID[schedule.ID] = schedule
	}
	for id := range checkedInUnbuiltSchedules {
		schedule, ok := byID[id]
		if !ok {
			t.Fatalf("checked-in unbuilt schedule %s is not declared", id)
		}
		producer, ok := producers.Producer(schedule.ProducerID)
		if !ok {
			t.Fatalf("schedule %s has no constructed producer", id)
		}
		outcome, err := producer.Produce(
			t.Context(), nil, schedule,
			schedulerfixed.Occurrence{ScheduleID: schedule.ID, TargetKind: schedule.TargetKind},
		)
		if err == nil {
			t.Fatalf("unbuilt producer for %s returned %+v instead of failing", id, outcome)
		}
	}
}

// While any declared schedule is unbuilt, the whole fixed-schedule runtime must
// refuse to construct, so the gap closes readiness at startup rather than up to
// 24 hours later when a daily stub first fires.
//
// The refusal must NAME each schedule and carry its reason: a bare "unavailable"
// would send an operator to read producer construction code to learn which lane
// owns the gap, which is the situation this whole change exists to end.
func TestUnbuiltFixedSchedulesRefuseTheRuntime(t *testing.T) {
	registry, err := jobruntime.Load(testContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	producers, err := buildFixedScheduleProducers(unconnectedPool(t), registry)
	if err != nil {
		t.Fatal(err)
	}
	schedules, err := schedulerfixed.Schedules()
	if err != nil {
		t.Fatal(err)
	}
	err = refuseUnbuiltFixedSchedules(producers, schedules)
	if len(checkedInUnbuiltSchedules) == 0 {
		if err != nil {
			t.Fatalf("the runtime was refused after every checked schedule was built: %v", err)
		}
	} else {
		if err == nil {
			t.Fatal("the runtime was accepted while schedules were unbuilt")
		}
		if !errors.Is(err, errFixedScheduleUnbuilt) {
			t.Fatalf("error = %v, want an unbuilt-schedule refusal", err)
		}
		for id, reason := range checkedInUnbuiltSchedules {
			if !strings.Contains(err.Error(), id) {
				t.Errorf("refusal does not name unbuilt schedule %s: %v", id, err)
			}
			if !strings.Contains(err.Error(), reason) {
				t.Errorf("refusal does not carry the reason for %s: %v", id, err)
			}
		}
	}
	// A fully built set must be accepted, or the gate would refuse the runtime
	// forever and could never be satisfied by finishing the work.
	built, err := schedulerfixed.NewProducerSet(schedulerfixed.NewHeartbeatProducer())
	if err != nil {
		t.Fatal(err)
	}
	heartbeat := scheduleWithID(t, schedules, "phone_home_heartbeat")
	if err := refuseUnbuiltFixedSchedules(built, []schedulerfixed.Schedule{heartbeat}); err != nil {
		t.Fatalf("a fully built schedule set was refused: %v", err)
	}
	stub, err := schedulerfixed.NewProducerSet(
		schedulerfixed.NewNotImplementedProducer(schedulerfixed.ProducerHeartbeat, "test stub"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := refuseUnbuiltFixedSchedules(stub, []schedulerfixed.Schedule{heartbeat}); !errors.Is(err, errFixedScheduleUnbuilt) {
		t.Fatalf("unbuilt producer error = %v, want unbuilt-schedule refusal", err)
	}
}

func scheduleWithID(
	t *testing.T,
	schedules []schedulerfixed.Schedule,
	id string,
) schedulerfixed.Schedule {
	t.Helper()
	for _, schedule := range schedules {
		if schedule.ID == id {
			return schedule
		}
	}
	t.Fatalf("schedule %s is not declared", id)
	return schedulerfixed.Schedule{}
}

// scheduled_reports_dispatch must NOT be in the unbuilt set: it is built, and a
// regression that reverted it to a stub would otherwise only surface as a
// runtime readiness failure.
func TestScheduledReportsProducerIsBuilt(t *testing.T) {
	registry, err := jobruntime.Load(testContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	producers, err := buildFixedScheduleProducers(unconnectedPool(t), registry)
	if err != nil {
		t.Fatal(err)
	}
	schedules, err := schedulerfixed.Schedules()
	if err != nil {
		t.Fatal(err)
	}
	if reason, unbuilt := producers.Unbuilt(schedules)["scheduled_reports_dispatch"]; unbuilt {
		t.Fatalf("scheduled_reports_dispatch reverted to an unbuilt producer: %s", reason)
	}
	if _, ok := producers.Producer(schedulerfixed.ProducerScheduledReports); !ok {
		t.Fatal("the scheduled reports producer is not constructed")
	}
}
