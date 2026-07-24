package fixed

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	testOrgA = "11111111-1111-4111-8111-111111111111"
	testOrgB = "22222222-2222-4222-8222-222222222222"
)

type recordingRemainingStore struct {
	requests []remaining.StartRunRequest
}

func (store *recordingRemainingStore) StartRunTx(
	_ context.Context,
	_ pgx.Tx,
	request remaining.StartRunRequest,
	_ remaining.PartitionPublisher,
) (remaining.Run, error) {
	store.requests = append(store.requests, request)
	return remaining.Run{ID: uuid.NewString(), OrganizationID: request.OrganizationID}, nil
}

type recordingGraphWriter struct {
	requests []workgraph.Request
}

func (writer *recordingGraphWriter) WriteTx(
	_ context.Context,
	_ pgx.Tx,
	request workgraph.Request,
) error {
	writer.requests = append(writer.requests, request)
	return nil
}

type fixedOrganizationLister struct {
	identifiers []string
	err         error
}

func (lister fixedOrganizationLister) ActiveOrganizationIDs(
	context.Context,
	pgx.Tx,
) ([]string, error) {
	return lister.identifiers, lister.err
}

type nopPartitionPublisher struct{}

func (nopPartitionPublisher) PublishPartitionTx(
	context.Context, pgx.Tx, remaining.Run, remaining.Partition, string,
) error {
	return nil
}

func fanoutProducer(
	t *testing.T,
	lister OrganizationLister,
) (*RemainingMetricsFanoutProducer, *recordingRemainingStore, *recordingGraphWriter) {
	t.Helper()
	store := &recordingRemainingStore{}
	graph := &recordingGraphWriter{}
	producer, err := NewRemainingMetricsFanoutProducer(
		store, nopPartitionPublisher{}, lister, graph,
	)
	if err != nil {
		t.Fatalf("NewRemainingMetricsFanoutProducer() = %v", err)
	}
	return producer.(*RemainingMetricsFanoutProducer), store, graph
}

func scheduleByID(t *testing.T, id string) Schedule {
	t.Helper()
	schedules, err := Schedules()
	if err != nil {
		t.Fatalf("Schedules() = %v", err)
	}
	for _, schedule := range schedules {
		if schedule.ID == id {
			return schedule
		}
	}
	t.Fatalf("schedule %s is not declared", id)
	return Schedule{}
}

// The remaining-metrics store requires an immutable generation seed for exactly
// the capacity family and rejects one for every other family. Without it every
// Monday occurrence rolled back before creating a forecast.
func TestCapacityFanoutSuppliesTheRequiredGenerationSeed(t *testing.T) {
	schedule := scheduleByID(t, "capacity_forecast_weekly_fanout")
	producer, store, _ := fanoutProducer(t, fixedOrganizationLister{
		identifiers: []string{testOrgA, testOrgB},
	})
	occurrence := NewOccurrence(
		schedule, mustTime(t, "2026-07-20T04:00:00Z"), mustTime(t, "2026-07-20T04:00:05Z"),
	)
	outcome, err := producer.Produce(context.Background(), &stubTx{}, schedule, occurrence)
	if err != nil {
		t.Fatalf("Produce() = %v", err)
	}
	if outcome.Handoffs != 2 || len(store.requests) != 2 {
		t.Fatalf("outcome=%+v requests=%d", outcome, len(store.requests))
	}
	for _, request := range store.requests {
		if request.Family != "capacity" {
			t.Fatalf("family = %s", request.Family)
		}
		if request.GenerationSeed == nil {
			t.Fatal("capacity run was started without the required generation seed")
		}
		if *request.GenerationSeed < 0 {
			t.Fatalf("generation seed %d is negative", *request.GenerationSeed)
		}
	}
	if *store.requests[0].GenerationSeed == *store.requests[1].GenerationSeed {
		t.Fatal("two organizations shared one generation seed")
	}

	// The store verifies the seed on every replay, so a repeated occurrence
	// must derive the same value or the retry would conflict with the run it
	// already created.
	replay, _, _ := fanoutProducer(t, fixedOrganizationLister{identifiers: []string{testOrgA}})
	replayed, err := replay.Produce(context.Background(), &stubTx{}, schedule, occurrence)
	if err != nil {
		t.Fatalf("replay Produce() = %v", err)
	}
	_ = replayed
	if got, want := deterministicGenerationSeed(occurrence, testOrgA), *store.requests[0].GenerationSeed; got != want {
		t.Fatalf("replayed seed = %d, want %d", got, want)
	}
}

// Non-capacity families must NOT carry a seed: the store rejects one.
func TestNonCapacityFanoutOmitsTheGenerationSeed(t *testing.T) {
	for _, id := range []string{
		"complexity_daily_fanout",
		"release_impact_daily_fanout",
		"recommendations_daily_fanout",
		"membership_backfill_daily_fanout",
	} {
		schedule := scheduleByID(t, id)
		producer, store, _ := fanoutProducer(t, fixedOrganizationLister{identifiers: []string{testOrgA}})
		occurrence := NewOccurrence(
			schedule, mustTime(t, "2026-07-24T03:30:00Z"), mustTime(t, "2026-07-24T03:30:05Z"),
		)
		if _, err := producer.Produce(context.Background(), &stubTx{}, schedule, occurrence); err != nil {
			t.Fatalf("%s Produce() = %v", id, err)
		}
		if len(store.requests) != 1 {
			t.Fatalf("%s started %d runs", id, len(store.requests))
		}
		if store.requests[0].GenerationSeed != nil {
			t.Fatalf("%s supplied a generation seed the store rejects", id)
		}
	}
}

// The legacy dispatcher chained run_work_graph_build before the membership
// projection. Projecting onto stale or absent edges produces zero membership
// and still reports success, so the safety net would look healthy while
// repairing nothing.
func TestMembershipFanoutChainsTheWorkGraphBuildFirst(t *testing.T) {
	schedule := scheduleByID(t, "membership_backfill_daily_fanout")
	producer, store, graph := fanoutProducer(t, fixedOrganizationLister{
		identifiers: []string{testOrgA, testOrgB},
	})
	occurrence := NewOccurrence(
		schedule, mustTime(t, "2026-07-24T03:30:00Z"), mustTime(t, "2026-07-24T03:30:05Z"),
	)
	outcome, err := producer.Produce(context.Background(), &stubTx{}, schedule, occurrence)
	if err != nil {
		t.Fatalf("Produce() = %v", err)
	}
	if len(graph.requests) != 2 {
		t.Fatalf("wrote %d work-graph builds for 2 organizations", len(graph.requests))
	}
	for index, request := range graph.requests {
		if request.Kind != workgraph.KindBuild {
			t.Fatalf("build %d kind = %s", index, request.Kind)
		}
		if _, err := uuid.Parse(request.ID); err != nil {
			t.Fatalf("build %d id %q is not a UUID", index, request.ID)
		}
	}
	if graph.requests[0].ID == graph.requests[1].ID {
		t.Fatal("two organizations shared one work-graph request identity")
	}
	if len(store.requests) != 2 {
		t.Fatalf("started %d membership runs", len(store.requests))
	}
	for index, request := range store.requests {
		if request.PrerequisiteCompletionKey == "" {
			t.Fatalf("membership run %d is eligible before its graph build completes", index)
		}
		if !strings.HasPrefix(request.PrerequisiteCompletionKey, "work_graph_execution_request:") {
			t.Fatalf("membership run %d waits on %q", index, request.PrerequisiteCompletionKey)
		}
	}
	// Two handoffs per organization: the build and the projection.
	if outcome.Handoffs != 4 {
		t.Fatalf("handoffs = %d, want one build and one projection per organization", outcome.Handoffs)
	}
}

// Families without the chain must not write a work-graph request.
func TestNonMembershipFanoutWritesNoWorkGraphBuild(t *testing.T) {
	schedule := scheduleByID(t, "complexity_daily_fanout")
	producer, _, graph := fanoutProducer(t, fixedOrganizationLister{identifiers: []string{testOrgA}})
	occurrence := NewOccurrence(
		schedule, mustTime(t, "2026-07-24T00:45:00Z"), mustTime(t, "2026-07-24T00:45:05Z"),
	)
	if _, err := producer.Produce(context.Background(), &stubTx{}, schedule, occurrence); err != nil {
		t.Fatalf("Produce() = %v", err)
	}
	if len(graph.requests) != 0 {
		t.Fatalf("complexity wrote %d work-graph builds", len(graph.requests))
	}
}

// An installation with no active organizations is a bounded, reportable
// condition, not silent success. The legacy Python fallback to a literal
// "default" organization cannot be reproduced because remaining_metric_runs
// types org_id as uuid, so the condition is surfaced instead of faked.
func TestEmptyOrganizationTableIsReportedRatherThanSilentlySkipped(t *testing.T) {
	schedule := scheduleByID(t, "recommendations_daily_fanout")
	producer, store, _ := fanoutProducer(t, fixedOrganizationLister{identifiers: nil})
	occurrence := NewOccurrence(
		schedule, mustTime(t, "2026-07-24T02:00:00Z"), mustTime(t, "2026-07-24T02:00:05Z"),
	)
	outcome, err := producer.Produce(context.Background(), &stubTx{}, schedule, occurrence)
	if err != nil {
		t.Fatalf("Produce() = %v", err)
	}
	if outcome.Handoffs != 0 || len(store.requests) != 0 {
		t.Fatalf("outcome=%+v requests=%d", outcome, len(store.requests))
	}
	if outcome.SkipReason != SkipNoActiveOrganizations {
		t.Fatalf("skip reason = %q, want a distinguishable condition", outcome.SkipReason)
	}
}

// A read failure must never be converted into an empty organization list: the
// legacy dispatchers used strict discovery precisely because a swallowed
// database error silently cancelled every nightly safety net.
func TestOrganizationReadFailurePropagates(t *testing.T) {
	schedule := scheduleByID(t, "recommendations_daily_fanout")
	producer, _, _ := fanoutProducer(t, fixedOrganizationLister{err: ErrProducerUnavailable})
	occurrence := NewOccurrence(
		schedule, mustTime(t, "2026-07-24T02:00:00Z"), mustTime(t, "2026-07-24T02:00:05Z"),
	)
	if _, err := producer.Produce(context.Background(), &stubTx{}, schedule, occurrence); err == nil {
		t.Fatal("a failed organization read was reported as no organizations")
	}
}

func TestRetentionProducerHonorsOperatorHorizonOverrides(t *testing.T) {
	t.Setenv("SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS", "7")
	t.Setenv("EXTERNAL_INGEST_STATUS_RETENTION_DAYS", "30")
	producer := NewRetentionProducer()

	for _, test := range []struct {
		scheduleID   string
		wantPolicy   string
		wantDeleteAt string
	}{
		{
			scheduleID:   "prune_rate_limit_observations",
			wantPolicy:   jobcontract.RetentionRateLimitObservation,
			wantDeleteAt: "2026-07-17T05:00:00Z",
		},
		{
			scheduleID:   "prune_external_ingest_batches",
			wantPolicy:   jobcontract.RetentionExternalIngestBatch,
			wantDeleteAt: "2026-06-24T05:15:00Z",
		},
	} {
		schedule := scheduleByID(t, test.scheduleID)
		dueTime := mustTime(t, "2026-07-24T05:00:00Z")
		if test.scheduleID == "prune_external_ingest_batches" {
			dueTime = mustTime(t, "2026-07-24T05:15:00Z")
		}
		occurrence := NewOccurrence(schedule, dueTime, dueTime)
		outcome, err := producer.Produce(context.Background(), &stubTx{}, schedule, occurrence)
		if err != nil {
			t.Fatalf("%s Produce() = %v", test.scheduleID, err)
		}
		if len(outcome.Requests) != 1 {
			t.Fatalf("%s produced %d requests", test.scheduleID, len(outcome.Requests))
		}
		payload, ok := outcome.Requests[0].Envelope.Payload.(jobcontract.RetentionCleanupPayload)
		if !ok {
			t.Fatalf("%s payload type %T", test.scheduleID, outcome.Requests[0].Envelope.Payload)
		}
		if payload.RetentionPolicy != test.wantPolicy {
			t.Fatalf("%s policy = %s", test.scheduleID, payload.RetentionPolicy)
		}
		if payload.DeleteBefore != test.wantDeleteAt {
			t.Fatalf("%s delete_before = %s, want %s",
				test.scheduleID, payload.DeleteBefore, test.wantDeleteAt)
		}
	}
}

// A malformed or non-positive override must keep the checked default rather
// than widening or zeroing a deletion range.
func TestRetentionProducerRejectsMalformedHorizonOverrides(t *testing.T) {
	for _, raw := range []string{"", "   ", "not-a-number", "0", "-5"} {
		t.Setenv("SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS", raw)
		producer := NewRetentionProducer()
		schedule := scheduleByID(t, "prune_rate_limit_observations")
		dueTime := mustTime(t, "2026-07-24T05:00:00Z")
		outcome, err := producer.Produce(
			context.Background(), &stubTx{}, schedule, NewOccurrence(schedule, dueTime, dueTime),
		)
		if err != nil {
			t.Fatalf("override %q: %v", raw, err)
		}
		payload := outcome.Requests[0].Envelope.Payload.(jobcontract.RetentionCleanupPayload)
		if payload.DeleteBefore != "2026-07-10T05:00:00Z" {
			t.Fatalf("override %q produced delete_before %s, want the 14 day default",
				raw, payload.DeleteBefore)
		}
	}
}

// Every produced envelope must satisfy the compiled contract. This is what
// catches a producer emitting a policy or payload shape the runtime rejects.
func TestEveryProducedEnvelopeSatisfiesTheCompiledContract(t *testing.T) {
	producers := map[string]Producer{
		"phone_home_heartbeat":          NewHeartbeatProducer(),
		"prune_rate_limit_observations": NewRetentionProducer(),
		"prune_external_ingest_batches": NewRetentionProducer(),
	}
	for id, producer := range producers {
		schedule := scheduleByID(t, id)
		dueTime := mustTime(t, "2026-07-24T05:00:00Z")
		outcome, err := producer.Produce(
			context.Background(), &stubTx{}, schedule, NewOccurrence(schedule, dueTime, dueTime),
		)
		if err != nil {
			t.Fatalf("%s Produce() = %v", id, err)
		}
		for _, request := range outcome.Requests {
			encoded, err := jobcontract.MarshalCanonical(request.Envelope)
			if err != nil {
				t.Fatalf("%s envelope is not contract-valid: %v", id, err)
			}
			if _, err := jobcontract.Decode(request.Kind, encoded); err != nil {
				t.Fatalf("%s envelope failed strict decode: %v", id, err)
			}
		}
	}
}

func TestFanoutScopesAreAcceptedByTheRemainingMetricsContract(t *testing.T) {
	// The scope shapes are validated inside the remaining-metrics store, so a
	// wrong field would only surface at runtime. Round-tripping them here keeps
	// the failure at build time.
	producer, _, _ := fanoutProducer(t, fixedOrganizationLister{identifiers: []string{testOrgA}})
	for id := range producer.byScheduleID {
		schedule := scheduleByID(t, id)
		binding := producer.byScheduleID[id]
		scope, err := binding.Scope("2026-07-24")
		if err != nil {
			t.Fatalf("%s scope: %v", schedule.ID, err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(scope, &decoded); err != nil {
			t.Fatalf("%s scope is not an object: %v", schedule.ID, err)
		}
		if decoded["version"] != float64(1) {
			t.Fatalf("%s scope version = %v", schedule.ID, decoded["version"])
		}
	}
}

func TestOccurrenceDomainIdentityIsAValidDeterministicUUID(t *testing.T) {
	schedule := scheduleByID(t, "phone_home_heartbeat")
	dueTime := mustTime(t, "2026-07-24T00:00:00Z")
	occurrence := NewOccurrence(schedule, dueTime, dueTime)
	first := OccurrenceDomainID(occurrence)
	if _, err := uuid.Parse(first); err != nil {
		t.Fatalf("domain id %q is not a UUID: %v", first, err)
	}
	later := NewOccurrence(schedule, dueTime, dueTime.Add(time.Hour))
	if OccurrenceDomainID(later) != first {
		t.Fatal("the observation instant leaked into the domain identity")
	}
}
