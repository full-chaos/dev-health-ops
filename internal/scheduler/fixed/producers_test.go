package fixed

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
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

type recordingDailyFanoutStore struct {
	requests []daily.ScheduledFanoutRequest
}

func (store *recordingDailyFanoutStore) StartScheduledFanoutRunTx(
	_ context.Context,
	_ pgx.Tx,
	request daily.ScheduledFanoutRequest,
	_ daily.RunPublisher,
) (daily.Run, error) {
	store.requests = append(store.requests, request)
	return daily.Run{ID: uuid.NewString(), OrganizationID: request.OrganizationID}, nil
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

type fixedAskDevRetentionAdmission struct {
	state AskDevRetentionState
	err   error
	calls int
}

func (admission *fixedAskDevRetentionAdmission) State(
	context.Context,
	pgx.Tx,
) (AskDevRetentionState, error) {
	admission.calls++
	return admission.state, admission.err
}

func askDevRetentionProducer(
	t *testing.T,
	admission AskDevRetentionAdmission,
	producerVersion int,
) Producer {
	t.Helper()
	producer, err := NewRetentionProducerForRoute(admission, producerVersion)
	if err != nil {
		t.Fatalf("NewRetentionProducerForRoute() = %v", err)
	}
	return producer
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

type nopDailyRunPublisher struct{}

func (nopDailyRunPublisher) PublishDispatchTx(
	context.Context, pgx.Tx, daily.Run, string,
) error {
	return nil
}

func dailyFanoutProducer(
	t *testing.T,
	lister OrganizationLister,
) (*DailyMetricsFanoutProducer, *recordingDailyFanoutStore) {
	t.Helper()
	store := &recordingDailyFanoutStore{}
	producer, err := NewDailyMetricsFanoutProducer(store, nopDailyRunPublisher{}, lister)
	if err != nil {
		t.Fatalf("NewDailyMetricsFanoutProducer() = %v", err)
	}
	return producer.(*DailyMetricsFanoutProducer), store
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

func TestSyncCoverageRefreshProducerEmitsOneBoundedDeterministicSweep(t *testing.T) {
	t.Parallel()
	schedule := scheduleByID(t, "sync_coverage_refresh")
	occurrence := NewOccurrence(
		schedule,
		mustTime(t, "2026-08-12T12:00:00Z"),
		mustTime(t, "2026-08-12T12:00:03Z"),
	)
	producer := NewSyncCoverageRefreshProducer()
	first, err := producer.Produce(context.Background(), &stubTx{}, schedule, occurrence)
	if err != nil {
		t.Fatalf("Produce() = %v", err)
	}
	second, err := producer.Produce(context.Background(), &stubTx{}, schedule, occurrence)
	if err != nil {
		t.Fatalf("replay Produce() = %v", err)
	}
	if len(first.Requests) != 1 || len(second.Requests) != 1 {
		t.Fatalf("requests = %d/%d, want one each", len(first.Requests), len(second.Requests))
	}
	request := first.Requests[0]
	if request.Kind != jobcontract.KindSyncCoverageRefresh || request.Envelope != second.Requests[0].Envelope {
		t.Fatalf("refresh request is not deterministic: first=%+v second=%+v", request, second.Requests[0])
	}
	payload, ok := request.Envelope.Payload.(jobcontract.SyncCoverageRefreshPayload)
	if !ok {
		t.Fatalf("payload = %T", request.Envelope.Payload)
	}
	if payload.Limit != 100 || payload.ScheduledFor != "2026-08-12T12:00:00Z" {
		t.Fatalf("payload = %+v", payload)
	}
	if request.Envelope.OrganizationID != nil || request.Envelope.Domain.Type != "schedule_occurrence" {
		t.Fatalf("global occurrence envelope = %+v", request.Envelope)
	}
}

func TestDailyMetricsFanoutCreatesOnlyDurablePerOrganizationRuns(t *testing.T) {
	schedule := scheduleByID(t, "daily_metrics_fanout")
	producer, store := dailyFanoutProducer(t, fixedOrganizationLister{identifiers: []string{testOrgA, testOrgB}})
	occurrence := NewOccurrence(
		schedule, mustTime(t, "2026-08-12T01:00:00Z"), mustTime(t, "2026-08-12T01:00:05Z"),
	)

	outcome, err := producer.Produce(context.Background(), &stubTx{}, schedule, occurrence)
	if err != nil {
		t.Fatalf("Produce() = %v", err)
	}
	if outcome.Handoffs != 2 || len(store.requests) != 2 {
		t.Fatalf("outcome=%+v requests=%d", outcome, len(store.requests))
	}
	for index, request := range store.requests {
		wantOrganization := []string{testOrgA, testOrgB}[index]
		if request.OrganizationID != wantOrganization ||
			request.TargetDay != mustTime(t, "2026-08-12T01:00:00Z") ||
			request.Generation != "fixed-schedule:daily_metrics_fanout:2026-08-12T01:00:00Z" {
			t.Fatalf("request[%d]=%+v", index, request)
		}
	}
}

func TestDailyMetricsFanoutReportsNoActiveOrganizations(t *testing.T) {
	schedule := scheduleByID(t, "daily_metrics_fanout")
	producer, store := dailyFanoutProducer(t, fixedOrganizationLister{})
	occurrence := NewOccurrence(
		schedule, mustTime(t, "2026-08-12T01:00:00Z"), mustTime(t, "2026-08-12T01:00:05Z"),
	)

	outcome, err := producer.Produce(context.Background(), &stubTx{}, schedule, occurrence)
	if err != nil {
		t.Fatalf("Produce() = %v", err)
	}
	if outcome.SkipReason != SkipNoActiveOrganizations || len(store.requests) != 0 {
		t.Fatalf("outcome=%+v requests=%d", outcome, len(store.requests))
	}
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
			wantPolicy:   jobcontract.RetentionRateLimitObservations,
			wantDeleteAt: "2026-07-17T05:00:00Z",
		},
		{
			scheduleID:   "prune_external_ingest_batches",
			wantPolicy:   jobcontract.RetentionExternalIngestBatches,
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
func TestEveryProducedEnvelopeSatisfiesTheCompiledContract(t *testing.T) {
	// Retention rejoins this strict round-trip now that the runtime-truth
	// lane's internal/jobcontract change has landed: the compiled contract
	// pins its schema enum to jobcontract.RetentionPolicies() element-by-
	// element, so both plural policy names now marshal and decode cleanly.
	retentionProducer := NewRetentionProducer()
	askDevProducer := askDevRetentionProducer(
		t,
		&fixedAskDevRetentionAdmission{state: AskDevRetentionState{FeatureEnabled: true}},
		jobcontract.ContractVersionV3,
	)
	producers := map[string]Producer{
		"phone_home_heartbeat":          NewHeartbeatProducer(),
		"prune_rate_limit_observations": retentionProducer,
		"prune_external_ingest_batches": retentionProducer,
		"prune_ask_dev_conversations":   askDevProducer,
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
		if len(outcome.Requests) != 1 {
			t.Fatalf("%s produced %d requests, want one", id, len(outcome.Requests))
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

// The retention handler rejects a payload that violates any of these, and a
// rejection is permanent rather than retried, so they are pinned here rather
// than discovered at execution. This stands in for the strict contract
// round-trip until the runtime-truth lane's jobcontract change merges.
func TestRetentionPayloadsSatisfyTheHandlerContract(t *testing.T) {
	legacyProducer := NewRetentionProducer()
	askDevProducer := askDevRetentionProducer(
		t,
		&fixedAskDevRetentionAdmission{state: AskDevRetentionState{FeatureEnabled: true}},
		jobcontract.ContractVersionV3,
	)
	for _, test := range []struct{ scheduleID, wantPolicy string }{
		{"prune_rate_limit_observations", jobcontract.RetentionRateLimitObservations},
		{"prune_external_ingest_batches", jobcontract.RetentionExternalIngestBatches},
		{"prune_ask_dev_conversations", jobcontract.RetentionAskDevConversations},
	} {
		producer := legacyProducer
		if test.scheduleID == "prune_ask_dev_conversations" {
			producer = askDevProducer
		}
		schedule := scheduleByID(t, test.scheduleID)
		dueTime := mustTime(t, "2026-07-24T05:00:00Z")
		outcome, err := producer.Produce(
			context.Background(), &stubTx{}, schedule, NewOccurrence(schedule, dueTime, dueTime),
		)
		if err != nil {
			t.Fatalf("%s Produce() = %v", test.scheduleID, err)
		}
		payload, ok := outcome.Requests[0].Envelope.Payload.(jobcontract.RetentionCleanupPayload)
		if !ok {
			t.Fatalf("%s payload type %T", test.scheduleID, outcome.Requests[0].Envelope.Payload)
		}
		if payload.RetentionPolicy != test.wantPolicy {
			t.Errorf("%s policy = %q, want the agreed plural name %q",
				test.scheduleID, payload.RetentionPolicy, test.wantPolicy)
		}
		// Pinned to exactly 500, not merely inside the 1..1000 schema bound:
		// it is a coordinated value with the retention handler's chunk size.
		if payload.BatchSize != 500 {
			t.Errorf("%s batch_size = %d, want the coordinated 500",
				test.scheduleID, payload.BatchSize)
		}
		if !strings.HasSuffix(payload.DeleteBefore, "Z") {
			t.Errorf("%s delete_before %q lacks the required trailing Z",
				test.scheduleID, payload.DeleteBefore)
		}
		parsed, err := time.Parse(time.RFC3339, payload.DeleteBefore)
		if err != nil {
			t.Errorf("%s delete_before %q is not RFC3339: %v",
				test.scheduleID, payload.DeleteBefore, err)
			continue
		}
		if parsed.Location() != time.UTC {
			t.Errorf("%s delete_before %q is not UTC; the handler treats that as permanent",
				test.scheduleID, payload.DeleteBefore)
		}
		// The universal invariant is that a cutoff is never AFTER its due time.
		// It is deliberately not "strictly before": a zero horizon is legal
		// configuration and makes the two equal. Strict inequality is asserted
		// separately for the default horizon this case actually exercises.
		if parsed.After(dueTime) {
			t.Errorf("%s cutoff %s is in the future relative to its due time",
				test.scheduleID, payload.DeleteBefore)
		}
		if test.scheduleID != "prune_ask_dev_conversations" && !parsed.Before(dueTime) {
			t.Errorf("%s cutoff %s is not before its due time under the default horizon",
				test.scheduleID, payload.DeleteBefore)
		}
	}
}

func TestAskDevRetentionAdmissionPreservesRollbackAndConsumerCompatibility(t *testing.T) {
	schedule := scheduleByID(t, "prune_ask_dev_conversations")
	dueTime := mustTime(t, "2026-07-24T05:30:00Z")
	occurrence := NewOccurrence(schedule, dueTime, dueTime)

	t.Run("never enabled with no rows does not emit", func(t *testing.T) {
		admission := &fixedAskDevRetentionAdmission{}
		producer := askDevRetentionProducer(t, admission, jobcontract.ContractVersionV3)
		outcome, err := producer.Produce(t.Context(), &stubTx{}, schedule, occurrence)
		if err != nil {
			t.Fatal(err)
		}
		if len(outcome.Requests) != 0 || outcome.SkipReason != "ask_dev_inactive_without_state" {
			t.Fatalf("outcome = %+v, want an explicit inactive/no-state skip", outcome)
		}
		if admission.calls != 1 {
			t.Fatalf("admission calls = %d, want one fresh decision", admission.calls)
		}
	})

	t.Run("disabled after use still emits for persisted state", func(t *testing.T) {
		admission := &fixedAskDevRetentionAdmission{state: AskDevRetentionState{
			FeatureEnabled:    false,
			HasPersistedState: true,
		}}
		producer := askDevRetentionProducer(t, admission, jobcontract.ContractVersionV3)
		outcome, err := producer.Produce(t.Context(), &stubTx{}, schedule, occurrence)
		if err != nil {
			t.Fatal(err)
		}
		if len(outcome.Requests) != 1 {
			t.Fatalf("outcome = %+v, disabled Ask Dev stranded persisted state", outcome)
		}
		if outcome.Requests[0].Envelope.ContractVersion != jobcontract.ContractVersionV3 {
			t.Fatalf("contract version = %d, want v3", outcome.Requests[0].Envelope.ContractVersion)
		}
	})

	t.Run("incompatible consumer route never emits v3", func(t *testing.T) {
		admission := &fixedAskDevRetentionAdmission{state: AskDevRetentionState{
			FeatureEnabled:    true,
			HasPersistedState: true,
		}}
		producer := askDevRetentionProducer(t, admission, jobcontract.ContractVersionV2)
		outcome, err := producer.Produce(t.Context(), &stubTx{}, schedule, occurrence)
		if err != nil {
			t.Fatal(err)
		}
		if len(outcome.Requests) != 0 || outcome.SkipReason != "consumer_version_incompatible" {
			t.Fatalf("outcome = %+v, want an explicit compatibility skip", outcome)
		}
		if admission.calls != 0 {
			t.Fatal("feature admission ran even though the active route cannot carry v3")
		}
	})

	t.Run("admission storage failure is loud", func(t *testing.T) {
		admission := &fixedAskDevRetentionAdmission{err: ErrProducerUnavailable}
		producer := askDevRetentionProducer(t, admission, jobcontract.ContractVersionV3)
		outcome, err := producer.Produce(t.Context(), &stubTx{}, schedule, occurrence)
		if err == nil || len(outcome.Requests) != 0 {
			t.Fatalf("outcome = %+v, error = %v; admission failure must not emit", outcome, err)
		}
	})
}

// Unset, empty, and unparseable overrides take the checked default, matching
// both legacy Python readers. Zero is legal: "retain nothing" is a coherent
// posture and is exactly what was typed. A negative value is not an expression
// of intent, and the legacy clamp turned it into "delete every terminal row"
// with no backstop, so it now fails the occurrence loudly.
func TestRetentionHorizonRejectsNegativeOverridesAndHonorsTheRest(t *testing.T) {
	schedule := scheduleByID(t, "prune_rate_limit_observations")
	dueTime := mustTime(t, "2026-07-24T05:00:00Z")

	for _, test := range []struct{ raw, wantCutoff string }{
		{"7", "2026-07-17T05:00:00Z"},
		// Zero is deliberate configuration, not a clamp artifact: the cutoff
		// becomes the due time itself.
		{"0", "2026-07-24T05:00:00Z"},
		{"garbage", "2026-07-10T05:00:00Z"},
		{"", "2026-07-10T05:00:00Z"},
		{"  21  ", "2026-07-03T05:00:00Z"},
	} {
		t.Setenv("SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS", test.raw)
		outcome, err := NewRetentionProducer().Produce(
			context.Background(), &stubTx{}, schedule, NewOccurrence(schedule, dueTime, dueTime),
		)
		if err != nil {
			t.Fatalf("override %q: %v", test.raw, err)
		}
		payload := outcome.Requests[0].Envelope.Payload.(jobcontract.RetentionCleanupPayload)
		if payload.DeleteBefore != test.wantCutoff {
			t.Errorf("override %q produced cutoff %s, want %s",
				test.raw, payload.DeleteBefore, test.wantCutoff)
		}
	}

	for _, raw := range []string{"-1", "-3", " -90 "} {
		t.Setenv("SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS", raw)
		outcome, err := NewRetentionProducer().Produce(
			context.Background(), &stubTx{}, schedule, NewOccurrence(schedule, dueTime, dueTime),
		)
		if !errors.Is(err, ErrRetentionConfiguration) {
			t.Fatalf("override %q = %v, want ErrRetentionConfiguration", raw, err)
		}
		// The delete-everything cutoff must never reach an envelope, not even
		// alongside the error.
		if len(outcome.Requests) != 0 || outcome.Handoffs != 0 {
			t.Fatalf("override %q emitted work anyway: %+v", raw, outcome)
		}
	}
}

// Both retention schedules honor their own override independently, so a bad
// value on one cannot silently disable the other.
func TestRetentionOverridesAreIndependentPerSchedule(t *testing.T) {
	t.Setenv("SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS", "-1")
	t.Setenv("EXTERNAL_INGEST_STATUS_RETENTION_DAYS", "30")
	producer := NewRetentionProducer()

	rateLimit := scheduleByID(t, "prune_rate_limit_observations")
	rateLimitDue := mustTime(t, "2026-07-24T05:00:00Z")
	if _, err := producer.Produce(
		context.Background(), &stubTx{}, rateLimit,
		NewOccurrence(rateLimit, rateLimitDue, rateLimitDue),
	); !errors.Is(err, ErrRetentionConfiguration) {
		t.Fatalf("rate-limit schedule = %v, want ErrRetentionConfiguration", err)
	}

	external := scheduleByID(t, "prune_external_ingest_batches")
	externalDue := mustTime(t, "2026-07-24T05:15:00Z")
	outcome, err := producer.Produce(
		context.Background(), &stubTx{}, external,
		NewOccurrence(external, externalDue, externalDue),
	)
	if err != nil {
		t.Fatalf("external-ingest schedule failed on the other schedule's bad override: %v", err)
	}
	payload := outcome.Requests[0].Envelope.Payload.(jobcontract.RetentionCleanupPayload)
	if payload.DeleteBefore != "2026-06-24T05:15:00Z" {
		t.Fatalf("external-ingest cutoff = %s", payload.DeleteBefore)
	}
}

// The scheduler is long-lived and the legacy tasks re-read their horizon on
// every run, so an operator changing the override must take effect on the next
// occurrence rather than at the next process restart. Caching it at producer
// construction would silently keep emitting the old cutoff.
func TestRetentionHorizonIsResolvedPerOccurrenceNotAtConstruction(t *testing.T) {
	t.Setenv("SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS", "14")
	producer := NewRetentionProducer()
	schedule := scheduleByID(t, "prune_rate_limit_observations")
	dueTime := mustTime(t, "2026-07-24T05:00:00Z")

	first, err := producer.Produce(
		context.Background(), &stubTx{}, schedule, NewOccurrence(schedule, dueTime, dueTime),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := first.Requests[0].Envelope.Payload.(jobcontract.RetentionCleanupPayload).DeleteBefore; got != "2026-07-10T05:00:00Z" {
		t.Fatalf("initial cutoff = %s", got)
	}

	// Same producer instance, changed override.
	t.Setenv("SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS", "30")
	second, err := producer.Produce(
		context.Background(), &stubTx{}, schedule, NewOccurrence(schedule, dueTime, dueTime),
	)
	if err != nil {
		t.Fatal(err)
	}
	got := second.Requests[0].Envelope.Payload.(jobcontract.RetentionCleanupPayload).DeleteBefore
	if got != "2026-06-24T05:00:00Z" {
		t.Fatalf("cutoff after override change = %s, want the 30 day horizon; "+
			"the producer cached its horizon at construction", got)
	}
}

// A zero horizon is legal and puts the cutoff exactly on the due time. That is
// the only configuration this producer can emit which does not sit strictly in
// the past, so it is the boundary case against the retention handler's
// future-cutoff guard and is pinned explicitly rather than left to the default
// horizon paths.
func TestZeroHorizonEmitsACutoffExactlyAtTheDueTime(t *testing.T) {
	t.Setenv("SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS", "0")
	schedule := scheduleByID(t, "prune_rate_limit_observations")
	dueTime := mustTime(t, "2026-07-24T05:00:00Z")
	outcome, err := NewRetentionProducer().Produce(
		context.Background(), &stubTx{}, schedule, NewOccurrence(schedule, dueTime, dueTime),
	)
	if err != nil {
		t.Fatalf("a zero horizon is legal configuration: %v", err)
	}
	payload := outcome.Requests[0].Envelope.Payload.(jobcontract.RetentionCleanupPayload)
	parsed, err := time.Parse(time.RFC3339, payload.DeleteBefore)
	if err != nil {
		t.Fatalf("cutoff %q is not RFC3339: %v", payload.DeleteBefore, err)
	}
	if !parsed.Equal(dueTime) {
		t.Fatalf("zero-horizon cutoff = %s, want the due time exactly", payload.DeleteBefore)
	}
	// It must never be emitted as a future cutoff, which is what the handler
	// refuses outright.
	if parsed.After(dueTime) {
		t.Fatalf("zero-horizon cutoff %s is in the future", payload.DeleteBefore)
	}
}
