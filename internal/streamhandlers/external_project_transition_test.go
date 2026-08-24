package streamhandlers

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// externalProjectTransitionPayload is the minimum shape CHAOS-4193's producers
// are gated on. It is spelled out here rather than derived from the schema
// table so a silent relaxation of a required field fails a test instead of
// widening what the sink accepts.
func externalProjectTransitionPayload(provider string, overrides map[string]any) map[string]any {
	payload := map[string]any{
		"externalKey": "7", "provider": provider,
		"eventId":      "evt-1",
		"workItemType": "issue",
		"occurredAt":   "2026-07-22T11:00:00Z",
		"toProjectId":  "ghprojv2:full-chaos#4",
		"toProjectKey": "PLATFORM",
		"actor":        "ada",
	}
	for key, value := range overrides {
		payload[key] = value
	}
	return payload
}

// TestExternalIngestRefusesUnregisteredKindRatherThanDroppingIt is the property
// CHAOS-4193 said it would verify before gating its producers on kind
// registration: an unregistered kind must be REFUSED and the refusal must be
// durably recorded, not swallowed.
//
// The distinction is the whole point. A silent drop and a refusal look
// identical from the sink's side -- zero rows either way -- so a producer
// shipping against an unregistered kind would see a clean, successful sync and
// no data. Only the recorded rejection tells the two apart. The kind used here
// is deliberately one that will never be registered, so this test stays green
// after `work_item_project_transition.v1` is added and keeps guarding the
// mechanism rather than a moment in time.
func TestExternalIngestRefusesUnregisteredKindRatherThanDroppingIt(t *testing.T) {
	pointer := externalTestPointer()
	payload := externalTestPayload(t, pointer, "legacy", []map[string]any{
		{
			"kind": "repository.v1", "externalId": "repo-1",
			"payload": map[string]any{
				"externalId": pointer.SourceInstance, "sourceSystem": "github",
			},
		},
		{
			"kind": "work_item_project_teleport.v1", "externalId": "never-registered-1",
			"payload": map[string]any{"externalKey": "7", "provider": "github"},
		},
	})
	repository := &externalRepositoryFake{
		allowed: true,
		batch: externalBatch{
			Pointer: pointer, SourceID: uuid.New(), EntityFamily: "legacy",
			ItemsReceived: 2, Payload: payload,
		},
	}
	sink := &externalSinkFake{}
	handler, err := NewExternalIngestHandler(repository, sink, &externalSchedulerFake{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.backoff = nil
	if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err != nil {
		t.Fatal(err)
	}
	if len(repository.completions) != 1 {
		t.Fatalf("completion count = %d", len(repository.completions))
	}
	completion := repository.completions[0]
	if completion.Rejected != 1 || len(completion.Rejections) != 1 {
		t.Fatalf("unregistered kind was not refused: %#v", completion)
	}
	rejection := completion.Rejections[0]
	if rejection.Code != "unsupported_kind_for_system" || rejection.Kind != "work_item_project_teleport.v1" ||
		rejection.ExternalID != "never-registered-1" {
		t.Fatalf("refusal is not attributable to the record: %#v", rejection)
	}
	// The refusal must not cost the batch its registered records, and the
	// refused record must not reach the sink by any other route.
	if completion.Accepted != 1 || len(sink.calls) != 1 || len(sink.calls[0].Records) != 1 ||
		sink.calls[0].Records[0].Kind != "repository.v1" {
		t.Fatalf("refusal did not leave the batch's registered records intact: %#v", sink.calls)
	}
	if completion.RecordCounts["work_item_project_teleport.v1"] != 0 {
		t.Fatalf("refused kind was counted as ingested: %#v", completion.RecordCounts)
	}
}

// TestExternalIngestAcceptsProjectTransitionForEveryWorkItemProvider is the
// red-first half: on origin/main `work_item_project_transition.v1` is not in
// any provider's kind set, so every provider here is refused with
// `unsupported_kind_for_system` -- the exact refusal the test above proves is
// real. Registering the kind is what turns it green.
func TestExternalIngestAcceptsProjectTransitionForEveryWorkItemProvider(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			pointer := externalTestPointer()
			pointer.SourceSystem = provider
			if provider == "jira" || provider == "linear" {
				pointer.SourceInstance = provider + ".example.test"
			}
			payload := externalTestPayload(t, pointer, "legacy", []map[string]any{{
				"kind": "work_item_project_transition.v1", "externalId": "transition-1",
				"payload": externalProjectTransitionPayload(provider, nil),
			}})
			repository := &externalRepositoryFake{
				allowed: true,
				batch: externalBatch{
					Pointer: pointer, SourceID: uuid.New(), EntityFamily: "legacy",
					ItemsReceived: 1, Payload: payload,
				},
			}
			sink := &externalSinkFake{}
			handler, err := NewExternalIngestHandler(repository, sink, &externalSchedulerFake{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			handler.backoff = nil
			if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err != nil {
				t.Fatal(err)
			}
			completion := repository.completions[0]
			if completion.Accepted != 1 || completion.Rejected != 0 {
				t.Fatalf("project transition not accepted for %s: %#v", provider, completion)
			}
			if len(sink.calls) != 1 || len(sink.calls[0].Records) != 1 ||
				sink.calls[0].Records[0].Kind != "work_item_project_transition.v1" {
				t.Fatalf("project transition never reached the sink for %s: %#v", provider, sink.calls)
			}
		})
	}
}

// TestExternalProjectTransitionRefusesPullRequestSubjects keeps the CHAOS-4193
// lock honest at the schema boundary. The locked table keys on work_item_id; a
// pull request is not a work item and has no row in `work_items` to join. If a
// producer sends one, the correct answer is a recorded refusal, not a row keyed
// by a work-item id that identifies a PR (CHAOS-4194's PR->project shape is
// still an open proposal).
func TestExternalProjectTransitionRefusesPullRequestSubjects(t *testing.T) {
	for _, workItemType := range []string{"pr", "merge_request"} {
		t.Run(workItemType, func(t *testing.T) {
			err := validateExternalRecord("work_item_project_transition.v1",
				externalProjectTransitionPayload("github", map[string]any{"workItemType": workItemType}))
			if err == nil {
				t.Fatalf("%s subject was accepted into a work-item-keyed table", workItemType)
			}
			if !strings.Contains(err.Error(), "workItemType") {
				t.Fatalf("refusal does not name the offending field: %v", err)
			}
		})
	}
	// The case an enum alone does not cover: a PR payload that simply omits the
	// field. While workItemType was optional this fell through to the
	// issue-shaped id derivation and was ACCEPTED -- the refusal has to require
	// a positive "issue" declaration, not merely reject a disqualifying value.
	t.Run("omitted", func(t *testing.T) {
		payload := externalProjectTransitionPayload("github", nil)
		delete(payload, "workItemType")
		err := validateExternalRecord("work_item_project_transition.v1", payload)
		if err == nil {
			t.Fatal("an undeclared subject type was accepted; a PR payload need only omit the field")
		}
		if !strings.Contains(err.Error(), "workItemType") {
			t.Fatalf("refusal does not name the offending field: %v", err)
		}
	})
}

// TestExternalProjectTransitionRequiresAProviderEventTime pins the deviation
// from CHAOS-4194's provisional "occurred_at falls back to last_synced"
// default. occurred_at is a member of the sorting key, so a sink-supplied
// timestamp differs on every re-sync of the same provider event: the keys
// differ, FINAL keeps both, and the table accumulates one row per sync of a
// single reassignment -- exactly the duplication event_id is in the key to
// prevent. The sink cannot invent a stable value; only the producer can.
func TestExternalProjectTransitionRequiresAProviderEventTime(t *testing.T) {
	payload := externalProjectTransitionPayload("github", nil)
	delete(payload, "occurredAt")
	err := validateExternalRecord("work_item_project_transition.v1", payload)
	if err == nil {
		t.Fatal("a transition with no provider event time was accepted; it cannot dedupe on re-sync")
	}
	if !strings.Contains(err.Error(), "occurredAt") {
		t.Fatalf("refusal does not name the offending field: %v", err)
	}
}

// TestExternalProjectTransitionIDMatchesItsWorkItemID is the join guarantee.
// The presence projection resolves these rows against `work_items`, so the two
// derivations must agree. They only do if this kind consults the record's own
// repositoryExternalId the way work_item.v1 does: with a batch pointed at an
// org while the records name org/repo, deriving from the pointer alone yields
// `gh:full-chaos#7` against the work item's `gh:full-chaos/dev-health#7` -- a
// well-formed row that joins to nothing.
func TestExternalProjectTransitionIDMatchesItsWorkItemID(t *testing.T) {
	pointer := externalTestPointer()
	pointer.SourceInstance = "full-chaos"
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	build := func(kind string, payload map[string]any) []any {
		t.Helper()
		batch := &productBatch{}
		connection := &productSink{batches: []*productBatch{batch}}
		sink, err := NewClickHouseExternalBatchSink(connection)
		if err != nil {
			t.Fatal(err)
		}
		sink.now = func() time.Time { return now }
		if _, err := sink.Write(context.Background(), externalSinkBatch{
			Pointer: pointer, SourceID: uuid.New(),
			Records: []externalSinkRecord{externalSinkFixture(kind, payload)},
		}); err != nil {
			t.Fatal(err)
		}
		return batch.rows[0]
	}
	workItemRow := build("work_item.v1", map[string]any{
		"externalKey": "7", "provider": "github", "title": "Issue", "type": "issue",
		"status": "todo", "createdAt": "2026-07-20T10:00:00Z",
		"repositoryExternalId": "full-chaos/dev-health",
	})
	transitionRow := build("work_item_project_transition.v1",
		externalProjectTransitionPayload("github", map[string]any{
			"repositoryExternalId": "full-chaos/dev-health",
		}))
	// work_item.v1 column 1 is work_item_id; the transition's is column 3.
	if workItemRow[1] != transitionRow[3] {
		t.Fatalf("work_item_id mismatch: work_item.v1=%v transition=%v", workItemRow[1], transitionRow[3])
	}
	if transitionRow[3] != "gh:full-chaos/dev-health#7" {
		t.Fatalf("derived work_item_id = %v", transitionRow[3])
	}
	// repo_id must agree too, or the row is scoped to a repository the work
	// item does not belong to.
	if workItemRow[0] != transitionRow[2] {
		t.Fatalf("repo_id mismatch: work_item.v1=%v transition=%v", workItemRow[0], transitionRow[2])
	}
}

// TestExternalProjectTransitionRefusesAProviderTheBatchDidNotComeFrom covers
// the contradiction the schema enum cannot see, because it validates the field
// in isolation while the conflict only exists relative to the batch pointer.
// Project ids are provider-scoped, so filing a jira project id under a github
// batch produces a row that resolves against the wrong catalogue.
func TestExternalProjectTransitionRefusesAProviderTheBatchDidNotComeFrom(t *testing.T) {
	pointer := externalTestPointer()
	payload := externalTestPayload(t, pointer, "legacy", []map[string]any{{
		"kind": "work_item_project_transition.v1", "externalId": "evt-1",
		"payload": externalProjectTransitionPayload("jira", nil),
	}})
	repository := &externalRepositoryFake{
		allowed: true,
		batch: externalBatch{
			Pointer: pointer, SourceID: uuid.New(), EntityFamily: "legacy",
			ItemsReceived: 1, Payload: payload,
		},
	}
	sink := &externalSinkFake{}
	handler, err := NewExternalIngestHandler(repository, sink, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.backoff = nil
	if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err != nil {
		t.Fatal(err)
	}
	completion := repository.completions[0]
	if completion.Accepted != 0 || completion.Rejected != 1 {
		t.Fatalf("a cross-provider transition was accepted: %#v", completion)
	}
	if completion.Rejections[0].Code != "invalid_field" ||
		!strings.Contains(completion.Rejections[0].Message, "source system") {
		t.Fatalf("refusal is not attributable: %#v", completion.Rejections[0])
	}
	if len(sink.calls) != 0 {
		t.Fatalf("the refused record reached the sink: %#v", sink.calls)
	}
}

// TestExternalProjectTransitionRefusesBlankIdempotencyMembers proves the
// required-string rule rejects whitespace, not merely absence. An empty
// event_id is worse than a missing one: it validates as "present", lands in the
// sorting key, and silently merges every timeless reassignment of a work item
// into one row.
func TestExternalProjectTransitionRefusesBlankIdempotencyMembers(t *testing.T) {
	for _, field := range []string{"eventId", "toProjectId", "externalKey"} {
		t.Run(field, func(t *testing.T) {
			for _, blank := range []string{"", "   "} {
				payload := externalProjectTransitionPayload("github", map[string]any{field: blank})
				if err := validateExternalRecord("work_item_project_transition.v1", payload); err == nil {
					t.Fatalf("%s = %q was accepted", field, blank)
				}
			}
		})
	}
}

// TestExternalProjectTransitionRequiresIdempotencyMembers pins the two payload
// fields that are load-bearing for dedupe. `eventId` is a member of the sorting
// key, so an empty one collapses genuinely distinct reassignments that share a
// timestamp into a single ReplacingMergeTree row. `toProjectId` is the
// destination the presence projection reads; the lock makes it non-nullable and
// gives no sentinel for "removed from every project", so an event without one
// is refused rather than encoded as an empty-string project that the projection
// would then present as a real current value.
func TestExternalProjectTransitionRequiresIdempotencyMembers(t *testing.T) {
	for _, field := range []string{"eventId", "toProjectId", "provider", "externalKey"} {
		t.Run(field, func(t *testing.T) {
			payload := externalProjectTransitionPayload("github", nil)
			delete(payload, field)
			err := validateExternalRecord("work_item_project_transition.v1", payload)
			if err == nil {
				t.Fatalf("%s is not required", field)
			}
			// Without this the whole subtest passes vacuously before the kind
			// is registered: every payload is refused as an unknown kind, so
			// "required" would be indistinguishable from "unrecognised".
			if !strings.Contains(err.Error(), field) {
				t.Fatalf("refusal does not name the missing field %s: %v", field, err)
			}
		})
	}
}

// TestClickHouseExternalSinkWritesProjectTransitionColumns pins the row the
// sink actually appends against the CHAOS-4193 locked column order. The
// insert statement and the value slice are written in two different files;
// nothing but a test compares them, and a silent reorder would land every
// project id in the project key column.
func TestClickHouseExternalSinkWritesProjectTransitionColumns(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	sourceID := uuid.MustParse("9749bda0-fc9f-4076-b19d-7b26c4f306ff")
	pointer := externalTestPointer()
	batch := &productBatch{}
	connection := &productSink{batches: []*productBatch{batch}}
	sink, err := NewClickHouseExternalBatchSink(connection)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return now }
	if _, err := sink.Write(context.Background(), externalSinkBatch{
		Pointer: pointer, SourceID: sourceID,
		Records: []externalSinkRecord{externalSinkFixture(
			"work_item_project_transition.v1",
			externalProjectTransitionPayload("github", map[string]any{"fromProjectId": "ghprojv2:full-chaos#1"}),
		)},
	}); err != nil {
		t.Fatal(err)
	}
	if len(connection.queries) != 1 {
		t.Fatalf("prepared queries = %#v", connection.queries)
	}
	query := connection.queries[0]
	const wantColumns = "(org_id,source_id,repo_id,work_item_id,provider,from_project_id,to_project_id," +
		"from_project_key,to_project_key,actor,occurred_at,last_synced,event_id)"
	if !strings.Contains(query, "INSERT INTO work_item_project_transitions "+wantColumns) {
		t.Fatalf("insert does not match the locked column order: %s", query)
	}
	if !batch.sent || len(batch.rows) != 1 {
		t.Fatalf("project transition row not durable: %#v", batch)
	}
	row := batch.rows[0]
	columns := strings.Split(strings.TrimSuffix(strings.TrimPrefix(wantColumns, "("), ")"), ",")
	if len(row) != len(columns) {
		t.Fatalf("row width %d does not match %d locked columns", len(row), len(columns))
	}
	want := map[string]any{
		"org_id":           pointer.OrgID,
		"source_id":        sourceID,
		"repo_id":          uuid.MustParse("00b02aea-81bc-1244-b364-f93a0276ede5"),
		"work_item_id":     "gh:full-chaos/dev-health#7",
		"provider":         "github",
		"from_project_id":  "ghprojv2:full-chaos#1",
		"to_project_id":    "ghprojv2:full-chaos#4",
		"from_project_key": "",
		"to_project_key":   "PLATFORM",
		"actor":            any("github:ada"),
		"occurred_at":      time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC),
		"last_synced":      now,
		"event_id":         "evt-1",
	}
	for index, column := range columns {
		if row[index] != want[column] {
			t.Errorf("%s = %#v, want %#v", column, row[index], want[column])
		}
	}
}

// TestExternalProjectTransitionTracksItsKindAndTimeInTheRecomputeScope pins the
// two things the sink must hand downstream: the kind, so the recompute
// controller knows what changed, and the event time, so the recomputed window
// covers the reassignment rather than only the sync.
//
// This replaces an earlier test asserting occurred_at fell back to last_synced
// when the provider carried no event time. That fallback was removed: it put a
// per-sync value into the sorting key and defeated dedupe on re-sync (codex
// adversarial review, round 1). See
// TestExternalProjectTransitionRequiresAProviderEventTime.
func TestExternalProjectTransitionTracksItsKindAndTimeInTheRecomputeScope(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	occurred := time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC)
	pointer := externalTestPointer()
	batch := &productBatch{}
	connection := &productSink{batches: []*productBatch{batch}}
	sink, err := NewClickHouseExternalBatchSink(connection)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return now }
	scope, err := sink.Write(context.Background(), externalSinkBatch{
		Pointer: pointer, SourceID: uuid.New(),
		Records: []externalSinkRecord{externalSinkFixture(
			"work_item_project_transition.v1", externalProjectTransitionPayload("github", nil))},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(scope.RecordKinds, "work_item_project_transition.v1") {
		t.Fatalf("recompute scope omits the kind: %#v", scope.RecordKinds)
	}
	if batch.rows[0][10] != occurred {
		t.Fatalf("occurred_at = %#v, want the provider event time %#v", batch.rows[0][10], occurred)
	}
	if batch.rows[0][11] != now {
		t.Fatalf("last_synced = %#v, want the sink observation %#v", batch.rows[0][11], now)
	}
	if scope.WindowStart == nil || !scope.WindowStart.Equal(occurred) {
		t.Fatalf("recompute window does not cover the reassignment: %#v", scope.WindowStart)
	}
}

type externalObserverFake struct {
	sunk     map[string]int
	refusals []string
}

func (f *externalObserverFake) ObserveExternalProjectTransitionsSunk(provider string, rows int) error {
	if f.sunk == nil {
		f.sunk = map[string]int{}
	}
	f.sunk[provider] += rows
	return nil
}

func (f *externalObserverFake) ObserveExternalKindRefused(sourceSystem, reason string) error {
	f.refusals = append(f.refusals, sourceSystem+":"+reason)
	return nil
}

// TestExternalIngestReportsSunkTransitionsAndRefusalsTogether covers the
// standing telemetry order for this change. The batch deliberately mixes a
// durable transition with a refused record: reporting either one alone is what
// makes the counters unreadable, since a flat sunk counter cannot be told from
// a registry refusing everything.
func TestExternalIngestReportsSunkTransitionsAndRefusalsTogether(t *testing.T) {
	pointer := externalTestPointer()
	payload := externalTestPayload(t, pointer, "legacy", []map[string]any{
		{
			"kind": "work_item_project_transition.v1", "externalId": "evt-1",
			"payload": externalProjectTransitionPayload("github", nil),
		},
		{
			"kind": "work_item_project_transition.v1", "externalId": "evt-2",
			"payload": externalProjectTransitionPayload("github", map[string]any{"eventId": "evt-2"}),
		},
		{
			"kind": "work_item_project_transition.v1", "externalId": "evt-3",
			"payload": externalProjectTransitionPayload("github", map[string]any{"workItemType": "pr"}),
		},
	})
	repository := &externalRepositoryFake{
		allowed: true,
		batch: externalBatch{
			Pointer: pointer, SourceID: uuid.New(), EntityFamily: "legacy",
			ItemsReceived: 3, Payload: payload,
		},
	}
	observer := &externalObserverFake{}
	handler, err := NewExternalIngestHandler(repository, &externalSinkFake{}, nil, observer)
	if err != nil {
		t.Fatal(err)
	}
	handler.backoff = nil
	if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err != nil {
		t.Fatal(err)
	}
	if observer.sunk["github"] != 2 {
		t.Errorf("sunk transitions = %#v, want 2 for github", observer.sunk)
	}
	if len(observer.refusals) != 1 || observer.refusals[0] != "github:invalid_field" {
		t.Errorf("refusals = %#v, want one github:invalid_field", observer.refusals)
	}
}

// TestExternalIngestReportsNothingWhenTheSinkNeverCommitted is the discipline
// the zero-unit finalization counter learned the hard way: a counter bumped
// before the write is durable overcounts every retry of a batch that
// eventually succeeds once. Here the sink never succeeds at all, so a handler
// that counted at classification time would report project transitions that do
// not exist in ClickHouse.
func TestExternalIngestReportsNothingWhenTheSinkNeverCommitted(t *testing.T) {
	pointer := externalTestPointer()
	payload := externalTestPayload(t, pointer, "legacy", []map[string]any{{
		"kind": "work_item_project_transition.v1", "externalId": "evt-1",
		"payload": externalProjectTransitionPayload("github", nil),
	}})
	repository := &externalRepositoryFake{
		allowed: true,
		batch: externalBatch{
			Pointer: pointer, SourceID: uuid.New(), EntityFamily: "legacy",
			ItemsReceived: 1, Payload: payload,
		},
	}
	sink := &externalSinkFake{errors: []error{errors.New("clickhouse unavailable")}}
	observer := &externalObserverFake{}
	handler, err := NewExternalIngestHandler(repository, sink, nil, observer)
	if err != nil {
		t.Fatal(err)
	}
	handler.backoff = nil
	if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err == nil {
		t.Fatal("a failed sink write was reported as a successful batch")
	}
	if len(observer.sunk) != 0 || len(observer.refusals) != 0 {
		t.Fatalf("telemetry recorded an uncommitted batch: sunk=%#v refusals=%#v", observer.sunk, observer.refusals)
	}
}

// TestExternalIngestHandlesABatchWithoutAnObserver kills the nil guard in
// observeOutcome. The stream-runner profiles that expose no Prometheus surface
// pass nil rather than a no-op double, so nil is a real production shape and
// not a test-only convenience.
func TestExternalIngestHandlesABatchWithoutAnObserver(t *testing.T) {
	pointer := externalTestPointer()
	payload := externalTestPayload(t, pointer, "legacy", []map[string]any{{
		"kind": "work_item_project_transition.v1", "externalId": "evt-1",
		"payload": externalProjectTransitionPayload("github", nil),
	}})
	repository := &externalRepositoryFake{
		allowed: true,
		batch: externalBatch{
			Pointer: pointer, SourceID: uuid.New(), EntityFamily: "legacy",
			ItemsReceived: 1, Payload: payload,
		},
	}
	sink := &externalSinkFake{}
	handler, err := NewExternalIngestHandler(repository, sink, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.backoff = nil
	if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err != nil {
		t.Fatalf("a nil observer broke ingest: %v", err)
	}
	if len(sink.calls) != 1 || len(sink.calls[0].Records) != 1 {
		t.Fatalf("record did not reach the sink without an observer: %#v", sink.calls)
	}
}
