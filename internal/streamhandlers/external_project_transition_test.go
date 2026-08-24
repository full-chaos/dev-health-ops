package streamhandlers

import (
	"context"
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
	handler, err := NewExternalIngestHandler(repository, sink, &externalSchedulerFake{})
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
			handler, err := NewExternalIngestHandler(repository, sink, &externalSchedulerFake{})
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

// TestExternalProjectTransitionFallsBackToLastSyncedWhenProviderHasNoEventTime
// pins the CHAOS-4194 provisional default: occurred_at is the provider event
// time when present, else the sink's own observation time. It matters that the
// fallback is last_synced and not the zero time -- occurred_at is in the
// sorting key, so a zero would sort every timeless event ahead of real history
// and the presence projection (latest transition wins) would answer with the
// oldest one.
func TestExternalProjectTransitionFallsBackToLastSyncedWhenProviderHasNoEventTime(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	pointer := externalTestPointer()
	batch := &productBatch{}
	connection := &productSink{batches: []*productBatch{batch}}
	sink, err := NewClickHouseExternalBatchSink(connection)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return now }
	payload := externalProjectTransitionPayload("github", nil)
	delete(payload, "occurredAt")
	scope, err := sink.Write(context.Background(), externalSinkBatch{
		Pointer: pointer, SourceID: uuid.New(),
		Records: []externalSinkRecord{externalSinkFixture("work_item_project_transition.v1", payload)},
	})
	if err != nil {
		t.Fatal(err)
	}
	row := batch.rows[0]
	if row[10] != now {
		t.Fatalf("occurred_at = %#v, want the last_synced fallback %#v", row[10], now)
	}
	if !slices.Contains(scope.RecordKinds, "work_item_project_transition.v1") {
		t.Fatalf("recompute scope omits the kind: %#v", scope.RecordKinds)
	}
}
