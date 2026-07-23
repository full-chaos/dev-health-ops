package streamhandlers

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
	"github.com/google/uuid"
)

type externalRepositoryFake struct {
	batch       externalBatch
	loadErr     error
	allowed     bool
	allowedErr  error
	completions []externalCompletion
	failed      []string
	completeErr error
	failErr     error
}

func (f *externalRepositoryFake) LoadForProcessing(context.Context, externalPointer) (externalBatch, error) {
	return f.batch, f.loadErr
}

func (f *externalRepositoryFake) OperationalAllowed(context.Context, string) (bool, error) {
	return f.allowed, f.allowedErr
}

func (f *externalRepositoryFake) Complete(_ context.Context, _ externalBatch, completion externalCompletion) error {
	f.completions = append(f.completions, completion)
	return f.completeErr
}

func (f *externalRepositoryFake) Fail(_ context.Context, _ externalPointer, reason string) error {
	f.failed = append(f.failed, reason)
	return f.failErr
}

type externalSinkFake struct {
	calls  []externalSinkBatch
	errors []error
	scope  ExternalRecomputeScope
}

func (f *externalSinkFake) Write(_ context.Context, batch externalSinkBatch) (ExternalRecomputeScope, error) {
	f.calls = append(f.calls, batch)
	if len(f.errors) > 0 {
		err := f.errors[0]
		f.errors = f.errors[1:]
		return ExternalRecomputeScope{}, err
	}
	return f.scope, nil
}

type externalSchedulerFake struct{ scopes []ExternalRecomputeScope }

func (f *externalSchedulerFake) Schedule(_ context.Context, scope ExternalRecomputeScope) error {
	f.scopes = append(f.scopes, scope)
	return nil
}

func externalTestPointer() externalPointer {
	return externalPointer{
		IngestionID:  uuid.MustParse("2dc94e6c-b35d-4b0f-839d-20720d48d7fa"),
		OrgID:        uuid.MustParse("2b237281-6b27-4b46-8b23-14f14f2cf429").String(),
		SourceSystem: "github", SourceInstance: "full-chaos/dev-health",
		SchemaVersion: externalSchemaVersion,
	}
}

func externalTestMessage(pointer externalPointer) streamrunner.Message {
	return streamrunner.Message{
		Stream: "external-ingest:" + pointer.OrgID + ":batches", ID: "1-0",
		Fields: map[string]string{
			"ingestion_id": pointer.IngestionID.String(),
			"org_id":       pointer.OrgID, "source_system": pointer.SourceSystem,
			"source_instance": pointer.SourceInstance, "schema_version": pointer.SchemaVersion,
		},
	}
}

func externalTestPayload(t *testing.T, pointer externalPointer, family string, records []map[string]any) []byte {
	t.Helper()
	value := map[string]any{
		"schemaVersion": externalSchemaVersion, "idempotencyKey": "customer-batch-1",
		"source": map[string]any{
			"type": "customer_push", "system": pointer.SourceSystem,
			"instance": pointer.SourceInstance, "entityFamily": family,
		},
		"window": map[string]any{
			"startedAt": "2026-07-20T00:00:00Z",
			"endedAt":   "2026-07-21T00:00:00Z",
		},
		"records": records,
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestExternalHandlerPersistsPartialOutcomeAndSchedulesAfterCommit(t *testing.T) {
	pointer := externalTestPointer()
	payload := externalTestPayload(t, pointer, "legacy", []map[string]any{
		{
			"kind": "repository.v1", "externalId": "repo-1",
			"payload": map[string]any{
				"externalId": pointer.SourceInstance, "sourceSystem": "github",
			},
		},
		{
			"kind": "repository.v1", "externalId": "repo-2",
			"payload": map[string]any{"externalId": pointer.SourceInstance},
		},
	})
	repository := &externalRepositoryFake{
		allowed: true,
		batch: externalBatch{
			Pointer: pointer, SourceID: uuid.New(), EntityFamily: "legacy",
			ItemsReceived: 2, Payload: payload,
		},
	}
	scope := ExternalRecomputeScope{
		OrgID: pointer.OrgID, IngestionID: pointer.IngestionID,
		RepoIDs: []string{"repo-id"}, RecordKinds: []string{"repository.v1"},
	}
	sink := &externalSinkFake{scope: scope}
	scheduler := &externalSchedulerFake{}
	handler, err := NewExternalIngestHandler(repository, sink, scheduler)
	if err != nil {
		t.Fatal(err)
	}
	handler.backoff = nil
	if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err != nil {
		t.Fatal(err)
	}
	if len(sink.calls) != 1 || len(sink.calls[0].Records) != 1 {
		t.Fatalf("accepted sink records = %#v", sink.calls)
	}
	if len(repository.completions) != 1 {
		t.Fatalf("completion count = %d", len(repository.completions))
	}
	completion := repository.completions[0]
	if completion.Accepted != 1 || completion.Rejected != 1 ||
		completion.RecordCounts["repository.v1"] != 1 ||
		completion.Rejections[0].Index != 1 {
		t.Fatalf("partial completion = %#v", completion)
	}
	if !reflect.DeepEqual(scheduler.scopes, []ExternalRecomputeScope{scope}) {
		t.Fatalf("scheduled scopes = %#v", scheduler.scopes)
	}
}

func TestExternalHandlerRetriesTransientSinkAndLeavesExhaustionNonTerminal(t *testing.T) {
	pointer := externalTestPointer()
	payload := externalTestPayload(t, pointer, "legacy", []map[string]any{{
		"kind": "repository.v1", "externalId": "repo-1",
		"payload": map[string]any{
			"externalId": pointer.SourceInstance, "sourceSystem": "github",
		},
	}})
	newRepository := func() *externalRepositoryFake {
		return &externalRepositoryFake{
			allowed: true,
			batch: externalBatch{
				Pointer: pointer, SourceID: uuid.New(), EntityFamily: "legacy",
				ItemsReceived: 1, Payload: payload,
			},
		}
	}
	t.Run("eventual success", func(t *testing.T) {
		repository := newRepository()
		sink := &externalSinkFake{
			errors: []error{errors.New("unavailable"), errors.New("unavailable")},
			scope:  ExternalRecomputeScope{OrgID: pointer.OrgID},
		}
		handler, err := NewExternalIngestHandler(repository, sink, nil)
		if err != nil {
			t.Fatal(err)
		}
		handler.backoff = []time.Duration{0, 0}
		if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err != nil {
			t.Fatal(err)
		}
		if len(sink.calls) != 3 || len(repository.completions) != 1 {
			t.Fatalf("retry outcome calls=%d completions=%d", len(sink.calls), len(repository.completions))
		}
	})
	t.Run("exhaustion", func(t *testing.T) {
		repository := newRepository()
		sink := &externalSinkFake{errors: []error{
			errors.New("unavailable"), errors.New("unavailable"), errors.New("unavailable"),
		}}
		handler, err := NewExternalIngestHandler(repository, sink, nil)
		if err != nil {
			t.Fatal(err)
		}
		handler.backoff = []time.Duration{0, 0}
		if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err == nil || streamrunner.IsPermanent(err) {
			t.Fatalf("sink exhaustion classification = %v", err)
		}
		if len(repository.completions) != 0 {
			t.Fatal("transient sink exhaustion finalized the batch")
		}
	})
}

func TestExternalHandlerSkipsTerminalAndFailsClosedOperationalEntitlement(t *testing.T) {
	pointer := externalTestPointer()
	t.Run("terminal skip", func(t *testing.T) {
		repository := &externalRepositoryFake{batch: externalBatch{Skip: true}}
		sink := &externalSinkFake{}
		handler, err := NewExternalIngestHandler(repository, sink, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err != nil {
			t.Fatal(err)
		}
		if len(sink.calls) != 0 || len(repository.completions) != 0 {
			t.Fatal("terminal batch was processed")
		}
	})
	t.Run("operational entitlement", func(t *testing.T) {
		payload := externalTestPayload(t, pointer, "operational", []map[string]any{{
			"kind": "operational_service.v1", "externalId": "service-1",
			"payload": map[string]any{
				"externalId": "service-1", "sourceVersionAt": "2026-07-20T00:00:00Z",
				"name": "Service",
			},
		}})
		repository := &externalRepositoryFake{
			allowed: false,
			batch: externalBatch{
				Pointer: pointer, SourceID: uuid.New(), EntityFamily: "operational",
				ItemsReceived: 1, Payload: payload,
			},
		}
		handler, err := NewExternalIngestHandler(repository, &externalSinkFake{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := handler.Handle(context.Background(), externalTestMessage(pointer)); !streamrunner.IsPermanent(err) {
			t.Fatalf("disabled operational feature classification = %v", err)
		}
	})
}

func TestExternalPermanentFinalizerMarksAddressableBatchOnly(t *testing.T) {
	pointer := externalTestPointer()
	repository := &externalRepositoryFake{}
	handler, err := NewExternalIngestHandler(repository, &externalSinkFake{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.FinalizePermanent(context.Background(), externalTestMessage(pointer), "schema_invalid"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(repository.failed, []string{"schema_invalid"}) {
		t.Fatalf("failed statuses = %v", repository.failed)
	}
	message := externalTestMessage(pointer)
	message.Fields["ingestion_id"] = "not-a-uuid"
	if err := handler.FinalizePermanent(context.Background(), message, "bad_uuid"); err != nil {
		t.Fatal(err)
	}
	if len(repository.failed) != 1 {
		t.Fatal("malformed unaddressable ID attempted status finalization")
	}
}
