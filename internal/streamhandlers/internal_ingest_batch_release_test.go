package streamhandlers

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

// Round 1 of the decoder fix found the same leak in the sibling consumer that shares the
// ClickHouse pool: internal ingest opened a batch, then refused an invalid commit (or failed
// an append/send) without aborting it, so four such entries exhausted the default pool and
// stalled every valid entry, telemetry included. Every exit after PrepareBatch that does not
// send must abort, for every entity.
func TestInternalIngestReleasesItsBatchOnEveryUnsentExit(t *testing.T) {
	validCommit := `{"hash":"h","message":"m","author_name":"n","author_email":"e@x","author_when":"2026-01-01T00:00:00Z"}`
	env := func(entity, items string) (string, string) {
		return "ingest:org:" + entity, `{"org_id":"org","repo_url":"https://example.test/r","items":[` + items + `]}`
	}
	cases := map[string]struct {
		entity, items string
		sendErr       error
		wantPermanent bool
		wantAbort     bool
	}{
		"invalid commit after a valid one": {"commits", validCommit + `,{"hash":""}`, nil, true, true},
		"invalid commit first":             {"commits", `{"hash":""}`, nil, true, true},
		"commit send fails":                {"commits", validCommit, errors.New("boom"), false, true},
		"commit sent":                      {"commits", validCommit, nil, false, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			batch := &productBatch{sendErr: tc.sendErr}
			handler, _ := NewInternalIngestHandler(&productSink{batch: batch})
			stream, payload := env(tc.entity, tc.items)
			err := handler.Handle(context.Background(), streamrunner.Message{Stream: stream, Fields: map[string]string{"payload": payload}})
			if tc.wantPermanent != streamrunner.IsPermanent(err) {
				t.Fatalf("err = %v, permanent want %v", err, tc.wantPermanent)
			}
			if (batch.aborts > 0) != tc.wantAbort {
				t.Fatalf("aborts = %d, want abort=%v (err=%v)", batch.aborts, tc.wantAbort, err)
			}
		})
	}
}

// The external batch sink is the third batch producer on the stream runner's ClickHouse
// connection: its one PrepareBatch site serves every kind, so one kind exercises it.
func TestExternalSinkReleasesItsBatchOnAppendAndSendFailure(t *testing.T) {
	for name, batch := range map[string]*productBatch{
		"append fails": {appendErr: errors.New("boom")},
		"send fails":   {sendErr: errors.New("boom")},
	} {
		t.Run(name, func(t *testing.T) {
			pointer := externalTestPointer()
			sink, err := NewClickHouseExternalBatchSink(&productSink{batch: batch})
			if err != nil {
				t.Fatal(err)
			}
			source := externalSinkBatch{Pointer: pointer, SourceID: uuid.New(), Records: []externalSinkRecord{
				externalSinkFixture("repository.v1", map[string]any{"externalId": pointer.SourceInstance, "sourceSystem": "github"}),
			}}
			if _, err := sink.Write(context.Background(), source); err == nil {
				t.Fatal("Write succeeded, want an error")
			}
			if batch.aborts != 1 {
				t.Fatalf("aborts = %d, want 1", batch.aborts)
			}
		})
	}
}

// Every internal-ingest batch aborts when its append fails. failAt is the 0-based position,
// among the batches one Handle opens, of the batch whose Append fails: earlier batches are
// sent, so each site (PR rows, reviews, incident service, mapping and incident batches, work
// items via appendAndSend) is reached and pinned by itself.
func TestInternalIngestReleasesItsBatchWhenAnAppendFails(t *testing.T) {
	const pr = `{"org_id":"org","repo_url":"https://example.test/r","items":[{"number":7,"title":"Ship it","state":"open","author_name":"Ada","created_at":"2026-07-23T12:00:00Z","reviews":[{"review_id":"review-1","reviewer":"Grace","state":"APPROVED","submitted_at":"2026-07-23T13:00:00Z"}]}]}`
	const inc = `{"org_id":"org","repo_url":"https://example.test/r","items":[{"incident_id":"inc-1","status":"resolved","started_at":"2026-07-23T12:00:00Z","resolved_at":"2026-07-23T13:00:00Z"}]}`
	const wi = `{"org_id":"org","items":[{"work_item_id":"jira:ABC-1","provider":"jira","title":"Fix it","type":"bug","status":"todo","created_at":"2026-07-23T12:00:00Z"}]}`
	cases := []struct {
		name, entity, payload string
		failAt                int
	}{
		{"pull request rows", "pull-requests", pr, 0},
		{"pull request reviews", "pull-requests", pr, 1},
		{"incident service", "incidents", inc, 0},
		{"incident mapping", "incidents", inc, 1},
		{"incident rows", "incidents", inc, 2},
		{"work items", "work-items", wi, 0},
		{"commits", "commits", `{"org_id":"org","repo_url":"https://example.test/r","items":[{"hash":"h","message":"m","author_name":"n","author_email":"e@x","author_when":"2026-01-01T00:00:00Z"}]}`, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			batches := make([]*productBatch, tc.failAt+1)
			sink := &productSink{}
			for i := range batches {
				batches[i] = &productBatch{}
				if i == tc.failAt {
					batches[i].appendErr = errors.New("boom")
				}
				sink.batches = append(sink.batches, batches[i])
			}
			handler, _ := NewInternalIngestHandler(sink)
			err := handler.Handle(context.Background(), streamrunner.Message{Stream: "ingest:org:" + tc.entity, Fields: map[string]string{"payload": tc.payload}})
			if err == nil {
				t.Fatal("Handle succeeded, want the append error")
			}
			if got := batches[tc.failAt].aborts; got != 1 {
				t.Fatalf("aborts of the failing batch = %d, want 1 (err=%v)", got, err)
			}
		})
	}
}
