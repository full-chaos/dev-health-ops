package streamhandlers

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

type productSink struct {
	query   string
	queries []string
	batch   *productBatch
	batches []*productBatch
	err     error
}

func (s *productSink) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	s.query = query
	s.queries = append(s.queries, query)
	if s.err != nil {
		return nil, s.err
	}
	if len(s.batches) > 0 {
		batch := s.batches[0]
		s.batches = s.batches[1:]
		return batch, nil
	}
	return s.batch, nil
}

type productBatch struct {
	rows    [][]any
	sent    bool
	sendErr error
}

func (b *productBatch) Abort() error { return nil }
func (b *productBatch) Append(v ...any) error {
	b.rows = append(b.rows, append([]any(nil), v...))
	return nil
}
func (b *productBatch) AppendStruct(any) error        { return errors.New("unused") }
func (b *productBatch) Column(int) driver.BatchColumn { return nil }
func (b *productBatch) Flush() error                  { return nil }
func (b *productBatch) Send() error                   { b.sent = true; return b.sendErr }
func (b *productBatch) IsSent() bool                  { return b.sent }
func (b *productBatch) Rows() int                     { return len(b.rows) }
func (b *productBatch) Columns() []column.Interface   { return nil }
func (b *productBatch) Close() error                  { return nil }

func TestProductTelemetryPersistsValidatedEventsThroughOneReusedSink(t *testing.T) {
	sink := &productSink{batch: &productBatch{}}
	handler, err := NewProductTelemetryHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Handle(context.Background(), streamrunner.Message{Fields: map[string]string{"events": `[{"name":"page_viewed","schemaVersion":"v1","eventId":"event-1","ts":"2026-07-23T12:00:00Z","sessionId":"session-1","anonymousUserId":"anon-1","payload":{"feature":"home"}}]`}})
	if err != nil {
		t.Fatal(err)
	}
	if !sink.batch.sent || len(sink.batch.rows) != 1 {
		t.Fatalf("durable product sink = %#v", sink.batch)
	}
	if len(sink.batch.rows[0]) != len(ProductTelemetryColumns) {
		t.Fatalf("columns = %d", len(sink.batch.rows[0]))
	}
	if got := sink.batch.rows[0][1]; got != "event-1" {
		t.Fatalf("event id = %v", got)
	}
}

func TestProductTelemetryRejectsPIIPayloadBeforeSinkWrite(t *testing.T) {
	sink := &productSink{batch: &productBatch{}}
	handler, err := NewProductTelemetryHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Handle(context.Background(), streamrunner.Message{Fields: map[string]string{"events": `[{"name":"page_viewed","schemaVersion":"v1","eventId":"event-1","ts":"2026-07-23T12:00:00Z","sessionId":"session-1","anonymousUserId":"anon-1","payload":{"email":"nope@example.test"}}]`}})
	if !streamrunner.IsPermanent(err) || len(sink.batch.rows) != 0 {
		t.Fatalf("PII result = %v rows=%v", err, sink.batch.rows)
	}
}

func TestInternalIngestCommitsUsesReplacingKeySinkBeforeAcknowledgement(t *testing.T) {
	sink := &productSink{batch: &productBatch{}}
	handler, err := NewInternalIngestHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Handle(context.Background(), streamrunner.Message{Stream: "ingest:org-1:commits", Fields: map[string]string{"payload": `{"org_id":"org-1","repo_url":"https://example.test/acme/repo","items":[{"hash":"abc","message":"message","author_name":"Ada","author_email":"ada@example.test","author_when":"2026-07-23T12:00:00Z"}]}`}})
	if err != nil {
		t.Fatal(err)
	}
	if !sink.batch.sent || len(sink.batch.rows) != 1 {
		t.Fatalf("internal sink = %#v", sink.batch)
	}
	if sink.batch.rows[0][2] != "abc" || sink.batch.rows[0][0] != "org-1" {
		t.Fatalf("commit identity = %v %v", sink.batch.rows[0][0], sink.batch.rows[0][2])
	}
}

func TestInternalIngestPersistsPullRequestReviewsAfterParentRows(t *testing.T) {
	prBatch, reviewBatch := &productBatch{}, &productBatch{}
	sink := &productSink{batches: []*productBatch{prBatch, reviewBatch}}
	handler, err := NewInternalIngestHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Handle(context.Background(), streamrunner.Message{
		Stream: "ingest:org-1:pull-requests",
		Fields: map[string]string{"payload": `{"org_id":"org-1","repo_url":"https://example.test/acme/repo","items":[{"number":7,"title":"Ship it","state":"open","author_name":"Ada","created_at":"2026-07-23T12:00:00Z","reviews":[{"review_id":"review-1","reviewer":"Grace","state":"APPROVED","submitted_at":"2026-07-23T13:00:00Z"}]}]}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !prBatch.sent || !reviewBatch.sent || len(prBatch.rows) != 1 || len(reviewBatch.rows) != 1 {
		t.Fatalf("PR/review batches = %#v %#v", prBatch, reviewBatch)
	}
	if !strings.Contains(sink.queries[0], "git_pull_requests") || !strings.Contains(sink.queries[1], "git_pull_request_reviews") {
		t.Fatalf("sink order = %v", sink.queries)
	}
	if got := reviewBatch.rows[0]; got[0] != "org-1" || got[2] != uint32(7) || got[3] != "review-1" {
		t.Fatalf("review identity = %v", got)
	}
}

func TestInternalIngestPersistsCanonicalIncidentGraph(t *testing.T) {
	serviceBatch, mappingBatch, incidentBatch := &productBatch{}, &productBatch{}, &productBatch{}
	sink := &productSink{batches: []*productBatch{serviceBatch, mappingBatch, incidentBatch}}
	handler, err := NewInternalIngestHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	handler.now = func() time.Time { return time.Date(2026, 7, 23, 14, 0, 0, 0, time.UTC) }
	err = handler.Handle(context.Background(), streamrunner.Message{
		Stream: "ingest:org-1:incidents",
		Fields: map[string]string{"payload": `{"org_id":"org-1","repo_url":"https://example.test/acme/repo","items":[{"incident_id":"inc-1","status":"resolved","started_at":"2026-07-23T12:00:00Z","resolved_at":"2026-07-23T13:00:00Z"}]}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, batch := range []*productBatch{serviceBatch, mappingBatch, incidentBatch} {
		if !batch.sent || len(batch.rows) != 1 {
			t.Fatalf("canonical batch %d = %#v", index, batch)
		}
	}
	if got := incidentBatch.rows[0]; got[0] != "org-1" || got[1] != "external" || got[3] != "issue" || got[4] != "inc-1" {
		t.Fatalf("incident provenance = %v", got[:6])
	}
	if got := incidentBatch.rows[0][10]; got != "54084280070b17bc196c07981e81eb8797d5161961923e95f0026cdd492feb46" {
		t.Fatalf("canonical incident id = %v", got)
	}
	if got := incidentBatch.rows[0][6].(*big.Int).String(); got != "32923962804988062841194168745065814" {
		t.Fatalf("canonical source revision = %s", got)
	}
	if got := incidentBatch.rows[0][20]; got != "resolved" {
		t.Fatalf("normalized status = %v", got)
	}
	if got := mappingBatch.rows[0]; got[23] != "native_repository_context" || got[24] != 1.0 {
		t.Fatalf("mapping provenance = %v %v", got[23], got[24])
	}
}

func TestInternalIngestPersistsWorkItemPriorityAndOrg(t *testing.T) {
	sink := &productSink{batch: &productBatch{}}
	handler, err := NewInternalIngestHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Handle(context.Background(), streamrunner.Message{
		Stream: "ingest:org-1:work-items",
		Fields: map[string]string{"payload": `{"org_id":"org-1","items":[{"work_item_id":"jira:ABC-1","provider":"jira","title":"Fix it","type":"bug","status":"todo","created_at":"2026-07-23T12:00:00Z","priority_raw":"P1"}]}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	row := sink.batch.rows[0]
	if row[0] != "org-1" || row[27] != "P1" {
		t.Fatalf("work item org/priority = %v %v", row[0], row[27])
	}
}
