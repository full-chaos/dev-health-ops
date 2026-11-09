package streamhandlers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
	"github.com/full-chaos/dev-health-ops/internal/teamcreated"
	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
	"github.com/full-chaos/dev-health-ops/internal/teamprovider/teamprovidertest"
)

type productSink struct {
	carryCountCalls int
	query           string
	queries         []string
	batch           *productBatch
	batches         []*productBatch
	err             error

	// queryRows/queryErr configure Query -- used by
	// ClickHouseExternalBatchSink's team.v1 manual_members preserve-lookup
	// (CHAOS-4321, team-lead ruling 2026-08-26). Unset (the default for
	// every test that doesn't care) means "no existing row for any team",
	// which is exactly the pre-fix behavior every existing test already
	// expects.
	queryRows     [][]any
	queryErr      error
	queryCalls    int
	lastQuery     string
	lastQueryArgs []any
	// teamRowIDs are the ids the read of "which of these team ids have a team
	// row" returns (the count of pushed identity team ids with no team row).
	teamRowIDs   []string
	teamRowErr   error
	teamRowCalls int
	// createdRows are the (id, created_at) rows the teamcreated read returns.
	createdRows  [][]any
	createdErr   error
	createdCalls int
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

// Exec stands in for the team.v1 write (teamprovider.Insert): it takes the
// next batch as PrepareBatch does and appends each row's stated values.
func (s *productSink) Exec(ctx context.Context, query string, args ...any) error {
	if !teamprovidertest.IsInsert(query) {
		return fmt.Errorf("productSink: unexpected Exec %q", query)
	}
	batch, err := s.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	_, writes, err := teamprovidertest.Decode(query, args)
	if err != nil {
		return err
	}
	for _, write := range writes {
		if err := batch.Append(write.Values...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (s *productSink) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	// The team id carry's count read (providersync.CarryTeamIDs) runs before
	// every team.v1 and identity.v1 write; this store holds no bare id.
	if strings.Contains(query, "argMax(is_active, (updated_at, last_synced))") {
		s.carryCountCalls++
		return &productRows{rows: [][]any{{uint64(0), uint64(0), uint64(0)}}}, nil
	}
	if query == externalTeamsWithARowQuery {
		s.teamRowCalls++
		if s.teamRowErr != nil {
			return nil, s.teamRowErr
		}
		rows := make([][]any, len(s.teamRowIDs))
		for index, id := range s.teamRowIDs {
			rows[index] = []any{id}
		}
		return &productRows{rows: rows}, nil
	}
	if query == teamprovider.Query {
		return &productRows{}, nil // no stored provider identity
	}
	if query == teamcreated.Query {
		s.createdCalls++
		if s.createdErr != nil {
			return nil, s.createdErr
		}
		return &productRows{rows: s.createdRows}, nil
	}
	s.queryCalls++
	s.lastQuery = query
	s.lastQueryArgs = args
	if s.queryErr != nil {
		return nil, s.queryErr
	}
	return &productRows{rows: s.queryRows}, nil
}

// productRows is a minimal driver.Rows double: only Next/Scan/Close/Err are
// ever exercised by preserveExistingManualMembers's read loop.
type productRows struct {
	rows []([]any)
	idx  int
}

func (r *productRows) Next() bool { return r.idx < len(r.rows) }
func (r *productRows) Scan(dest ...any) error {
	row := r.rows[r.idx]
	r.idx++
	for i, d := range dest {
		switch target := d.(type) {
		case *string:
			*target, _ = row[i].(string)
		case *[]string:
			*target, _ = row[i].([]string)
		case *time.Time:
			*target, _ = row[i].(time.Time)
		case *uint64:
			*target, _ = row[i].(uint64)
		default:
			return fmt.Errorf("productRows: unsupported scan dest %T", d)
		}
	}
	return nil
}
func (r *productRows) ScanStruct(any) error             { return errors.New("unused") }
func (r *productRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *productRows) Totals(...any) error              { return errors.New("unused") }
func (r *productRows) Columns() []string                { return nil }
func (r *productRows) Close() error                     { return nil }
func (r *productRows) Err() error                       { return nil }
func (r *productRows) HasData() bool                    { return len(r.rows) > 0 }

type productBatch struct {
	rows      [][]any
	sent      bool
	sendErr   error
	aborts    int
	appendErr error
}

func (b *productBatch) Abort() error { b.aborts++; return nil }
func (b *productBatch) Append(v ...any) error {
	if b.appendErr != nil {
		return b.appendErr
	}
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

// The web client's Ask Dev interaction event is stored by the consumer, as the intake accepts it.
func TestProductTelemetryStoresTheAskDevInteractionEvent(t *testing.T) {
	sink := &productSink{batch: &productBatch{}}
	handler, err := NewProductTelemetryHandler(sink)
	if err != nil {
		t.Fatal(err)
	}
	err = handler.Handle(context.Background(), streamrunner.Message{Fields: map[string]string{"events": `[{"name":"ask_dev_interacted","schemaVersion":"v1","eventId":"event-1","ts":"2026-07-23T12:00:00Z","sessionId":"session-1","anonymousUserId":"anon-1","payload":{"action":"terminal","surface":"panel","terminal":"limited","turnCount":3}}]`}})
	if err != nil {
		t.Fatalf("handle = %v", err)
	}
	if !sink.batch.sent || len(sink.batch.rows) != 1 {
		t.Fatalf("durable product sink = %#v", sink.batch)
	}
}
