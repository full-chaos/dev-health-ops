package remaining

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type releaseImpactFanoutConn struct {
	driver.Conn
	pairs    int
	failRef  string
	inFlight atomic.Int32
	maxSeen  atomic.Int32
	mu       sync.Mutex
	written  []string
}

func (conn *releaseImpactFanoutConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return &releaseImpactFanoutRows{count: conn.pairs}, nil
}

func (conn *releaseImpactFanoutConn) QueryRow(_ context.Context, _ string, args ...any) driver.Row {
	ref := ""
	for _, arg := range args {
		if named, ok := arg.(driver.NamedValue); ok && named.Name == "release_ref" {
			ref, _ = named.Value.(string)
		}
	}
	return &releaseImpactFanoutRow{conn: conn, ref: ref}
}

func (conn *releaseImpactFanoutConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return &releaseImpactFanoutBatch{conn: conn}, nil
}

type releaseImpactFanoutRow struct {
	conn *releaseImpactFanoutConn
	ref  string
}

func (row *releaseImpactFanoutRow) Err() error                { return nil }
func (row *releaseImpactFanoutRow) ScanStruct(dest any) error { return nil }
func (row *releaseImpactFanoutRow) Scan(dest ...any) error {
	now := row.conn.inFlight.Add(1)
	for {
		seen := row.conn.maxSeen.Load()
		if now <= seen || row.conn.maxSeen.CompareAndSwap(seen, now) {
			break
		}
	}
	time.Sleep(5 * time.Millisecond)
	row.conn.inFlight.Add(-1)
	if row.ref == "" {
		if len(dest) == 1 {
			if count, ok := dest[0].(*uint64); ok {
				*count = 20
			}
		}
		return nil
	}
	if row.ref == row.conn.failRef {
		return errors.New("read failed")
	}
	if text, ok := dest[0].(*string); ok {
		*text = row.ref
		return nil
	}
	if len(dest) == 1 {
		if _, ok := dest[0].(**time.Time); ok {
			return sql.ErrNoRows
		}
		reflect.ValueOf(dest[0]).Elem().SetZero()
	}
	return nil
}

type releaseImpactFanoutRows struct {
	driver.Rows
	count int
	at    int
}

func (rows *releaseImpactFanoutRows) Next() bool { rows.at++; return rows.at <= rows.count }
func (rows *releaseImpactFanoutRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = fmt.Sprintf("rel-%03d", rows.at-1)
	*(dest[1].(*string)) = "prod"
	return nil
}
func (rows *releaseImpactFanoutRows) Close() error { return nil }
func (rows *releaseImpactFanoutRows) Err() error   { return nil }

type releaseImpactFanoutBatch struct {
	driver.Batch
	conn *releaseImpactFanoutConn
}

func (batch *releaseImpactFanoutBatch) Append(values ...any) error {
	batch.conn.mu.Lock()
	defer batch.conn.mu.Unlock()
	batch.conn.written = append(batch.conn.written, values[2].(string))
	return nil
}
func (batch *releaseImpactFanoutBatch) Send() error  { return nil }
func (batch *releaseImpactFanoutBatch) Abort() error { return nil }

func newFanoutExecutor(conn *releaseImpactFanoutConn) *ReleaseImpactExecutor {
	logger := slog.New(slog.DiscardHandler)
	return &ReleaseImpactExecutor{
		reader: NewReleaseImpactReader(conn, logger),
		logger: logger,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}
}

func TestReleaseImpactComputeOneDayRunsPairsConcurrentlyInOrder(t *testing.T) {
	conn := &releaseImpactFanoutConn{pairs: 24}
	executor := newFanoutExecutor(conn)
	written, err := executor.computeOneDay(context.Background(), "org", time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if written != 24 || len(conn.written) != 24 {
		t.Fatalf("written=%d rows=%d want 24", written, len(conn.written))
	}
	for index, ref := range conn.written {
		if want := fmt.Sprintf("rel-%03d", index); ref != want {
			t.Fatalf("row %d is %s want %s: pair order lost", index, ref, want)
		}
	}
	if got := conn.maxSeen.Load(); got < 2 || got > releaseImpactPairConcurrency {
		t.Fatalf("max in-flight reads=%d want 2..%d", got, releaseImpactPairConcurrency)
	}
}

func TestReleaseImpactComputeOneDayFailsAndWritesNothingOnAPairError(t *testing.T) {
	conn := &releaseImpactFanoutConn{pairs: 24, failRef: "rel-011"}
	executor := newFanoutExecutor(conn)
	if _, err := executor.computeOneDay(context.Background(), "org", time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Now()); err == nil {
		t.Fatal("pair error swallowed")
	}
	if len(conn.written) != 0 {
		t.Fatalf("rows written after a failed pair: %d", len(conn.written))
	}
}
