//go:build integration

package clickhouse

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// lateWatchdogContext holds clickhouse-go's batch-Send watchdog goroutine at its
// ctx.Done() call until release, as a starved runner schedules it late.
type lateWatchdogContext struct {
	context.Context
	gate chan struct{}
	once sync.Once
}

func (c *lateWatchdogContext) release() { c.once.Do(func() { close(c.gate) }) }

func (c *lateWatchdogContext) Done() <-chan struct{} {
	if calledFromClickHouseWatchdog() {
		<-c.gate
	}
	return c.Context.Done()
}

func calledFromClickHouseWatchdog() bool {
	pcs := make([]uintptr, 32)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "clickhouse-go/v2.contextWatchdog") {
			return true
		}
		if !more {
			return false
		}
	}
}

// CHAOS-14054: a batch whose context the caller cancels right after Send (a
// write under its own WithTimeout, then the next write) must not lose the pooled
// connection the next batch holds. On clickhouse-go v2.49 a late watchdog closed
// it ("use of closed network connection"); detachedBatchConn gives the driver a
// context its watchdog never fires on.
func TestABatchContextCanceledAfterSendLeavesThePooledConnectionOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	config := DefaultConfig(instance.URI)
	config.MaxOpenConns, config.MaxIdleConns = 1, 1
	conn, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, `CREATE TABLE batch_watchdog (x UInt64) ENGINE = Memory`); err != nil {
		t.Fatal(err)
	}
	insert := func(ctx context.Context, x uint64) error {
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO batch_watchdog (x)`)
		if err != nil {
			return err
		}
		if err := batch.Append(x); err != nil {
			return err
		}
		return batch.Send()
	}

	// Each round loses the connection with probability 1/2 on a driver with the
	// race (the watchdog's select sees exit and Done ready together).
	const rounds = 16
	for round := 0; round < rounds; round++ {
		parent, cancelWrite := context.WithCancel(ctx)
		writeCtx := &lateWatchdogContext{Context: parent, gate: make(chan struct{})}
		// A driver whose Send waits for its watchdog would wait on the gate: open it
		// after a bound and before the cancel, so that Send ends uncancelled.
		timer := time.AfterFunc(200*time.Millisecond, writeCtx.release)
		if err := insert(writeCtx, 1); err != nil {
			t.Fatalf("round %d: first write: %v", round, err)
		}
		cancelWrite()
		next, err := conn.PrepareBatch(ctx, `INSERT INTO batch_watchdog (x)`)
		if err != nil {
			t.Fatalf("round %d: prepare the next write: %v", round, err)
		}
		if err := next.Append(uint64(2)); err != nil {
			t.Fatalf("round %d: append the next write: %v", round, err)
		}
		writeCtx.release()
		timer.Stop()
		time.Sleep(20 * time.Millisecond)
		if err := next.Send(); err != nil {
			t.Fatalf("round %d: the next write lost its pooled connection: %v", round, err)
		}
	}
}
