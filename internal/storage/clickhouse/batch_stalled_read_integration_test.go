//go:build integration

package clickhouse

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// stallProxy forwards TCP both ways until stall; then it holds every byte the
// server sends until resume, so the client's next read waits on a stalled server.
type stallProxy struct {
	listener   net.Listener
	target     string
	stalled    atomic.Bool
	resume     chan struct{}
	resumeOnce sync.Once
	done       chan struct{}
	wg         sync.WaitGroup
}

func (p *stallProxy) unstall() { p.resumeOnce.Do(func() { close(p.resume) }) }

func startStallProxy(t *testing.T, target string) *stallProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &stallProxy{listener: listener, target: target, resume: make(chan struct{}), done: make(chan struct{})}
	p.wg.Add(1)
	go p.accept()
	t.Cleanup(func() {
		close(p.done)
		_ = listener.Close()
		p.wg.Wait()
	})
	return p
}

func (p *stallProxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.wg.Add(2)
		go func() {
			defer p.wg.Done()
			_, _ = io.Copy(server, client)
		}()
		go func() {
			defer p.wg.Done()
			p.serverToClient(server, client)
		}()
		go func() {
			<-p.done
			_ = client.Close()
			_ = server.Close()
		}()
	}
}

func (p *stallProxy) serverToClient(server, client net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := server.Read(buf)
		if n > 0 {
			if p.stalled.Load() {
				select {
				case <-p.resume:
				case <-p.done:
					return
				}
			}
			if _, werr := client.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func openThroughStallProxy(t *testing.T, ctx context.Context, readTimeout, batchBound time.Duration) (*stallProxy, driver.Conn) {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	dsn, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	proxy := startStallProxy(t, dsn.Host)
	dsn.Host = proxy.listener.Addr().String()
	config := DefaultConfig(dsn.String())
	config.MaxOpenConns, config.MaxIdleConns = 1, 1
	config.ReadTimeout = readTimeout
	config.BatchWriteBound = batchBound
	conn, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, `CREATE TABLE batch_stalled_read (x UInt64) ENGINE = Memory`); err != nil {
		t.Fatal(err)
	}
	return proxy, conn
}

func prepareOneRow(t *testing.T, ctx context.Context, conn driver.Conn) driver.Batch {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO batch_stalled_read (x)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(uint64(1)); err != nil {
		t.Fatal(err)
	}
	return batch
}

// CHAOS-14054: a caller cancel does not abort an in-flight batch write: the
// server stalls its answer, the caller cancels, the server answers, the row lands.
func TestACallerCancelDoesNotAbortAnInFlightBatchWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	proxy, conn := openThroughStallProxy(t, ctx, time.Minute, time.Minute)
	callerCtx, cancelCaller := context.WithCancel(ctx)
	defer cancelCaller()
	batch := prepareOneRow(t, callerCtx, conn)
	proxy.stalled.Store(true)
	sent := make(chan error, 1)
	go func() { sent <- batch.Send() }()
	time.Sleep(200 * time.Millisecond)
	cancelCaller()
	time.Sleep(200 * time.Millisecond)
	proxy.unstall()
	if err := <-sent; err != nil {
		t.Fatalf("the caller's cancel aborted the in-flight write: %v", err)
	}
	var rows uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM batch_stalled_read`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("rows = %d (%v); want the one row the write sent", rows, err)
	}
}

// CHAOS-14054 bound: with the caller cancel detached, the batch's own bound ends
// a write the server stalls, before the (longer) ReadTimeout: ErrBatchWriteBound
// wrapping the driver's deadline error. Asserted by error, not by elapsed time.
func TestAStalledBatchWriteEndsWithErrBatchWriteBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	proxy, conn := openThroughStallProxy(t, ctx, time.Minute, 2*time.Second)
	batch := prepareOneRow(t, ctx, conn)
	proxy.stalled.Store(true)
	err := batch.Send()
	if !errors.Is(err, ErrBatchWriteBound) {
		t.Fatalf("Send to a stalled server = %v; want ErrBatchWriteBound", err)
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send to a stalled server = %v; want the driver's deadline error under ErrBatchWriteBound", err)
	}
	t.Logf("stalled write ended with: %v", err)
}
