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

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// stallProxy forwards TCP both ways until stall; then it holds every byte the
// server sends, so the client's next read waits on a server that stalled.
type stallProxy struct {
	listener net.Listener
	target   string
	stalled  atomic.Bool
	done     chan struct{}
	wg       sync.WaitGroup
}

func startStallProxy(t *testing.T, target string) *stallProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &stallProxy{listener: listener, target: target, done: make(chan struct{})}
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
				<-p.done
				return
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

// CHAOS-14054 bound: the driver's batch context is never cancelled, so a cancel
// does not end an in-flight Send. The driver's ReadTimeout still does: a server
// that stalls its answer to a batch ends Send with a timeout within ReadTimeout.
func TestAStalledServerReadStillEndsADetachedBatchSendWithinReadTimeout(t *testing.T) {
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
	dsn, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	proxy := startStallProxy(t, dsn.Host)
	dsn.Host = proxy.listener.Addr().String()
	const readTimeout = 3 * time.Second
	config := DefaultConfig(dsn.String())
	config.MaxOpenConns, config.MaxIdleConns = 1, 1
	config.ReadTimeout = readTimeout
	conn, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, `CREATE TABLE batch_stalled_read (x UInt64) ENGINE = Memory`); err != nil {
		t.Fatal(err)
	}

	callerCtx, cancelCaller := context.WithCancel(ctx)
	defer cancelCaller()
	batch, err := conn.PrepareBatch(callerCtx, `INSERT INTO batch_stalled_read (x)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(uint64(1)); err != nil {
		t.Fatal(err)
	}
	proxy.stalled.Store(true)
	cancelAt := time.AfterFunc(200*time.Millisecond, cancelCaller)
	defer cancelAt.Stop()
	start := time.Now()
	err = batch.Send()
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Send to a stalled server returned no error")
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Send error is not the read deadline: %v", err)
	}
	if elapsed < readTimeout-500*time.Millisecond || elapsed > readTimeout+5*time.Second {
		t.Fatalf("Send ended after %s; want about ReadTimeout %s (the cancel at 200ms does not end it)", elapsed, readTimeout)
	}
}
