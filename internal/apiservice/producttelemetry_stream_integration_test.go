//go:build integration

package apiservice

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/producttelemetry"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The product-telemetry stream is opened on first use. A failed open must
// not stick: the Python api's Redis client reconnects per command, so the
// next batch writes as soon as Valkey answers again. Two failure causes are
// checked: the first request's context is cancelled, and Valkey is not
// reachable at first.
func TestProductTelemetryStreamRecoversAfterAFailedFirstOpen(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatalf("start valkey: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	fields := [][2]string{{"ingestion_id", "i"}, {"events", "[]"}}

	t.Run("cancelled first request", func(t *testing.T) {
		streams := &producttelemetry.ValkeyStreams{URI: instance.URI}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if err := streams.Append(cancelled, "product-telemetry:a:events", fields); err == nil {
			t.Fatal("append under a cancelled context succeeded")
		}
		if err := streams.Append(ctx, "product-telemetry:a:events", fields); err != nil {
			t.Fatalf("append after a cancelled first request: %v", err)
		}
	})

	t.Run("valkey unreachable at first", func(t *testing.T) {
		target := strings.TrimPrefix(instance.URI, "redis://")
		target = target[:strings.Index(target, "/")]
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		_ = listener.Close() // nothing listens: the first open is refused
		streams := &producttelemetry.ValkeyStreams{URI: "redis://" + address + "/1"}
		if err := streams.Append(ctx, "product-telemetry:b:events", fields); err == nil {
			t.Fatal("append with Valkey unreachable succeeded")
		}
		proxy, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("re-listen %s: %v", address, err)
		}
		t.Cleanup(func() { _ = proxy.Close() })
		go forward(proxy, target)
		if err := streams.Append(ctx, "product-telemetry:b:events", fields); err != nil {
			t.Fatalf("append after Valkey came back: %v", err)
		}
	})
}

func forward(listener net.Listener, target string) {
	for {
		client, err := listener.Accept()
		if err != nil {
			return
		}
		go func() {
			defer client.Close()
			server, err := net.Dial("tcp", target)
			if err != nil {
				return
			}
			defer server.Close()
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); _, _ = io.Copy(server, client); server.(*net.TCPConn).CloseWrite() }()
			go func() { defer wg.Done(); _, _ = io.Copy(client, server); client.(*net.TCPConn).CloseWrite() }()
			wg.Wait()
		}()
	}
}
