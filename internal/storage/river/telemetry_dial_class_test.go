package riverstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-11608: the telemetry sanitizer drops the driver text but must keep a
// refused or pooler-refused dial transient. Before this, every dial error
// became the bare ErrQueueTelemetryUnavailable, execution_liveness read as a
// hard error, and every worker group exited on preclaim attempt 1.
func TestQueueTelemetryReadFailureKeepsOnlyTheDialClass(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err       error
		transient bool
	}{
		"econnrefused":   {&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, true},
		"econnreset":     {fmt.Errorf("read: %w", syscall.ECONNRESET), true},
		"eof":            {fmt.Errorf("receive message: %w", io.ErrUnexpectedEOF), true},
		"pooler_08P01":   {&pgconn.PgError{Code: "08P01", Message: "server login has been failing"}, true},
		"cannot_connect": {&pgconn.PgError{Code: "57P03"}, true},
		"wrong_password": {&pgconn.PgError{Code: "28P01"}, false},
		"permission":     {&pgconn.PgError{Code: "42501"}, false},
		"canceled":       {fmt.Errorf("acquire: %w", context.Canceled), false},
		"deadline":       {fmt.Errorf("acquire: %w", context.DeadlineExceeded), false},
		"other":          {errors.New("decode row args secret"), false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := queueTelemetryReadFailure(tc.err)
			if !errors.Is(got, ErrQueueTelemetryUnavailable) {
				t.Fatalf("queueTelemetryReadFailure = %v, want ErrQueueTelemetryUnavailable", got)
			}
			if postgres.IsTransientDialError(got) != tc.transient || errors.Is(got, postgres.ErrDialUnreachable) != tc.transient {
				t.Fatalf("queueTelemetryReadFailure(%v) = %v, transient = %v, want %v", tc.err, got, postgres.IsTransientDialError(got), tc.transient)
			}
			if strings.Contains(got.Error(), "secret") || strings.Contains(got.Error(), "server login") {
				t.Fatalf("queueTelemetryReadFailure leaked driver text: %v", got)
			}
		})
	}
}

func TestQueueTelemetrySnapshotKeepsARealRefusedDialTransient(t *testing.T) {
	t.Parallel()
	pooler := fakepg.NewPoolerRefusing(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	for name, addr := range map[string]string{
		"pooler_08P01": fmt.Sprintf("%s:%d", pooler.Host, pooler.Port),
		"econnrefused": closed,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if name == "pooler_08P01" {
				t.Cleanup(func() {
					if pooler.Connections() == 0 {
						t.Error("no dial reached the pooler: the 08P01 case measured nothing")
					}
				})
			}
			pool, err := pgxpool.New(context.Background(), "postgres://worker:pw@"+addr+"/db?sslmode=disable&connect_timeout=2")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pool.Close)
			sampler, err := NewQueueTelemetrySampler(pool, validQueueTelemetryConfig())
			if err != nil {
				t.Fatal(err)
			}
			_, err = sampler.Snapshot(context.Background())
			if !errors.Is(err, ErrQueueTelemetryUnavailable) || !errors.Is(err, postgres.ErrDialUnreachable) {
				t.Fatalf("Snapshot = %v, want ErrQueueTelemetryUnavailable wrapping postgres.ErrDialUnreachable", err)
			}
			if strings.Contains(err.Error(), addr) || strings.Contains(err.Error(), "server_login_retry") {
				t.Fatalf("Snapshot leaked driver text: %v", err)
			}
		})
	}
}
