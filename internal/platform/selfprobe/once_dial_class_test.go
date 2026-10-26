package selfprobe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-11608: Once must keep a refused or pooler-refused dial classifiable as
// transient, without the driver text, while a wrong password and a cancel stay
// hard failures. Every case runs the real driver against a real endpoint.

func dialClassPool(t *testing.T, addr string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://worker:pw@"+addr+"/db?sslmode=disable&connect_timeout=2")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func dialClassClosedPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func TestOnceKeepsARefusedDialTransientWithoutDriverText(t *testing.T) {
	t.Parallel()
	pooler := fakepg.NewPoolerRefusing(t)
	for name, addr := range map[string]string{
		"pooler_08P01": fmt.Sprintf("%s:%d", pooler.Host, pooler.Port),
		"econnrefused": dialClassClosedPort(t),
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
			err := Once(context.Background(), NewPool(dialClassPool(t, addr)))
			if !errors.Is(err, postgres.ErrDialUnreachable) || !postgres.IsTransientDialError(err) {
				t.Fatalf("Once = %v, want a transient error wrapping postgres.ErrDialUnreachable", err)
			}
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Once = %v, a refused dial is not a timeout", err)
			}
			for _, leaked := range []string{addr, "server_login_retry", "pw", "worker"} {
				if strings.Contains(err.Error(), leaked) {
					t.Fatalf("Once = %q, carries %q", err.Error(), leaked)
				}
			}
		})
	}
}

func TestOnceKeepsAWrongPasswordAndACancelNonTransient(t *testing.T) {
	t.Parallel()
	server := fakepg.New(t)
	refusing := fmt.Sprintf("%s:%d", server.Host, server.Port)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for name, tc := range map[string]struct {
		ctx  context.Context
		addr string
	}{
		"wrong_password_28P01": {context.Background(), refusing},
		"canceled":             {canceled, dialClassClosedPort(t)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := Once(tc.ctx, NewPool(dialClassPool(t, tc.addr)))
			if err == nil {
				t.Fatal("Once = nil, want a failure")
			}
			if postgres.IsTransientDialError(err) || errors.Is(err, postgres.ErrDialUnreachable) {
				t.Fatalf("Once = %v, classified transient", err)
			}
		})
	}
}
