package jobruntime

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestWorkerPresenceStartFailureWithoutAnAnswerMarksTheIdentityMaybeRegistered(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot reserve a closed loopback port: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@"+addr+"/d?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	presence, err := NewWorkerPresence(pool, "tenant-worker-a", []string{"webhooks"}, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := presence.Start(ctx); err == nil {
		t.Fatal("Start() = nil against a closed port")
	}
	if !presence.maybeRegistered {
		t.Fatal("a failed INSERT must mark the identity maybe-registered so a retry accepts its own row")
	}
}
