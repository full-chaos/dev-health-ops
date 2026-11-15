package jobruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/refusedaddr"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestWorkerPresenceStartFailureWithoutAnAnswerMarksTheIdentityMaybeRegistered(t *testing.T) {
	addr := refusedaddr.Addr(t)
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
