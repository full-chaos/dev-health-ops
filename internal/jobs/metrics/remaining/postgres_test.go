package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestStartRunWrapsUnavailableWithOperationAndUnderlyingError is CHAOS's
// regression for the boot-storm diagnosis gap: "fixed schedule failed /
// work_item_attribution_daily_fanout / remaining metrics durable state is
// unavailable" named neither the failing step nor the pgx error underneath,
// so diagnosing it needed ~15 minutes of grant/SQL replay on a live host
// instead of reading the log line. No live Postgres is needed to trigger
// this: a pool closed before use fails Begin() the same way a connection
// timeout under WORKER_COORDINATOR_DATABASE_MAX_CONNS contention would --
// synchronously, with no network round trip.
func TestStartRunWrapsUnavailableWithOperationAndUnderlyingError(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://user:pass@127.0.0.1:5432/db")
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	pool.Close()

	_, wantErr := pool.Begin(ctx)
	if wantErr == nil {
		t.Fatal("Begin() on a closed pool unexpectedly succeeded")
	}

	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatalf("NewPostgresStore() error = %v", err)
	}
	seed := int64(1)
	_, err = store.StartRun(ctx, StartRunRequest{
		OrganizationID: "00000000-0000-4000-8000-000000000001",
		Family:         "capacity",
		Generation:     "capacity-v1",
		ScopeKey:       "all-teams",
		GenerationSeed: &seed,
		Scopes:         []json.RawMessage{json.RawMessage(`{}`)},
	})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("StartRun() error = %v, want errors.Is(err, ErrUnavailable)", err)
	}
	if !strings.Contains(err.Error(), "begin run tx") {
		t.Fatalf("StartRun() error = %v, want it to name the failing operation (\"begin run tx\")", err)
	}
	if !strings.Contains(err.Error(), wantErr.Error()) {
		t.Fatalf("StartRun() error = %v, want it to carry the underlying pgx error (%v)", err, wantErr)
	}
}

func TestDeterministicRunIDUnambiguouslyEncodesGenerationAndScope(t *testing.T) {
	base := StartRunRequest{
		OrganizationID: "00000000-0000-4000-8000-000000000119",
		Family:         "capacity",
	}
	left := base
	left.Generation = "a/b"
	left.ScopeKey = "c"
	right := base
	right.Generation = "a"
	right.ScopeKey = "b/c"
	if deterministicRunID(left) == deterministicRunID(right) {
		t.Fatalf("distinct generation/scope tuples collided: left=%#v right=%#v", left, right)
	}
}
