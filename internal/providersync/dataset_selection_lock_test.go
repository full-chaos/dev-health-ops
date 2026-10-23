package providersync

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

type recordingExecer struct{ keys []string }

func (r *recordingExecer) Exec(_ context.Context, _ string, arguments ...any) (pgconn.CommandTag, error) {
	r.keys = append(r.keys, arguments[0].(string))
	return pgconn.CommandTag{}, nil
}

func TestLockIntegrationSelectionsKeepsTheDeployedKeyAndTheOrder(t *testing.T) {
	org := "org-1"
	parent := uuid.MustParse("ffffffff-0000-0000-0000-000000000000")
	low := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	high := uuid.MustParse("00000000-0000-0000-0000-000000000009")
	recorder := &recordingExecer{}
	if err := LockIntegrationSelections(context.Background(), recorder, org, parent, high, low, parent, high); err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, id := range []uuid.UUID{parent, low, high} {
		want = append(want, fmt.Sprintf("sync-target-dataset-reconcile:%s:%s", org, id))
	}
	if fmt.Sprint(recorder.keys) != fmt.Sprint(want) {
		t.Fatalf("locks taken %v, want the parent first, then ascending, each once: %v", recorder.keys, want)
	}
}

func TestIsDeadlockDetectedReadsOnlySQLState40P01(t *testing.T) {
	for code, want := range map[string]bool{"40P01": true, "40001": false, "23505": false} {
		err := fmt.Errorf("commit: %w", &pgconn.PgError{Code: code})
		if got := IsDeadlockDetected(err); got != want {
			t.Errorf("%s: %v, want %v", code, got, want)
		}
	}
	if IsDeadlockDetected(errors.New("deadlock detected")) {
		t.Error("a plain error with the words is not SQLSTATE 40P01")
	}
}
