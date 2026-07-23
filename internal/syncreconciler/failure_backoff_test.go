package syncreconciler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestTransportPublishBackoffPythonParityAndBounds(t *testing.T) {
	tests := []struct {
		attempt int64
		want    time.Duration
	}{
		{attempt: -1, want: time.Minute},
		{attempt: 0, want: time.Minute},
		{attempt: 1, want: time.Minute},
		{attempt: 2, want: 2 * time.Minute},
		{attempt: 3, want: 4 * time.Minute},
		{attempt: 4, want: 8 * time.Minute},
		{attempt: 5, want: 15 * time.Minute},
		{attempt: 6, want: 15 * time.Minute},
		{attempt: 1 << 62, want: 15 * time.Minute},
	}
	for _, test := range tests {
		t.Run(test.want.String(), func(t *testing.T) {
			if got := transportPublishBackoff(test.attempt); got != test.want {
				t.Fatalf("transportPublishBackoff(%d) = %s, want %s", test.attempt, got, test.want)
			}
		})
	}
}

func TestPublishFailureRecorderPersistsOnlyBoundedEvidence(t *testing.T) {
	now := time.Date(2026, time.July, 23, 15, 0, 0, 0, time.FixedZone("offset", -7*60*60))
	tx := &fakeFailureTransaction{rowsAffected: 1}
	recorder := mustFailureRecorder(t, func(context.Context) (failureTransaction, error) {
		return tx, nil
	})
	secret := "postgres://operator:raw-secret@example.test/db?token=do-not-store"
	if err := recorder.Record(context.Background(), failureClaim(), now, errors.New(secret)); err != nil {
		t.Fatal(err)
	}
	if !tx.committed || tx.rollbackCalls != 1 {
		t.Fatalf("transaction committed:%v rollback calls:%d", tx.committed, tx.rollbackCalls)
	}
	if len(tx.args) != 7 {
		t.Fatalf("Exec args = %d, want 7", len(tx.args))
	}
	if got := tx.args[4]; got != now.UTC() {
		t.Fatalf("recorded now = %v, want %v", got, now.UTC())
	}
	if got := tx.args[5]; got != now.UTC().Add(4*time.Minute) {
		t.Fatalf("available_at = %v, want %v", got, now.UTC().Add(4*time.Minute))
	}
	evidence, ok := tx.args[6].(string)
	if !ok || evidence != transportPublishFailureEvidence || len(evidence) > 64 || evidence == secret {
		t.Fatalf("persisted evidence = %#v", tx.args[6])
	}
}

func TestPublishFailureRecorderClassifiesCASAndPersistenceFailures(t *testing.T) {
	persistenceErr := errors.New("database unavailable")
	tests := []struct {
		name      string
		begin     failureBegin
		want      error
		wantRolls int
	}{
		{
			name: "begin outage",
			begin: func(context.Context) (failureTransaction, error) {
				return nil, persistenceErr
			},
			want: ErrUnavailable,
		},
		{
			name: "exec outage rolls back",
			begin: func(context.Context) (failureTransaction, error) {
				return &fakeFailureTransaction{execErr: persistenceErr}, nil
			},
			want:      ErrUnavailable,
			wantRolls: 1,
		},
		{
			name: "stale claim rolls back",
			begin: func(context.Context) (failureTransaction, error) {
				return &fakeFailureTransaction{}, nil
			},
			want:      ErrLeaseLost,
			wantRolls: 1,
		},
		{
			name: "commit outage rolls back",
			begin: func(context.Context) (failureTransaction, error) {
				return &fakeFailureTransaction{rowsAffected: 1, commitErr: persistenceErr}, nil
			},
			want:      ErrUnavailable,
			wantRolls: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var tx *fakeFailureTransaction
			recorder := mustFailureRecorder(t, func(ctx context.Context) (failureTransaction, error) {
				started, err := test.begin(ctx)
				if value, ok := started.(*fakeFailureTransaction); ok {
					tx = value
				}
				return started, err
			})
			err := recorder.Record(context.Background(), failureClaim(), time.Now(), errors.New("publish failed"))
			if !errors.Is(err, test.want) {
				t.Fatalf("Record() error = %v, want %v", err, test.want)
			}
			if tx != nil && tx.rollbackCalls != test.wantRolls {
				t.Fatalf("Rollback() calls = %d, want %d", tx.rollbackCalls, test.wantRolls)
			}
		})
	}
}

func TestPublishFailureRecorderRejectsPostSyncBeforePersistence(t *testing.T) {
	began := false
	recorder := mustFailureRecorder(t, func(context.Context) (failureTransaction, error) {
		began = true
		return &fakeFailureTransaction{rowsAffected: 1}, nil
	})
	claim := failureClaim()
	claim.Kind = syncdispatchcontract.KindPostSync
	err := recorder.Record(context.Background(), claim, time.Now(), errors.New("publish failed"))
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("Record(post_sync) error = %v, want ErrInvalidConfiguration", err)
	}
	if began {
		t.Fatal("post_sync failure recorder began a persistence transaction")
	}
}

func failureClaim() TransportClaim {
	return TransportClaim{
		ID:              "00000000-0000-4000-8000-000000003901",
		Kind:            "dispatch_sync_run",
		ClaimToken:      "10000000-0000-4000-8000-000000003901",
		RouteGeneration: 7,
		AvailableAt:     time.Date(2026, time.July, 23, 14, 0, 0, 0, time.UTC),
		Attempts:        3,
	}
}

func mustFailureRecorder(t *testing.T, begin failureBegin) *PublishFailureRecorder {
	t.Helper()
	recorder, err := newPublishFailureRecorder(begin)
	if err != nil {
		t.Fatal(err)
	}
	return recorder
}

type fakeFailureTransaction struct {
	args          []any
	rowsAffected  int64
	execErr       error
	commitErr     error
	committed     bool
	rollbackCalls int
}

func (tx *fakeFailureTransaction) Exec(
	_ context.Context,
	_ string,
	args ...any,
) (pgconn.CommandTag, error) {
	tx.args = args
	return pgconn.NewCommandTag("UPDATE " + string(rune('0'+tx.rowsAffected))), tx.execErr
}

func (tx *fakeFailureTransaction) Commit(context.Context) error {
	tx.committed = tx.commitErr == nil
	return tx.commitErr
}

func (tx *fakeFailureTransaction) Rollback(context.Context) error {
	tx.rollbackCalls++
	return nil
}
