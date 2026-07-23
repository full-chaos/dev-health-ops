package syncreconciler

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type materializerExec struct {
	sql  string
	args []any
}

type fakeMaterializerTx struct {
	pgx.Tx
	affected  []int64
	failAt    int
	execs     []materializerExec
	committed bool
	rolled    bool
}

func (tx *fakeMaterializerTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, materializerExec{sql: sql, args: append([]any(nil), args...)})
	if tx.failAt > 0 && len(tx.execs) == tx.failAt {
		return pgconn.CommandTag{}, errors.New("injected materializer statement failure")
	}
	index := len(tx.execs) - 1
	affected := int64(0)
	if index < len(tx.affected) {
		affected = tx.affected[index]
	}
	return pgconn.NewCommandTag("INSERT 0 " + strconv.FormatInt(affected, 10)), nil
}

func (tx *fakeMaterializerTx) Commit(context.Context) error {
	tx.committed = true
	return nil
}

func (tx *fakeMaterializerTx) Rollback(context.Context) error {
	tx.rolled = true
	return nil
}

func TestMaterializerRunsOneBoundedTransportNeutralTransaction(t *testing.T) {
	now := time.Date(2026, time.July, 23, 18, 0, 0, 0, time.UTC)
	cutoff := now.Add(-15 * time.Minute)
	tx := &fakeMaterializerTx{affected: []int64{2, 1, 2, 1}}
	materializer, err := newMaterializer(func(context.Context) (pgx.Tx, error) {
		return tx, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := materializer.Step(context.Background(), now, cutoff, 2)
	if err != nil {
		t.Fatal(err)
	}
	if result != (MaterializerResult{Dispatch: 2, Finalize: 1, Discovery: 2, PostSync: 1}) {
		t.Fatalf("Step() result = %#v", result)
	}
	if !tx.committed || !tx.rolled || len(tx.execs) != 4 {
		t.Fatalf("transaction = committed:%t rolled:%t execs:%d", tx.committed, tx.rolled, len(tx.execs))
	}

	wantArgs := [][]any{
		{now, cutoff, 2},
		{now, 2},
		{now, 2},
		{now, 2},
	}
	for index, execution := range tx.execs {
		if !reflect.DeepEqual(execution.args, wantArgs[index]) {
			t.Fatalf("statement %d arguments = %#v, want %#v", index, execution.args, wantArgs[index])
		}
		upper := strings.ToUpper(execution.sql)
		for _, forbidden := range []string{
			"SYNC_DISPATCH_TRANSPORT_ROUTES",
			"CLAIM_TOKEN = GEN_RANDOM_UUID()",
			"ATTEMPTS = SYNC_DISPATCH_OUTBOX.ATTEMPTS + 1",
		} {
			if strings.Contains(upper, forbidden) {
				t.Fatalf("statement %d crosses transport boundary %q:\n%s", index, forbidden, execution.sql)
			}
		}
		if !strings.Contains(upper, "LIMIT $") ||
			!strings.Contains(upper, "ON CONFLICT (SYNC_RUN_ID, KIND)") {
			t.Fatalf("statement %d is not bounded/idempotent:\n%s", index, execution.sql)
		}
	}
	for _, rearmSQL := range []string{
		tx.execs[0].sql,
		tx.execs[1].sql,
		tx.execs[2].sql,
	} {
		upper := strings.ToUpper(rearmSQL)
		for _, required := range []string{
			"EXCLUDED.AVAILABLE_AT < SYNC_DISPATCH_OUTBOX.AVAILABLE_AT",
			"SYNC_DISPATCH_OUTBOX.CLAIM_EXPIRES_AT > $1",
			"LAST_ERROR = 'FEATURE_DISABLED'",
			"CLAIM_ROUTE_GENERATION = CASE",
			"DISPATCHED_ROUTE_GENERATION = CASE",
			"WHERE SYNC_DISPATCH_OUTBOX.STATUS <> 'PENDING'",
		} {
			if !strings.Contains(upper, required) {
				t.Fatalf("rearm SQL missing %q:\n%s", required, rearmSQL)
			}
		}
	}
	postSyncUpper := strings.ToUpper(tx.execs[3].sql)
	if !strings.Contains(postSyncUpper, "LEFT JOIN PUBLIC.SYNC_DISPATCH_OUTBOX") ||
		!strings.Contains(postSyncUpper, "OUTBOX.ID IS NULL") ||
		!strings.Contains(postSyncUpper, "DO NOTHING") ||
		strings.Contains(postSyncUpper, "DO UPDATE") {
		t.Fatalf("post_sync must remain insert-only:\n%s", tx.execs[3].sql)
	}
}

func TestMaterializerStatementFailureRollsBackWholeStep(t *testing.T) {
	now := time.Date(2026, time.July, 23, 18, 0, 0, 0, time.UTC)
	for failAt := 1; failAt <= 4; failAt++ {
		t.Run(strconv.Itoa(failAt), func(t *testing.T) {
			tx := &fakeMaterializerTx{affected: []int64{1, 1, 1, 1}, failAt: failAt}
			materializer, err := newMaterializer(func(context.Context) (pgx.Tx, error) {
				return tx, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := materializer.Step(context.Background(), now, now.Add(-time.Minute), 1)
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Step() error = %v", err)
			}
			if result != (MaterializerResult{}) || tx.committed || !tx.rolled || len(tx.execs) != failAt {
				t.Fatalf("failed step = result:%#v committed:%t rolled:%t execs:%d",
					result, tx.committed, tx.rolled, len(tx.execs))
			}
		})
	}
}

func TestMaterializerRejectsInvalidBoundariesBeforeOpeningTransaction(t *testing.T) {
	now := time.Date(2026, time.July, 23, 18, 0, 0, 0, time.UTC)
	begins := 0
	materializer, err := newMaterializer(func(context.Context) (pgx.Tx, error) {
		begins++
		return &fakeMaterializerTx{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		ctx    context.Context
		now    time.Time
		cutoff time.Time
		limit  int
	}{
		{"nil context", nil, now, now.Add(-time.Minute), 1},
		{"zero now", context.Background(), time.Time{}, now.Add(-time.Minute), 1},
		{"zero cutoff", context.Background(), now, time.Time{}, 1},
		{"future cutoff", context.Background(), now, now.Add(time.Second), 1},
		{"zero limit", context.Background(), now, now.Add(-time.Minute), 0},
		{"over limit", context.Background(), now, now.Add(-time.Minute), maximumStepLimit + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := materializer.Step(test.ctx, test.now, test.cutoff, test.limit); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("Step() error = %v", err)
			}
		})
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := materializer.Step(cancelled, now, now.Add(-time.Minute), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Step() error = %v", err)
	}
	if begins != 0 {
		t.Fatalf("invalid calls opened %d transactions", begins)
	}
}
