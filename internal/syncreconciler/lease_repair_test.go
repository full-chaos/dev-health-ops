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

type fakeLeaseRepairRows struct {
	pgx.Rows
	candidates []expiredLeaseCandidate
	index      int
	err        error
}

func (rows *fakeLeaseRepairRows) Next() bool { return rows.index < len(rows.candidates) }

func (rows *fakeLeaseRepairRows) Scan(dest ...any) error {
	if rows.index >= len(rows.candidates) {
		return errors.New("scan past rows")
	}
	candidate := rows.candidates[rows.index]
	rows.index++
	values := []any{
		candidate.id,
		candidate.syncRunID,
		candidate.leaseOwner,
		candidate.provider,
		candidate.mode,
		candidate.datasetKey,
		candidate.orgID,
		candidate.costClass,
		candidate.retryCount,
	}
	for index, destination := range dest {
		switch typed := destination.(type) {
		case *string:
			*typed = values[index].(string)
		case *int64:
			*typed = values[index].(int64)
		default:
			return errors.New("unexpected scan destination")
		}
	}
	return nil
}

func (rows *fakeLeaseRepairRows) Err() error { return rows.err }
func (*fakeLeaseRepairRows) Close()          {}

type fakeLeaseRepairTx struct {
	pgx.Tx
	candidates []expiredLeaseCandidate
	querySQL   string
	queryArgs  []any
	execSQL    []string
	execArgs   [][]any
	affected   []int64
	execErr    error
	commitErr  error
	commit     bool
	rollback   bool
}

func (tx *fakeLeaseRepairTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	tx.querySQL = sql
	tx.queryArgs = args
	return &fakeLeaseRepairRows{candidates: append([]expiredLeaseCandidate(nil), tx.candidates...)}, nil
}

func (tx *fakeLeaseRepairTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.execSQL = append(tx.execSQL, sql)
	tx.execArgs = append(tx.execArgs, args)
	if tx.execErr != nil {
		return pgconn.CommandTag{}, tx.execErr
	}
	index := len(tx.execSQL) - 1
	affected := int64(1)
	if index < len(tx.affected) {
		affected = tx.affected[index]
	}
	return pgconn.NewCommandTag("UPDATE " + strconv.FormatInt(affected, 10)), nil
}

func (tx *fakeLeaseRepairTx) Commit(context.Context) error {
	tx.commit = true
	return tx.commitErr
}

func (tx *fakeLeaseRepairTx) Rollback(context.Context) error {
	tx.rollback = true
	return nil
}

func repairCandidate(provider, mode, dataset string, retries int64) expiredLeaseCandidate {
	return expiredLeaseCandidate{
		id:         candidateID1,
		syncRunID:  "00000000-0000-4000-8000-000000003301",
		leaseOwner: "worker-a",
		provider:   provider,
		mode:       mode,
		datasetKey: dataset,
		orgID:      "org-a",
		costClass:  "standard",
		retryCount: retries,
	}
}

func TestExpiredLeaseDecisionIsFailClosedOutsideExactLinearBackfillSurface(t *testing.T) {
	tests := []struct {
		name      string
		candidate expiredLeaseCandidate
		want      expiredLeaseDecision
	}{
		{"eligible below ceiling", repairCandidate("linear", "backfill", "work_items", 0), expiredLeaseDecision{retry: true}},
		{"eligible at ceiling", repairCandidate("linear", "backfill", "work_items", 1), expiredLeaseDecision{exhausted: true}},
		{"wrong provider", repairCandidate("github", "backfill", "work_items", 0), expiredLeaseDecision{}},
		{"wrong mode", repairCandidate("linear", "incremental", "work_items", 0), expiredLeaseDecision{}},
		{"wrong dataset", repairCandidate("linear", "backfill", "repositories", 0), expiredLeaseDecision{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := decideExpiredLeaseRepair(test.candidate, DefaultLeaseRepairConfig()); got != test.want {
				t.Fatalf("decideExpiredLeaseRepair() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestLeaseRepairConfigDefaultsAndBucketHashMatchPythonContract(t *testing.T) {
	defaults := DefaultLeaseRepairConfig()
	if defaults.MaximumRetries != 1 || defaults.RetryBackoff != time.Minute || !defaults.valid() {
		t.Fatalf("default config = %#v", defaults)
	}
	if leaseRepairBucketAdvisoryID("org-a", "linear", "standard") != 3882165252103971925 {
		t.Fatalf("bucket advisory id diverged from Python SHA-256 contract")
	}
	if _, err := newLeaseRepairWithConfig(func(context.Context) (pgx.Tx, error) { return nil, nil }, LeaseRepairConfig{MaximumRetries: -1}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("negative retry config error = %v", err)
	}
	if got := decideExpiredLeaseRepair(repairCandidate("linear", "backfill", "work_items", 2), LeaseRepairConfig{MaximumRetries: 3}); !got.retry || got.exhausted {
		t.Fatalf("operator config decision = %#v", got)
	}
}

func TestLeaseRepairStepUsesCASAndRollsBackOnFault(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	tx := &fakeLeaseRepairTx{
		candidates: []expiredLeaseCandidate{
			repairCandidate("linear", "backfill", "work_items", 0),
			func() expiredLeaseCandidate {
				candidate := repairCandidate("github", "backfill", "work_items", 0)
				candidate.id = candidateID2
				return candidate
			}(),
		},
	}
	repair, err := newLeaseRepair(func(context.Context) (pgx.Tx, error) { return tx, nil })
	if err != nil {
		t.Fatal(err)
	}
	result, err := repair.Step(context.Background(), now, 2)
	if err != nil {
		t.Fatal(err)
	}
	if result != (LeaseRepairResult{Selected: 2, Retried: 1, Failed: 1}) || !tx.commit {
		t.Fatalf("Step() = %#v, commit=%t", result, tx.commit)
	}
	upperSelect := strings.ToUpper(tx.querySQL)
	for _, required := range []string{
		"RUN.STATUS NOT IN ('SUCCESS', 'PARTIAL_FAILED', 'FAILED')",
		"RUN.ORG_ID = UNIT.ORG_ID",
		"ORDER BY UNIT.LEASE_EXPIRES_AT, UNIT.ID",
		"LIMIT $2",
	} {
		if !strings.Contains(upperSelect, required) {
			t.Fatalf("selection SQL missing %q:\n%s", required, tx.querySQL)
		}
	}
	if len(tx.execSQL) != 4 || tx.execSQL[0] != "SELECT pg_advisory_xact_lock($1)" ||
		tx.execSQL[1] != "SELECT pg_advisory_xact_lock($1)" ||
		!strings.Contains(tx.execSQL[2], "rate_limit_deferrals = 0") ||
		!strings.Contains(tx.execSQL[2], "unit.lease_owner = $2") ||
		!strings.Contains(tx.execSQL[3], "'error_category', $4::text") {
		t.Fatalf("write SQL = %v", tx.execSQL)
	}
	if got, want := tx.execArgs[2][4], linearBackfillRetrySurfaces; !reflect.DeepEqual(got, want) {
		t.Fatalf("retry surfaces = %#v, want %#v", got, want)
	}

	faultTx := &fakeLeaseRepairTx{candidates: []expiredLeaseCandidate{repairCandidate("linear", "backfill", "work_items", 0)}, execErr: errors.New("injected write fault")}
	faultRepair, err := newLeaseRepair(func(context.Context) (pgx.Tx, error) { return faultTx, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := faultRepair.Step(context.Background(), now, 1); !errors.Is(err, ErrUnavailable) || faultTx.commit || !faultTx.rollback {
		t.Fatalf("fault Step() err=%v commit=%t rollback=%t", err, faultTx.commit, faultTx.rollback)
	}
}

func TestLeaseRepairRejectsInvalidBoundsWithoutBeginning(t *testing.T) {
	called := false
	repair, err := newLeaseRepair(func(context.Context) (pgx.Tx, error) {
		called = true
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{0, leaseRepairMaximumLimit + 1} {
		if _, err := repair.Step(context.Background(), time.Now(), limit); !errors.Is(err, ErrInvalidConfiguration) || called {
			t.Fatalf("limit %d error = %v begin=%t", limit, err, called)
		}
	}
}
