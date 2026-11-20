package syncdispatchruntime

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// CHAOS-14304: held-back units follow the guard's own slot order. The cap table is medium 2, heavy 1, light 4.

func TestBackfillWaitHeldBackTicketCase(t *testing.T) {
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "8")
	cases := []struct {
		name                                string
		costClass                           string
		waiting, active, activeBackfill, wa int
	}{
		{"medium cap 2, 1 backfill active, 60 wait", "medium", 60, 1, 1, 60},
		{"medium, nothing active", "medium", 60, 0, 0, 0},
		{"medium, 1 non-backfill active keeps the share", "medium", 60, 1, 0, 0},
		{"medium, both slots live", "medium", 60, 2, 1, 60},
		{"heavy cap 1, 1 active", "heavy", 60, 1, 0, 60},
		{"heavy cap 1, free", "heavy", 60, 0, 0, 0},
		{"light cap 4, 3 backfill active", "light", 60, 4, 3, 60},
		{"light cap 4, 1 backfill active", "light", 60, 2, 1, 0},
		{"over-full bucket", "medium", 3, 5, 2, 3},
	}
	for _, c := range cases {
		if got := backfillWaitHeldBack(c.costClass, c.waiting, c.active, c.activeBackfill, admissionNow); got != c.wa {
			t.Errorf("%s: held_back = %d, want %d", c.name, got, c.wa)
		}
	}
}

// The gauge and the guard agree: a backfill pass admits nothing exactly when the gauge says every waiting unit is held back.
func TestBackfillWaitHeldBackAgreesWithTheGuardDecision(t *testing.T) {
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "8")
	for _, costClass := range []string{"light", "medium", "heavy"} {
		capValue := concurrencyCapForCostClass(costClass)
		for active := 0; active <= capValue+1; active++ {
			for activeBackfill := 0; activeBackfill <= active; activeBackfill++ {
				state := slotShareBucket{active: active, activeBackfill: activeBackfill}
				admitted, _ := slotSharePass(t, shareBackfill, costClass, 5, state)
				held := backfillWaitHeldBack(costClass, 5, active, activeBackfill, admissionNow)
				if (admitted == 0) != (held == 5) || (admitted > 0) != (held == 0) {
					t.Errorf("%s active=%d backfill=%d: guard admits %d, gauge holds back %d of 5", costClass, active, activeBackfill, admitted, held)
				}
			}
		}
	}
}

type failingWaitDB struct{ err error }

func (db failingWaitDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) { return nil, db.err }

func TestBackfillWaitSourceFailedReadRecordsNoSample(t *testing.T) {
	source, err := NewBackfillWaitSource(failingWaitDB{err: errors.New("boom")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := source.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if !strings.Contains(text, "sync_dispatch_backfill_wait_read_ok 0\n") {
		t.Fatalf("read_ok must be 0:\n%s", text)
	}
	for _, name := range []string{"sync_dispatch_backfill_wait_units", "sync_dispatch_backfill_wait_held_back_units", "sync_dispatch_backfill_open_runs"} {
		if strings.Contains(text, name) {
			t.Fatalf("no sample of %s on a failed read:\n%s", name, text)
		}
	}
}

func TestBackfillWaitSourceCachesOneReadPerPeriod(t *testing.T) {
	db := &countingWaitDB{err: errors.New("boom")}
	source, _ := NewBackfillWaitSource(db, nil)
	now := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	source.now = func() time.Time { return now }
	for range 3 {
		_ = source.WritePrometheus(&bytes.Buffer{})
	}
	if db.calls != 1 {
		t.Fatalf("reads inside one period = %d, want 1", db.calls)
	}
	now = now.Add(backfillWaitCachePeriod)
	_ = source.WritePrometheus(&bytes.Buffer{})
	if db.calls != 2 {
		t.Fatalf("reads after the period = %d, want 2", db.calls)
	}
}

type countingWaitDB struct {
	err   error
	calls int
}

func (db *countingWaitDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	db.calls++
	return nil, db.err
}

func TestNewBackfillWaitSourceNeedsADatabase(t *testing.T) {
	if _, err := NewBackfillWaitSource(nil, nil); err == nil {
		t.Fatal("a nil database must be refused")
	}
}
