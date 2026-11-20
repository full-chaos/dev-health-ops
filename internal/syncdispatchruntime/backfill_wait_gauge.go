package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/puddle/v2"
)

// CHAOS-14304: an operator gauge for the serial backfill drain. It reports, per (provider, cost class), how many units
// of open backfill runs wait, how many bucket slots are live, and how many waiting units the guard's backfill share
// holds back. It is observe-only: it never changes the share, the caps or the admission, and a failed read records no
// sample and never fails a start (D6465).
const (
	backfillWaitCachePeriod = 60 * time.Second
	backfillWaitReadTimeout = 5 * time.Second
	// backfillWaitRowLimit bounds Q1: two rows (planned, retrying) per (org, provider, cost class) bucket.
	backfillWaitRowLimit = 1000
)

// backfillWaitOpenSQL is the open-backfill-run unit read. The backfill rule is the one readRunOrder applies
// (admission_order.go); backfill_wait_gauge_test.go pins the two against each other. Plain SELECT: no row or bucket lock.
const backfillWaitOpenSQL = `
SELECT unit.org_id, unit.provider, unit.cost_class, unit.status, count(*)::int
FROM public.sync_runs AS run
JOIN public.sync_run_units AS unit ON unit.sync_run_id = run.id
WHERE run.status <> ALL($1::text[])
  AND (run.triggered_by = 'backfill' OR run.mode = 'backfill')
  AND unit.status IN ('planned', 'retrying')
GROUP BY unit.org_id, unit.provider, unit.cost_class, unit.status
ORDER BY count(*) DESC
LIMIT $2`

const backfillWaitOpenRunsSQL = `
SELECT count(*)::int
FROM public.sync_runs AS run
WHERE run.status <> ALL($1::text[])
  AND (run.triggered_by = 'backfill' OR run.mode = 'backfill')`

// BackfillWaitDB is the Postgres handle of the gauge (a pgx pool of the domain role).
type BackfillWaitDB interface {
	BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error)
}

type backfillWaitKey struct{ provider, costClass string }

type backfillWaitSample struct {
	planned, retrying, active, heldBack int
}

type backfillWaitSnapshot struct {
	openRuns int
	series   map[backfillWaitKey]*backfillWaitSample
}

// BackfillWaitSource is a health.MetricsSource. One database read per cache period, whatever the number of scrapes; a failed
// read records no sample (read_ok 0) and is not retried before the period ends.
type BackfillWaitSource struct {
	db     BackfillWaitDB
	logger *slog.Logger
	now    func() time.Time

	mu       sync.Mutex
	readAt   time.Time
	snapshot *backfillWaitSnapshot
}

func NewBackfillWaitSource(db BackfillWaitDB, logger *slog.Logger) (*BackfillWaitSource, error) {
	if db == nil {
		return nil, errors.New("syncdispatchruntime: the backfill wait gauge needs a database")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &BackfillWaitSource{db: db, logger: logger, now: time.Now}, nil
}

// backfillWaitHeldBack is the number of waiting backfill units the guard would not admit now: the guard's own slot order
// (dispatch_guard.go: slots = cap - active, then the backfill share), called not copied. Older-run hold-back is not
// included: with no older runs it reads the bucket-level wait.
func backfillWaitHeldBack(costClass string, waiting, active, activeBackfill int, now time.Time) int {
	capValue := concurrencyCapForCostClass(costClass)
	slots := capValue - active
	if slots < 0 {
		slots = 0
	}
	if slots > 0 {
		slots = min(slots, backfillSlotLimit(runOrder{backfill: true}, capValue, activeBackfill, nil, now, admissionLivenessBound()))
	}
	if slots > 0 {
		return 0
	}
	return waiting
}

func (s *BackfillWaitSource) read(ctx context.Context, now time.Time) (*backfillWaitSnapshot, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), backfillWaitReadTimeout)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	terminal := []string{syncRunStatusSuccess, syncRunStatusPartialFailed, syncRunStatusFailed}
	snapshot := &backfillWaitSnapshot{series: map[backfillWaitKey]*backfillWaitSample{}}
	if err := tx.QueryRow(ctx, backfillWaitOpenRunsSQL, terminal).Scan(&snapshot.openRuns); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, backfillWaitOpenSQL, terminal, backfillWaitRowLimit)
	if err != nil {
		return nil, err
	}
	waiting := map[dispatchBucket]int{}
	for rows.Next() {
		var bucket dispatchBucket
		var status string
		var n int
		if err := rows.Scan(&bucket.orgID, &bucket.provider, &bucket.costClass, &status, &n); err != nil {
			rows.Close()
			return nil, err
		}
		waiting[bucket] += n
		sample := snapshot.sample(bucket)
		if status == syncRunUnitStatusRetrying {
			sample.retrying += n
		} else {
			sample.planned += n
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	staleCutoff := now.Add(-staleDispatchSeconds())
	buckets := make([]dispatchBucket, 0, len(waiting))
	for bucket := range waiting {
		buckets = append(buckets, bucket)
	}
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].orgID+"/"+buckets[i].provider+"/"+buckets[i].costClass < buckets[j].orgID+"/"+buckets[j].provider+"/"+buckets[j].costClass
	})
	for _, bucket := range buckets {
		active, activeBackfill, err := countActiveBucketUnits(ctx, tx, bucket, staleCutoff, now)
		if err != nil {
			return nil, err
		}
		sample := snapshot.sample(bucket)
		sample.active += active
		sample.heldBack += backfillWaitHeldBack(bucket.costClass, waiting[bucket], active, activeBackfill, now)
	}
	return snapshot, nil
}

func (snapshot *backfillWaitSnapshot) sample(bucket dispatchBucket) *backfillWaitSample {
	key := backfillWaitKey{provider: bucket.provider, costClass: bucket.costClass}
	if snapshot.series[key] == nil {
		snapshot.series[key] = &backfillWaitSample{}
	}
	return snapshot.series[key]
}

func (s *BackfillWaitSource) current(ctx context.Context) *backfillWaitSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if !s.readAt.IsZero() && now.Sub(s.readAt) < backfillWaitCachePeriod {
		return s.snapshot
	}
	s.readAt = now
	snapshot, err := s.read(ctx, now)
	if err != nil {
		s.snapshot = nil
		if errors.Is(err, puddle.ErrClosedPool) {
			s.logger.InfoContext(ctx, "backfill wait gauge read skipped: the pool is closed (shutdown)")
			return nil
		}
		s.logger.WarnContext(ctx, "backfill wait gauge read failed", "error", err)
		return nil
	}
	s.snapshot = snapshot
	return snapshot
}

// WritePrometheus implements health.MetricsSource. The family names are literals: the label census reads them from here.
func (s *BackfillWaitSource) WritePrometheus(w io.Writer) error {
	snapshot := s.current(context.Background())
	readOK := 0
	if snapshot != nil {
		readOK = 1
	}
	if _, err := fmt.Fprintf(w, "# HELP sync_dispatch_backfill_wait_read_ok 1 when the last read of the backfill wait gauge worked, 0 when it failed (no wait samples then).\n# TYPE sync_dispatch_backfill_wait_read_ok gauge\nsync_dispatch_backfill_wait_read_ok %d\n", readOK); err != nil || snapshot == nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "# HELP sync_dispatch_backfill_open_runs Open backfill runs.\n# TYPE sync_dispatch_backfill_open_runs gauge\nsync_dispatch_backfill_open_runs %d\n", snapshot.openRuns); err != nil {
		return err
	}
	keys := make([]backfillWaitKey, 0, len(snapshot.series))
	for key := range snapshot.series {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].provider != keys[j].provider {
			return keys[i].provider < keys[j].provider
		}
		return keys[i].costClass < keys[j].costClass
	})
	if len(keys) == 0 {
		return nil
	}
	if _, err := io.WriteString(w, "# HELP sync_dispatch_backfill_wait_units Units of open backfill runs by provider and cost class: planned and retrying wait for a slot; active = live units of the buckets they wait in (any run).\n# TYPE sync_dispatch_backfill_wait_units gauge\n"); err != nil {
		return err
	}
	for _, key := range keys {
		sample := snapshot.series[key]
		for _, state := range []struct {
			name string
			n    int
		}{{"planned", sample.planned}, {"retrying", sample.retrying}, {"active", sample.active}} {
			if _, err := fmt.Fprintf(w, "sync_dispatch_backfill_wait_units{provider=%q,cost_class=%q,state=%q} %d\n", key.provider, key.costClass, state.name, state.n); err != nil {
				return err
			}
		}
	}
	if _, err := io.WriteString(w, "# HELP sync_dispatch_backfill_wait_held_back_units Waiting backfill units the guard's backfill share holds back now (older-run hold-back not included).\n# TYPE sync_dispatch_backfill_wait_held_back_units gauge\n"); err != nil {
		return err
	}
	for _, key := range keys {
		if _, err := fmt.Fprintf(w, "sync_dispatch_backfill_wait_held_back_units{provider=%q,cost_class=%q} %d\n", key.provider, key.costClass, snapshot.series[key].heldBack); err != nil {
			return err
		}
	}
	return nil
}
