package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/puddle/v2"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// CHAOS-14304: an operator gauge for the serial backfill drain. It reports, per (provider, cost class), how many units
// of open backfill runs wait, how many bucket slots are live, and how many waiting units the guard's backfill share
// holds back. It is observe-only: it never changes the share, the caps or the admission, and a failed read records no
// sample and never fails a start (D6465).
const (
	backfillWaitCachePeriod = 60 * time.Second
	backfillWaitReadTimeout = 5 * time.Second
	// backfillWaitRowLimit bounds the buckets (org, provider, cost class) reported one by one. The buckets past it (the
	// smallest) are folded, whole, by the SQL into one row per status with NULL org, provider and cost class: a real group has
	// none of them NULL (columns NOT NULL), so the fold cannot collide with a real key. The gauge then carries them under backfillWaitFoldedLabel, keyed by a flag
	// and not by that string, so a provider that carried the same name could not merge into it either.
	backfillWaitRowLimit = 1000
	// backfillWaitFoldedLabel is the label value of the folded series. A provider or cost class is a registry slug of
	// [a-z0-9_-], so it never starts with "~".
	backfillWaitFoldedLabel = "~other"
)

// backfillWaitOpenSQL is the open-backfill-run unit read. The backfill rule is the one readRunOrder applies
// (admission_order.go); backfill_wait_gauge_test.go pins the two against each other. Plain SELECT: no row or bucket lock.
const backfillWaitBucketsCTE = `
WITH grouped AS (
    SELECT unit.org_id, unit.provider, unit.cost_class, unit.status, count(*)::int AS n
    FROM public.sync_runs AS run
    JOIN public.sync_run_units AS unit ON unit.sync_run_id = run.id
    WHERE run.status <> ALL($1::text[])
      AND (run.triggered_by = 'backfill' OR run.mode = 'backfill')
      AND unit.status IN ('planned', 'retrying')
    GROUP BY unit.org_id, unit.provider, unit.cost_class, unit.status
), buckets AS (
    SELECT org_id, provider, cost_class, sum(n)::int AS total,
           row_number() OVER (ORDER BY sum(n) DESC, org_id, provider, cost_class) AS rn
    FROM grouped
    GROUP BY org_id, provider, cost_class
)`

// backfillWaitOpenSQL: the waiting rows. Buckets ranked past $2 (the smallest) fold, whole, into one row per status with
// NULL org, provider and cost class.
const backfillWaitOpenSQL = backfillWaitBucketsCTE + `
SELECT CASE WHEN b.rn <= $2 THEN g.org_id END, CASE WHEN b.rn <= $2 THEN g.provider END, CASE WHEN b.rn <= $2 THEN g.cost_class END,
       g.status, sum(g.n)::int
FROM grouped AS g
JOIN buckets AS b ON b.org_id = g.org_id AND b.provider = g.provider AND b.cost_class = g.cost_class
GROUP BY 1, 2, 3, 4`

// backfillWaitActiveSQL reads, for every waiting bucket in one statement, its rank, waiting total and active counts, from
// the guard's own count clauses (dispatch_guard.go activeBucketUnits*). $2 is the read limit (the rank is compared in Go);
// $3 is the unused bucket slot of the guard's numbering.
const backfillWaitActiveSQL = backfillWaitBucketsCTE + `
SELECT b.org_id, b.provider, b.cost_class, b.total, b.rn, ` + activeBucketUnitsCounts + `
FROM buckets AS b
JOIN public.sync_run_units AS unit ON unit.org_id = b.org_id AND unit.provider = b.provider AND unit.cost_class = b.cost_class
LEFT JOIN public.sync_runs AS run ON run.id = unit.sync_run_id
WHERE $2::bigint > 0 AND $3::boolean
  AND ` + activeBucketUnitsLive + `
GROUP BY b.org_id, b.provider, b.cost_class, b.total, b.rn`

const backfillWaitOpenRunsSQL = `
SELECT count(*)::int
FROM public.sync_runs AS run
WHERE run.status <> ALL($1::text[])
  AND (run.triggered_by = 'backfill' OR run.mode = 'backfill')`

// BackfillWaitDB is the Postgres handle of the gauge (a pgx pool of the domain role).
type BackfillWaitDB interface {
	BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error)
}

type backfillWaitKey struct {
	provider, costClass string
	folded              bool
}

type backfillWaitSample struct {
	planned, retrying, active, heldBack int
}

type backfillWaitSnapshot struct {
	openRuns  int
	truncated bool
	series    map[backfillWaitKey]*backfillWaitSample
}

// BackfillWaitSource is a health.MetricsSource. One database read per cache period, whatever the number of scrapes; a failed
// read records no sample (read_ok 0) and is not retried before the period ends.
type BackfillWaitSource struct {
	db     BackfillWaitDB
	logger *synclog.Logger
	now    func() time.Time

	mu       sync.Mutex
	readAt   time.Time
	snapshot *backfillWaitSnapshot
	// truncations counts the reads that folded groups past the limit.
	truncations int64
}

func NewBackfillWaitSource(db BackfillWaitDB, logger *synclog.Logger) (*BackfillWaitSource, error) {
	if db == nil {
		return nil, errors.New("syncdispatchruntime: the backfill wait gauge needs a database")
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
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
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
	folded := &backfillWaitSample{}
	for rows.Next() {
		var org, provider, costClass *string
		var status string
		var n int
		if err := rows.Scan(&org, &provider, &costClass, &status, &n); err != nil {
			rows.Close()
			return nil, err
		}
		sample := folded
		if org != nil && provider != nil && costClass != nil {
			bucket := dispatchBucket{orgID: *org, provider: *provider, costClass: *costClass}
			sample = snapshot.sample(bucket)
		} else {
			snapshot.truncated = true
		}
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
	// ONE grouped statement in the same snapshot gives the active counts of every waiting bucket (the guard's own clause); the
	// held_back rule is arithmetic on (cost class, waiting, active, active backfill), run in memory per bucket. A bucket with
	// no live unit has no row: active 0 and held_back 0.
	staleCutoff := now.Add(-staleDispatchSeconds())
	counts, err := readBucketActive(ctx, tx, terminal, backfillWaitRowLimit, staleCutoff, now)
	if err != nil {
		return nil, err
	}
	for _, item := range counts {
		heldBack := backfillWaitHeldBack(item.bucket.costClass, item.total, item.active, item.activeBackfill, now)
		sample := folded
		if item.rank <= backfillWaitRowLimit {
			sample = snapshot.sample(item.bucket)
		}
		sample.active += item.active
		sample.heldBack += heldBack
	}
	if !snapshot.truncated {
		return snapshot, nil
	}
	snapshot.series[backfillWaitKey{folded: true}] = folded
	return snapshot, nil
}

func (snapshot *backfillWaitSnapshot) sample(bucket dispatchBucket) *backfillWaitSample {
	key := backfillWaitKey{provider: bucket.provider, costClass: bucket.costClass}
	if snapshot.series[key] == nil {
		snapshot.series[key] = &backfillWaitSample{}
	}
	return snapshot.series[key]
}

func (s *BackfillWaitSource) current(ctx context.Context) (*backfillWaitSnapshot, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if !s.readAt.IsZero() && now.Sub(s.readAt) < backfillWaitCachePeriod {
		return s.snapshot, s.truncations
	}
	s.readAt = now
	snapshot, err := s.read(ctx, now)
	if err != nil {
		s.snapshot = nil
		// The last scrape at pod stop can run after the pool is closed: shutdown, not a failed read.
		if !errors.Is(err, puddle.ErrClosedPool) {
			s.logger.Warn(ctx, synclog.MsgBackfillWaitGaugeReadFailed, synclog.Failure(err))
		}
		return nil, s.truncations
	}
	s.snapshot = snapshot
	if snapshot.truncated {
		s.truncations++
		s.logger.Warn(ctx, synclog.MsgBackfillWaitGaugeTruncated, synclog.Count(synclog.KeyBackfillWaitGroupLimit, backfillWaitRowLimit))
	}
	return snapshot, s.truncations
}

// WritePrometheus implements health.MetricsSource. The family names are literals: the label census reads them from here.
func (s *BackfillWaitSource) WritePrometheus(w io.Writer) error {
	snapshot, truncations := s.current(context.Background())
	readOK := 0
	if snapshot != nil {
		readOK = 1
	}
	if _, err := fmt.Fprintf(w, "# HELP sync_dispatch_backfill_wait_truncated_total Reads that folded waiting groups past the read limit into the ~other series.\n# TYPE sync_dispatch_backfill_wait_truncated_total counter\nsync_dispatch_backfill_wait_truncated_total %d\n", truncations); err != nil {
		return err
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
		if keys[i].folded != keys[j].folded {
			return !keys[i].folded
		}
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
		provider, costClass := key.provider, key.costClass
		if key.folded {
			provider, costClass = backfillWaitFoldedLabel, backfillWaitFoldedLabel
		}
		for _, state := range []struct {
			name string
			n    int
		}{{"planned", sample.planned}, {"retrying", sample.retrying}, {"active", sample.active}} {
			if _, err := fmt.Fprintf(w, "sync_dispatch_backfill_wait_units{provider=%q,cost_class=%q,state=%q} %d\n", provider, costClass, state.name, state.n); err != nil {
				return err
			}
		}
	}
	if _, err := io.WriteString(w, "# HELP sync_dispatch_backfill_wait_held_back_units Waiting backfill units the guard's backfill share holds back now (older-run hold-back not included).\n# TYPE sync_dispatch_backfill_wait_held_back_units gauge\n"); err != nil {
		return err
	}
	for _, key := range keys {
		provider, costClass := key.provider, key.costClass
		if key.folded {
			provider, costClass = backfillWaitFoldedLabel, backfillWaitFoldedLabel
		}
		if _, err := fmt.Fprintf(w, "sync_dispatch_backfill_wait_held_back_units{provider=%q,cost_class=%q} %d\n", provider, costClass, snapshot.series[key].heldBack); err != nil {
			return err
		}
	}
	return nil
}

type bucketActive struct {
	bucket         dispatchBucket
	total          int
	rank           int
	active         int
	activeBackfill int
}

// readBucketActive returns the active counts of every waiting bucket with its rank. A bucket with no live unit has no row:
// its active counts are 0 and its held_back is 0 (a free bucket holds nothing back).
func readBucketActive(ctx context.Context, tx pgx.Tx, terminal []string, limit int, staleCutoff, now time.Time) ([]bucketActive, error) {
	rows, err := tx.Query(ctx, backfillWaitActiveSQL,
		terminal, limit, true,
		syncRunUnitStatusDispatching, staleCutoff, syncRunUnitStatusRunning, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []bucketActive
	for rows.Next() {
		var item bucketActive
		if err := rows.Scan(&item.bucket.orgID, &item.bucket.provider, &item.bucket.costClass, &item.total, &item.rank, &item.active, &item.activeBackfill); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
