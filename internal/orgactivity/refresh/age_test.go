package refresh

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type fakeRow struct {
	age *float64
	err error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(**float64)) = r.age
	return nil
}

type fakeAgeDB struct {
	row       fakeRow
	reads     int
	remaining time.Duration
	hasLimit  bool
}

func (d *fakeAgeDB) QueryRow(ctx context.Context, _ string, _ ...any) pgx.Row {
	d.reads++
	if deadline, ok := ctx.Deadline(); ok {
		d.hasLimit, d.remaining = true, time.Until(deadline)
	}
	return d.row
}

func float(v float64) *float64 { return &v }

type ageHarness struct {
	t      *testing.T
	reader *metric.ManualReader
	db     *fakeAgeDB
	clock  time.Time
}

func newAgeHarness(t *testing.T, row fakeRow) *ageHarness {
	t.Helper()
	h := &ageHarness{t: t, reader: metric.NewManualReader(), db: &fakeAgeDB{row: row}, clock: time.Unix(1_000_000, 0)}
	provider := metric.NewMeterProvider(metric.WithReader(h.reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	if _, err := RegisterAgeProbe(provider.Meter("age-test"), h.db, nil, func() time.Time { return h.clock }); err != nil {
		t.Fatal(err)
	}
	return h
}

// collect returns the gauge values and the failure-counter values of one collection.
func (h *ageHarness) collect() (gauge []float64, failed []int64) {
	h.t.Helper()
	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &rm); err != nil {
		h.t.Fatal(err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Gauge[float64]:
				if m.Name == AgeGaugeName {
					for _, p := range data.DataPoints {
						gauge = append(gauge, p.Value)
					}
				}
			case metricdata.Sum[int64]:
				if m.Name == AgeReadFailedCounterName {
					for _, p := range data.DataPoints {
						failed = append(failed, p.Value)
					}
				}
			}
		}
	}
	return gauge, failed
}

func TestTheAgeGaugeReportsTheNewestRowAgeAndTheFailureCounterStartsAtZero(t *testing.T) {
	h := newAgeHarness(t, fakeRow{age: float(7200)})
	gauge, failed := h.collect()
	if len(gauge) != 1 || gauge[0] != 7200 {
		t.Errorf("gauge = %v, want [7200]", gauge)
	}
	if len(failed) != 1 || failed[0] != 0 {
		t.Errorf("read-failure counter = %v, want one series at 0", failed)
	}
}

func TestAnEmptyTableRecordsNoSampleAndNoFailure(t *testing.T) {
	h := newAgeHarness(t, fakeRow{age: nil})
	gauge, failed := h.collect()
	if len(gauge) != 0 {
		t.Errorf("gauge = %v, want no sample for an empty table", gauge)
	}
	if len(failed) != 1 || failed[0] != 0 {
		t.Errorf("read-failure counter = %v, want 0", failed)
	}
}

func TestAFailedReadRecordsNoSampleNotZeroAndCountsTheFailure(t *testing.T) {
	h := newAgeHarness(t, fakeRow{err: errors.New("connection refused")})
	gauge, failed := h.collect()
	if len(gauge) != 0 {
		t.Errorf("gauge = %v, want no sample after a failed read (never 0)", gauge)
	}
	if len(failed) != 1 || failed[0] != 1 {
		t.Errorf("read-failure counter = %v, want 1", failed)
	}
}

func TestTheDatabaseIsReadOncePerCachePeriodWhateverTheCollections(t *testing.T) {
	h := newAgeHarness(t, fakeRow{age: float(10)})
	for i := 0; i < 5; i++ {
		h.collect()
	}
	if h.db.reads != 1 {
		t.Fatalf("%d reads in five collections inside one period, want 1", h.db.reads)
	}
	h.clock = h.clock.Add(ageCachePeriod - time.Second)
	h.collect()
	if h.db.reads != 1 {
		t.Fatalf("%d reads just before the period ends, want 1", h.db.reads)
	}
	h.clock = h.clock.Add(time.Second)
	h.collect()
	if h.db.reads != 2 {
		t.Fatalf("%d reads after the period ended, want 2", h.db.reads)
	}
}

func TestAFailedReadIsNotRetriedBeforeThePeriodEnds(t *testing.T) {
	h := newAgeHarness(t, fakeRow{err: errors.New("boom")})
	for i := 0; i < 4; i++ {
		h.collect()
	}
	if h.db.reads != 1 {
		t.Fatalf("%d reads after a failure inside one period, want 1 (no hammering of a sick database)", h.db.reads)
	}
	_, failed := h.collect()
	if len(failed) != 1 || failed[0] != 1 {
		t.Errorf("failure counter = %v, want 1: only the real read counts", failed)
	}
}

func TestTheReadCarriesATwoSecondDeadlineAndTheCachePeriodIsSixtySeconds(t *testing.T) {
	h := newAgeHarness(t, fakeRow{age: float(1)})
	h.collect()
	if !h.db.hasLimit || h.db.remaining <= time.Second || h.db.remaining > 2*time.Second {
		t.Errorf("the read context has deadline=%v remaining=%v, want a deadline of at most 2s", h.db.hasLimit, h.db.remaining)
	}
	if ageCachePeriod != 60*time.Second {
		t.Errorf("cache period %v, want 60s", ageCachePeriod)
	}
}
