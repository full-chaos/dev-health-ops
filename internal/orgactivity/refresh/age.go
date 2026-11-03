package refresh

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/metric"
)

const (
	// AgeGaugeName is the age, in seconds, of the newest public.org_activity row (now - max(refreshed_at)).
	AgeGaugeName = "dev_health_org_activity_refresh_age_seconds"
	// AgeReadFailedCounterName counts failed reads behind the gauge, so a gauge that stopped reporting is visible.
	AgeReadFailedCounterName = "dev_health_org_activity_refresh_age_read_failed_total"

	ageReadTimeout = 2 * time.Second
	ageCachePeriod = 60 * time.Second
)

// ageSQL is NULL when the table is empty.
const ageSQL = `SELECT EXTRACT(EPOCH FROM (now() - max(refreshed_at)))::float8 FROM public.org_activity`

// AgeDB is the Postgres handle the age gauge reads through (a pgx pool of the domain role).
type AgeDB interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// AgeProbe reports how stale the org activity projection is. SigNoz cannot read Postgres, so the worker that writes the projection
// exports this one number. One database read per cache period, whatever the number of collections (the pull and the push reader
// both collect); a failed or empty read records NO sample (never 0), counts a failure, and is not retried before the period ends.
type AgeProbe struct {
	db     AgeDB
	logger *slog.Logger
	now    func() time.Time

	mu      sync.Mutex
	readAt  time.Time
	value   float64
	hasData bool
	failed  metric.Int64Counter
}

// RegisterAgeProbe registers the gauge and the read-failure counter (seeded at 0) on meter. now nil means time.Now.
func RegisterAgeProbe(meter metric.Meter, db AgeDB, logger *slog.Logger, now func() time.Time) (*AgeProbe, error) {
	if meter == nil || db == nil {
		return nil, errors.New("orgactivity: the age probe needs a meter and a database")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if now == nil {
		now = time.Now
	}
	probe := &AgeProbe{db: db, logger: logger, now: now}
	failed, err := meter.Int64Counter(AgeReadFailedCounterName, metric.WithUnit("{read}"),
		metric.WithDescription("reads of the org activity age (now - max(refreshed_at)) that failed or timed out"))
	if err != nil {
		return nil, err
	}
	failed.Add(context.Background(), 0)
	probe.failed = failed
	if _, err := meter.Float64ObservableGauge(AgeGaugeName, metric.WithUnit("s"),
		metric.WithDescription("seconds since the newest public.org_activity row was refreshed; no sample when the table is empty or unreadable"),
		metric.WithFloat64Callback(probe.observe)); err != nil {
		return nil, err
	}
	return probe, nil
}

func (p *AgeProbe) observe(ctx context.Context, observer metric.Float64Observer) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if p.readAt.IsZero() || now.Sub(p.readAt) >= ageCachePeriod {
		p.read(ctx, now)
	}
	if p.hasData {
		observer.Observe(p.value)
	}
	return nil
}

func (p *AgeProbe) read(ctx context.Context, now time.Time) {
	p.readAt = now
	p.hasData = false
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ageReadTimeout)
	defer cancel()
	var age *float64
	if err := p.db.QueryRow(readCtx, ageSQL).Scan(&age); err != nil {
		p.failed.Add(context.WithoutCancel(ctx), 1)
		p.logger.WarnContext(ctx, "org activity age read failed", "error", err)
		return
	}
	if age == nil {
		return
	}
	p.value, p.hasData = *age, true
}
