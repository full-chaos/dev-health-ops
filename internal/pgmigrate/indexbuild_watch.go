package pgmigrate

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// blocker is one backend that blocks the build backend. Only these fields are ever logged: no query text, no values.
type blocker struct {
	PID           int32
	Application   string
	User          string
	State         string
	XactAgeSecond int64
	WaitEventType string
	Hidden        bool
}

// blockerWatch samples pg_blocking_pids(<build backend>) from a second connection, because the build backend itself is
// not waiting any more by the time its statement has failed. A nil watch does nothing.
type blockerWatch struct {
	conn      *pgx.Conn
	buildPID  int32
	logger    *slog.Logger
	sample    time.Duration
	heartbeat time.Duration
	started   time.Time
	cancel    context.CancelFunc
	done      chan struct{}
	mu        sync.Mutex
	current   []blocker
	last      []blocker
}

const blockersSQL = `
SELECT b.pid, COALESCE(a.application_name, ''), COALESCE(a.usename::text, ''), COALESCE(a.state, ''),
       COALESCE(EXTRACT(EPOCH FROM (now() - a.xact_start))::bigint, -1), COALESCE(a.wait_event_type, '')
FROM unnest(pg_blocking_pids($1::int)) AS b(pid)
LEFT JOIN pg_catalog.pg_stat_activity a ON a.pid = b.pid`

func startBlockerWatch(ctx context.Context, build *pgx.Conn, options IndexBuildOptions, logger *slog.Logger) *blockerWatch {
	var buildPID int32
	if err := build.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&buildPID); err != nil {
		logger.Warn("build-index: blocker watch not started", "stage", "pid", "sqlstate", sqlState(err))
		return nil
	}
	config := build.Config().Copy()
	if config.RuntimeParams == nil {
		config.RuntimeParams = map[string]string{}
	}
	config.RuntimeParams["application_name"] = "dho-build-index-watch"
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		logger.Warn("build-index: blocker watch not started", "stage", "connect", "sqlstate", sqlState(err))
		return nil
	}
	w := &blockerWatch{conn: conn, buildPID: buildPID, logger: logger, sample: options.SampleEvery, heartbeat: options.HeartbeatEvery, started: time.Now(), done: make(chan struct{})}
	if w.sample <= 0 {
		w.sample = defaultSampleEvery
	}
	if w.heartbeat <= 0 {
		w.heartbeat = defaultHeartbeatEvery
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	w.cancel = cancel
	go w.run(runCtx)
	return w
}

func (w *blockerWatch) run(ctx context.Context) {
	defer close(w.done)
	sampleTick := time.NewTicker(w.sample)
	defer sampleTick.Stop()
	heartbeatTick := time.NewTicker(w.heartbeat)
	defer heartbeatTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sampleTick.C:
			rows, err := w.read(ctx)
			if err != nil {
				continue
			}
			w.mu.Lock()
			w.current = rows
			if len(rows) > 0 {
				w.last = rows
			}
			w.mu.Unlock()
		case <-heartbeatTick.C:
			w.mu.Lock()
			current := w.current
			w.mu.Unlock()
			if len(current) > 0 {
				w.logRows("build-index: waiting", "wait", 0, current)
			}
		}
	}
}

func (w *blockerWatch) read(ctx context.Context) ([]blocker, error) {
	rows, err := w.conn.Query(ctx, blockersSQL, w.buildPID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []blocker
	for rows.Next() {
		var b blocker
		if err := rows.Scan(&b.PID, &b.Application, &b.User, &b.State, &b.XactAgeSecond, &b.WaitEventType); err != nil {
			return nil, err
		}
		b.Hidden = b.State == "" || b.State == "<insufficient privilege>"
		out = append(out, b)
	}
	return out, rows.Err()
}

// logBlockers logs the blockers last seen for the failed attempt, then forgets them.
func (w *blockerWatch) logBlockers(stage string, attempt int) {
	if w == nil {
		return
	}
	w.mu.Lock()
	rows := w.last
	w.last = nil
	w.mu.Unlock()
	if len(rows) == 0 {
		w.logger.Warn("build-index: no blocker seen", "stage", stage, "attempt", attempt)
		return
	}
	w.logRows("build-index: blocker", stage, attempt, rows)
}

func (w *blockerWatch) logRows(message, stage string, attempt int, rows []blocker) {
	for _, b := range rows {
		w.logger.Warn(message, "stage", stage, "attempt", attempt, "elapsed_s", int64(time.Since(w.started).Seconds()),
			"pid", b.PID, "application_name", b.Application, "usename", b.User, "state", b.State,
			"xact_age_s", b.XactAgeSecond, "wait_event_type", b.WaitEventType, "hidden", b.Hidden)
	}
}

func (w *blockerWatch) stop() {
	if w == nil {
		return
	}
	w.cancel()
	<-w.done
	w.conn.Close(context.Background())
}
