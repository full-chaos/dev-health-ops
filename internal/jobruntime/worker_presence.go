package jobruntime

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultWorkerPresenceTTL = 30 * time.Second
	maxWorkerGroupLength     = 64
	maxWorkerQueues          = 64
	maxWorkerQueueLength     = 96
)

var (
	workerGroupPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
	workerQueuePattern = regexp.MustCompile(`^[a-z][a-z0-9._-]*$`)
)

// WorkerPresenceSummary contains process counts for one worker group and its
// selected queue set. It cannot expose a process identifier, River arguments,
// or job payloads through an operator surface.
type WorkerPresenceSummary struct {
	WorkerGroup string
	Queues      []string
	Live        int
	Draining    int
}

// WorkerPresence is one expiring worker-instance registration. The row is
// renewed while the process accepts work, marked draining before River clients
// stop, and removed only after they have stopped.
type WorkerPresence struct {
	pool        *pgxpool.Pool
	instanceID  uuid.UUID
	workerGroup string
	queues      []string
	queuesJSON  string
	ttl         time.Duration

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
	errors chan error
}

// NewWorkerPresence validates and canonicalizes the worker identity and the
// selected queue set before any database state is created. Queue order is
// normalized so equivalent worker replicas share one presence identity.
func NewWorkerPresence(pool *pgxpool.Pool, workerGroup string, queues []string, instanceID string) (*WorkerPresence, error) {
	parsed, err := uuid.Parse(instanceID)
	canonicalQueues, queuesJSON, queuesErr := canonicalWorkerQueues(queues)
	if pool == nil || err != nil || !validWorkerGroup(workerGroup) || queuesErr != nil {
		return nil, errors.New("worker presence configuration is invalid")
	}
	return &WorkerPresence{
		pool: pool, instanceID: parsed, workerGroup: workerGroup,
		queues: canonicalQueues, queuesJSON: queuesJSON,
		ttl: defaultWorkerPresenceTTL, errors: make(chan error, 1),
	}, nil
}

func (presence *WorkerPresence) Name() string { return "worker-instance-presence" }

func (presence *WorkerPresence) Start(ctx context.Context) error {
	if presence == nil || presence.pool == nil || ctx == nil {
		return errors.New("worker presence is unavailable")
	}
	presence.mu.Lock()
	defer presence.mu.Unlock()
	if presence.cancel != nil {
		return errors.New("worker presence is already started")
	}
	result, err := presence.pool.Exec(ctx, `
		INSERT INTO public.worker_instances (
			instance_id, worker_group, queues, state, started_at, heartbeat_at, expires_at
		) VALUES ($1, $2, $3, 'accepting', statement_timestamp(), statement_timestamp(),
			statement_timestamp() + $4::interval)
		ON CONFLICT (instance_id) DO NOTHING`,
		presence.instanceID, presence.workerGroup, presence.queuesJSON, presence.ttl.String(),
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return errors.New("worker presence identity is already registered")
	}
	runCtx, cancel := context.WithCancel(context.Background())
	presence.cancel = cancel
	presence.done = make(chan struct{})
	go presence.renew(runCtx, presence.done)
	return nil
}

func (presence *WorkerPresence) BeginDrain(ctx context.Context) error {
	if presence == nil {
		return errors.New("worker presence is unavailable")
	}
	return presence.update(ctx, `
		UPDATE public.worker_instances
		SET state = 'draining', heartbeat_at = statement_timestamp(),
			expires_at = statement_timestamp() + $2::interval
		WHERE instance_id = $1`, presence.instanceID, presence.ttl.String())
}

func (presence *WorkerPresence) Shutdown(ctx context.Context) error {
	if presence == nil || presence.pool == nil || ctx == nil {
		return errors.New("worker presence is unavailable")
	}
	presence.mu.Lock()
	cancel, done := presence.cancel, presence.done
	presence.cancel = nil
	presence.done = nil
	presence.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
	return presence.update(ctx, `DELETE FROM public.worker_instances WHERE instance_id = $1`, presence.instanceID)
}

// Errors never emits. Presence is observability-only, so a renewal failure is
// logged and retried rather than surfaced to lifecycle.Runtime, which treats
// any component error as fatal. The channel stays part of the component
// contract so the runtime's error-source plumbing is uniform.
func (presence *WorkerPresence) Errors() <-chan error {
	if presence == nil {
		return nil
	}
	return presence.errors
}

func (presence *WorkerPresence) renew(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(presence.ttl / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(context.Background(), presence.ttl/3)
			err := presence.update(renewCtx, `
				UPDATE public.worker_instances
				SET heartbeat_at = statement_timestamp(),
					expires_at = statement_timestamp() + $2::interval
				WHERE instance_id = $1`, presence.instanceID, presence.ttl.String())
			cancel()
			if err != nil {
				// Presence is observability-only: it feeds worker_instances
				// counts, nothing claims or executes work through it. Treating
				// the first failed heartbeat as fatal meant a 10-second domain
				// database hiccup -- a failover, a PgBouncer restart -- drained
				// every replica at once, a self-inflicted fleet-wide restart
				// (CHAOS-3866). Keep ticking instead: the TTL is three
				// intervals wide, and a recovered database revives the row on
				// the next successful renewal.
				slog.Default().WarnContext(ctx, "worker presence heartbeat failed",
					"error_category", "worker_presence_renewal",
					"worker_group", presence.workerGroup,
					"worker_instance_id", presence.instanceID.String(),
				)
			}
		}
	}
}

func (presence *WorkerPresence) update(ctx context.Context, query string, arguments ...any) error {
	if presence == nil || presence.pool == nil || ctx == nil {
		return errors.New("worker presence is unavailable")
	}
	result, err := presence.pool.Exec(ctx, query, arguments...)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return errors.New("worker presence ownership was lost")
	}
	return nil
}

func ReadWorkerPresence(ctx context.Context, pool *pgxpool.Pool) ([]WorkerPresenceSummary, error) {
	if pool == nil || ctx == nil {
		return nil, errors.New("worker presence is unavailable")
	}
	rows, err := pool.Query(ctx, `
		SELECT worker_group, queues::text, count(*)::integer,
			count(*) FILTER (WHERE state = 'draining')::integer
		FROM public.worker_instances
		WHERE expires_at > statement_timestamp()
		GROUP BY worker_group, queues::text
		ORDER BY worker_group, queues::text`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var summaries []WorkerPresenceSummary
	for rows.Next() {
		var summary WorkerPresenceSummary
		var encodedQueues string
		if err := rows.Scan(&summary.WorkerGroup, &encodedQueues, &summary.Live, &summary.Draining); err != nil {
			return nil, err
		}
		queues, err := decodeCanonicalWorkerQueues(encodedQueues)
		if err != nil || !validWorkerGroup(summary.WorkerGroup) || summary.Live < 1 ||
			summary.Draining < 0 || summary.Draining > summary.Live {
			return nil, errors.New("invalid worker presence summary")
		}
		summary.Queues = queues
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return summaries, nil
}

func validWorkerGroup(value string) bool {
	return len(value) >= 1 && len(value) <= maxWorkerGroupLength && workerGroupPattern.MatchString(value)
}

func canonicalWorkerQueues(values []string) ([]string, string, error) {
	if len(values) == 0 || len(values) > maxWorkerQueues {
		return nil, "", errors.New("worker queue set is invalid")
	}
	queues := append([]string(nil), values...)
	for _, queue := range queues {
		if len(queue) == 0 || len(queue) > maxWorkerQueueLength || !workerQueuePattern.MatchString(queue) {
			return nil, "", errors.New("worker queue set is invalid")
		}
	}
	sort.Strings(queues)
	for index := 1; index < len(queues); index++ {
		if queues[index] == queues[index-1] {
			return nil, "", errors.New("worker queue set is not unique")
		}
	}
	encoded, err := json.Marshal(queues)
	if err != nil {
		return nil, "", err
	}
	return queues, string(encoded), nil
}

func decodeCanonicalWorkerQueues(encoded string) ([]string, error) {
	var queues []string
	if err := json.Unmarshal([]byte(encoded), &queues); err != nil {
		return nil, err
	}
	canonical, canonicalJSON, err := canonicalWorkerQueues(queues)
	if err != nil || canonicalJSON != encoded {
		return nil, errors.New("worker queue set is not canonical")
	}
	return canonical, nil
}
