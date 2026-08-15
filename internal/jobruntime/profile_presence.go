package jobruntime

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultProfilePresenceTTL = 30 * time.Second

var presenceProfilePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

// ProfilePresenceSummary contains only process counts. It cannot expose a
// process identifier, River arguments, or job payloads through workerctl.
type ProfilePresenceSummary struct {
	Profile  string
	Live     int
	Draining int
}

// ProfilePresence is one expiring process registration. The row is renewed
// while the process is healthy, marked draining before River clients stop,
// and removed only after they have stopped.
type ProfilePresence struct {
	pool       *pgxpool.Pool
	instanceID uuid.UUID
	profile    string
	ttl        time.Duration

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
	errors chan error
}

func NewProfilePresence(pool *pgxpool.Pool, profile, instanceID string) (*ProfilePresence, error) {
	parsed, err := uuid.Parse(instanceID)
	if pool == nil || err != nil || !presenceProfilePattern.MatchString(profile) {
		return nil, errors.New("profile presence configuration is invalid")
	}
	return &ProfilePresence{
		pool: pool, instanceID: parsed, profile: profile, ttl: defaultProfilePresenceTTL,
		errors: make(chan error, 1),
	}, nil
}

func (presence *ProfilePresence) Name() string { return "worker-profile-presence" }

func (presence *ProfilePresence) Start(ctx context.Context) error {
	if presence == nil || presence.pool == nil || ctx == nil {
		return errors.New("profile presence is unavailable")
	}
	presence.mu.Lock()
	defer presence.mu.Unlock()
	if presence.cancel != nil {
		return errors.New("profile presence is already started")
	}
	result, err := presence.pool.Exec(ctx, `
		INSERT INTO public.worker_profile_instances (
			instance_id, profile, state, started_at, heartbeat_at, expires_at
		) VALUES ($1, $2, 'active', statement_timestamp(), statement_timestamp(),
			statement_timestamp() + $3::interval)
		ON CONFLICT (instance_id) DO NOTHING`,
		presence.instanceID, presence.profile, presence.ttl.String(),
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return errors.New("profile presence identity is already registered")
	}
	runCtx, cancel := context.WithCancel(context.Background())
	presence.cancel = cancel
	presence.done = make(chan struct{})
	go presence.renew(runCtx, presence.done)
	return nil
}

func (presence *ProfilePresence) BeginDrain(ctx context.Context) error {
	return presence.update(ctx, `
		UPDATE public.worker_profile_instances
		SET state = 'draining', heartbeat_at = statement_timestamp(),
			expires_at = statement_timestamp() + $2::interval
		WHERE instance_id = $1`, presence.instanceID, presence.ttl.String())
}

func (presence *ProfilePresence) Shutdown(ctx context.Context) error {
	if presence == nil || presence.pool == nil || ctx == nil {
		return errors.New("profile presence is unavailable")
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
	return presence.update(ctx, `DELETE FROM public.worker_profile_instances WHERE instance_id = $1`, presence.instanceID)
}

func (presence *ProfilePresence) Errors() <-chan error {
	if presence == nil {
		return nil
	}
	return presence.errors
}

func (presence *ProfilePresence) renew(ctx context.Context, done chan<- struct{}) {
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
				UPDATE public.worker_profile_instances
				SET heartbeat_at = statement_timestamp(),
					expires_at = statement_timestamp() + $2::interval
				WHERE instance_id = $1`, presence.instanceID, presence.ttl.String())
			cancel()
			if err != nil {
				select {
				case presence.errors <- err:
				default:
				}
				return
			}
		}
	}
}

func (presence *ProfilePresence) update(ctx context.Context, query string, arguments ...any) error {
	result, err := presence.pool.Exec(ctx, query, arguments...)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return errors.New("profile presence ownership was lost")
	}
	return nil
}

func ReadProfilePresence(ctx context.Context, pool *pgxpool.Pool) ([]ProfilePresenceSummary, error) {
	if pool == nil || ctx == nil {
		return nil, errors.New("profile presence is unavailable")
	}
	rows, err := pool.Query(ctx, `
		SELECT profile, count(*)::integer,
			count(*) FILTER (WHERE state = 'draining')::integer
		FROM public.worker_profile_instances
		WHERE expires_at > statement_timestamp()
		GROUP BY profile
		ORDER BY profile`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var summaries []ProfilePresenceSummary
	for rows.Next() {
		var summary ProfilePresenceSummary
		if err := rows.Scan(&summary.Profile, &summary.Live, &summary.Draining); err != nil {
			return nil, err
		}
		if !presenceProfilePattern.MatchString(summary.Profile) || summary.Live < 1 ||
			summary.Draining < 0 || summary.Draining > summary.Live {
			return nil, fmt.Errorf("invalid profile presence summary")
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return summaries, nil
}
