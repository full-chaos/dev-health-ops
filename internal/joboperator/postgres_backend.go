package joboperator

import (
	"context"
	"errors"
	"regexp"
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

var (
	ErrBackendConfiguration = errors.New("worker operator backend configuration is invalid")
	riverSchemaIdentifier   = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
)

const mutationAdvisoryKey = int64(30330002)

// PostgresBackend implements the operator Backend against a direct River
// PostgreSQL endpoint. It uses River's supported transactional mutation APIs
// after locking and comparing the state observed by Service, preserving River
// notifications while adding the required state CAS.
type PostgresBackend struct {
	pool       *pgxpool.Pool
	client     *river.Client[pgx.Tx]
	registry   RuntimeRegistry
	jobTable   string
	queueTable string
}

func NewDirectPostgresBackend(pool *pgxpool.Pool, schema string, registry RuntimeRegistry) (*PostgresBackend, error) {
	if pool == nil || registry == nil || !riverSchemaIdentifier.MatchString(schema) {
		return nil, ErrBackendConfiguration
	}
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{Schema: schema})
	if err != nil {
		return nil, ErrBackendConfiguration
	}
	return &PostgresBackend{
		pool:       pool,
		client:     client,
		registry:   registry,
		jobTable:   pgx.Identifier{schema, "river_job"}.Sanitize(),
		queueTable: pgx.Identifier{schema, "river_queue"}.Sanitize(),
	}, nil
}

func (backend *PostgresBackend) SupportsRunningCancellation() bool { return true }

func (backend *PostgresBackend) Get(ctx context.Context, id int64) (JobSummary, error) {
	if backend == nil || backend.pool == nil || id < 1 {
		return JobSummary{}, ErrBackendConfiguration
	}
	return scanProjectedSummary(backend.pool.QueryRow(ctx, backend.summaryQuery()+" WHERE id = $1", id))
}

func (backend *PostgresBackend) List(ctx context.Context, filter ListFilter) ([]JobSummary, error) {
	if backend == nil || backend.pool == nil {
		return nil, ErrBackendConfiguration
	}
	states := make([]string, len(filter.States))
	for index, state := range filter.States {
		states[index] = string(state)
	}
	rows, err := backend.pool.Query(ctx, backend.summaryQuery()+`
		WHERE state::text = ANY($1::text[])
			AND ($2::text = '' OR kind = $2)
			AND ($3::text = '' OR queue = $3)
		ORDER BY id DESC
		LIMIT $4`, states, filter.Kind, filter.Queue, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]JobSummary, 0)
	for rows.Next() {
		summary, scanErr := scanProjectedSummary(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (backend *PostgresBackend) Queues(ctx context.Context, profile string) ([]QueueSummary, error) {
	if backend == nil || backend.pool == nil {
		return nil, ErrBackendConfiguration
	}
	queues := backend.profileQueues(profile)
	if len(queues) == 0 {
		return nil, ErrBackendConfiguration
	}
	query := `
		WITH expected(name) AS (SELECT unnest($1::text[])), counts AS (
			SELECT queue,
				count(*) FILTER (WHERE state = 'available') AS available,
				count(*) FILTER (WHERE state = 'running') AS running,
				count(*) FILTER (WHERE state = 'retryable') AS retryable,
				count(*) FILTER (WHERE state = 'scheduled') AS scheduled,
				min(scheduled_at) FILTER (WHERE state = 'available') AS oldest_available_at
			FROM ` + backend.jobTable + `
			WHERE queue = ANY($1::text[])
			GROUP BY queue
		)
		SELECT expected.name, queue.paused_at IS NOT NULL,
			coalesce(counts.available, 0), coalesce(counts.running, 0),
			coalesce(counts.retryable, 0), coalesce(counts.scheduled, 0),
			counts.oldest_available_at
		FROM expected
		LEFT JOIN ` + backend.queueTable + ` AS queue ON queue.name = expected.name
		LEFT JOIN counts ON counts.queue = expected.name
		ORDER BY expected.name`
	rows, err := backend.pool.Query(ctx, query, queues)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]QueueSummary, 0, len(queues))
	for rows.Next() {
		var summary QueueSummary
		summary.Profile = profile
		if err := rows.Scan(
			&summary.Name,
			&summary.Paused,
			&summary.Available,
			&summary.Running,
			&summary.Retryable,
			&summary.Scheduled,
			&summary.OldestAvailableAt,
		); err != nil {
			return nil, err
		}
		result = append(result, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (backend *PostgresBackend) Cancel(ctx context.Context, id int64, mutation Mutation) (JobSummary, error) {
	if backend == nil || backend.pool == nil || backend.client == nil {
		return JobSummary{}, ErrBackendConfiguration
	}
	tx, err := backend.beginMutation(ctx)
	if err != nil {
		return JobSummary{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := backend.compareLockedState(ctx, tx, id, mutation.ExpectedState); err != nil {
		return JobSummary{}, err
	}
	row, err := backend.client.JobCancelTx(ctx, tx, id)
	if err != nil {
		return JobSummary{}, mapRiverError(err)
	}
	summary, err := summaryFromRiverRow(row)
	if err != nil {
		return JobSummary{}, err
	}
	if err := commitMutation(ctx, tx); err != nil {
		return JobSummary{}, err
	}
	return summary, nil
}

func (backend *PostgresBackend) Retry(ctx context.Context, id int64, mutation Mutation) (JobSummary, error) {
	if backend == nil || backend.pool == nil || backend.client == nil {
		return JobSummary{}, ErrBackendConfiguration
	}
	tx, err := backend.beginMutation(ctx)
	if err != nil {
		return JobSummary{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := backend.compareLockedState(ctx, tx, id, mutation.ExpectedState); err != nil {
		return JobSummary{}, err
	}
	row, err := backend.client.JobRetryTx(ctx, tx, id)
	if err != nil {
		return JobSummary{}, mapRiverError(err)
	}
	summary, err := summaryFromRiverRow(row)
	if err != nil {
		return JobSummary{}, err
	}
	if err := commitMutation(ctx, tx); err != nil {
		return JobSummary{}, err
	}
	return summary, nil
}

func (backend *PostgresBackend) PauseQueue(ctx context.Context, queue string, _ Mutation) error {
	return backend.setQueuePaused(ctx, queue, true)
}

func (backend *PostgresBackend) ResumeQueue(ctx context.Context, queue string, _ Mutation) error {
	return backend.setQueuePaused(ctx, queue, false)
}

func (backend *PostgresBackend) Drain(ctx context.Context, profile string, _ Mutation) (DrainResult, error) {
	if backend == nil || backend.pool == nil || backend.client == nil {
		return DrainResult{}, ErrBackendConfiguration
	}
	queues := backend.profileQueues(profile)
	if len(queues) == 0 {
		return DrainResult{}, ErrBackendConfiguration
	}
	tx, err := backend.beginMutation(ctx)
	if err != nil {
		return DrainResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, "SELECT name FROM "+backend.queueTable+" WHERE name = ANY($1::text[]) ORDER BY name FOR UPDATE", queues)
	if err != nil {
		return DrainResult{}, err
	}
	locked := 0
	for rows.Next() {
		var queue string
		if err := rows.Scan(&queue); err != nil {
			rows.Close()
			return DrainResult{}, err
		}
		locked++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return DrainResult{}, err
	}
	if locked != len(queues) {
		return DrainResult{}, ErrStateConflict
	}
	var running int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+backend.jobTable+" WHERE queue = ANY($1::text[]) AND state = 'running'", queues).Scan(&running); err != nil {
		return DrainResult{}, err
	}
	for _, queue := range queues {
		if err := backend.client.QueuePauseTx(ctx, tx, queue, nil); err != nil {
			return DrainResult{}, err
		}
	}
	if err := commitMutation(ctx, tx); err != nil {
		return DrainResult{}, err
	}
	return DrainResult{Profile: profile, QueuesPaused: len(queues), RunningAtStart: running}, nil
}

func (backend *PostgresBackend) setQueuePaused(ctx context.Context, queue string, paused bool) error {
	if backend == nil || backend.pool == nil || backend.client == nil {
		return ErrBackendConfiguration
	}
	tx, err := backend.beginMutation(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var existing string
	if err := tx.QueryRow(ctx, "SELECT name FROM "+backend.queueTable+" WHERE name = $1 FOR UPDATE", queue).Scan(&existing); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if paused {
		err = backend.client.QueuePauseTx(ctx, tx, queue, nil)
	} else {
		err = backend.client.QueueResumeTx(ctx, tx, queue, nil)
	}
	if err != nil {
		return err
	}
	return commitMutation(ctx, tx)
}

// beginMutation takes a transaction-scoped lock in the same transaction as
// the River state change. The command-level invocation lock rejects ordinary
// overlap; this second lock preserves mutation serialization if that separate
// lock connection drops while an invocation is still running.
func (backend *PostgresBackend) beginMutation(ctx context.Context) (pgx.Tx, error) {
	if backend == nil || backend.pool == nil {
		return nil, ErrBackendConfiguration
	}
	tx, err := backend.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", mutationAdvisoryKey); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

func commitMutation(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		return ErrMutationOutcomeUnknown
	}
	return nil
}

func (backend *PostgresBackend) compareLockedState(ctx context.Context, tx pgx.Tx, id int64, expected JobState) error {
	var actual string
	err := tx.QueryRow(ctx, "SELECT state::text FROM "+backend.jobTable+" WHERE id = $1 FOR UPDATE", id).Scan(&actual)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if JobState(actual) != expected {
		return ErrStateConflict
	}
	return nil
}

func (backend *PostgresBackend) profileQueues(profile string) []string {
	descriptors := backend.registry.Profile(profile)
	seen := make(map[string]struct{}, len(descriptors))
	queues := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		if _, duplicate := seen[descriptor.Queue]; duplicate {
			continue
		}
		seen[descriptor.Queue] = struct{}{}
		queues = append(queues, descriptor.Queue)
	}
	sort.Strings(queues)
	return queues
}

func (backend *PostgresBackend) summaryQuery() string {
	return `SELECT id, kind, queue, state::text, attempt, max_attempts,
		created_at, scheduled_at, attempted_at, finalized_at,
		args->>'correlation_id', nullif(args->>'organization_id', ''),
		args->'domain'->>'type', args->'domain'->>'id'
		FROM ` + backend.jobTable
}

type summaryScanner interface {
	Scan(...any) error
}

func scanProjectedSummary(scanner summaryScanner) (JobSummary, error) {
	var summary JobSummary
	var state string
	if err := scanner.Scan(
		&summary.ID,
		&summary.Kind,
		&summary.Queue,
		&state,
		&summary.Attempt,
		&summary.MaxAttempts,
		&summary.CreatedAt,
		&summary.ScheduledAt,
		&summary.AttemptedAt,
		&summary.FinalizedAt,
		&summary.CorrelationID,
		&summary.OrganizationID,
		&summary.Domain.Type,
		&summary.Domain.ID,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return JobSummary{}, ErrNotFound
		}
		return JobSummary{}, err
	}
	summary.State = JobState(state)
	return summary, nil
}

func summaryFromRiverRow(row *rivertype.JobRow) (JobSummary, error) {
	if row == nil {
		return JobSummary{}, ErrBackendConfiguration
	}
	envelope, err := jobcontract.Decode(row.Kind, row.EncodedArgs)
	if err != nil {
		return JobSummary{}, err
	}
	return JobSummary{
		ID:             row.ID,
		Kind:           row.Kind,
		Queue:          row.Queue,
		State:          JobState(row.State),
		Attempt:        row.Attempt,
		MaxAttempts:    row.MaxAttempts,
		CreatedAt:      row.CreatedAt,
		ScheduledAt:    row.ScheduledAt,
		AttemptedAt:    row.AttemptedAt,
		FinalizedAt:    row.FinalizedAt,
		CorrelationID:  envelope.CorrelationID,
		OrganizationID: envelope.OrganizationID,
		Domain:         envelope.Domain,
	}, nil
}

func mapRiverError(err error) error {
	if errors.Is(err, river.ErrNotFound) {
		return ErrNotFound
	}
	return err
}
