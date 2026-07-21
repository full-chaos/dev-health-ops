package riverstore

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultQueueTelemetryTimeout     = 2 * time.Second
	maximumQueueTelemetryTimeout     = 30 * time.Second
	maximumQueueTelemetryJobs        = 512
	maximumQueueTelemetryQueues      = 64
	maximumSupportedVersionsPerJob   = 16
	maximumQueueTelemetryLabelLength = 96
)

var (
	ErrQueueTelemetryConfiguration = errors.New("invalid River queue telemetry configuration")
	ErrQueueTelemetryUnavailable   = errors.New("River queue telemetry is unavailable")
	// ErrUnsupportedAvailableContractVersion is deliberately stable and does
	// not disclose a River row, kind, version, or encoded argument.
	ErrUnsupportedAvailableContractVersion = errors.New("an available River job has an unsupported contract version")
)

// QueueTelemetryQueue is one queue consumed by a River client. MaxWorkers
// must be the same value supplied to river.QueueConfig for this process.
type QueueTelemetryQueue struct {
	Name       string
	MaxWorkers int
}

// QueueTelemetryJob is one pre-registered queue/kind pair and its complete
// bounded consumer-version window.
type QueueTelemetryJob struct {
	Queue             string
	Kind              string
	SupportedVersions []int
}

// QueueTelemetryConfig binds database observations to one concrete River
// client. ClientID must be client.ID(), after River has applied defaults.
type QueueTelemetryConfig struct {
	Schema       string
	Profile      string
	ClientID     string
	QueryTimeout time.Duration
	Queues       []QueueTelemetryQueue
	Jobs         []QueueTelemetryJob
}

// QueueJobTelemetry contains only pre-registered, low-cardinality labels.
type QueueJobTelemetry struct {
	Queue     string
	Kind      string
	Available int64
}

// QueueAgeTelemetry reports time since the oldest registered job became
// eligible for fetching. A queue with no eligible jobs has age zero.
type QueueAgeTelemetry struct {
	Queue              string
	OldestAvailableAge time.Duration
}

// QueueTelemetrySnapshot is one database-consistent, read-only observation.
// LocalRunning is restricted to jobs whose latest attempted_by entry is this
// River client ID, so the saturation denominator is this process's capacity,
// not a fleet-wide count.
type QueueTelemetrySnapshot struct {
	Profile             string
	Jobs                []QueueJobTelemetry
	Queues              []QueueAgeTelemetry
	LocalRunning        int64
	ExecutionSaturation float64
}

type queueJobKey struct {
	queue string
	kind  string
}

type normalizedQueueTelemetryConfig struct {
	profile           string
	clientID          string
	queryTimeout      time.Duration
	executionCapacity int64
	queues            []QueueTelemetryQueue
	jobs              []QueueTelemetryJob
	jobQueues         []string
	jobKinds          []string
	queueNames        []string
	supportedQueues   []string
	supportedKinds    []string
	supportedVersions []int32
}

type queueTelemetryRow struct {
	queue                string
	kind                 string
	available            int64
	oldestAgeSeconds     float64
	localRunning         int64
	unsupportedAvailable bool
}

type queueTelemetryReadFunc func(context.Context) ([]queueTelemetryRow, error)

// QueueTelemetrySampler performs one bounded SELECT against the pinned River
// schema. It never starts a River client, claims a job, locks a row, or applies
// a migration.
type QueueTelemetrySampler struct {
	config normalizedQueueTelemetryConfig
	read   queueTelemetryReadFunc
}

func NewQueueTelemetrySampler(pool *pgxpool.Pool, config QueueTelemetryConfig) (*QueueTelemetrySampler, error) {
	if pool == nil {
		return nil, ErrQueueTelemetryConfiguration
	}
	normalized, err := normalizeQueueTelemetryConfig(config)
	if err != nil {
		return nil, err
	}
	table := pgx.Identifier{config.Schema, "river_job"}.Sanitize()
	return &QueueTelemetrySampler{
		config: normalized,
		read: func(ctx context.Context) ([]queueTelemetryRow, error) {
			return readQueueTelemetry(ctx, pool, table, normalized)
		},
	}, nil
}

// Snapshot reads current fetchable counts, oldest queue ages, and this
// client's execution saturation. Unsupported available contracts do not hide
// backlog metrics; readiness checks them independently with
// CheckAvailableContractVersions.
func (sampler *QueueTelemetrySampler) Snapshot(ctx context.Context) (QueueTelemetrySnapshot, error) {
	result, _, err := sampler.sample(ctx)
	return result, err
}

// CheckAvailableContractVersions fails closed if any state=available row in a
// configured profile queue has an unknown queue/kind pairing, a missing or
// non-integer contract_version, or a version outside the registered window.
// It returns stable errors only and never loads or returns encoded arguments.
func (sampler *QueueTelemetrySampler) CheckAvailableContractVersions(ctx context.Context) error {
	_, unsupported, err := sampler.sample(ctx)
	if err != nil {
		return err
	}
	if unsupported {
		return ErrUnsupportedAvailableContractVersion
	}
	return nil
}

func (sampler *QueueTelemetrySampler) sample(ctx context.Context) (QueueTelemetrySnapshot, bool, error) {
	if sampler == nil || sampler.read == nil || ctx == nil {
		return QueueTelemetrySnapshot{}, false, ErrQueueTelemetryUnavailable
	}
	queryContext, cancel := context.WithTimeout(ctx, sampler.config.queryTimeout)
	defer cancel()
	rows, err := sampler.read(queryContext)
	if err != nil {
		return QueueTelemetrySnapshot{}, false, ErrQueueTelemetryUnavailable
	}
	return sampler.snapshot(rows)
}

func (sampler *QueueTelemetrySampler) snapshot(rows []queueTelemetryRow) (QueueTelemetrySnapshot, bool, error) {
	if len(rows) != len(sampler.config.jobs) || len(rows) == 0 {
		return QueueTelemetrySnapshot{}, false, ErrQueueTelemetryUnavailable
	}
	available := make(map[queueJobKey]int64, len(rows))
	ages := make(map[string]time.Duration, len(sampler.config.queues))
	seenAges := make(map[string]float64, len(sampler.config.queues))
	var localRunning int64
	var unsupported bool
	for index, row := range rows {
		key := queueJobKey{queue: row.queue, kind: row.kind}
		if row.available < 0 || row.localRunning < 0 || math.IsNaN(row.oldestAgeSeconds) ||
			math.IsInf(row.oldestAgeSeconds, 0) || row.oldestAgeSeconds < 0 {
			return QueueTelemetrySnapshot{}, false, ErrQueueTelemetryUnavailable
		}
		if _, duplicate := available[key]; duplicate || !sampler.hasJob(key) {
			return QueueTelemetrySnapshot{}, false, ErrQueueTelemetryUnavailable
		}
		available[key] = row.available
		if previous, ok := seenAges[row.queue]; ok && previous != row.oldestAgeSeconds {
			return QueueTelemetrySnapshot{}, false, ErrQueueTelemetryUnavailable
		}
		seenAges[row.queue] = row.oldestAgeSeconds
		ages[row.queue] = durationFromSeconds(row.oldestAgeSeconds)
		if index == 0 {
			localRunning = row.localRunning
			unsupported = row.unsupportedAvailable
		} else if localRunning != row.localRunning || unsupported != row.unsupportedAvailable {
			return QueueTelemetrySnapshot{}, false, ErrQueueTelemetryUnavailable
		}
	}

	result := QueueTelemetrySnapshot{
		Profile:             sampler.config.profile,
		Jobs:                make([]QueueJobTelemetry, 0, len(sampler.config.jobs)),
		Queues:              make([]QueueAgeTelemetry, 0, len(sampler.config.queues)),
		LocalRunning:        localRunning,
		ExecutionSaturation: executionSaturation(localRunning, sampler.config.executionCapacity),
	}
	for _, job := range sampler.config.jobs {
		key := queueJobKey{queue: job.Queue, kind: job.Kind}
		result.Jobs = append(result.Jobs, QueueJobTelemetry{
			Queue: job.Queue, Kind: job.Kind, Available: available[key],
		})
	}
	for _, queue := range sampler.config.queues {
		result.Queues = append(result.Queues, QueueAgeTelemetry{
			Queue: queue.Name, OldestAvailableAge: ages[queue.Name],
		})
	}
	return result, unsupported, nil
}

func (sampler *QueueTelemetrySampler) hasJob(key queueJobKey) bool {
	for _, job := range sampler.config.jobs {
		if job.Queue == key.queue && job.Kind == key.kind {
			return true
		}
	}
	return false
}

func normalizeQueueTelemetryConfig(config QueueTelemetryConfig) (normalizedQueueTelemetryConfig, error) {
	if !validIdentifier(config.Schema) || !telemetryLabel(config.Profile, 32) ||
		len(config.ClientID) == 0 || len(config.ClientID) > 100 || strings.ContainsRune(config.ClientID, '\x00') ||
		len(config.Queues) == 0 || len(config.Queues) > maximumQueueTelemetryQueues ||
		len(config.Jobs) == 0 || len(config.Jobs) > maximumQueueTelemetryJobs ||
		config.QueryTimeout < 0 || config.QueryTimeout > maximumQueueTelemetryTimeout {
		return normalizedQueueTelemetryConfig{}, ErrQueueTelemetryConfiguration
	}
	timeout := config.QueryTimeout
	if timeout == 0 {
		timeout = defaultQueueTelemetryTimeout
	}

	queues := append([]QueueTelemetryQueue(nil), config.Queues...)
	sort.Slice(queues, func(left, right int) bool { return queues[left].Name < queues[right].Name })
	queueSet := make(map[string]QueueTelemetryQueue, len(queues))
	var capacity int64
	for _, queue := range queues {
		if !telemetryLabel(queue.Name, maximumQueueTelemetryLabelLength) || queue.MaxWorkers < 1 || queue.MaxWorkers > 10_000 {
			return normalizedQueueTelemetryConfig{}, ErrQueueTelemetryConfiguration
		}
		if _, duplicate := queueSet[queue.Name]; duplicate {
			return normalizedQueueTelemetryConfig{}, ErrQueueTelemetryConfiguration
		}
		queueSet[queue.Name] = queue
		capacity += int64(queue.MaxWorkers)
	}

	jobs := make([]QueueTelemetryJob, len(config.Jobs))
	for index, job := range config.Jobs {
		jobs[index] = QueueTelemetryJob{
			Queue: job.Queue, Kind: job.Kind, SupportedVersions: append([]int(nil), job.SupportedVersions...),
		}
	}
	sort.Slice(jobs, func(left, right int) bool {
		if jobs[left].Queue != jobs[right].Queue {
			return jobs[left].Queue < jobs[right].Queue
		}
		return jobs[left].Kind < jobs[right].Kind
	})
	jobSet := make(map[queueJobKey]struct{}, len(jobs))
	usedQueues := make(map[string]struct{}, len(queues))
	for _, job := range jobs {
		if !telemetryLabel(job.Queue, maximumQueueTelemetryLabelLength) ||
			!telemetryLabel(job.Kind, maximumQueueTelemetryLabelLength) || len(job.SupportedVersions) == 0 ||
			len(job.SupportedVersions) > maximumSupportedVersionsPerJob {
			return normalizedQueueTelemetryConfig{}, ErrQueueTelemetryConfiguration
		}
		if _, ok := queueSet[job.Queue]; !ok {
			return normalizedQueueTelemetryConfig{}, ErrQueueTelemetryConfiguration
		}
		key := queueJobKey{queue: job.Queue, kind: job.Kind}
		if _, duplicate := jobSet[key]; duplicate {
			return normalizedQueueTelemetryConfig{}, ErrQueueTelemetryConfiguration
		}
		jobSet[key] = struct{}{}
		usedQueues[job.Queue] = struct{}{}
		previous := 0
		for _, version := range job.SupportedVersions {
			if version < 1 || version <= previous || version > math.MaxInt32 {
				return normalizedQueueTelemetryConfig{}, ErrQueueTelemetryConfiguration
			}
			previous = version
		}
	}
	if len(usedQueues) != len(queueSet) {
		return normalizedQueueTelemetryConfig{}, ErrQueueTelemetryConfiguration
	}

	normalized := normalizedQueueTelemetryConfig{
		profile:           config.Profile,
		clientID:          config.ClientID,
		queryTimeout:      timeout,
		executionCapacity: capacity,
		queues:            queues,
		jobs:              jobs,
	}
	for _, queue := range queues {
		normalized.queueNames = append(normalized.queueNames, queue.Name)
	}
	for _, job := range jobs {
		normalized.jobQueues = append(normalized.jobQueues, job.Queue)
		normalized.jobKinds = append(normalized.jobKinds, job.Kind)
		for _, version := range job.SupportedVersions {
			normalized.supportedQueues = append(normalized.supportedQueues, job.Queue)
			normalized.supportedKinds = append(normalized.supportedKinds, job.Kind)
			normalized.supportedVersions = append(normalized.supportedVersions, int32(version))
		}
	}
	return normalized, nil
}

func telemetryLabel(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') && character != '.' && character != '_' &&
			character != '-' && character != ':' {
			return false
		}
	}
	return true
}

func executionSaturation(running, capacity int64) float64 {
	if running <= 0 || capacity <= 0 {
		return 0
	}
	if running >= capacity {
		return 1
	}
	return float64(running) / float64(capacity)
}

func durationFromSeconds(seconds float64) time.Duration {
	maximum := float64(math.MaxInt64) / float64(time.Second)
	if seconds >= maximum {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(seconds * float64(time.Second))
}

func readQueueTelemetry(
	ctx context.Context,
	pool *pgxpool.Pool,
	table string,
	config normalizedQueueTelemetryConfig,
) ([]queueTelemetryRow, error) {
	statement := strings.ReplaceAll(queueTelemetrySQL, "{{river_job}}", table)
	rows, err := pool.Query(
		ctx,
		statement,
		config.jobQueues,
		config.jobKinds,
		config.queueNames,
		config.supportedQueues,
		config.supportedKinds,
		config.supportedVersions,
		config.clientID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]queueTelemetryRow, 0, len(config.jobs))
	for rows.Next() {
		var row queueTelemetryRow
		if err := rows.Scan(
			&row.queue,
			&row.kind,
			&row.available,
			&row.oldestAgeSeconds,
			&row.localRunning,
			&row.unsupportedAvailable,
		); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

const queueTelemetrySQL = `
WITH expected_jobs(queue, kind) AS (
    SELECT * FROM unnest($1::text[], $2::text[])
),
supported_versions(queue, kind, version) AS (
    SELECT * FROM unnest($4::text[], $5::text[], $6::integer[])
),
available_counts AS (
    SELECT
        expected_jobs.queue,
        expected_jobs.kind,
        count(river_job.id)::bigint AS available,
        min(river_job.scheduled_at) AS oldest_scheduled_at
    FROM expected_jobs
    LEFT JOIN {{river_job}} AS river_job
        ON river_job.queue = expected_jobs.queue
        AND river_job.kind = expected_jobs.kind
        AND river_job.state = 'available'
        AND river_job.scheduled_at <= statement_timestamp()
    GROUP BY expected_jobs.queue, expected_jobs.kind
),
queue_ages AS (
    SELECT
        queue,
        coalesce(
            greatest(extract(epoch FROM statement_timestamp() - min(oldest_scheduled_at)), 0),
            0
        )::double precision AS oldest_age_seconds
    FROM available_counts
    GROUP BY queue
),
local_running AS (
    SELECT count(*)::bigint AS count
    FROM {{river_job}} AS river_job
    WHERE river_job.queue = ANY($3::text[])
        AND river_job.state = 'running'
        AND river_job.attempted_by[array_upper(river_job.attempted_by, 1)] = $7::text
),
unsupported_available AS (
    SELECT EXISTS (
        SELECT 1
        FROM {{river_job}} AS river_job
        WHERE river_job.queue = ANY($3::text[])
            AND river_job.state = 'available'
            AND NOT EXISTS (
                SELECT 1
                FROM supported_versions
                WHERE supported_versions.queue = river_job.queue
                    AND supported_versions.kind = river_job.kind
                    AND jsonb_typeof(river_job.args -> 'contract_version') = 'number'
                    AND river_job.args ->> 'contract_version' = supported_versions.version::text
            )
    ) AS present
)
SELECT
    available_counts.queue,
    available_counts.kind,
    available_counts.available,
    queue_ages.oldest_age_seconds,
    local_running.count,
    unsupported_available.present
FROM available_counts
JOIN queue_ages USING (queue)
CROSS JOIN local_running
CROSS JOIN unsupported_available
ORDER BY available_counts.queue, available_counts.kind
`
