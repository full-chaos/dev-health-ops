package riverstore

import (
	"context"
	"errors"
	"math"
	"slices"
	"sort"
	"strconv"
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
	// ErrUnsupportedAvailableContractVersion is the stable sentinel every
	// caller matches on. It never discloses a River row or an encoded
	// argument. UnsupportedContractVersionError wraps it and adds the
	// offending queue/kind/version labels; see that type for why those three
	// are safe to name when the arguments are not.
	ErrUnsupportedAvailableContractVersion = errors.New("an available River job has an unsupported contract version")
)

// maximumUnsupportedOffenders bounds how many distinct queue/kind/version
// labels the refusal names. The point is to identify the offending contract,
// not to enumerate a backlog.
const maximumUnsupportedOffenders = 8

// maximumOffenderVersionLength bounds the contract-version text in an offender
// label. Versions are small integers; this only stops an absurd JSON number
// from widening the label.
const maximumOffenderVersionLength = 16

// UnsupportedContractVersionError names the bounded labels of the available
// rows the check refused.
//
// Only queue, kind, and contract version are disclosed. All three are
// low-cardinality bounded vocabulary that this process already publishes:
// queue and kind are pre-registered Prometheus label values, and the version
// is a small integer from the registered window. Every label is re-validated
// in Go against the same character class and length limit the configuration
// side enforces before it reaches an error string, and a version that is not
// a JSON number is reported as "none" rather than echoed. Encoded arguments,
// row identifiers, and payloads are never read.
//
// Naming them is the point: production saw only
// "failed_checks=queued_contract_versions" and could not tell which kind,
// which queue, or which version had refused startup, which is the same
// bounded-code diagnosis gap as CHAOS-3928.
type UnsupportedContractVersionError struct {
	// Offenders are "queue/kind@version" labels, sorted and deduplicated.
	Offenders []string
}

func (failure *UnsupportedContractVersionError) Error() string {
	if failure == nil || len(failure.Offenders) == 0 {
		return ErrUnsupportedAvailableContractVersion.Error()
	}
	return ErrUnsupportedAvailableContractVersion.Error() + ": " + strings.Join(failure.Offenders, ",")
}

func (*UnsupportedContractVersionError) Unwrap() error {
	return ErrUnsupportedAvailableContractVersion
}

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

// QueueTelemetryOccupant is one queue/kind pair that may legitimately hold
// available rows in a configured queue although this process reports no
// backlog metric for it.
//
// Jobs and Occupants are separate on purpose. Jobs is the metrics dimension:
// every entry becomes a pre-registered jobs_available label, so it must stay
// exactly the set of kinds the metrics collector was built with. Occupants is
// the RESOLVABILITY set: kinds that share a River queue from another route
// plane, which a reader of river_job must be able to resolve but must not
// invent metric labels for. Folding the second into the first would make the
// metrics write fail on an unregistered dimension instead.
//
// A River queue can be shared by more than one plane -- queue "sync" carries
// the bounded registry's sync.team_autoimport AND the sync-dispatch plane's
// dispatch_sync_run -- and a contract-version check that knows only one plane
// reads the other plane's perfectly valid rows as unsupported, which refused
// worker startup for as long as any were pending (CHAOS-3938).
type QueueTelemetryOccupant struct {
	Queue             string
	Kind              string
	SupportedVersions []int
}

// QueueTelemetryConfig binds database observations to one concrete River
// client. ClientID must be client.ID(), after River has applied defaults.
// Queue selection is the only scope label; profile names are intentionally not
// part of the queue-telemetry contract.
type QueueTelemetryConfig struct {
	Schema       string
	ClientID     string
	QueryTimeout time.Duration
	Queues       []QueueTelemetryQueue
	Jobs         []QueueTelemetryJob
	// Occupants are non-metric kinds that may legitimately occupy the
	// configured queues. See QueueTelemetryOccupant.
	Occupants []QueueTelemetryOccupant
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

// QueueCapacityTelemetry reports one queue's live running count and capacity,
// both scoped to THIS process. Running counts only jobs this client claimed and
// Capacity is this client's MaxWorkers for the queue, so Saturation is a
// per-process ratio. Averaging it across replicas gives fleet utilization;
// summing Running across replicas gives the fleet's in-flight count.
type QueueCapacityTelemetry struct {
	Queue      string
	Capacity   int64
	Running    int64
	Saturation float64
}

// QueueTelemetrySnapshot is one database-consistent, read-only observation.
// LocalRunning is restricted to jobs whose latest attempted_by entry is this
// River client ID, so the saturation denominator is this process's capacity,
// not a fleet-wide count.
type QueueTelemetrySnapshot struct {
	Jobs                []QueueJobTelemetry
	Queues              []QueueAgeTelemetry
	QueueCapacities     []QueueCapacityTelemetry
	LocalRunning        int64
	ExecutionSaturation float64
}

type queueJobKey struct {
	queue string
	kind  string
}

type normalizedQueueTelemetryConfig struct {
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
	queueRunning         int64
	unsupportedOccupants []string
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
// configured queue has an unknown queue/kind pairing, a missing or
// non-integer contract_version, or a version outside the registered window.
// It never loads or returns encoded arguments. A refusal is an
// *UnsupportedContractVersionError wrapping
// ErrUnsupportedAvailableContractVersion, carrying the offending
// queue/kind/version labels so the caller can say WHICH contract refused.
func (sampler *QueueTelemetrySampler) CheckAvailableContractVersions(ctx context.Context) error {
	_, offenders, err := sampler.sample(ctx)
	if err != nil {
		return err
	}
	if len(offenders) > 0 {
		return &UnsupportedContractVersionError{Offenders: offenders}
	}
	return nil
}

func (sampler *QueueTelemetrySampler) sample(ctx context.Context) (QueueTelemetrySnapshot, []string, error) {
	if sampler == nil || sampler.read == nil || ctx == nil {
		return QueueTelemetrySnapshot{}, nil, ErrQueueTelemetryUnavailable
	}
	queryContext, cancel := context.WithTimeout(ctx, sampler.config.queryTimeout)
	defer cancel()
	rows, err := sampler.read(queryContext)
	if err != nil {
		return QueueTelemetrySnapshot{}, nil, ErrQueueTelemetryUnavailable
	}
	return sampler.snapshot(rows)
}

func (sampler *QueueTelemetrySampler) snapshot(rows []queueTelemetryRow) (QueueTelemetrySnapshot, []string, error) {
	if len(rows) != len(sampler.config.jobs) || len(rows) == 0 {
		return QueueTelemetrySnapshot{}, nil, ErrQueueTelemetryUnavailable
	}
	available := make(map[queueJobKey]int64, len(rows))
	ages := make(map[string]time.Duration, len(sampler.config.queues))
	seenAges := make(map[string]float64, len(sampler.config.queues))
	running := make(map[string]int64, len(sampler.config.queues))
	var localRunning int64
	var offenders []string
	for index, row := range rows {
		key := queueJobKey{queue: row.queue, kind: row.kind}
		if row.available < 0 || row.localRunning < 0 || row.queueRunning < 0 ||
			math.IsNaN(row.oldestAgeSeconds) || math.IsInf(row.oldestAgeSeconds, 0) ||
			row.oldestAgeSeconds < 0 {
			return QueueTelemetrySnapshot{}, nil, ErrQueueTelemetryUnavailable
		}
		if _, duplicate := available[key]; duplicate || !sampler.hasJob(key) {
			return QueueTelemetrySnapshot{}, nil, ErrQueueTelemetryUnavailable
		}
		available[key] = row.available
		if previous, ok := seenAges[row.queue]; ok && previous != row.oldestAgeSeconds {
			return QueueTelemetrySnapshot{}, nil, ErrQueueTelemetryUnavailable
		}
		seenAges[row.queue] = row.oldestAgeSeconds
		ages[row.queue] = durationFromSeconds(row.oldestAgeSeconds)
		if previous, ok := running[row.queue]; ok && previous != row.queueRunning {
			return QueueTelemetrySnapshot{}, nil, ErrQueueTelemetryUnavailable
		}
		running[row.queue] = row.queueRunning
		// Both are whole-selection aggregates: every row of one read must
		// carry the identical value, so a disagreement means the result set is
		// not the single consistent observation this sampler contracts for.
		if index == 0 {
			localRunning = row.localRunning
			offenders = sanitizeOffenderLabels(row.unsupportedOccupants)
		} else if localRunning != row.localRunning ||
			!slices.Equal(offenders, sanitizeOffenderLabels(row.unsupportedOccupants)) {
			return QueueTelemetrySnapshot{}, nil, ErrQueueTelemetryUnavailable
		}
	}

	result := QueueTelemetrySnapshot{
		Jobs:                make([]QueueJobTelemetry, 0, len(sampler.config.jobs)),
		Queues:              make([]QueueAgeTelemetry, 0, len(sampler.config.queues)),
		QueueCapacities:     make([]QueueCapacityTelemetry, 0, len(sampler.config.queues)),
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
		queueRunning := running[queue.Name]
		result.QueueCapacities = append(result.QueueCapacities, QueueCapacityTelemetry{
			Queue:      queue.Name,
			Capacity:   int64(queue.MaxWorkers),
			Running:    queueRunning,
			Saturation: executionSaturation(queueRunning, int64(queue.MaxWorkers)),
		})
	}
	return result, offenders, nil
}

// unprintableOffender replaces a label that is not in the bounded vocabulary.
// It is deliberately a whole label, not a marker spliced into a real one: the
// point is that nothing derived from row content reaches a log line unchecked.
const unprintableOffender = "unprintable/unprintable@none"

// sanitizeOffenderLabels re-checks the bounded labels the database aggregated
// and REPLACES anything outside the vocabulary, so a value that somehow
// escaped the SQL sanitisation -- or a row written outside this application's
// insert paths -- can never reach an error string or a log line.
//
// It replaces rather than rejects on purpose. Rejecting made the whole read
// unreadable, and this sampler's Snapshot contract is that an unsupported
// contract never hides backlog metrics: an odd kind must cost the DIAGNOSTIC
// its precision, never the metrics their availability. The refusal still
// happens, because the row is still unsupported.
//
// The label is "queue/kind@version"; queue and kind must satisfy the same
// character class and length limit the configuration side enforces, and the
// version must be digits, "none", or "invalid".
func sanitizeOffenderLabels(labels []string) []string {
	if len(labels) == 0 {
		return nil
	}
	sanitized := make([]string, 0, min(len(labels), maximumUnsupportedOffenders))
	for _, label := range labels {
		if len(sanitized) == maximumUnsupportedOffenders {
			break
		}
		sanitized = append(sanitized, sanitizeOffenderLabel(label))
	}
	return sanitized
}

func sanitizeOffenderLabel(label string) string {
	queueAndRest, version, found := strings.Cut(label, "@")
	if !found || !validOffenderVersion(version) {
		return unprintableOffender
	}
	queue, kind, found := strings.Cut(queueAndRest, "/")
	if !found || !telemetryLabel(queue, maximumQueueTelemetryLabelLength) ||
		!telemetryLabel(kind, maximumQueueTelemetryLabelLength) {
		return unprintableOffender
	}
	return label
}

func validOffenderVersion(version string) bool {
	// "none" is a missing or non-numeric contract_version; "invalid" is a JSON
	// number that is not a canonical unsigned integer. Both are emitted by the
	// query instead of the offending text itself.
	if version == "none" || version == "invalid" {
		return true
	}
	if len(version) == 0 || len(version) > maximumOffenderVersionLength {
		return false
	}
	for _, character := range version {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
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
	if !validIdentifier(config.Schema) ||
		len(config.ClientID) == 0 || len(config.ClientID) > 100 || strings.ContainsRune(config.ClientID, '\x00') ||
		len(config.Queues) == 0 || len(config.Queues) > maximumQueueTelemetryQueues ||
		len(config.Jobs) == 0 || len(config.Jobs) > maximumQueueTelemetryJobs ||
		len(config.Occupants) > maximumQueueTelemetryJobs ||
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
	occupants := make([]QueueTelemetryOccupant, len(config.Occupants))
	for index, occupant := range config.Occupants {
		occupants[index] = QueueTelemetryOccupant{
			Queue:             occupant.Queue,
			Kind:              occupant.Kind,
			SupportedVersions: append([]int(nil), occupant.SupportedVersions...),
		}
	}
	sort.Slice(occupants, func(left, right int) bool {
		if occupants[left].Queue != occupants[right].Queue {
			return occupants[left].Queue < occupants[right].Queue
		}
		return occupants[left].Kind < occupants[right].Kind
	})
	for _, occupant := range occupants {
		if !telemetryLabel(occupant.Queue, maximumQueueTelemetryLabelLength) ||
			!telemetryLabel(occupant.Kind, maximumQueueTelemetryLabelLength) ||
			len(occupant.SupportedVersions) == 0 ||
			len(occupant.SupportedVersions) > maximumSupportedVersionsPerJob {
			return normalizedQueueTelemetryConfig{}, ErrQueueTelemetryConfiguration
		}
		if _, ok := queueSet[occupant.Queue]; !ok {
			return normalizedQueueTelemetryConfig{}, ErrQueueTelemetryConfiguration
		}
		// One (queue, kind) may be declared once, by exactly one plane. A
		// duplicate would silently union two version windows and let a version
		// neither plane accepts pass the check.
		key := queueJobKey{queue: occupant.Queue, kind: occupant.Kind}
		if _, duplicate := jobSet[key]; duplicate {
			return normalizedQueueTelemetryConfig{}, ErrQueueTelemetryConfiguration
		}
		jobSet[key] = struct{}{}
		usedQueues[occupant.Queue] = struct{}{}
		previous := 0
		for _, version := range occupant.SupportedVersions {
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
	// Occupants widen only the supported-version set. They deliberately do NOT
	// enter jobQueues/jobKinds: those drive expected_jobs, which becomes one
	// pre-registered jobs_available metric label per entry.
	for _, occupant := range occupants {
		for _, version := range occupant.SupportedVersions {
			normalized.supportedQueues = append(normalized.supportedQueues, occupant.Queue)
			normalized.supportedKinds = append(normalized.supportedKinds, occupant.Kind)
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
	// The SQL truncation limits and the Go re-validation limits are the SAME
	// constants, substituted here rather than restated as literals: a
	// duplicated bound that drifts would make legitimate labels fail
	// validation and read as an unreadable snapshot.
	for placeholder, value := range map[string]int{
		"{{offender_limit}}": maximumUnsupportedOffenders,
		"{{label_length}}":   maximumQueueTelemetryLabelLength,
		"{{version_length}}": maximumOffenderVersionLength,
	} {
		statement = strings.ReplaceAll(statement, placeholder, strconv.Itoa(value))
	}
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
			&row.queueRunning,
			&row.unsupportedOccupants,
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
queue_running AS (
    -- Scoped to THIS client, like local_running above. The capacity this count
    -- is divided by is QueueCapacityTelemetry.Capacity, which is this
    -- process's MaxWorkers, so a fleet-wide numerator made every replica
    -- report saturation of roughly N at N replicas -- clamped to 1.0, so the
    -- signal an operator would use to decide scale-out was pegged at 100%
    -- exactly under scale-out (CHAOS-3867). Per-process is also what the
    -- sibling LocalRunning/ExecutionSaturation metrics already report, and it
    -- aggregates correctly: averaging the ratio across replicas gives fleet
    -- utilization without depending on presence-table freshness.
    SELECT
        river_job.queue,
        count(*)::bigint AS running
    FROM {{river_job}} AS river_job
    WHERE river_job.queue = ANY($3::text[])
        AND river_job.state = 'running'
        AND river_job.attempted_by[array_upper(river_job.attempted_by, 1)] = $7::text
    GROUP BY river_job.queue
),
-- The refusal names the offending contract instead of only asserting that one
-- exists: an EXISTS boolean told an operator a version was unsupported but not
-- which kind, queue, or version, so the only way to find out was to query the
-- table by hand while the worker crash-looped (CHAOS-3938). Labels are
-- truncated here and re-validated in Go; the version is emitted only when it
-- is a JSON number, so nothing unbounded from args can reach a log line.
unsupported_available AS (
    SELECT coalesce(
        (
            SELECT array_agg(offender ORDER BY offender)
            FROM (
                SELECT DISTINCT
                    -- Sanitised in SQL, not merely truncated. left() counts
                    -- CHARACTERS while the Go re-validation counts BYTES, and
                    -- the Go class is ASCII-only, so a multi-byte or
                    -- out-of-class kind would produce a label the Go side
                    -- rejects. That used to invalidate the whole read -- which
                    -- would take the BACKLOG METRICS down with it, breaking
                    -- this sampler's stated invariant that an unsupported
                    -- contract never hides them. Emitting a placeholder keeps
                    -- the refusal (the row really is unsupported) without
                    -- costing the metrics.
                    CASE
                        WHEN river_job.queue ~ '^[A-Za-z0-9._:-]{1,{{label_length}}}$'
                        THEN river_job.queue
                        ELSE 'unprintable'
                    END || '/' ||
                    CASE
                        WHEN river_job.kind ~ '^[A-Za-z0-9._:-]{1,{{label_length}}}$'
                        THEN river_job.kind
                        ELSE 'unprintable'
                    END || '@' ||
                    CASE
                        -- Three outcomes, not two. A JSON number that is not a
                        -- canonical unsigned integer -- -1, 1.5, 1e3 -- is a
                        -- real unsupported version, so it must reach the
                        -- refusal as 'invalid' rather than be emitted verbatim:
                        -- the Go re-validation would reject the verbatim text
                        -- and turn the whole read into an unreadable snapshot,
                        -- losing both the diagnostic and the queue metrics for
                        -- exactly the malformed rows this check exists to name.
                        -- IS DISTINCT FROM, not <>: an ABSENT key makes the
                        -- args lookup SQL NULL, so jsonb_typeof returns NULL
                        -- and a plain <> is unknown rather than true -- which
                        -- would fall through and report a missing version as a
                        -- malformed one.
                        WHEN jsonb_typeof(river_job.args -> 'contract_version') IS DISTINCT FROM 'number'
                        THEN 'none'
                        WHEN river_job.args ->> 'contract_version' ~ '^[0-9]{1,{{version_length}}}$'
                        THEN river_job.args ->> 'contract_version'
                        ELSE 'invalid'
                    END AS offender
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
                ORDER BY offender
                LIMIT {{offender_limit}}
            ) AS unsupported_rows
        ),
        ARRAY[]::text[]
    ) AS offenders
)
SELECT
    available_counts.queue,
    available_counts.kind,
    available_counts.available,
    queue_ages.oldest_age_seconds,
    local_running.count,
    coalesce(queue_running.running, 0),
    unsupported_available.offenders
FROM available_counts
JOIN queue_ages USING (queue)
LEFT JOIN queue_running USING (queue)
CROSS JOIN local_running
CROSS JOIN unsupported_available
ORDER BY available_counts.queue, available_counts.kind
`
