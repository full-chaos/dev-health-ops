package syncreconciler

import (
	"context"
	"log/slog"
	"math"
	"regexp"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
)

const (
	parityObservationEvent = "sync_dispatch_parity_observation"
	parityRuntime          = "go_observer"

	defaultRecorderCadence = 60 * time.Second
	minRecorderCadence     = time.Millisecond
	maxRecorderCadence     = 15 * time.Minute
	recorderQueueCapacity  = 1
)

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// ObservationRecorder is deliberately a non-blocking offer interface.
// TryRecord must return immediately and may drop an observation when busy.
// Loop contains panics but cannot enforce this timing contract for arbitrary
// implementations without creating an unbounded goroutine failure mode.
type ObservationRecorder interface {
	TryRecord(Observation) bool
}

// SlogObservationRecorder asynchronously emits redacted parity observations.
// Its one-slot queue bounds memory and makes TryRecord non-blocking. The owner
// that constructs it must call Shutdown after every Loop using it has stopped.
// Shutdown honors its context; a permanently blocked slog Handler can strand
// at most this recorder's single worker, never a goroutine per observation.
type SlogObservationRecorder struct {
	logger  *slog.Logger
	cadence time.Duration
	now     func() time.Time

	mu      sync.Mutex
	emitted bool
	last    time.Time
	closed  bool
	queue   chan Observation
	done    chan struct{}
}

// NewSlogObservationRecorder constructs the production recorder with an
// initial emission followed by at most one successful enqueue per 60 seconds.
func NewSlogObservationRecorder(logger *slog.Logger) (*SlogObservationRecorder, error) {
	return newSlogObservationRecorder(logger, defaultRecorderCadence, time.Now)
}

func newSlogObservationRecorder(
	logger *slog.Logger,
	cadence time.Duration,
	now func() time.Time,
) (*SlogObservationRecorder, error) {
	if logger == nil || now == nil || cadence < minRecorderCadence || cadence > maxRecorderCadence {
		return nil, ErrInvalidConfiguration
	}
	recorder := &SlogObservationRecorder{
		logger:  logger,
		cadence: cadence,
		now:     now,
		queue:   make(chan Observation, recorderQueueCapacity),
		done:    make(chan struct{}),
	}
	go recorder.run()
	return recorder, nil
}

// TryRecord validates and copies one observation before a non-blocking queue
// offer. Invalid, rate-limited, busy, and post-shutdown offers are dropped.
func (recorder *SlogObservationRecorder) TryRecord(observation Observation) bool {
	if recorder == nil || !validRecordedObservation(observation) {
		return false
	}
	observation = copyObservation(observation)
	now := recorder.now()

	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.closed {
		return false
	}
	if recorder.emitted {
		elapsed := now.Sub(recorder.last)
		if elapsed < recorder.cadence {
			return false
		}
	}
	select {
	case recorder.queue <- observation:
		recorder.emitted = true
		recorder.last = now
		return true
	default:
		return false
	}
}

func (recorder *SlogObservationRecorder) run() {
	defer close(recorder.done)
	for observation := range recorder.queue {
		recorder.emitSafely(observation)
	}
}

func (recorder *SlogObservationRecorder) emitSafely(observation Observation) {
	defer func() { _ = recover() }()
	kinds := make([]recordedKind, 0, len(observation.Kinds))
	for _, kind := range observation.Kinds {
		kinds = append(kinds, recordedKind{
			Kind: kind.Kind, Route: kind.Route,
			DuePending: kind.DuePending, ExpiredClaims: kind.ExpiredClaims,
		})
	}
	recorder.logger.LogAttrs(
		context.Background(),
		slog.LevelInfo,
		parityObservationEvent,
		slog.String("event", parityObservationEvent),
		slog.String("runtime", parityRuntime),
		slog.String("observed_at", canonicalObservedAt(observation.ObservedAt)),
		slog.Int("limit", observation.Limit),
		slog.String("predicate_version", observation.PredicateVersion),
		slog.String("digest_version", observation.DigestVersion),
		slog.String("candidate_digest", observation.CandidateDigest),
		slog.Int64("sampled_candidates", observation.SampledCandidates),
		slog.Bool("truncated", observation.Truncated),
		slog.Int64("unknown_kind_count", observation.UnknownKindCount),
		slog.Int64("celery_due_pending", observation.CeleryDuePending),
		slog.Int64("river_due_pending", observation.RiverDuePending),
		slog.Any("kinds", kinds),
	)
}

type recordedKind struct {
	Kind          string `json:"kind"`
	Route         string `json:"route"`
	DuePending    int64  `json:"due_pending"`
	ExpiredClaims int64  `json:"expired_claims"`
}

// Shutdown stops future offers and waits for already accepted records. It does
// not own or close the underlying slog Handler.
func (recorder *SlogObservationRecorder) Shutdown(ctx context.Context) error {
	if recorder == nil || ctx == nil {
		return ErrInvalidConfiguration
	}
	recorder.mu.Lock()
	if !recorder.closed {
		recorder.closed = true
		close(recorder.queue)
	}
	done := recorder.done
	recorder.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func validRecordedObservation(observation Observation) bool {
	if len(observation.Kinds) != len(frozenKinds) ||
		observation.ObservedAt.IsZero() || observation.ObservedAt.Location() != time.UTC ||
		observation.Limit < minimumStepLimit || observation.Limit > maximumStepLimit ||
		observation.PredicateVersion != PredicateVersion ||
		observation.DigestVersion != DigestVersion ||
		!digestPattern.MatchString(observation.CandidateDigest) ||
		observation.SampledCandidates < 0 ||
		observation.SampledCandidates > int64(observation.Limit) ||
		observation.UnknownKindCount < 0 ||
		observation.UnknownKindCount > observation.SampledCandidates ||
		(observation.Truncated && observation.SampledCandidates != int64(observation.Limit)) {
		return false
	}

	var celery, river, known int64
	for index, kind := range observation.Kinds {
		if kind.Kind != frozenKinds[index] || kind.DuePending < 0 ||
			kind.ExpiredClaims < 0 || kind.ExpiredClaims > kind.DuePending {
			return false
		}
		switch kind.Route {
		case syncdispatchcontract.RouteCelery:
			if celery > math.MaxInt64-kind.DuePending {
				return false
			}
			celery += kind.DuePending
		case syncdispatchcontract.RouteRiver:
			if river > math.MaxInt64-kind.DuePending {
				return false
			}
			river += kind.DuePending
		default:
			return false
		}
		if known > math.MaxInt64-kind.DuePending {
			return false
		}
		known += kind.DuePending
	}
	if known > math.MaxInt64-observation.UnknownKindCount {
		return false
	}
	return celery == observation.CeleryDuePending &&
		river == observation.RiverDuePending &&
		known+observation.UnknownKindCount == observation.SampledCandidates
}

// compile-time guard that keeps the production implementation on the narrow
// non-blocking recorder seam.
var _ ObservationRecorder = (*SlogObservationRecorder)(nil)
