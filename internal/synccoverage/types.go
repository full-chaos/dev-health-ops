// Package synccoverage builds the durable sync-coverage projection consumed by
// the Python API. It is intentionally independent of River job wiring so the
// projector can be called by scheduled and write-side runtimes.
package synccoverage

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const (
	HistoryLookbackDays = 3650
	projectionVersion   = 1
	maxProjectionRows   = 1_000_000
	maxSources          = 5_000
	maxDatasets         = 100
	maxPairs            = 20_000
	maxCompactWindows   = 30_000
	maxBackfillPairs    = 20_000
)

var (
	minimumStaleGrace  = 6 * time.Hour
	fallbackStaleGrace = 48 * time.Hour
)

type Option func(*Projector)

// WithClock supplies the time used for a projection build. It is useful for
// deterministic tests and does not change persisted source timestamps.
func WithClock(clock func() time.Time) Option {
	return func(projector *Projector) {
		if clock != nil {
			projector.clock = clock
		}
	}
}

type RefreshFailure struct {
	OrgID    string
	ConfigID uuid.UUID
	Err      error
}

type RefreshResult struct {
	Refreshed int
	Failed    int
	Failures  []RefreshFailure
}

type syncConfig struct {
	ID             uuid.UUID
	OrgID          string
	Provider       string
	SyncTargets    []string
	Active         bool
	PlannerManaged bool
	IntegrationID  *uuid.UUID
	SourceID       *uuid.UUID
}

type source struct {
	ID       uuid.UUID
	Name     string
	FullName string
}

type effectiveScope struct {
	IntegrationID *uuid.UUID
	Sources       []source
	DatasetKeys   []string
}

type unitWindow struct {
	Since      time.Time
	Before     time.Time
	SourceID   string
	DatasetKey string
	Status     string
	RunTime    time.Time
}

type coverageInterval struct {
	Since       time.Time
	Before      time.Time
	SourceIDs   []string
	RunIDs      []string
	DatasetKeys []string
}

type pairState struct {
	Requested []coverageInterval
	Covered   []coverageInterval
	Failed    []coverageInterval
}

type pairCoverage struct {
	SourceID       string
	DatasetKey     string
	Requested      []coverageInterval
	Covered        []coverageInterval
	Gaps           []coverageInterval
	StaleRanges    []coverageInterval
	FailedRanges   []coverageInterval
	CoveredThrough *time.Time
	Status         string
}

type datasetCoverage struct {
	DatasetKey     string
	Requested      []coverageInterval
	Covered        []coverageInterval
	Gaps           []coverageInterval
	StaleRanges    []coverageInterval
	FailedRanges   []coverageInterval
	CoveredThrough *time.Time
	Status         string
}

type schedule struct {
	Cron      string
	NextRunAt *time.Time
}

type statusParts struct {
	FailedCount int
	GapCount    int
	StaleStatus string
	HasData     bool
	Running     bool
}

type backfillJob struct {
	ID         uuid.UUID
	TaskID     *string
	SinceDate  time.Time
	BeforeDate time.Time
}

type projectionPayload map[string]any

func marshalPayload(payload projectionPayload) (json.RawMessage, error) {
	encoded, err := json.Marshal(payload)
	return json.RawMessage(encoded), err
}
