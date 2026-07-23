// Package report contains the dormant, dependency-injected report execution
// kernel. It intentionally has no process wiring while report jobs remain
// Celery-routed. ReportRun, not River state, remains authoritative.
package report

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

var (
	ErrDependencyUnavailable = errors.New("report execution dependency is unavailable")
	ErrContractMismatch      = errors.New("report execution contract does not match its run")
	ErrArtifactConflict      = errors.New("report artifact conflicts with completed run")
)

// QueryInput contains stable identifiers only. Implementations must load the
// plan and source data from authoritative stores; no report data travels in a
// job payload.
type QueryInput struct {
	ReportID string
	RunID    string
}

// QueryResult is the normalized, renderer-ready report input. It has no
// serialization contract because it never crosses the queue boundary.
type QueryResult struct {
	Plan     Plan
	Charts   []ChartResult
	Metadata map[string]string
}

type Plan struct {
	PlanID              string
	ReportType          string
	Audience            string
	ScopeTeams          []string
	ScopeRepos          []string
	ScopeServices       []string
	TimeRangeStart      string
	TimeRangeEnd        string
	ComparisonPeriod    string
	Sections            []string
	RequestedMetrics    []string
	ConfidenceThreshold string
	CreatedAt           time.Time
	OrganizationID      string
}

type ChartSpec struct {
	ChartID        string
	PlanID         string
	ChartType      string
	Metric         string
	GroupBy        string
	FilterTeams    []string
	FilterRepos    []string
	TimeRangeStart string
	TimeRangeEnd   string
	Title          string
	OrganizationID string
}

type DataPoint struct {
	X     string
	Y     float64
	Group string
}

type ChartResult struct {
	Spec        ChartSpec
	DataPoints  []DataPoint
	Title       string
	SourceTable string
	Unit        string
}

type ProvenanceRecord struct {
	ProvenanceID string `json:"provenance_id"`
	ArtifactType string `json:"artifact_type"`
	ArtifactID   string `json:"artifact_id"`
	SourceTable  string `json:"-"`
	Metric       string `json:"-"`
}

type Artifact struct {
	Markdown    string
	Fingerprint string
	Metadata    map[string]string
	Provenance  []ProvenanceRecord
}

// QueryAdapter, RendererAdapter, ArtifactAdapter, and NotificationAdapter
// make the external dependencies explicit and independently replaceable for
// parity/golden tests.
type QueryAdapter interface {
	Query(context.Context, QueryInput) (QueryResult, error)
}

type RendererAdapter interface {
	Render(context.Context, QueryResult) (Artifact, error)
}

type ArtifactAdapter interface {
	Store(context.Context, string, Artifact) (Artifact, error)
}

type NotificationAdapter interface {
	Notify(context.Context, string, string) error
}

// NotificationClaim fences the notification state transition. A retried
// worker may reclaim an expired delivery lease, but a worker that crashed and
// later resumes must not complete or release the newer claimant's delivery.
// The notification key remains the downstream idempotency identity.
type NotificationClaim struct {
	Key   string
	Token string
}

type RunStore interface {
	// Claim atomically transitions pending/failed -> running. Failed is allowed
	// so a bounded River retry can reuse the authoritative ReportRun; explicit
	// Python retries first reset failed -> pending and reach the same CAS.
	// A false claim is an idempotent no-op for completed, canceled, or
	// concurrently-running runs.
	Claim(ctx context.Context, runID, reportID string) (bool, error)
	// Complete atomically persists the artifact only if it has the same
	// fingerprint on a retry. It returns false for canceled/already-completed.
	Complete(ctx context.Context, runID string, artifact Artifact) (bool, error)
	Fail(ctx context.Context, runID, code string) error
	// ClaimNotification reserves the side effect by its durable key with a
	// bounded lease. A nil claim means delivery already completed or another
	// worker still owns the unexpired lease.
	ClaimNotification(ctx context.Context, runID string) (*NotificationClaim, error)
	CompleteNotification(ctx context.Context, runID string, claim NotificationClaim) error
	ReleaseNotification(ctx context.Context, runID string, claim NotificationClaim) error
}

type Dependencies struct {
	Runs          RunStore
	Query         QueryAdapter
	Renderer      RendererAdapter
	Artifacts     ArtifactAdapter
	Notifications NotificationAdapter
}

func (dependencies Dependencies) validate() error {
	if dependencies.Runs == nil || dependencies.Query == nil || dependencies.Renderer == nil ||
		dependencies.Artifacts == nil || dependencies.Notifications == nil {
		return ErrDependencyUnavailable
	}
	return nil
}

// NewOnDemandHandler creates a typed runtime handler but does not register it.
func NewOnDemandHandler(dependencies Dependencies) jobruntime.Handler[jobruntime.OnDemandReportExecutionArgs] {
	return jobruntime.HandlerFunc[jobruntime.OnDemandReportExecutionArgs](func(ctx context.Context, execution *jobruntime.Execution[jobruntime.OnDemandReportExecutionArgs]) error {
		payload, ok := execution.Args.ContractEnvelope().Payload.(jobcontract.OnDemandReportExecutionPayload)
		if !ok {
			return ErrContractMismatch
		}
		return execute(ctx, execution.Args.ContractEnvelope(), payload.ReportID, dependencies)
	})
}

// NewScheduledHandler creates a typed runtime handler but does not register it.
func NewScheduledHandler(dependencies Dependencies) jobruntime.Handler[jobruntime.ScheduledReportExecutionArgs] {
	return jobruntime.HandlerFunc[jobruntime.ScheduledReportExecutionArgs](func(ctx context.Context, execution *jobruntime.Execution[jobruntime.ScheduledReportExecutionArgs]) error {
		payload, ok := execution.Args.ContractEnvelope().Payload.(jobcontract.ScheduledReportExecutionPayload)
		if !ok {
			return ErrContractMismatch
		}
		return execute(ctx, execution.Args.ContractEnvelope(), payload.ReportID, dependencies)
	})
}

func execute(ctx context.Context, envelope jobcontract.Envelope, reportID string, dependencies Dependencies) error {
	if err := dependencies.validate(); err != nil {
		return err
	}
	if envelope.Domain.Type != "report_run" || reportID == "" {
		return ErrContractMismatch
	}
	runID := envelope.Domain.ID
	claimed, err := dependencies.Runs.Claim(ctx, runID, reportID)
	if err != nil {
		return fmt.Errorf("claim report run: %w", err)
	}
	if !claimed {
		return notify(ctx, dependencies, runID, reportID)
	}
	input, err := dependencies.Query.Query(ctx, QueryInput{ReportID: reportID, RunID: runID})
	if err != nil {
		return fail(ctx, dependencies.Runs, runID, "query_failed", err)
	}
	artifact, err := dependencies.Renderer.Render(ctx, input)
	if err != nil {
		return fail(ctx, dependencies.Runs, runID, "render_failed", err)
	}
	artifact, err = dependencies.Artifacts.Store(ctx, runID, artifact)
	if err != nil {
		return fail(ctx, dependencies.Runs, runID, "storage_failed", err)
	}
	completed, err := dependencies.Runs.Complete(ctx, runID, artifact)
	if err != nil {
		return fmt.Errorf("complete report run: %w", err)
	}
	if !completed {
		return notify(ctx, dependencies, runID, reportID)
	}
	return notify(ctx, dependencies, runID, reportID)
}

func notify(ctx context.Context, dependencies Dependencies, runID, reportID string) error {
	claim, err := dependencies.Runs.ClaimNotification(ctx, runID)
	if err != nil {
		return fmt.Errorf("claim report notification: %w", err)
	}
	if claim == nil {
		return nil
	}
	if err := dependencies.Notifications.Notify(ctx, reportID, claim.Key); err != nil {
		if releaseErr := dependencies.Runs.ReleaseNotification(ctx, runID, *claim); releaseErr != nil {
			return fmt.Errorf("notify report: %w; release notification: %v", err, releaseErr)
		}
		return fmt.Errorf("notify report: %w", err)
	}
	if err := dependencies.Runs.CompleteNotification(ctx, runID, *claim); err != nil {
		return fmt.Errorf("complete report notification: %w", err)
	}
	return nil
}

func fail(ctx context.Context, store RunStore, runID, code string, cause error) error {
	if err := store.Fail(ctx, runID, code); err != nil {
		return fmt.Errorf("%s: %w", code, err)
	}
	return fmt.Errorf("%s: %w", code, cause)
}
