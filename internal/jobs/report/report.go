// Package report contains the dormant, dependency-injected report execution
// kernel. It intentionally has no process wiring while report jobs remain
// Celery-routed. ReportRun, not River state, remains authoritative.
package report

import (
	"context"
	"errors"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

var (
	ErrDependencyUnavailable = errors.New("report execution dependency is unavailable")
	ErrContractMismatch      = errors.New("report execution contract does not match its run")
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
	Plan     any
	Data     any
	Metadata map[string]string
}

type Artifact struct {
	Markdown    string
	Fingerprint string
	Metadata    map[string]string
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

type RunStore interface {
	// Claim atomically transitions pending -> running. A false claim is an
	// idempotent no-op for completed, canceled, or concurrently-running runs.
	Claim(ctx context.Context, runID, reportID string) (bool, error)
	// Complete atomically persists the artifact only if it has the same
	// fingerprint on a retry. It returns false for canceled/already-completed.
	Complete(ctx context.Context, runID string, artifact Artifact) (bool, error)
	Fail(ctx context.Context, runID, code string) error
	// ClaimNotification reserves the side effect by its durable key. A false
	// claim means a prior successful notification must not be repeated.
	ClaimNotification(ctx context.Context, runID string) (key string, claimed bool, err error)
	CompleteNotification(ctx context.Context, runID string) error
	ReleaseNotification(ctx context.Context, runID string) error
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
		return nil
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
		return nil
	}
	key, notify, err := dependencies.Runs.ClaimNotification(ctx, runID)
	if err != nil {
		return fmt.Errorf("claim report notification: %w", err)
	}
	if !notify {
		return nil
	}
	if err := dependencies.Notifications.Notify(ctx, reportID, key); err != nil {
		if releaseErr := dependencies.Runs.ReleaseNotification(ctx, runID); releaseErr != nil {
			return fmt.Errorf("notify report: %w; release notification: %v", err, releaseErr)
		}
		return fmt.Errorf("notify report: %w", err)
	}
	if err := dependencies.Runs.CompleteNotification(ctx, runID); err != nil {
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
