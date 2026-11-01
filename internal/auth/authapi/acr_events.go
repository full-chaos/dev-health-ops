package authapi

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/outboxprojection"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

const PatternAcrCredentialEvents = "/internal/v1/acr/credential-events"

const (
	opAcrCredentialEvent     = "acr_credential_event"
	acrEventsAction          = "credentials.project"
	maxAcrEventBody          = 16 << 10
	reasonProjectionRetry    = "projection_retry"
	reasonProjectionConflict = "projection_conflict"
	reasonProjectionRefused  = "projection_refused"
)

type AcrWorkloads interface {
	LookupPrincipal(ctx context.Context, principal uuid.UUID) (*workload.Info, error)
}

type CredentialProjector interface {
	Project(ctx context.Context, in outboxprojection.Input) (outboxprojection.Result, error)
}

type AcrEventDeps struct {
	Workload  *SessionIssueWorkload
	Workloads AcrWorkloads
	Projector CredentialProjector
	Logger    *slog.Logger
	Meter     metric.Meter
}

func AcrEventRoutes(deps AcrEventDeps) ([]httpapi.Route, error) { return acrEventRoutes(deps, nil) }

func acrEventRoutes(AcrEventDeps, []byte) ([]httpapi.Route, error) { return nil, nil }
