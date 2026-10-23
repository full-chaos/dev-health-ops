// Package entitlement is the auth-service's bounded view of what an organization's plan allows.
// Billing and licensing stay the authority; this package holds a short-lived snapshot of their answer.
// Entitlement is an input of its own, never part of RBAC and never read from a token claim.
package entitlement

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric"
)

const (
	// ReasonUnavailable denies when no valid snapshot exists.
	ReasonUnavailable = "entitlement_unavailable"
	// ReasonNotEntitled denies when a valid snapshot does not grant the feature.
	ReasonNotEntitled = "entitlement_not_granted"
	// ReasonEntitled allows.
	ReasonEntitled = "entitled"

	// MaxTTL is the staleness bound of a snapshot.
	MaxTTL = 60 * time.Second
)

// Content is what a Source reports for one organization.
type Content struct {
	Tier     string
	Features map[string]bool
}

// Source reads the authoritative entitlement of one organization.
type Source interface {
	Load(ctx context.Context, orgID uuid.UUID) (Content, error)
}

// Snapshot is one organization's entitlement at one read.
type Snapshot struct {
	OrgID          uuid.UUID
	Tier           string
	Features       map[string]bool
	SourceRevision string
	FetchedAt      time.Time
	ExpiresAt      time.Time
}

// Decision is the answer of Check.
type Decision struct {
	Allowed  bool
	Reason   string
	Revision string
}

// Config builds an Adapter.
type Config struct {
	Source  Source
	Now     func() time.Time
	TTL     time.Duration
	MaxOrgs int
	Meter   metric.Meter
	Logger  *slog.Logger
}

// Adapter serves bounded snapshots.
type Adapter struct{}

// New validates the configuration.
func New(Config) (*Adapter, error) { return &Adapter{}, nil }

// Refresh reads the source and replaces the organization's snapshot.
func (*Adapter) Refresh(context.Context, uuid.UUID) error { return nil }

// Check decides whether the organization's plan grants the feature.
func (*Adapter) Check(context.Context, uuid.UUID, string) Decision { return Decision{} }

// Revision is the content revision of a source answer.
func Revision(Content) string { return "" }
