// Package entitlement is the auth-service's bounded view of what an organization's plan allows.
// Billing and licensing stay the authority; this package holds a short-lived snapshot of their answer.
// Entitlement is an input of its own, never part of RBAC and never read from a token claim.
package entitlement

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/singleflight"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

const (
	outcomeAllowed     = "allowed"
	outcomeNotGranted  = "not_granted"
	outcomeUnavailable = "unavailable"

	defaultMaxOrgs = 4096
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

// Adapter serves bounded snapshots. A snapshot is read on first use and again once it expires; a failed read never extends it.
type Adapter struct {
	src      Source
	now      func() time.Time
	ttl      time.Duration
	maxOrgs  int
	logger   *slog.Logger
	outcomes metric.Int64Counter

	flight singleflight.Group
	mu     sync.Mutex
	byOrg  map[uuid.UUID]*list.Element
	lru    *list.List
}

// New validates the configuration. A TTL of zero or above MaxTTL is MaxTTL.
func New(cfg Config) (*Adapter, error) {
	if cfg.Source == nil {
		return nil, errors.New("entitlement: no source")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.TTL <= 0 || cfg.TTL > MaxTTL {
		cfg.TTL = MaxTTL
	}
	if cfg.MaxOrgs <= 0 {
		cfg.MaxOrgs = defaultMaxOrgs
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Meter == nil {
		cfg.Meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/entitlement")
	}
	counter, err := cfg.Meter.Int64Counter("auth.entitlement.outcomes",
		metric.WithDescription("Entitlement checks by outcome: allowed, not_granted or unavailable."))
	if err != nil {
		return nil, err
	}
	return &Adapter{
		src: cfg.Source, now: cfg.Now, ttl: cfg.TTL, maxOrgs: cfg.MaxOrgs, logger: cfg.Logger, outcomes: counter,
		byOrg: map[uuid.UUID]*list.Element{}, lru: list.New(),
	}, nil
}

// Refresh reads the source and replaces the organization's snapshot. On error the stored snapshot, and its expiry, stay as they were.
func (a *Adapter) Refresh(ctx context.Context, orgID uuid.UUID) error {
	_, err, _ := a.flight.Do(orgID.String(), func() (any, error) {
		content, err := a.src.Load(ctx, orgID)
		if err != nil {
			return nil, err
		}
		now := a.now()
		a.store(Snapshot{
			OrgID: orgID, Tier: content.Tier, Features: copyFeatures(content.Features),
			SourceRevision: Revision(content), FetchedAt: now, ExpiresAt: now.Add(a.ttl),
		})
		return nil, nil
	})
	return err
}

// Check decides whether the organization's plan grants the feature. Without a valid snapshot it denies with ReasonUnavailable.
func (a *Adapter) Check(ctx context.Context, orgID uuid.UUID, feature string) Decision {
	snap, ok := a.valid(orgID)
	if !ok {
		if err := a.Refresh(ctx, orgID); err != nil {
			a.logger.WarnContext(ctx, "entitlement refresh failed", "org_id", orgID.String(), logging.ErrorAttr(err))
			return a.record(ctx, outcomeUnavailable, Decision{Reason: ReasonUnavailable})
		}
		if snap, ok = a.valid(orgID); !ok {
			return a.record(ctx, outcomeUnavailable, Decision{Reason: ReasonUnavailable})
		}
	}
	if snap.Features[feature] {
		return a.record(ctx, outcomeAllowed, Decision{Allowed: true, Reason: ReasonEntitled, Revision: snap.SourceRevision})
	}
	return a.record(ctx, outcomeNotGranted, Decision{Reason: ReasonNotEntitled, Revision: snap.SourceRevision})
}

func (a *Adapter) record(ctx context.Context, outcome string, d Decision) Decision {
	a.outcomes.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	return d
}

func (a *Adapter) valid(orgID uuid.UUID) (Snapshot, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	el, ok := a.byOrg[orgID]
	if !ok {
		return Snapshot{}, false
	}
	snap := el.Value.(Snapshot)
	if !a.now().Before(snap.ExpiresAt) {
		return Snapshot{}, false
	}
	a.lru.MoveToFront(el)
	return snap, true
}

func (a *Adapter) store(snap Snapshot) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if el, ok := a.byOrg[snap.OrgID]; ok {
		el.Value = snap
		a.lru.MoveToFront(el)
		return
	}
	a.byOrg[snap.OrgID] = a.lru.PushFront(snap)
	for a.lru.Len() > a.maxOrgs {
		oldest := a.lru.Back()
		delete(a.byOrg, oldest.Value.(Snapshot).OrgID)
		a.lru.Remove(oldest)
	}
}

func copyFeatures(in map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// Revision is the content revision of a source answer: a digest of the tier and the granted features, so any change of plan changes it.
func Revision(c Content) string {
	granted := make([]string, 0, len(c.Features))
	for k, v := range c.Features {
		if v {
			granted = append(granted, k)
		}
	}
	sort.Strings(granted)
	sum := sha256.Sum256([]byte(c.Tier + "\n" + strings.Join(granted, "\n")))
	return hex.EncodeToString(sum[:16])
}
