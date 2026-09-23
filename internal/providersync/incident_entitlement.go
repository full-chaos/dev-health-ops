package providersync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const canonicalIncidentFeatureKey = "canonical_incident_ingestion"

// ErrIncidentEntitlementDisabled is a POLICY refusal: the feature state was
// read and the decision is closed. It is deterministic given the stored state,
// which is why providerunit terminalizes it as feature_disabled.
var ErrIncidentEntitlementDisabled = errors.New(
	"canonical incident ingestion entitlement is disabled",
)

// ErrIncidentEntitlementUnavailable means the feature state could NOT be read
// (query error, pool exhaustion, scan failure). It still refuses the unit --
// the check fails closed -- but it is deliberately NOT the disabled sentinel:
// a transient domain-Postgres fault must keep the ordinary bounded-retry path,
// never durably terminalize a healthy unit as if the organization had
// disabled the feature (codex round 1 on CHAOS-4219).
var ErrIncidentEntitlementUnavailable = errors.New(
	"canonical incident ingestion entitlement could not be evaluated",
)

// IncidentEntitlement is the execution-time re-check of the
// canonical_incident_ingestion feature. It gates every dataset whose legacy
// sync target is "incidents" or "operational" (src/dev_health_ops/sync/
// datasets.py, _GATED_SYNC_TARGETS): Jira incidents and EVERY PagerDuty
// dataset. One implementation serves both providers; only the feature key is
// provider-specific, and it is shared.
//
// A gated route calls it twice -- before provider fetch and again at the
// ClickHouse write boundary. This deliberately mirrors Python's
// require_canonical_incident_feature_for_update_sync calls on both sides of
// the producer (workers/sync_units.py:1289 and :1311) while also covering Go
// effect-ledger work between collection and persistence. The dispatch-side
// gate (CanonicalIncidentDecision) is NON-locking by ruling (CHAOS-4209), so a
// disable committed after its read is only harmless because this re-check
// refuses the unit at execution.
type IncidentEntitlement interface {
	Require(context.Context, string) error
}

// Seam labels for the refusal counter. Bounded because they become a
// Prometheus label; anything else collapses to "other" in providerfoundation.
const (
	IncidentEntitlementSeamCollect = "collect"
	IncidentEntitlementSeamWrite   = "write"
)

// requireIncidentEntitlement is the ONE call every gated route makes at each
// of its two seams. A nil entitlement is a construction defect and fails
// closed as ErrInvalidConfiguration rather than passing the unit through. A
// refusal is counted by provider, dataset and seam BEFORE it is returned, so a
// disable that landed after the dispatch gate's read is visible on
// dev_health_provider_incident_entitlement_refused_total, not merely harmless.
//
// Every refusal also emits ONE structured log line carrying the tenant and
// unit identity and the seam -- never a credential value. A policy refusal is
// a WARN (expected operator-driven state); an unevaluable entitlement is an
// ERROR (infrastructure), and it is not counted as a refusal because it is
// not one.
func requireIncidentEntitlement(
	ctx context.Context, entitlement IncidentEntitlement,
	metrics *providerfoundation.Metrics, claim Claim, seam string,
) error {
	if entitlement == nil {
		return ErrInvalidConfiguration
	}
	err := entitlement.Require(ctx, claim.OrgID)
	if err == nil {
		return nil
	}
	attributes := []slog.Attr{
		slog.String("org_id", claim.OrgID),
		slog.String("provider", claim.Provider),
		slog.String("dataset", claim.Dataset),
		slog.String("seam", seam),
		slog.String("sync_run_id", claim.SyncRunID),
		slog.String("unit_id", claim.ID),
	}
	if errors.Is(err, ErrIncidentEntitlementDisabled) {
		metrics.RecordIncidentEntitlementRefused(claim.Provider, claim.Dataset, seam)
		slog.Default().LogAttrs(ctx, slog.LevelWarn, incidentEntitlementRefusedEvent,
			attributes...)
		return err
	}
	slog.Default().LogAttrs(ctx, slog.LevelError, incidentEntitlementUnavailableEvent,
		append(attributes, slog.String("error", err.Error()))...)
	return err
}

const (
	incidentEntitlementRefusedEvent     = "sync_provider_unit.entitlement_refused"
	incidentEntitlementUnavailableEvent = "sync_provider_unit.entitlement_unavailable"
)

type PostgresIncidentEntitlement struct {
	Pool *pgxpool.Pool
	Now  func() time.Time
}

func (entitlement PostgresIncidentEntitlement) Require(
	ctx context.Context, orgID string,
) error {
	if ctx == nil || entitlement.Pool == nil {
		return ErrInvalidConfiguration
	}
	return entitlement.require(ctx, entitlement.Pool, orgID)
}

// require separates the two ways the check can refuse. A construction defect
// (unparseable organization id) is ErrInvalidConfiguration; a state that
// could not be READ is ErrIncidentEntitlementUnavailable wrapping the cause;
// a state that was read and decided closed -- via internal/api/licensing's
// shared decision engine, the same one CHAOS-6244's acr route consumes -- is
// ErrIncidentEntitlementDisabled. Malformed license-override JSON used to be
// folded into ErrIncidentEntitlementDisabled directly (a local, non-Python
// decode); it is now ErrIncidentEntitlementUnavailable like any other read
// failure, and syntactically valid but non-object JSON (e.g. `[]`) is no
// longer an error at all -- see licensing.LoadState's own doc comment.
func (entitlement PostgresIncidentEntitlement) require(
	ctx context.Context, queryer licensing.Queryer, orgID string,
) error {
	parsedOrgID, err := uuid.Parse(strings.TrimSpace(orgID))
	if err != nil {
		return ErrInvalidConfiguration
	}
	evaluatedAt := time.Now().UTC()
	if entitlement.Now != nil {
		evaluatedAt = entitlement.Now().UTC()
	}
	state, err := licensing.LoadState(ctx, queryer, parsedOrgID.String(), canonicalIncidentFeatureKey, evaluatedAt)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrIncidentEntitlementUnavailable, err)
	}
	if !licensing.Decide(canonicalIncidentFeatureKey, state).Allowed {
		return ErrIncidentEntitlementDisabled
	}
	return nil
}

var _ IncidentEntitlement = PostgresIncidentEntitlement{}
