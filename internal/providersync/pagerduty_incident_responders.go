package providersync

import (
	"context"
	"math/big"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
	"time"
)

// operational_incident_responders is the eighth operational_* table and the
// only one with no pull-sync producer: PagerDuty's REST API exposes responder
// requests through the incident log, not as a listable collection, so the
// eleven native pull routes never had a reason to write it. Python's webhook
// reconciler was its sole writer (webhooks.py built the IncidentResponder
// inline, without a normalizer), and CHAOS-4105 moves that writer here.
//
// It is NOT a twelfth PagerDuty dataset. A dataset is a pull-sync capability
// -- it appears in the provider matrix, the sync planner and the migration
// docs -- and inventing one for a table nothing pulls would claim a route
// that does not exist. Responders are only ever written alongside the
// incident they belong to, in the same reconciliation, so they carry the
// incident's dataset.
//
// They get their OWN sink type rather than a fifth destination on
// PagerDutyIncidentFamilyClickHouseEffects. The first draft widened that
// sink to accept operational_incident_responders under the "incidents"
// dataset, and codex r1 showed with a live probe that this admitted a
// responder effect from an ORDINARY PULL claim -- the pull route emits no
// such effect today, so nothing was corrupted, but the sink's
// one-dataset-one-table invariant had been traded away for a webhook-only
// need. A separate type restores it: only the webhook path constructs this
// sink, so "only the webhook writes responders" is a fact about the type
// graph rather than a rule a future caller has to remember.

const pagerDutyResponderColumns = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at,id,source_id,source_url,source_event_at,source_event_id,observed_at,last_synced,raw_status,raw_severity,raw_priority,normalized_status,normalized_severity,normalized_priority,relationship_provenance,relationship_confidence,incident_id,user_id,responder_name,role,responder_assignment_id,requested_at,assigned_at,acknowledged_at,completed_at"

// pagerDutyResponderRow mirrors every field on Python's IncidentResponder
// dataclass, in declaration order -- the order is load-bearing twice over: it
// is the ClickHouse column order and it is the field order the conflict-key
// digest is computed over.
type pagerDutyResponderRow struct {
	OrgID                  string     `json:"org_id"`
	Provider               string     `json:"provider"`
	ProviderInstanceID     string     `json:"provider_instance_id"`
	SourceEntityType       string     `json:"source_entity_type"`
	ExternalID             string     `json:"external_id"`
	SourceVersionAt        time.Time  `json:"source_version_at"`
	SourceRevision         *big.Int   `json:"source_revision"`
	SourceConflictKey      string     `json:"source_conflict_key"`
	IngestRevision         *big.Int   `json:"ingest_revision"`
	OrderingContract       uint8      `json:"ordering_contract"`
	ID                     string     `json:"id"`
	SourceID               *uuid.UUID `json:"source_id"`
	SourceURL              *string    `json:"source_url"`
	SourceEventAt          *time.Time `json:"source_event_at"`
	SourceEventID          *string    `json:"source_event_id"`
	ObservedAt             time.Time  `json:"observed_at"`
	LastSynced             time.Time  `json:"last_synced"`
	RawStatus              *string    `json:"raw_status"`
	RawSeverity            *string    `json:"raw_severity"`
	RawPriority            *string    `json:"raw_priority"`
	NormalizedStatus       *string    `json:"normalized_status"`
	NormalizedSeverity     *string    `json:"normalized_severity"`
	NormalizedPriority     *string    `json:"normalized_priority"`
	RelationshipProvenance *string    `json:"relationship_provenance"`
	RelationshipConfidence *float64   `json:"relationship_confidence"`
	IncidentID             string     `json:"incident_id"`
	UserID                 *string    `json:"user_id"`
	ResponderName          *string    `json:"responder_name"`
	Role                   *string    `json:"role"`
	ResponderAssignmentID  *string    `json:"responder_assignment_id"`
	RequestedAt            *time.Time `json:"requested_at"`
	AssignedAt             *time.Time `json:"assigned_at"`
	AcknowledgedAt         *time.Time `json:"acknowledged_at"`
	CompletedAt            *time.Time `json:"completed_at"`
}

func fillPagerDutyResponderOrdering(row *pagerDutyResponderRow) error {
	if row == nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	fields := gitLabOperationalBaseFields(
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, row.SourceID, row.SourceURL,
		row.SourceEventAt, row.SourceEventID, row.RawStatus, row.RawSeverity,
		row.RawPriority, row.NormalizedStatus, row.NormalizedSeverity,
		row.NormalizedPriority, row.RelationshipProvenance, row.RelationshipConfidence,
	)
	fields = append(fields,
		jiraOperationalField{"incident_id", row.IncidentID},
		jiraOperationalField{"user_id", jiraStringValue(row.UserID)},
		jiraOperationalField{"responder_name", jiraStringValue(row.ResponderName)},
		jiraOperationalField{"role", jiraStringValue(row.Role)},
		jiraOperationalField{"responder_assignment_id", jiraStringValue(row.ResponderAssignmentID)},
		jiraOperationalField{"requested_at", jiraTimeValue(row.RequestedAt)},
		jiraOperationalField{"assigned_at", jiraTimeValue(row.AssignedAt)},
		jiraOperationalField{"acknowledged_at", jiraTimeValue(row.AcknowledgedAt)},
		jiraOperationalField{"completed_at", jiraTimeValue(row.CompletedAt)},
	)
	id, conflict, sourceRevision, ingestRevision, err := deriveGitLabOperationalOrdering(
		"operational_incident_responder", row.OrgID, row.Provider, row.ProviderInstanceID,
		row.ExternalID, row.SourceVersionAt, row.ObservedAt, row.LastSynced, fields,
	)
	if err != nil {
		return err
	}
	row.ID, row.SourceConflictKey = id, conflict
	row.SourceRevision, row.IngestRevision = sourceRevision, ingestRevision
	row.OrderingContract = 2
	return nil
}

func validatePagerDutyResponderRows(
	claim Claim, providerInstance string, rows []pagerDutyResponderRow,
) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, ok := seen[row.ID]; ok {
			return ErrInvalidConfiguration
		}
		seen[row.ID] = struct{}{}
		if row.OrgID != claim.OrgID || row.Provider != "pagerduty" ||
			!pagerDutyProviderInstanceMatches(row.ProviderInstanceID, providerInstance) ||
			row.SourceEntityType != "responder" || row.ExternalID == "" || row.ID == "" ||
			row.IncidentID == "" || row.SourceRevision == nil || row.IngestRevision == nil ||
			row.SourceConflictKey == "" || row.OrderingContract != 2 || row.SourceVersionAt.IsZero() ||
			row.ObservedAt.IsZero() || row.LastSynced.IsZero() {
			return providerfoundation.ErrInvalidScope
		}
		canonical := row
		canonical.ID, canonical.SourceConflictKey = "", ""
		canonical.SourceRevision, canonical.IngestRevision = nil, nil
		canonical.OrderingContract = 0
		if err := fillPagerDutyResponderOrdering(&canonical); err != nil ||
			canonical.ID != row.ID || canonical.SourceConflictKey != row.SourceConflictKey ||
			canonical.SourceRevision.Cmp(row.SourceRevision) != 0 ||
			canonical.IngestRevision.Cmp(row.IngestRevision) != 0 ||
			canonical.OrderingContract != row.OrderingContract {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func pagerDutyResponderValues(row pagerDutyResponderRow) []any {
	return []any{row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType, row.ExternalID, row.SourceVersionAt, row.ID, row.SourceID, row.SourceURL, row.SourceEventAt, row.SourceEventID, row.ObservedAt, row.LastSynced, row.RawStatus, row.RawSeverity, row.RawPriority, row.NormalizedStatus, row.NormalizedSeverity, row.NormalizedPriority, row.RelationshipProvenance, row.RelationshipConfidence, row.IncidentID, row.UserID, row.ResponderName, row.Role, row.ResponderAssignmentID, row.RequestedAt, row.AssignedAt, row.AcknowledgedAt, row.CompletedAt}
}

func pagerDutyResponderScanValues(row *pagerDutyResponderRow) []any {
	return []any{&row.OrgID, &row.Provider, &row.ProviderInstanceID, &row.SourceEntityType, &row.ExternalID, &row.SourceVersionAt, &row.ID, &row.SourceID, &row.SourceURL, &row.SourceEventAt, &row.SourceEventID, &row.ObservedAt, &row.LastSynced, &row.RawStatus, &row.RawSeverity, &row.RawPriority, &row.NormalizedStatus, &row.NormalizedSeverity, &row.NormalizedPriority, &row.RelationshipProvenance, &row.RelationshipConfidence, &row.IncidentID, &row.UserID, &row.ResponderName, &row.Role, &row.ResponderAssignmentID, &row.RequestedAt, &row.AssignedAt, &row.AcknowledgedAt, &row.CompletedAt}
}

// PagerDutyWebhookRespondersClickHouseEffects is the responder sink. It
// mirrors the incident family's shape -- same tenant scope, same lease fence,
// same incident entitlement, same readback inspection -- and differs only in
// being reachable from one call site.
type PagerDutyWebhookRespondersClickHouseEffects struct {
	Conn               driver.Conn
	Lease              providerfoundation.LeaseGuard
	ProviderInstanceID string
	Entitlement        IncidentEntitlement
	Metrics            *providerfoundation.Metrics
}

func (sink PagerDutyWebhookRespondersClickHouseEffects) validateRequest(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if ctx == nil || sink.Conn == nil || sink.Lease == nil || sink.Entitlement == nil ||
		claim.Validate() != nil || claim.Provider != "pagerduty" ||
		strings.TrimSpace(sink.ProviderInstanceID) == "" {
		return ErrInvalidConfiguration
	}
	if claim.Dataset != "incidents" || effect.Destination != "operational_incident_responders" {
		return ErrInvalidConfiguration
	}
	return sink.Lease.Assert(ctx)
}

func (sink PagerDutyWebhookRespondersClickHouseEffects) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return err
	}
	if err := requireIncidentEntitlement(
		ctx, sink.Entitlement, sink.Metrics, claim, IncidentEntitlementSeamWrite,
	); err != nil {
		return err
	}
	rows, err := decodeEffectRows[pagerDutyResponderRow](effect)
	if err != nil {
		return err
	}
	if err := validatePagerDutyResponderRows(claim, sink.ProviderInstanceID, rows); err != nil {
		return err
	}
	return sink.writeResponderRows(ctx, rows)
}

func (sink PagerDutyWebhookRespondersClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return EffectConflict, err
	}
	rows, err := decodeEffectRows[pagerDutyResponderRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := validatePagerDutyResponderRows(claim, sink.ProviderInstanceID, rows); err != nil {
		return EffectConflict, err
	}
	return sink.inspectResponderRows(ctx, claim, rows)
}

func (sink PagerDutyWebhookRespondersClickHouseEffects) writeResponderRows(
	ctx context.Context, rows []pagerDutyResponderRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, "INSERT INTO operational_incident_responders ("+pagerDutyResponderColumns+")")
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(pagerDutyResponderValues(row)...); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink PagerDutyWebhookRespondersClickHouseEffects) inspectResponderRows(
	ctx context.Context, claim Claim, rows []pagerDutyResponderRow,
) (EffectInspection, error) {
	return inspectPagerDutyRows(rows, func(row pagerDutyResponderRow) string { return row.ID }, func(row pagerDutyResponderRow) *big.Int { return row.SourceRevision }, func(id string) (pagerDutyResponderRow, bool, error) {
		return sink.loadResponderRow(ctx, claim, id)
	})
}

func (sink PagerDutyWebhookRespondersClickHouseEffects) loadResponderRow(
	ctx context.Context, claim Claim, id string,
) (pagerDutyResponderRow, bool, error) {
	rows, err := sink.Conn.Query(ctx, "SELECT "+pagerDutyResponderColumns+" FROM operational_incident_responders FINAL WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND id = ? LIMIT 1", claim.OrgID, claim.Provider, strings.ToLower(strings.TrimSpace(sink.ProviderInstanceID)), "responder", id)
	if err != nil {
		return pagerDutyResponderRow{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return pagerDutyResponderRow{}, false, rows.Err()
	}
	var row pagerDutyResponderRow
	if err := rows.Scan(pagerDutyResponderScanValues(&row)...); err != nil {
		return pagerDutyResponderRow{}, false, err
	}
	// The readback returns the deployed contract's columns only, so the
	// ordering fields come back zero and must be re-derived before the
	// inspection compares revisions -- the same treatment loadNoteRow gives.
	if err := fillPagerDutyResponderOrdering(&row); err != nil {
		return pagerDutyResponderRow{}, false, err
	}
	return row, true, rows.Err()
}
