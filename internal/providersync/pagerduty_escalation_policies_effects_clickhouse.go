package providersync

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The current migrated operational table predates the Go ordering columns.
// Keep the effect row complete for parity/readback validation, but write only
// columns that the canonical migration actually provides. The follow-up
// migration lane owns making source_revision durable in ClickHouse.
const pagerDutyEscalationPoliciesColumns = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at,id,source_id,source_url,source_event_at,source_event_id,observed_at,last_synced,raw_status,raw_severity,raw_priority,normalized_status,normalized_severity,normalized_priority,relationship_provenance,relationship_confidence,name,description,is_deleted,deleted_at"

type PagerDutyEscalationPoliciesClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink PagerDutyEscalationPoliciesClickHouseEffects) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return err
	}
	rows, err := decodeEffectRows[pagerDutyEscalationPolicyRow](effect)
	if err != nil {
		return err
	}
	if err := validatePagerDutyEscalationPolicyRows(claim, rows); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(
		ctx, "INSERT INTO operational_escalation_policies ("+pagerDutyEscalationPoliciesColumns+")",
	)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(pagerDutyEscalationPolicyValues(row)...); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink PagerDutyEscalationPoliciesClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[pagerDutyEscalationPolicyRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := validatePagerDutyEscalationPolicyRows(claim, expected); err != nil {
		return EffectConflict, err
	}
	if len(expected) == 0 {
		return EffectAbsent, nil
	}
	exact, absent := 0, 0
	for _, row := range expected {
		inspection, err := sink.inspectEscalationPolicy(ctx, row)
		if err != nil {
			return EffectConflict, err
		}
		switch inspection {
		case EffectExact:
			exact++
		case EffectAbsent:
			absent++
		default:
			return EffectConflict, nil
		}
	}
	if exact == len(expected) {
		return EffectExact, nil
	}
	if absent == len(expected) {
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

func (sink PagerDutyEscalationPoliciesClickHouseEffects) validateRequest(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if ctx == nil || sink.Conn == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "pagerduty" || claim.Dataset != "escalation-policies" ||
		effect.Destination != "operational_escalation_policies" {
		return ErrInvalidConfiguration
	}
	return sink.Lease.Assert(ctx)
}

func validatePagerDutyEscalationPolicyRows(
	claim Claim, rows []pagerDutyEscalationPolicyRow,
) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, duplicate := seen[row.ID]; duplicate {
			return ErrInvalidConfiguration
		}
		seen[row.ID] = struct{}{}
		if row.OrgID != claim.OrgID || row.Provider != "pagerduty" ||
			row.ProviderInstanceID == "" || row.SourceEntityType != "escalation_policy" ||
			row.ExternalID == "" || row.Name == "" || row.ID == "" ||
			row.SourceRevision == nil || row.IngestRevision == nil ||
			row.SourceConflictKey == "" || row.OrderingContract != 2 ||
			row.SourceVersionAt.IsZero() || row.ObservedAt.IsZero() || row.LastSynced.IsZero() {
			return providerfoundation.ErrInvalidScope
		}
		canonical := row
		canonical.ID, canonical.SourceConflictKey = "", ""
		canonical.SourceRevision, canonical.IngestRevision = nil, nil
		canonical.OrderingContract = 0
		if err := fillPagerDutyEscalationPolicyOrdering(&canonical); err != nil ||
			canonical.ID != row.ID || canonical.SourceConflictKey != row.SourceConflictKey ||
			canonical.SourceRevision.Cmp(row.SourceRevision) != 0 ||
			canonical.IngestRevision.Cmp(row.IngestRevision) != 0 ||
			canonical.OrderingContract != row.OrderingContract {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func pagerDutyEscalationPolicyValues(row pagerDutyEscalationPolicyRow) []any {
	return []any{
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, row.ID, row.SourceID, row.SourceURL,
		row.SourceEventAt, row.SourceEventID, row.ObservedAt, row.LastSynced,
		row.RawStatus, row.RawSeverity, row.RawPriority, row.NormalizedStatus,
		row.NormalizedSeverity, row.NormalizedPriority, row.RelationshipProvenance,
		row.RelationshipConfidence, row.Name, row.Description, row.IsDeleted,
		row.DeletedAt,
	}
}

func (sink PagerDutyEscalationPoliciesClickHouseEffects) inspectEscalationPolicy(
	ctx context.Context, expected pagerDutyEscalationPolicyRow,
) (EffectInspection, error) {
	rows, err := sink.Conn.Query(
		ctx,
		"SELECT "+pagerDutyEscalationPoliciesColumns+
			" FROM operational_escalation_policies FINAL WHERE org_id = ? AND id = ? LIMIT 1",
		expected.OrgID, expected.ID,
	)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var actual pagerDutyEscalationPolicyRow
	found := false
	for rows.Next() {
		if err := rows.Scan(pagerDutyEscalationPolicyScanValues(&actual)...); err != nil {
			return EffectConflict, err
		}
		if err := fillPagerDutyEscalationPolicyOrdering(&actual); err != nil {
			return EffectConflict, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return comparePagerDutyEscalationPolicyVersion(expected, actual, found), nil
}

func pagerDutyEscalationPolicyScanValues(row *pagerDutyEscalationPolicyRow) []any {
	return []any{
		&row.OrgID, &row.Provider, &row.ProviderInstanceID, &row.SourceEntityType,
		&row.ExternalID, &row.SourceVersionAt, &row.ID, &row.SourceID, &row.SourceURL,
		&row.SourceEventAt, &row.SourceEventID, &row.ObservedAt, &row.LastSynced,
		&row.RawStatus, &row.RawSeverity, &row.RawPriority, &row.NormalizedStatus,
		&row.NormalizedSeverity, &row.NormalizedPriority, &row.RelationshipProvenance,
		&row.RelationshipConfidence, &row.Name, &row.Description, &row.IsDeleted,
		&row.DeletedAt,
	}
}

func comparePagerDutyEscalationPolicyVersion(
	expected, actual pagerDutyEscalationPolicyRow, found bool,
) EffectInspection {
	if !found || actual.SourceRevision == nil {
		return EffectAbsent
	}
	comparison := actual.SourceRevision.Cmp(expected.SourceRevision)
	if comparison < 0 {
		return EffectAbsent
	}
	if comparison > 0 {
		return EffectConflict
	}
	expectedJSON, expectedErr := json.Marshal(expected)
	actualJSON, actualErr := json.Marshal(actual)
	if expectedErr != nil || actualErr != nil || !bytes.Equal(expectedJSON, actualJSON) {
		return EffectConflict
	}
	return EffectExact
}

var _ EffectSink = PagerDutyEscalationPoliciesClickHouseEffects{}
var _ EffectReadback = PagerDutyEscalationPoliciesClickHouseEffects{}
