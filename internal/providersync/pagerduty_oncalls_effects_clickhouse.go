package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// This is the exact column order of migration 066's
// operational_on_call_assignments table. The table intentionally has no
// tombstone columns: Python's on-call branch is an upsert stream, not a
// complete snapshot reconciliation.
const pagerDutyOnCallsColumns = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at,id,source_id,source_url,source_event_at,source_event_id,observed_at,last_synced,raw_status,raw_severity,raw_priority,normalized_status,normalized_severity,normalized_priority,relationship_provenance,relationship_confidence,schedule_id,user_id,escalation_policy_id,escalation_level,starts_at,ends_at"

// PagerDutyOnCallsClickHouseEffects persists typed assignment rows and
// performs tenant/provider-instance-fenced readback. Ordering values are not
// columns in the migrated table, so they are reconstructed from the exact
// persisted fields before comparison.
type PagerDutyOnCallsClickHouseEffects struct {
	Conn               driver.Conn
	Lease              providerfoundation.LeaseGuard
	ProviderInstanceID string
}

func (sink PagerDutyOnCallsClickHouseEffects) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return err
	}
	rows, err := decodeEffectRows[pagerDutyOnCallRow](effect)
	if err != nil {
		return err
	}
	if err := validatePagerDutyOnCallRows(claim, rows); err != nil {
		return err
	}
	if _, err := sink.providerInstance(rows); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(
		ctx, "INSERT INTO operational_on_call_assignments ("+pagerDutyOnCallsColumns+")",
	)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(pagerDutyOnCallValues(row)...); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink PagerDutyOnCallsClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[pagerDutyOnCallRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := validatePagerDutyOnCallRows(claim, expected); err != nil {
		return EffectConflict, err
	}
	if _, err := sink.providerInstance(expected); err != nil {
		return EffectConflict, err
	}
	if len(expected) == 0 {
		return EffectAbsent, nil
	}
	exact, absent := 0, 0
	for _, row := range expected {
		actual, found, loadErr := sink.loadOnCall(ctx, claim, row.ProviderInstanceID, row.ID)
		if loadErr != nil {
			return EffectConflict, loadErr
		}
		switch comparePagerDutyOnCallVersion(row, actual, found) {
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

func (sink PagerDutyOnCallsClickHouseEffects) validateRequest(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if ctx == nil || sink.Conn == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "pagerduty" || claim.Dataset != "on-calls" ||
		effect.Destination != "operational_on_call_assignments" {
		return ErrInvalidConfiguration
	}
	return sink.Lease.Assert(ctx)
}

func (sink PagerDutyOnCallsClickHouseEffects) providerInstance(
	rows []pagerDutyOnCallRow,
) (string, error) {
	instance := strings.ToLower(strings.TrimSpace(sink.ProviderInstanceID))
	for _, row := range rows {
		rowInstance := strings.ToLower(strings.TrimSpace(row.ProviderInstanceID))
		if rowInstance == "" {
			return "", providerfoundation.ErrInvalidScope
		}
		if instance == "" {
			instance = rowInstance
		}
		if instance != rowInstance {
			return "", providerfoundation.ErrInvalidScope
		}
	}
	if instance == "" {
		return "", ErrInvalidConfiguration
	}
	return instance, nil
}

func validatePagerDutyOnCallRows(
	claim Claim, rows []pagerDutyOnCallRow,
) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, duplicate := seen[row.ID]; duplicate {
			return ErrInvalidConfiguration
		}
		seen[row.ID] = struct{}{}
		if row.OrgID != claim.OrgID || row.Provider != "pagerduty" ||
			row.ProviderInstanceID == "" || row.ProviderInstanceID != strings.ToLower(row.ProviderInstanceID) ||
			row.SourceEntityType != "oncall" || row.ExternalID == "" || row.ID == "" ||
			row.SourceRevision == nil || row.IngestRevision == nil ||
			row.SourceConflictKey == "" || row.OrderingContract != 2 ||
			row.SourceVersionAt.IsZero() || row.ObservedAt.IsZero() || row.LastSynced.IsZero() ||
			(row.ScheduleID != nil && *row.ScheduleID == "") ||
			(row.UserID != nil && *row.UserID == "") ||
			(row.EscalationPolicyID != nil && *row.EscalationPolicyID == "") {
			return providerfoundation.ErrInvalidScope
		}
		canonical := row
		canonical.ID, canonical.SourceConflictKey = "", ""
		canonical.SourceRevision, canonical.IngestRevision = nil, nil
		canonical.OrderingContract = 0
		if err := fillPagerDutyOnCallOrdering(&canonical); err != nil ||
			canonical.ID != row.ID || canonical.SourceConflictKey != row.SourceConflictKey ||
			canonical.SourceRevision.Cmp(row.SourceRevision) != 0 ||
			canonical.IngestRevision.Cmp(row.IngestRevision) != 0 ||
			canonical.OrderingContract != row.OrderingContract {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func pagerDutyOnCallValues(row pagerDutyOnCallRow) []any {
	return []any{
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, row.ID, row.SourceID, row.SourceURL,
		row.SourceEventAt, row.SourceEventID, row.ObservedAt, row.LastSynced,
		row.RawStatus, row.RawSeverity, row.RawPriority, row.NormalizedStatus,
		row.NormalizedSeverity, row.NormalizedPriority, row.RelationshipProvenance,
		row.RelationshipConfidence, row.ScheduleID, row.UserID,
		row.EscalationPolicyID, row.EscalationLevel, row.StartsAt, row.EndsAt,
	}
}

func pagerDutyOnCallScanValues(row *pagerDutyOnCallRow) []any {
	return []any{
		&row.OrgID, &row.Provider, &row.ProviderInstanceID, &row.SourceEntityType,
		&row.ExternalID, &row.SourceVersionAt, &row.ID, &row.SourceID, &row.SourceURL,
		&row.SourceEventAt, &row.SourceEventID, &row.ObservedAt, &row.LastSynced,
		&row.RawStatus, &row.RawSeverity, &row.RawPriority, &row.NormalizedStatus,
		&row.NormalizedSeverity, &row.NormalizedPriority, &row.RelationshipProvenance,
		&row.RelationshipConfidence, &row.ScheduleID, &row.UserID,
		&row.EscalationPolicyID, &row.EscalationLevel, &row.StartsAt, &row.EndsAt,
	}
}

func (sink PagerDutyOnCallsClickHouseEffects) loadOnCall(
	ctx context.Context, claim Claim, providerInstance, id string,
) (pagerDutyOnCallRow, bool, error) {
	rows, err := sink.Conn.Query(
		ctx, "SELECT "+pagerDutyOnCallsColumns+
			" FROM operational_on_call_assignments FINAL WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND id = ? LIMIT 1",
		claim.OrgID, claim.Provider, providerInstance, "oncall", id,
	)
	if err != nil {
		return pagerDutyOnCallRow{}, false, err
	}
	defer rows.Close()
	var actual pagerDutyOnCallRow
	found := false
	for rows.Next() {
		if err := rows.Scan(pagerDutyOnCallScanValues(&actual)...); err != nil {
			return pagerDutyOnCallRow{}, false, err
		}
		if err := fillPagerDutyOnCallOrdering(&actual); err != nil {
			return pagerDutyOnCallRow{}, false, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return pagerDutyOnCallRow{}, false, err
	}
	return actual, found, nil
}

func comparePagerDutyOnCallVersion(
	expected, actual pagerDutyOnCallRow, found bool,
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

var _ EffectSink = PagerDutyOnCallsClickHouseEffects{}
var _ EffectReadback = PagerDutyOnCallsClickHouseEffects{}
