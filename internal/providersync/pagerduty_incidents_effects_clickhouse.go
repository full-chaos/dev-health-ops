package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const pagerDutyIncidentColumns = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at,id,source_id,source_url,source_event_at,source_event_id,observed_at,last_synced,raw_status,raw_severity,raw_priority,normalized_status,normalized_severity,normalized_priority,relationship_provenance,relationship_confidence,service_id,service_external_id,escalation_policy_id,title,description,started_at,resolved_at,is_deleted,deleted_at"
const pagerDutyAlertColumns = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at,id,source_id,source_url,source_event_at,source_event_id,observed_at,last_synced,raw_status,raw_severity,raw_priority,normalized_status,normalized_severity,normalized_priority,relationship_provenance,relationship_confidence,service_id,incident_id,title,description,triggered_at,acknowledged_at,resolved_at,is_deleted,deleted_at"
const pagerDutyLogEntryColumns = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at,id,source_id,source_url,source_event_at,source_event_id,observed_at,last_synced,raw_status,raw_severity,raw_priority,normalized_status,normalized_severity,normalized_priority,relationship_provenance,relationship_confidence,incident_id,event_type,body,actor_type,actor_id,occurred_at"
const pagerDutyNoteColumns = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at,id,source_id,source_url,source_event_at,source_event_id,observed_at,last_synced,raw_status,raw_severity,raw_priority,normalized_status,normalized_severity,normalized_priority,relationship_provenance,relationship_confidence,incident_id,body,author_user_id,created_at"

// PagerDutyIncidentFamilyClickHouseEffects is the sink for the four atomic
// Python incident-family destinations. It deliberately does not reconcile
// omitted rows: these datasets are windowed incident streams, not complete
// catalog snapshots. Every write and readback is tenant/provider-instance
// scoped and lease fenced.
type PagerDutyIncidentFamilyClickHouseEffects struct {
	Conn               driver.Conn
	Lease              providerfoundation.LeaseGuard
	ProviderInstanceID string
	Entitlement        IncidentEntitlement
	Metrics            *providerfoundation.Metrics
}

func (sink PagerDutyIncidentFamilyClickHouseEffects) WriteEffect(
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
	switch effect.Destination {
	case "operational_incidents":
		rows, err := decodeEffectRows[pagerDutyIncidentRow](effect)
		if err != nil {
			return err
		}
		if err := validatePagerDutyIncidentRows(claim, sink.ProviderInstanceID, rows); err != nil {
			return err
		}
		return sink.writeIncidentRows(ctx, rows)
	case "operational_alerts":
		rows, err := decodeEffectRows[pagerDutyAlertRow](effect)
		if err != nil {
			return err
		}
		if err := validatePagerDutyAlertRows(claim, sink.ProviderInstanceID, rows); err != nil {
			return err
		}
		return sink.writeAlertRows(ctx, rows)
	case "operational_incident_timeline_events":
		rows, err := decodeEffectRows[pagerDutyLogEntryRow](effect)
		if err != nil {
			return err
		}
		if err := validatePagerDutyLogEntryRows(claim, sink.ProviderInstanceID, rows); err != nil {
			return err
		}
		return sink.writeLogEntryRows(ctx, rows)
	case "operational_incident_notes":
		rows, err := decodeEffectRows[pagerDutyNoteRow](effect)
		if err != nil {
			return err
		}
		if err := validatePagerDutyNoteRows(claim, sink.ProviderInstanceID, rows); err != nil {
			return err
		}
		return sink.writeNoteRows(ctx, rows)
	default:
		return ErrInvalidConfiguration
	}
}

func (sink PagerDutyIncidentFamilyClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return EffectConflict, err
	}
	switch effect.Destination {
	case "operational_incidents":
		rows, err := decodeEffectRows[pagerDutyIncidentRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		if err := validatePagerDutyIncidentRows(claim, sink.ProviderInstanceID, rows); err != nil {
			return EffectConflict, err
		}
		return sink.inspectIncidentRows(ctx, claim, rows)
	case "operational_alerts":
		rows, err := decodeEffectRows[pagerDutyAlertRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		if err := validatePagerDutyAlertRows(claim, sink.ProviderInstanceID, rows); err != nil {
			return EffectConflict, err
		}
		return sink.inspectAlertRows(ctx, claim, rows)
	case "operational_incident_timeline_events":
		rows, err := decodeEffectRows[pagerDutyLogEntryRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		if err := validatePagerDutyLogEntryRows(claim, sink.ProviderInstanceID, rows); err != nil {
			return EffectConflict, err
		}
		return sink.inspectLogEntryRows(ctx, claim, rows)
	case "operational_incident_notes":
		rows, err := decodeEffectRows[pagerDutyNoteRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		if err := validatePagerDutyNoteRows(claim, sink.ProviderInstanceID, rows); err != nil {
			return EffectConflict, err
		}
		return sink.inspectNoteRows(ctx, claim, rows)
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

func (sink PagerDutyIncidentFamilyClickHouseEffects) validateRequest(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if ctx == nil || sink.Conn == nil || sink.Lease == nil || sink.Entitlement == nil || claim.Validate() != nil ||
		claim.Provider != "pagerduty" || strings.TrimSpace(sink.ProviderInstanceID) == "" {
		return ErrInvalidConfiguration
	}
	validDataset := (claim.Dataset == "incidents" && effect.Destination == "operational_incidents") ||
		(claim.Dataset == "incident-alerts" && effect.Destination == "operational_alerts") ||
		(claim.Dataset == "incident-log-entries" && effect.Destination == "operational_incident_timeline_events") ||
		(claim.Dataset == "incident-notes" && effect.Destination == "operational_incident_notes")
	if !validDataset {
		return ErrInvalidConfiguration
	}
	return sink.Lease.Assert(ctx)
}

func (sink PagerDutyIncidentFamilyClickHouseEffects) writeIncidentRows(
	ctx context.Context, rows []pagerDutyIncidentRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, "INSERT INTO operational_incidents ("+pagerDutyIncidentColumns+")")
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(pagerDutyIncidentValues(row)...); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink PagerDutyIncidentFamilyClickHouseEffects) writeAlertRows(
	ctx context.Context, rows []pagerDutyAlertRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, "INSERT INTO operational_alerts ("+pagerDutyAlertColumns+")")
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(pagerDutyAlertValues(row)...); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink PagerDutyIncidentFamilyClickHouseEffects) writeLogEntryRows(
	ctx context.Context, rows []pagerDutyLogEntryRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, "INSERT INTO operational_incident_timeline_events ("+pagerDutyLogEntryColumns+")")
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(pagerDutyLogEntryValues(row)...); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink PagerDutyIncidentFamilyClickHouseEffects) writeNoteRows(
	ctx context.Context, rows []pagerDutyNoteRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, "INSERT INTO operational_incident_notes ("+pagerDutyNoteColumns+")")
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(pagerDutyNoteValues(row)...); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func validatePagerDutyIncidentRows(claim Claim, providerInstance string, rows []pagerDutyIncidentRow) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, ok := seen[row.ID]; ok {
			return ErrInvalidConfiguration
		}
		seen[row.ID] = struct{}{}
		if row.OrgID != claim.OrgID || row.Provider != "pagerduty" ||
			!pagerDutyProviderInstanceMatches(row.ProviderInstanceID, providerInstance) ||
			row.SourceEntityType != "incident" || row.ExternalID == "" || row.ID == "" ||
			row.Title == "" || row.SourceRevision == nil || row.IngestRevision == nil ||
			row.SourceConflictKey == "" || row.OrderingContract != 2 || row.SourceVersionAt.IsZero() ||
			row.ObservedAt.IsZero() || row.LastSynced.IsZero() || (row.IsDeleted != (row.DeletedAt != nil)) {
			return providerfoundation.ErrInvalidScope
		}
		canonical := row
		canonical.ID, canonical.SourceConflictKey = "", ""
		canonical.SourceRevision, canonical.IngestRevision = nil, nil
		canonical.OrderingContract = 0
		if err := fillPagerDutyIncidentOrdering(&canonical); err != nil ||
			canonical.ID != row.ID || canonical.SourceConflictKey != row.SourceConflictKey ||
			canonical.SourceRevision.Cmp(row.SourceRevision) != 0 || canonical.IngestRevision.Cmp(row.IngestRevision) != 0 ||
			canonical.OrderingContract != row.OrderingContract {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func validatePagerDutyAlertRows(claim Claim, providerInstance string, rows []pagerDutyAlertRow) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, ok := seen[row.ID]; ok {
			return ErrInvalidConfiguration
		}
		seen[row.ID] = struct{}{}
		if row.OrgID != claim.OrgID || row.Provider != "pagerduty" ||
			!pagerDutyProviderInstanceMatches(row.ProviderInstanceID, providerInstance) ||
			row.SourceEntityType != "alert" || row.ExternalID == "" || row.ID == "" ||
			row.Title == "" || row.IncidentID == nil || *row.IncidentID == "" ||
			row.SourceRevision == nil || row.IngestRevision == nil || row.SourceConflictKey == "" ||
			row.OrderingContract != 2 || row.SourceVersionAt.IsZero() || row.ObservedAt.IsZero() || row.LastSynced.IsZero() ||
			(row.IsDeleted != (row.DeletedAt != nil)) {
			return providerfoundation.ErrInvalidScope
		}
		canonical := row
		canonical.ID, canonical.SourceConflictKey = "", ""
		canonical.SourceRevision, canonical.IngestRevision = nil, nil
		canonical.OrderingContract = 0
		if err := fillPagerDutyAlertOrdering(&canonical); err != nil || canonical.ID != row.ID ||
			canonical.SourceConflictKey != row.SourceConflictKey || canonical.SourceRevision.Cmp(row.SourceRevision) != 0 ||
			canonical.IngestRevision.Cmp(row.IngestRevision) != 0 || canonical.OrderingContract != row.OrderingContract {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func validatePagerDutyLogEntryRows(claim Claim, providerInstance string, rows []pagerDutyLogEntryRow) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, ok := seen[row.ID]; ok {
			return ErrInvalidConfiguration
		}
		seen[row.ID] = struct{}{}
		if row.OrgID != claim.OrgID || row.Provider != "pagerduty" ||
			!pagerDutyProviderInstanceMatches(row.ProviderInstanceID, providerInstance) || row.SourceEntityType != "log_entry" ||
			row.ExternalID == "" || row.ID == "" || row.IncidentID == "" || row.EventType == "" ||
			row.SourceRevision == nil || row.IngestRevision == nil || row.SourceConflictKey == "" || row.OrderingContract != 2 ||
			row.SourceVersionAt.IsZero() || row.ObservedAt.IsZero() || row.LastSynced.IsZero() {
			return providerfoundation.ErrInvalidScope
		}
		canonical := row
		canonical.ID, canonical.SourceConflictKey = "", ""
		canonical.SourceRevision, canonical.IngestRevision = nil, nil
		canonical.OrderingContract = 0
		if err := fillPagerDutyLogEntryOrdering(&canonical); err != nil || canonical.ID != row.ID ||
			canonical.SourceConflictKey != row.SourceConflictKey || canonical.SourceRevision.Cmp(row.SourceRevision) != 0 ||
			canonical.IngestRevision.Cmp(row.IngestRevision) != 0 || canonical.OrderingContract != row.OrderingContract {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func validatePagerDutyNoteRows(claim Claim, providerInstance string, rows []pagerDutyNoteRow) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, ok := seen[row.ID]; ok {
			return ErrInvalidConfiguration
		}
		seen[row.ID] = struct{}{}
		if row.OrgID != claim.OrgID || row.Provider != "pagerduty" ||
			!pagerDutyProviderInstanceMatches(row.ProviderInstanceID, providerInstance) || row.SourceEntityType != "note" ||
			row.ExternalID == "" || row.ID == "" || row.IncidentID == "" || row.SourceRevision == nil ||
			row.IngestRevision == nil || row.SourceConflictKey == "" || row.OrderingContract != 2 || row.SourceVersionAt.IsZero() ||
			row.ObservedAt.IsZero() || row.LastSynced.IsZero() {
			return providerfoundation.ErrInvalidScope
		}
		canonical := row
		canonical.ID, canonical.SourceConflictKey = "", ""
		canonical.SourceRevision, canonical.IngestRevision = nil, nil
		canonical.OrderingContract = 0
		if err := fillPagerDutyNoteOrdering(&canonical); err != nil || canonical.ID != row.ID ||
			canonical.SourceConflictKey != row.SourceConflictKey || canonical.SourceRevision.Cmp(row.SourceRevision) != 0 ||
			canonical.IngestRevision.Cmp(row.IngestRevision) != 0 || canonical.OrderingContract != row.OrderingContract {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func pagerDutyProviderInstanceMatches(row, expected string) bool {
	return row != "" && row == strings.ToLower(row) && strings.ToLower(strings.TrimSpace(expected)) == row
}

func pagerDutyIncidentValues(row pagerDutyIncidentRow) []any {
	return []any{row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType, row.ExternalID, row.SourceVersionAt, row.ID, row.SourceID, row.SourceURL, row.SourceEventAt, row.SourceEventID, row.ObservedAt, row.LastSynced, row.RawStatus, row.RawSeverity, row.RawPriority, row.NormalizedStatus, row.NormalizedSeverity, row.NormalizedPriority, row.RelationshipProvenance, row.RelationshipConfidence, row.ServiceID, row.ServiceExternalID, row.EscalationPolicyID, row.Title, row.Description, row.StartedAt, row.ResolvedAt, row.IsDeleted, row.DeletedAt}
}

func pagerDutyAlertValues(row pagerDutyAlertRow) []any {
	return []any{row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType, row.ExternalID, row.SourceVersionAt, row.ID, row.SourceID, row.SourceURL, row.SourceEventAt, row.SourceEventID, row.ObservedAt, row.LastSynced, row.RawStatus, row.RawSeverity, row.RawPriority, row.NormalizedStatus, row.NormalizedSeverity, row.NormalizedPriority, row.RelationshipProvenance, row.RelationshipConfidence, row.ServiceID, row.IncidentID, row.Title, row.Description, row.TriggeredAt, row.AcknowledgedAt, row.ResolvedAt, row.IsDeleted, row.DeletedAt}
}

func pagerDutyLogEntryValues(row pagerDutyLogEntryRow) []any {
	return []any{row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType, row.ExternalID, row.SourceVersionAt, row.ID, row.SourceID, row.SourceURL, row.SourceEventAt, row.SourceEventID, row.ObservedAt, row.LastSynced, row.RawStatus, row.RawSeverity, row.RawPriority, row.NormalizedStatus, row.NormalizedSeverity, row.NormalizedPriority, row.RelationshipProvenance, row.RelationshipConfidence, row.IncidentID, row.EventType, row.Body, row.ActorType, row.ActorID, row.OccurredAt}
}

func pagerDutyNoteValues(row pagerDutyNoteRow) []any {
	return []any{row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType, row.ExternalID, row.SourceVersionAt, row.ID, row.SourceID, row.SourceURL, row.SourceEventAt, row.SourceEventID, row.ObservedAt, row.LastSynced, row.RawStatus, row.RawSeverity, row.RawPriority, row.NormalizedStatus, row.NormalizedSeverity, row.NormalizedPriority, row.RelationshipProvenance, row.RelationshipConfidence, row.IncidentID, row.Body, row.AuthorUserID, row.CreatedAt}
}

func pagerDutyIncidentScanValues(row *pagerDutyIncidentRow) []any {
	return []any{&row.OrgID, &row.Provider, &row.ProviderInstanceID, &row.SourceEntityType, &row.ExternalID, &row.SourceVersionAt, &row.ID, &row.SourceID, &row.SourceURL, &row.SourceEventAt, &row.SourceEventID, &row.ObservedAt, &row.LastSynced, &row.RawStatus, &row.RawSeverity, &row.RawPriority, &row.NormalizedStatus, &row.NormalizedSeverity, &row.NormalizedPriority, &row.RelationshipProvenance, &row.RelationshipConfidence, &row.ServiceID, &row.ServiceExternalID, &row.EscalationPolicyID, &row.Title, &row.Description, &row.StartedAt, &row.ResolvedAt, &row.IsDeleted, &row.DeletedAt}
}

func pagerDutyAlertScanValues(row *pagerDutyAlertRow) []any {
	return []any{&row.OrgID, &row.Provider, &row.ProviderInstanceID, &row.SourceEntityType, &row.ExternalID, &row.SourceVersionAt, &row.ID, &row.SourceID, &row.SourceURL, &row.SourceEventAt, &row.SourceEventID, &row.ObservedAt, &row.LastSynced, &row.RawStatus, &row.RawSeverity, &row.RawPriority, &row.NormalizedStatus, &row.NormalizedSeverity, &row.NormalizedPriority, &row.RelationshipProvenance, &row.RelationshipConfidence, &row.ServiceID, &row.IncidentID, &row.Title, &row.Description, &row.TriggeredAt, &row.AcknowledgedAt, &row.ResolvedAt, &row.IsDeleted, &row.DeletedAt}
}

func pagerDutyLogEntryScanValues(row *pagerDutyLogEntryRow) []any {
	return []any{&row.OrgID, &row.Provider, &row.ProviderInstanceID, &row.SourceEntityType, &row.ExternalID, &row.SourceVersionAt, &row.ID, &row.SourceID, &row.SourceURL, &row.SourceEventAt, &row.SourceEventID, &row.ObservedAt, &row.LastSynced, &row.RawStatus, &row.RawSeverity, &row.RawPriority, &row.NormalizedStatus, &row.NormalizedSeverity, &row.NormalizedPriority, &row.RelationshipProvenance, &row.RelationshipConfidence, &row.IncidentID, &row.EventType, &row.Body, &row.ActorType, &row.ActorID, &row.OccurredAt}
}

func pagerDutyNoteScanValues(row *pagerDutyNoteRow) []any {
	return []any{&row.OrgID, &row.Provider, &row.ProviderInstanceID, &row.SourceEntityType, &row.ExternalID, &row.SourceVersionAt, &row.ID, &row.SourceID, &row.SourceURL, &row.SourceEventAt, &row.SourceEventID, &row.ObservedAt, &row.LastSynced, &row.RawStatus, &row.RawSeverity, &row.RawPriority, &row.NormalizedStatus, &row.NormalizedSeverity, &row.NormalizedPriority, &row.RelationshipProvenance, &row.RelationshipConfidence, &row.IncidentID, &row.Body, &row.AuthorUserID, &row.CreatedAt}
}

func equalPagerDutyRow(expected, actual any) bool {
	expectedJSON, expectedErr := json.Marshal(expected)
	actualJSON, actualErr := json.Marshal(actual)
	return expectedErr == nil && actualErr == nil && bytes.Equal(expectedJSON, actualJSON)
}

func inspectPagerDutyRows[T any](
	rows []T,
	id func(T) string,
	sourceRevision func(T) *big.Int,
	load func(string) (T, bool, error),
) (EffectInspection, error) {
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	exact, absent := 0, 0
	for _, expected := range rows {
		actual, found, err := load(id(expected))
		if err != nil {
			return EffectConflict, err
		}
		if !found || sourceRevision(actual) == nil {
			absent++
			continue
		}
		comparison := sourceRevision(actual).Cmp(sourceRevision(expected))
		switch {
		case comparison < 0:
			absent++
		case comparison > 0:
			return EffectConflict, nil
		case !equalPagerDutyRow(expected, actual):
			return EffectConflict, nil
		default:
			exact++
		}
	}
	switch {
	case exact == len(rows):
		return EffectExact, nil
	case absent == len(rows):
		return EffectAbsent, nil
	default:
		return EffectConflict, nil
	}
}

func (sink PagerDutyIncidentFamilyClickHouseEffects) inspectIncidentRows(
	ctx context.Context, claim Claim, rows []pagerDutyIncidentRow,
) (EffectInspection, error) {
	return inspectPagerDutyRows(rows, func(row pagerDutyIncidentRow) string { return row.ID }, func(row pagerDutyIncidentRow) *big.Int { return row.SourceRevision }, func(id string) (pagerDutyIncidentRow, bool, error) {
		return sink.loadIncidentRow(ctx, claim, id)
	})
}

func (sink PagerDutyIncidentFamilyClickHouseEffects) inspectAlertRows(
	ctx context.Context, claim Claim, rows []pagerDutyAlertRow,
) (EffectInspection, error) {
	return inspectPagerDutyRows(rows, func(row pagerDutyAlertRow) string { return row.ID }, func(row pagerDutyAlertRow) *big.Int { return row.SourceRevision }, func(id string) (pagerDutyAlertRow, bool, error) {
		return sink.loadAlertRow(ctx, claim, id)
	})
}

func (sink PagerDutyIncidentFamilyClickHouseEffects) inspectLogEntryRows(
	ctx context.Context, claim Claim, rows []pagerDutyLogEntryRow,
) (EffectInspection, error) {
	return inspectPagerDutyRows(rows, func(row pagerDutyLogEntryRow) string { return row.ID }, func(row pagerDutyLogEntryRow) *big.Int { return row.SourceRevision }, func(id string) (pagerDutyLogEntryRow, bool, error) {
		return sink.loadLogEntryRow(ctx, claim, id)
	})
}

func (sink PagerDutyIncidentFamilyClickHouseEffects) inspectNoteRows(
	ctx context.Context, claim Claim, rows []pagerDutyNoteRow,
) (EffectInspection, error) {
	return inspectPagerDutyRows(rows, func(row pagerDutyNoteRow) string { return row.ID }, func(row pagerDutyNoteRow) *big.Int { return row.SourceRevision }, func(id string) (pagerDutyNoteRow, bool, error) {
		return sink.loadNoteRow(ctx, claim, id)
	})
}

func (sink PagerDutyIncidentFamilyClickHouseEffects) loadIncidentRow(
	ctx context.Context, claim Claim, id string,
) (pagerDutyIncidentRow, bool, error) {
	rows, err := sink.Conn.Query(ctx, "SELECT "+pagerDutyIncidentColumns+" FROM operational_incidents FINAL WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND id = ? LIMIT 1", claim.OrgID, claim.Provider, strings.ToLower(strings.TrimSpace(sink.ProviderInstanceID)), "incident", id)
	if err != nil {
		return pagerDutyIncidentRow{}, false, err
	}
	defer rows.Close()
	var row pagerDutyIncidentRow
	found := false
	for rows.Next() {
		if err := rows.Scan(pagerDutyIncidentScanValues(&row)...); err != nil {
			return pagerDutyIncidentRow{}, false, err
		}
		if err := fillPagerDutyIncidentOrdering(&row); err != nil {
			return pagerDutyIncidentRow{}, false, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return pagerDutyIncidentRow{}, false, err
	}
	return row, found, nil
}

func (sink PagerDutyIncidentFamilyClickHouseEffects) loadAlertRow(
	ctx context.Context, claim Claim, id string,
) (pagerDutyAlertRow, bool, error) {
	rows, err := sink.Conn.Query(ctx, "SELECT "+pagerDutyAlertColumns+" FROM operational_alerts FINAL WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND id = ? LIMIT 1", claim.OrgID, claim.Provider, strings.ToLower(strings.TrimSpace(sink.ProviderInstanceID)), "alert", id)
	if err != nil {
		return pagerDutyAlertRow{}, false, err
	}
	defer rows.Close()
	var row pagerDutyAlertRow
	found := false
	for rows.Next() {
		if err := rows.Scan(pagerDutyAlertScanValues(&row)...); err != nil {
			return pagerDutyAlertRow{}, false, err
		}
		if err := fillPagerDutyAlertOrdering(&row); err != nil {
			return pagerDutyAlertRow{}, false, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return pagerDutyAlertRow{}, false, err
	}
	return row, found, nil
}

func (sink PagerDutyIncidentFamilyClickHouseEffects) loadLogEntryRow(
	ctx context.Context, claim Claim, id string,
) (pagerDutyLogEntryRow, bool, error) {
	rows, err := sink.Conn.Query(ctx, "SELECT "+pagerDutyLogEntryColumns+" FROM operational_incident_timeline_events FINAL WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND id = ? LIMIT 1", claim.OrgID, claim.Provider, strings.ToLower(strings.TrimSpace(sink.ProviderInstanceID)), "log_entry", id)
	if err != nil {
		return pagerDutyLogEntryRow{}, false, err
	}
	defer rows.Close()
	var row pagerDutyLogEntryRow
	found := false
	for rows.Next() {
		if err := rows.Scan(pagerDutyLogEntryScanValues(&row)...); err != nil {
			return pagerDutyLogEntryRow{}, false, err
		}
		if err := fillPagerDutyLogEntryOrdering(&row); err != nil {
			return pagerDutyLogEntryRow{}, false, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return pagerDutyLogEntryRow{}, false, err
	}
	return row, found, nil
}

func (sink PagerDutyIncidentFamilyClickHouseEffects) loadNoteRow(
	ctx context.Context, claim Claim, id string,
) (pagerDutyNoteRow, bool, error) {
	rows, err := sink.Conn.Query(ctx, "SELECT "+pagerDutyNoteColumns+" FROM operational_incident_notes FINAL WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND id = ? LIMIT 1", claim.OrgID, claim.Provider, strings.ToLower(strings.TrimSpace(sink.ProviderInstanceID)), "note", id)
	if err != nil {
		return pagerDutyNoteRow{}, false, err
	}
	defer rows.Close()
	var row pagerDutyNoteRow
	found := false
	for rows.Next() {
		if err := rows.Scan(pagerDutyNoteScanValues(&row)...); err != nil {
			return pagerDutyNoteRow{}, false, err
		}
		if err := fillPagerDutyNoteOrdering(&row); err != nil {
			return pagerDutyNoteRow{}, false, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return pagerDutyNoteRow{}, false, err
	}
	return row, found, nil
}

var _ EffectSink = PagerDutyIncidentFamilyClickHouseEffects{}
var _ EffectReadback = PagerDutyIncidentFamilyClickHouseEffects{}
