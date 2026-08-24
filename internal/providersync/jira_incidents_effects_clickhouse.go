package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const jiraIncidentColumns = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at,source_revision,source_conflict_key,ingest_revision,ordering_contract,id,source_id,source_url,source_event_at,source_event_id,observed_at,last_synced,raw_status,raw_severity,raw_priority,normalized_status,normalized_severity,normalized_priority,relationship_provenance,relationship_confidence,service_id,service_external_id,escalation_policy_id,title,description,started_at,resolved_at,is_deleted,deleted_at"

type JiraIncidentClickHouseEffects struct {
	Writer      jiraIncidentBatchPreparer
	Lease       providerfoundation.LeaseGuard
	Entitlement IncidentEntitlement
	Metrics     *providerfoundation.Metrics
}

// JiraIncidentClickHouseReadback deliberately has no entitlement dependency.
// Recovery must remain able to classify a write that was authorized when it
// began even if the organization entitlement is revoked before a retry.
type JiraIncidentClickHouseReadback struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

type jiraIncidentBatchPreparer interface {
	PrepareBatch(
		context.Context, string, ...driver.PrepareBatchOption,
	) (driver.Batch, error)
}

func (sink JiraIncidentClickHouseEffects) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return err
	}
	rows, err := decodeEffectRows[jiraIncidentRow](effect)
	if err != nil {
		return err
	}
	if err := validateJiraIncidentScope(claim, rows); err != nil {
		return err
	}
	// Preserve Python's second entitlement check at the actual persistence
	// boundary. Provider collection and effect-ledger preparation can take long
	// enough for an earlier grant to be revoked.
	if err := requireIncidentEntitlement(
		ctx, sink.Entitlement, sink.Metrics, claim, IncidentEntitlementSeamWrite,
	); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Writer.PrepareBatch(ctx, "INSERT INTO operational_incidents ("+jiraIncidentColumns+")")
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(jiraIncidentValues(row)...); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (readback JiraIncidentClickHouseReadback) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	if err := readback.validateRequest(ctx, claim, effect); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[jiraIncidentRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := validateJiraIncidentScope(claim, expected); err != nil {
		return EffectConflict, err
	}
	if len(expected) == 0 {
		return EffectAbsent, nil
	}
	if readback.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	exact, absent := 0, 0
	for _, row := range expected {
		inspection, err := readback.inspectIncident(ctx, row)
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

func (sink JiraIncidentClickHouseEffects) validateRequest(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "jira" || claim.Dataset != "incidents" ||
		effect.Destination != "operational_incidents" || sink.Writer == nil ||
		sink.Entitlement == nil {
		return ErrInvalidConfiguration
	}
	return sink.Lease.Assert(ctx)
}

func (readback JiraIncidentClickHouseReadback) validateRequest(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if ctx == nil || readback.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "jira" || claim.Dataset != "incidents" ||
		effect.Destination != "operational_incidents" {
		return ErrInvalidConfiguration
	}
	return readback.Lease.Assert(ctx)
}

func validateJiraIncidentScope(claim Claim, rows []jiraIncidentRow) error {
	for _, row := range rows {
		if row.OrgID != claim.OrgID || row.Provider != "jira" ||
			row.ProviderInstanceID == "" || row.SourceEntityType != "jsm_incident" ||
			row.ExternalID == "" || row.ID == "" || row.SourceRevision == nil ||
			row.IngestRevision == nil || row.SourceConflictKey == "" ||
			row.OrderingContract != 2 || row.SourceVersionAt.IsZero() ||
			row.ObservedAt.IsZero() || row.LastSynced.IsZero() || row.Title == "" {
			return providerfoundation.ErrInvalidScope
		}
		canonical := row
		canonical.ID, canonical.SourceConflictKey = "", ""
		canonical.SourceRevision, canonical.IngestRevision = nil, nil
		canonical.OrderingContract = 0
		if err := fillJiraIncidentOrdering(&canonical); err != nil ||
			canonical.ID != row.ID || canonical.SourceConflictKey != row.SourceConflictKey ||
			canonical.SourceRevision.Cmp(row.SourceRevision) != 0 ||
			canonical.IngestRevision.Cmp(row.IngestRevision) != 0 ||
			canonical.OrderingContract != row.OrderingContract {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func jiraIncidentValues(row jiraIncidentRow) []any {
	return []any{
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, row.SourceRevision, row.SourceConflictKey,
		row.IngestRevision, row.OrderingContract, row.ID, row.SourceID,
		row.SourceURL, row.SourceEventAt, row.SourceEventID, row.ObservedAt,
		row.LastSynced, row.RawStatus, row.RawSeverity, row.RawPriority,
		row.NormalizedStatus, row.NormalizedSeverity, row.NormalizedPriority,
		row.RelationshipProvenance, row.RelationshipConfidence, row.ServiceID,
		row.ServiceExternalID, row.EscalationPolicyID, row.Title, row.Description,
		row.StartedAt, row.ResolvedAt, row.IsDeleted, row.DeletedAt,
	}
}

func (readback JiraIncidentClickHouseReadback) inspectIncident(
	ctx context.Context, expected jiraIncidentRow,
) (EffectInspection, error) {
	rows, err := readback.Conn.Query(
		ctx, "SELECT "+jiraIncidentColumns+" FROM operational_incidents FINAL WHERE org_id = ? AND id = ? LIMIT 1",
		expected.OrgID, expected.ID,
	)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var actual jiraIncidentRow
	found := false
	for rows.Next() {
		var sourceRevision, ingestRevision big.Int
		values := jiraIncidentScanValues(&actual, &sourceRevision, &ingestRevision)
		if err := rows.Scan(values...); err != nil {
			return EffectConflict, err
		}
		actual.SourceRevision = new(big.Int).Set(&sourceRevision)
		actual.IngestRevision = new(big.Int).Set(&ingestRevision)
		found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return compareJiraIncidentVersion(expected, actual, found), nil
}

func jiraIncidentScanValues(row *jiraIncidentRow, sourceRevision, ingestRevision *big.Int) []any {
	return []any{
		&row.OrgID, &row.Provider, &row.ProviderInstanceID, &row.SourceEntityType,
		&row.ExternalID, &row.SourceVersionAt, sourceRevision, &row.SourceConflictKey,
		ingestRevision, &row.OrderingContract, &row.ID, &row.SourceID, &row.SourceURL,
		&row.SourceEventAt, &row.SourceEventID, &row.ObservedAt, &row.LastSynced,
		&row.RawStatus, &row.RawSeverity, &row.RawPriority, &row.NormalizedStatus,
		&row.NormalizedSeverity, &row.NormalizedPriority, &row.RelationshipProvenance,
		&row.RelationshipConfidence, &row.ServiceID, &row.ServiceExternalID,
		&row.EscalationPolicyID, &row.Title, &row.Description, &row.StartedAt,
		&row.ResolvedAt, &row.IsDeleted, &row.DeletedAt,
	}
}

func compareJiraIncidentVersion(
	expected, actual jiraIncidentRow, found bool,
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

var _ EffectSink = JiraIncidentClickHouseEffects{}
var _ EffectReadback = JiraIncidentClickHouseReadback{}
