package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const pagerDutyTeamsColumns = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at,id,source_id,source_url,source_event_at,source_event_id,observed_at,last_synced,raw_status,raw_severity,raw_priority,normalized_status,normalized_severity,normalized_priority,relationship_provenance,relationship_confidence,name,description,is_deleted,deleted_at"

// PagerDutyTeamsClickHouseEffects persists a complete teams snapshot and
// reconciles active rows omitted from that successful snapshot into Python-
// compatible tombstones. Every read is fenced by organization, provider
// instance, and source family.
type PagerDutyTeamsClickHouseEffects struct {
	Conn               driver.Conn
	Lease              providerfoundation.LeaseGuard
	ProviderInstanceID string
	Now                func() time.Time
	Entitlement        IncidentEntitlement
	Metrics            *providerfoundation.Metrics
}

func (sink PagerDutyTeamsClickHouseEffects) WriteEffect(
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
	rows, err := decodeEffectRows[pagerDutyTeamRow](effect)
	if err != nil {
		return err
	}
	if err := validatePagerDutyTeamRows(claim, rows); err != nil {
		return err
	}
	providerInstance, err := sink.providerInstance(rows)
	if err != nil {
		return err
	}
	active, err := sink.loadActiveTeams(ctx, claim, providerInstance)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		seen[row.ID] = struct{}{}
	}
	observedAt := sink.snapshotObservedAt(rows, claim)
	allRows := append([]pagerDutyTeamRow(nil), rows...)
	for _, existing := range active {
		if _, present := seen[existing.ID]; present {
			continue
		}
		tombstone, tombstoneErr := pagerDutyTeamTombstone(existing, observedAt)
		if tombstoneErr != nil {
			return tombstoneErr
		}
		allRows = append(allRows, tombstone)
	}
	if len(allRows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(
		ctx, "INSERT INTO operational_teams ("+pagerDutyTeamsColumns+")",
	)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range allRows {
		if err := batch.Append(pagerDutyTeamValues(row)...); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink PagerDutyTeamsClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[pagerDutyTeamRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := validatePagerDutyTeamRows(claim, expected); err != nil {
		return EffectConflict, err
	}
	providerInstance, err := sink.providerInstance(expected)
	if err != nil {
		return EffectConflict, err
	}
	active, err := sink.loadActiveTeams(ctx, claim, providerInstance)
	if err != nil {
		return EffectConflict, err
	}
	seen := make(map[string]struct{}, len(expected))
	matched, absent := 0, 0
	for _, row := range expected {
		seen[row.ID] = struct{}{}
		actual, found, loadErr := sink.loadTeam(ctx, claim, providerInstance, row.ID)
		if loadErr != nil {
			return EffectConflict, loadErr
		}
		inspection := comparePagerDutyTeamVersion(row, actual, found)
		switch inspection {
		case EffectExact:
			matched++
		case EffectAbsent:
			absent++
		default:
			return EffectConflict, nil
		}
	}
	for _, row := range active {
		if _, present := seen[row.ID]; !present {
			return EffectConflict, nil
		}
	}
	if matched == len(expected) {
		return EffectExact, nil
	}
	if absent == len(expected) {
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

func (sink PagerDutyTeamsClickHouseEffects) providerInstance(
	rows []pagerDutyTeamRow,
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

func (sink PagerDutyTeamsClickHouseEffects) validateRequest(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if ctx == nil || sink.Conn == nil || sink.Lease == nil || sink.Entitlement == nil || claim.Validate() != nil ||
		claim.Provider != "pagerduty" || claim.Dataset != "teams" ||
		effect.Destination != "operational_teams" {
		return ErrInvalidConfiguration
	}
	return sink.Lease.Assert(ctx)
}

func validatePagerDutyTeamRows(claim Claim, rows []pagerDutyTeamRow) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, duplicate := seen[row.ID]; duplicate {
			return ErrInvalidConfiguration
		}
		seen[row.ID] = struct{}{}
		if row.OrgID != claim.OrgID || row.Provider != "pagerduty" ||
			row.ProviderInstanceID == "" || row.ProviderInstanceID != strings.ToLower(row.ProviderInstanceID) ||
			row.SourceEntityType != "team" || row.ExternalID == "" || row.Name == "" ||
			row.ID == "" || row.SourceRevision == nil || row.IngestRevision == nil ||
			row.SourceConflictKey == "" || row.OrderingContract != 2 ||
			row.SourceVersionAt.IsZero() || row.ObservedAt.IsZero() || row.LastSynced.IsZero() ||
			(row.IsDeleted != (row.DeletedAt != nil)) {
			return providerfoundation.ErrInvalidScope
		}
		canonical := row
		canonical.ID, canonical.SourceConflictKey = "", ""
		canonical.SourceRevision, canonical.IngestRevision = nil, nil
		canonical.OrderingContract = 0
		if err := fillPagerDutyTeamOrdering(&canonical); err != nil ||
			canonical.ID != row.ID || canonical.SourceConflictKey != row.SourceConflictKey ||
			canonical.SourceRevision.Cmp(row.SourceRevision) != 0 ||
			canonical.IngestRevision.Cmp(row.IngestRevision) != 0 ||
			canonical.OrderingContract != row.OrderingContract {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func pagerDutyTeamValues(row pagerDutyTeamRow) []any {
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

func pagerDutyTeamScanValues(row *pagerDutyTeamRow) []any {
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

func (sink PagerDutyTeamsClickHouseEffects) loadActiveTeams(
	ctx context.Context, claim Claim, providerInstance string,
) ([]pagerDutyTeamRow, error) {
	rows, err := sink.Conn.Query(
		ctx,
		"SELECT "+pagerDutyTeamsColumns+
			" FROM operational_teams FINAL WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND is_deleted = 0",
		claim.OrgID, claim.Provider, providerInstance, "team",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	active := make([]pagerDutyTeamRow, 0)
	for rows.Next() {
		var row pagerDutyTeamRow
		if err := rows.Scan(pagerDutyTeamScanValues(&row)...); err != nil {
			return nil, err
		}
		if err := fillPagerDutyTeamOrdering(&row); err != nil {
			return nil, err
		}
		active = append(active, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return active, nil
}

func (sink PagerDutyTeamsClickHouseEffects) loadTeam(
	ctx context.Context, claim Claim, providerInstance, id string,
) (pagerDutyTeamRow, bool, error) {
	rows, err := sink.Conn.Query(
		ctx,
		"SELECT "+pagerDutyTeamsColumns+
			" FROM operational_teams FINAL WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND id = ? LIMIT 1",
		claim.OrgID, claim.Provider, providerInstance, "team", id,
	)
	if err != nil {
		return pagerDutyTeamRow{}, false, err
	}
	defer rows.Close()
	var actual pagerDutyTeamRow
	found := false
	for rows.Next() {
		if err := rows.Scan(pagerDutyTeamScanValues(&actual)...); err != nil {
			return pagerDutyTeamRow{}, false, err
		}
		if err := fillPagerDutyTeamOrdering(&actual); err != nil {
			return pagerDutyTeamRow{}, false, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return pagerDutyTeamRow{}, false, err
	}
	return actual, found, nil
}

func comparePagerDutyTeamVersion(
	expected, actual pagerDutyTeamRow, found bool,
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

func (sink PagerDutyTeamsClickHouseEffects) snapshotObservedAt(
	rows []pagerDutyTeamRow, claim Claim,
) time.Time {
	for _, row := range rows {
		if !row.ObservedAt.IsZero() {
			return row.ObservedAt.UTC().Truncate(time.Microsecond)
		}
	}
	if sink.Now != nil {
		return sink.Now().UTC().Truncate(time.Microsecond)
	}
	if claim.BeforeAt != nil && !claim.BeforeAt.IsZero() {
		return claim.BeforeAt.UTC().Truncate(time.Microsecond)
	}
	return time.Now().UTC().Truncate(time.Microsecond)
}

var _ EffectSink = PagerDutyTeamsClickHouseEffects{}
var _ EffectReadback = PagerDutyTeamsClickHouseEffects{}
