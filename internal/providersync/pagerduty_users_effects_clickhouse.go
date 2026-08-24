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

const pagerDutyUsersColumns = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at,id,source_id,source_url,source_event_at,source_event_id,observed_at,last_synced,raw_status,raw_severity,raw_priority,normalized_status,normalized_severity,normalized_priority,relationship_provenance,relationship_confidence,display_name,email,is_deleted,deleted_at"

// PagerDutyUsersClickHouseEffects persists a complete users snapshot and
// reconciles active rows omitted from that successful snapshot into Python-
// compatible tombstones. The query is fenced by org, provider instance, and
// source family so one PagerDuty account cannot deactivate another account's
// users.
type PagerDutyUsersClickHouseEffects struct {
	Conn               driver.Conn
	Lease              providerfoundation.LeaseGuard
	ProviderInstanceID string
	Now                func() time.Time
	Entitlement        IncidentEntitlement
	Metrics            *providerfoundation.Metrics
}

func (sink PagerDutyUsersClickHouseEffects) WriteEffect(
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
	rows, err := decodeEffectRows[pagerDutyUserRow](effect)
	if err != nil {
		return err
	}
	if err := validatePagerDutyUserRows(claim, rows); err != nil {
		return err
	}
	providerInstance, err := sink.providerInstance(rows)
	if err != nil {
		return err
	}
	active, err := sink.loadActiveUsers(ctx, claim, providerInstance)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		seen[row.ID] = struct{}{}
	}
	observedAt := sink.snapshotObservedAt(rows, claim)
	allRows := append([]pagerDutyUserRow(nil), rows...)
	for _, existing := range active {
		if _, present := seen[existing.ID]; present {
			continue
		}
		tombstone, tombstoneErr := pagerDutyUserTombstone(existing, observedAt)
		if tombstoneErr != nil {
			return tombstoneErr
		}
		allRows = append(allRows, tombstone)
	}
	if len(allRows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(
		ctx, "INSERT INTO operational_users ("+pagerDutyUsersColumns+")",
	)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range allRows {
		if err := batch.Append(pagerDutyUserValues(row)...); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink PagerDutyUsersClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[pagerDutyUserRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := validatePagerDutyUserRows(claim, expected); err != nil {
		return EffectConflict, err
	}
	providerInstance, err := sink.providerInstance(expected)
	if err != nil {
		return EffectConflict, err
	}
	active, err := sink.loadActiveUsers(ctx, claim, providerInstance)
	if err != nil {
		return EffectConflict, err
	}
	seen := make(map[string]struct{}, len(expected))
	matched, absent := 0, 0
	for _, row := range expected {
		seen[row.ID] = struct{}{}
		actual, found, loadErr := sink.loadUser(ctx, claim, providerInstance, row.ID)
		if loadErr != nil {
			return EffectConflict, loadErr
		}
		inspection := comparePagerDutyUserVersion(row, actual, found)
		switch inspection {
		case EffectExact:
			matched++
		case EffectAbsent:
			absent++
		default:
			return EffectConflict, nil
		}
	}
	// A complete snapshot is exact only if every active source row omitted by
	// the payload has been replaced by a tombstone. This is the readback fence
	// that distinguishes a partial write from a completed reconciliation.
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

func (sink PagerDutyUsersClickHouseEffects) providerInstance(
	rows []pagerDutyUserRow,
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

func (sink PagerDutyUsersClickHouseEffects) validateRequest(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if ctx == nil || sink.Conn == nil || sink.Lease == nil || sink.Entitlement == nil || claim.Validate() != nil ||
		claim.Provider != "pagerduty" || claim.Dataset != "users" ||
		effect.Destination != "operational_users" {
		return ErrInvalidConfiguration
	}
	return sink.Lease.Assert(ctx)
}

func validatePagerDutyUserRows(claim Claim, rows []pagerDutyUserRow) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, duplicate := seen[row.ID]; duplicate {
			return ErrInvalidConfiguration
		}
		seen[row.ID] = struct{}{}
		normalizedInstance := strings.ToLower(strings.TrimSpace(row.ProviderInstanceID))
		if row.OrgID != claim.OrgID || row.Provider != "pagerduty" ||
			row.ProviderInstanceID == "" || row.ProviderInstanceID != normalizedInstance ||
			row.SourceEntityType != "user" ||
			row.ExternalID == "" || row.DisplayName == "" || row.ID == "" ||
			row.SourceRevision == nil || row.IngestRevision == nil ||
			row.SourceConflictKey == "" || row.OrderingContract != 2 ||
			row.SourceVersionAt.IsZero() || row.ObservedAt.IsZero() ||
			row.LastSynced.IsZero() || (row.IsDeleted != (row.DeletedAt != nil)) {
			return providerfoundation.ErrInvalidScope
		}
		canonical := row
		canonical.ID, canonical.SourceConflictKey = "", ""
		canonical.SourceRevision, canonical.IngestRevision = nil, nil
		canonical.OrderingContract = 0
		if err := fillPagerDutyUserOrdering(&canonical); err != nil ||
			canonical.ID != row.ID || canonical.SourceConflictKey != row.SourceConflictKey ||
			canonical.SourceRevision.Cmp(row.SourceRevision) != 0 ||
			canonical.IngestRevision.Cmp(row.IngestRevision) != 0 ||
			canonical.OrderingContract != row.OrderingContract {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func pagerDutyUserValues(row pagerDutyUserRow) []any {
	return []any{
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, row.ID, row.SourceID, row.SourceURL,
		row.SourceEventAt, row.SourceEventID, row.ObservedAt, row.LastSynced,
		row.RawStatus, row.RawSeverity, row.RawPriority, row.NormalizedStatus,
		row.NormalizedSeverity, row.NormalizedPriority, row.RelationshipProvenance,
		row.RelationshipConfidence, row.DisplayName, row.Email, row.IsDeleted,
		row.DeletedAt,
	}
}

func pagerDutyUserScanValues(row *pagerDutyUserRow) []any {
	return []any{
		&row.OrgID, &row.Provider, &row.ProviderInstanceID, &row.SourceEntityType,
		&row.ExternalID, &row.SourceVersionAt, &row.ID, &row.SourceID, &row.SourceURL,
		&row.SourceEventAt, &row.SourceEventID, &row.ObservedAt, &row.LastSynced,
		&row.RawStatus, &row.RawSeverity, &row.RawPriority, &row.NormalizedStatus,
		&row.NormalizedSeverity, &row.NormalizedPriority, &row.RelationshipProvenance,
		&row.RelationshipConfidence, &row.DisplayName, &row.Email, &row.IsDeleted,
		&row.DeletedAt,
	}
}

func (sink PagerDutyUsersClickHouseEffects) loadActiveUsers(
	ctx context.Context, claim Claim, providerInstance string,
) ([]pagerDutyUserRow, error) {
	rows, err := sink.Conn.Query(
		ctx,
		"SELECT "+pagerDutyUsersColumns+
			" FROM operational_users FINAL WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND is_deleted = 0",
		claim.OrgID, claim.Provider, providerInstance, "user",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	active := make([]pagerDutyUserRow, 0)
	for rows.Next() {
		var row pagerDutyUserRow
		if err := rows.Scan(pagerDutyUserScanValues(&row)...); err != nil {
			return nil, err
		}
		if err := fillPagerDutyUserOrdering(&row); err != nil {
			return nil, err
		}
		active = append(active, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return active, nil
}

func (sink PagerDutyUsersClickHouseEffects) loadUser(
	ctx context.Context, claim Claim, providerInstance, id string,
) (pagerDutyUserRow, bool, error) {
	rows, err := sink.Conn.Query(
		ctx,
		"SELECT "+pagerDutyUsersColumns+
			" FROM operational_users FINAL WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND id = ? LIMIT 1",
		claim.OrgID, claim.Provider, providerInstance, "user", id,
	)
	if err != nil {
		return pagerDutyUserRow{}, false, err
	}
	defer rows.Close()
	var actual pagerDutyUserRow
	found := false
	for rows.Next() {
		if err := rows.Scan(pagerDutyUserScanValues(&actual)...); err != nil {
			return pagerDutyUserRow{}, false, err
		}
		if err := fillPagerDutyUserOrdering(&actual); err != nil {
			return pagerDutyUserRow{}, false, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return pagerDutyUserRow{}, false, err
	}
	return actual, found, nil
}

func comparePagerDutyUserVersion(
	expected, actual pagerDutyUserRow, found bool,
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

func (sink PagerDutyUsersClickHouseEffects) snapshotObservedAt(
	rows []pagerDutyUserRow, claim Claim,
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

var _ EffectSink = PagerDutyUsersClickHouseEffects{}
var _ EffectReadback = PagerDutyUsersClickHouseEffects{}
