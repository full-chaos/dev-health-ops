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

// The migrated operational_on_call_schedules table predates durable ordering
// columns. Keep the typed effect row complete for parity/readback validation,
// but insert the exact columns provided by the production migration.
const pagerDutySchedulesColumns = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at,id,source_id,source_url,source_event_at,source_event_id,observed_at,last_synced,raw_status,raw_severity,raw_priority,normalized_status,normalized_severity,normalized_priority,relationship_provenance,relationship_confidence,name,description,timezone,is_deleted,deleted_at"

// PagerDutySchedulesClickHouseEffects persists a complete schedules snapshot
// and fences omitted active rows with typed tombstones. Tenant and provider
// instance predicates keep one PagerDuty account from mutating another.
type PagerDutySchedulesClickHouseEffects struct {
	Conn               driver.Conn
	Lease              providerfoundation.LeaseGuard
	ProviderInstanceID string
	Now                func() time.Time
}

func (sink PagerDutySchedulesClickHouseEffects) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return err
	}
	rows, err := decodeEffectRows[pagerDutyScheduleRow](effect)
	if err != nil {
		return err
	}
	if err := validatePagerDutyScheduleRows(claim, rows); err != nil {
		return err
	}
	providerInstance, err := sink.providerInstance(rows)
	if err != nil {
		return err
	}
	active, err := sink.loadActiveSchedules(ctx, claim, providerInstance)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		seen[row.ID] = struct{}{}
	}
	observedAt := sink.snapshotObservedAt(rows, claim)
	allRows := append([]pagerDutyScheduleRow(nil), rows...)
	for _, existing := range active {
		if _, present := seen[existing.ID]; present {
			continue
		}
		tombstone, tombstoneErr := pagerDutyScheduleTombstone(existing, observedAt)
		if tombstoneErr != nil {
			return tombstoneErr
		}
		allRows = append(allRows, tombstone)
	}
	if len(allRows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(
		ctx, "INSERT INTO operational_on_call_schedules ("+pagerDutySchedulesColumns+")",
	)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range allRows {
		if err := batch.Append(pagerDutyScheduleValues(row)...); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink PagerDutySchedulesClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[pagerDutyScheduleRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := validatePagerDutyScheduleRows(claim, expected); err != nil {
		return EffectConflict, err
	}
	providerInstance, err := sink.providerInstance(expected)
	if err != nil {
		return EffectConflict, err
	}
	active, err := sink.loadActiveSchedules(ctx, claim, providerInstance)
	if err != nil {
		return EffectConflict, err
	}
	seen := make(map[string]struct{}, len(expected))
	exact, absent := 0, 0
	for _, row := range expected {
		seen[row.ID] = struct{}{}
		actual, found, loadErr := sink.loadSchedule(
			ctx, claim, providerInstance, row.ID,
		)
		if loadErr != nil {
			return EffectConflict, loadErr
		}
		switch comparePagerDutyScheduleVersion(row, actual, found) {
		case EffectExact:
			exact++
		case EffectAbsent:
			absent++
		default:
			return EffectConflict, nil
		}
	}
	// A complete source snapshot is exact only when every active source row is
	// represented by the returned identity set or has been tombstoned.
	for _, row := range active {
		if _, present := seen[row.ID]; !present {
			return EffectConflict, nil
		}
	}
	switch {
	case exact == len(expected) && absent == 0:
		return EffectExact, nil
	case absent == len(expected):
		return EffectAbsent, nil
	default:
		return EffectConflict, nil
	}
}

func (sink PagerDutySchedulesClickHouseEffects) validateRequest(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if ctx == nil || sink.Conn == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "pagerduty" || claim.Dataset != "schedules" ||
		effect.Destination != "operational_on_call_schedules" {
		return ErrInvalidConfiguration
	}
	return sink.Lease.Assert(ctx)
}

func validatePagerDutyScheduleRows(claim Claim, rows []pagerDutyScheduleRow) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, duplicate := seen[row.ID]; duplicate {
			return ErrInvalidConfiguration
		}
		seen[row.ID] = struct{}{}
		if row.OrgID != claim.OrgID || row.Provider != "pagerduty" ||
			row.ProviderInstanceID == "" || row.ProviderInstanceID != strings.ToLower(row.ProviderInstanceID) ||
			row.SourceEntityType != "schedule" || row.ExternalID == "" || row.Name == "" ||
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
		if err := fillPagerDutyScheduleOrdering(&canonical); err != nil ||
			canonical.ID != row.ID || canonical.SourceConflictKey != row.SourceConflictKey ||
			canonical.SourceRevision.Cmp(row.SourceRevision) != 0 ||
			canonical.IngestRevision.Cmp(row.IngestRevision) != 0 ||
			canonical.OrderingContract != row.OrderingContract {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func (sink PagerDutySchedulesClickHouseEffects) providerInstance(
	rows []pagerDutyScheduleRow,
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

func pagerDutyScheduleValues(row pagerDutyScheduleRow) []any {
	return []any{
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, row.ID, row.SourceID, row.SourceURL,
		row.SourceEventAt, row.SourceEventID, row.ObservedAt, row.LastSynced,
		row.RawStatus, row.RawSeverity, row.RawPriority, row.NormalizedStatus,
		row.NormalizedSeverity, row.NormalizedPriority, row.RelationshipProvenance,
		row.RelationshipConfidence, row.Name, row.Description, row.Timezone,
		row.IsDeleted, row.DeletedAt,
	}
}

func pagerDutyScheduleScanValues(row *pagerDutyScheduleRow) []any {
	return []any{
		&row.OrgID, &row.Provider, &row.ProviderInstanceID, &row.SourceEntityType,
		&row.ExternalID, &row.SourceVersionAt, &row.ID, &row.SourceID, &row.SourceURL,
		&row.SourceEventAt, &row.SourceEventID, &row.ObservedAt, &row.LastSynced,
		&row.RawStatus, &row.RawSeverity, &row.RawPriority, &row.NormalizedStatus,
		&row.NormalizedSeverity, &row.NormalizedPriority, &row.RelationshipProvenance,
		&row.RelationshipConfidence, &row.Name, &row.Description, &row.Timezone,
		&row.IsDeleted, &row.DeletedAt,
	}
}

func (sink PagerDutySchedulesClickHouseEffects) loadActiveSchedules(
	ctx context.Context, claim Claim, providerInstance string,
) ([]pagerDutyScheduleRow, error) {
	rows, err := sink.Conn.Query(
		ctx, "SELECT "+pagerDutySchedulesColumns+
			" FROM operational_on_call_schedules FINAL WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND is_deleted = 0",
		claim.OrgID, claim.Provider, providerInstance, "schedule",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	active := make([]pagerDutyScheduleRow, 0)
	for rows.Next() {
		var row pagerDutyScheduleRow
		if err := rows.Scan(pagerDutyScheduleScanValues(&row)...); err != nil {
			return nil, err
		}
		if err := fillPagerDutyScheduleOrdering(&row); err != nil {
			return nil, err
		}
		active = append(active, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return active, nil
}

func (sink PagerDutySchedulesClickHouseEffects) loadSchedule(
	ctx context.Context, claim Claim, providerInstance, id string,
) (pagerDutyScheduleRow, bool, error) {
	rows, err := sink.Conn.Query(
		ctx, "SELECT "+pagerDutySchedulesColumns+
			" FROM operational_on_call_schedules FINAL WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND id = ? LIMIT 1",
		claim.OrgID, claim.Provider, providerInstance, "schedule", id,
	)
	if err != nil {
		return pagerDutyScheduleRow{}, false, err
	}
	defer rows.Close()
	var actual pagerDutyScheduleRow
	found := false
	for rows.Next() {
		if err := rows.Scan(pagerDutyScheduleScanValues(&actual)...); err != nil {
			return pagerDutyScheduleRow{}, false, err
		}
		if err := fillPagerDutyScheduleOrdering(&actual); err != nil {
			return pagerDutyScheduleRow{}, false, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return pagerDutyScheduleRow{}, false, err
	}
	return actual, found, nil
}

func comparePagerDutyScheduleVersion(
	expected, actual pagerDutyScheduleRow, found bool,
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

func (sink PagerDutySchedulesClickHouseEffects) snapshotObservedAt(
	rows []pagerDutyScheduleRow, claim Claim,
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

var _ EffectSink = PagerDutySchedulesClickHouseEffects{}
var _ EffectReadback = PagerDutySchedulesClickHouseEffects{}
