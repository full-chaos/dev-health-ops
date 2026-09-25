package operationalbackfill

import (
	"context"
	"math/big"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// The legacy Atlassian Ops rows (atlassian_ops_incidents, _alerts, _schedules)
// and their mapping into canonical operational rows
// (providers/operational_migration.py: map_atlassian_ops_batch).

const (
	atlassianProvider = "atlassian"

	// Entity families (the dataclass entity_family of each canonical entity).
	familyIncident = "operational_incident"
	familyAlert    = "operational_alert"
	familySchedule = "operational_on_call_schedule"

	tableIncidents = "operational_incidents"
	tableAlerts    = "operational_alerts"
	tableSchedules = "operational_on_call_schedules"
)

// Legacy row shapes.
type legacyIncident struct {
	ID          string
	URL         *string
	Summary     string
	Description *string
	Status      string
	Severity    string
	CreatedAt   time.Time
	ProviderID  *string
	LastSynced  time.Time
}

type legacyAlert struct {
	ID             string
	Status         string
	Priority       string
	CreatedAt      time.Time
	AcknowledgedAt *time.Time
	SnoozedAt      *time.Time
	ClosedAt       *time.Time
	LastSynced     time.Time
}

type legacySchedule struct {
	ID         string
	Name       string
	Timezone   *string
	LastSynced time.Time
}

// Legacy is every legacy row of one organization.
type Legacy struct {
	Incidents []legacyIncident
	Alerts    []legacyAlert
	Schedules []legacySchedule
}

// utc normalizes a stored time the way _utc_datetime does: UTC, at the
// microsecond the DateTime64(6) column holds.
func utc(value time.Time) time.Time { return value.UTC().Truncate(time.Microsecond) }

func utcPointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := utc(*value)
	return &normalized
}

// LoadLegacy reads the legacy tables of the organization (latest version of
// each row: FINAL).
func LoadLegacy(ctx context.Context, conn driver.Conn, orgID string) (Legacy, error) {
	var legacy Legacy
	org := clickhouse.Named("org_id", orgID)

	rows, err := conn.Query(ctx, "SELECT id, url, summary, description, status, severity, created_at, "+
		"provider_id, last_synced FROM atlassian_ops_incidents FINAL WHERE org_id = {org_id:String}", org)
	if err != nil {
		return Legacy{}, err
	}
	for rows.Next() {
		var row legacyIncident
		if err := rows.Scan(&row.ID, &row.URL, &row.Summary, &row.Description, &row.Status,
			&row.Severity, &row.CreatedAt, &row.ProviderID, &row.LastSynced); err != nil {
			_ = rows.Close()
			return Legacy{}, err
		}
		row.CreatedAt, row.LastSynced = utc(row.CreatedAt), utc(row.LastSynced)
		legacy.Incidents = append(legacy.Incidents, row)
	}
	if err := closeRows(rows); err != nil {
		return Legacy{}, err
	}

	rows, err = conn.Query(ctx, "SELECT id, status, priority, created_at, acknowledged_at, snoozed_at, "+
		"closed_at, last_synced FROM atlassian_ops_alerts FINAL WHERE org_id = {org_id:String}", org)
	if err != nil {
		return Legacy{}, err
	}
	for rows.Next() {
		var row legacyAlert
		if err := rows.Scan(&row.ID, &row.Status, &row.Priority, &row.CreatedAt, &row.AcknowledgedAt,
			&row.SnoozedAt, &row.ClosedAt, &row.LastSynced); err != nil {
			_ = rows.Close()
			return Legacy{}, err
		}
		row.CreatedAt, row.LastSynced = utc(row.CreatedAt), utc(row.LastSynced)
		row.AcknowledgedAt, row.SnoozedAt, row.ClosedAt =
			utcPointer(row.AcknowledgedAt), utcPointer(row.SnoozedAt), utcPointer(row.ClosedAt)
		legacy.Alerts = append(legacy.Alerts, row)
	}
	if err := closeRows(rows); err != nil {
		return Legacy{}, err
	}

	rows, err = conn.Query(ctx, "SELECT id, name, timezone, last_synced FROM atlassian_ops_schedules FINAL "+
		"WHERE org_id = {org_id:String}", org)
	if err != nil {
		return Legacy{}, err
	}
	for rows.Next() {
		var row legacySchedule
		if err := rows.Scan(&row.ID, &row.Name, &row.Timezone, &row.LastSynced); err != nil {
			_ = rows.Close()
			return Legacy{}, err
		}
		row.LastSynced = utc(row.LastSynced)
		legacy.Schedules = append(legacy.Schedules, row)
	}
	if err := closeRows(rows); err != nil {
		return Legacy{}, err
	}
	return legacy, nil
}

func closeRows(rows driver.Rows) error {
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	return rows.Close()
}

// The three normalizers, as _normalized_status / _normalized_priority /
// _normalized_severity: strip, lower (severity also drops hyphens), then a
// fixed vocabulary; anything else is NULL.
var statusVocabulary = map[string]string{
	"active": "active", "acknowledged": "acknowledged", "closed": "resolved", "open": "open",
	"opened": "open", "resolved": "resolved", "suppressed": "suppressed",
}

var priorityVocabulary = map[string]string{
	"critical": "critical", "high": "high", "low": "low", "medium": "medium",
	"p1": "critical", "p2": "high", "p3": "medium", "p4": "low",
}

var severityVocabulary = map[string]string{
	"critical": "critical", "high": "high", "info": "info", "low": "low", "medium": "medium",
	"sev1": "critical", "sev2": "high", "sev3": "medium", "sev4": "low",
}

func vocabulary(table map[string]string, raw string, dropHyphens bool) *string {
	key := pyLower(pyStrip(raw))
	if dropHyphens {
		key = strings.ReplaceAll(key, "-", "")
	}
	if mapped, ok := table[key]; ok {
		return &mapped
	}
	return nil
}

// Row is one canonical operational row of a single family, ready to insert:
// the named fields of the Python dataclass in declaration order, with the
// derived id and ordering values.
type Row struct {
	Family           string
	Table            string
	ID               string
	SourceRevision   *big.Int
	SourceConflict   string
	IngestRevision   *big.Int
	OrderingContract uint8
	Fields           []field
}

// value returns the named field, for the columns that are not derived.
func (row Row) value(name string) (any, bool) {
	for _, item := range row.Fields {
		if item.name == name {
			return item.value, true
		}
	}
	return nil, false
}

// derived is the value of a derived column.
func (row Row) derived(name string) (any, bool) {
	switch name {
	case "source_revision":
		return row.SourceRevision, true
	case "source_conflict_key":
		return row.SourceConflict, true
	case "ingest_revision":
		return row.IngestRevision, true
	case "ordering_contract":
		return row.OrderingContract, true
	case "id":
		return row.ID, true
	}
	return nil, false
}

func stringPointer(value string) *string { return &value }

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return *value
}

// baseFields is the CanonicalOperationalEntity fields that are neither derived
// nor excluded from the conflict key, then the two excluded timestamps, then
// the rest of the base, in declaration order.
type entityInput struct {
	orgID, instance, sourceEntityType, externalID string
	sourceVersionAt                               time.Time
	sourceURL                                     *string
	rawStatus, rawSeverity, rawPriority           *string
	normStatus, normSeverity, normPriority        *string
	extra                                         []field
}

// buildRow assembles one entity as Python's dataclass does: the fields in
// declaration order, the id from the identity seed, and the ordering values
// from the conflict key (every field except the id, the ordering fields and the
// two observation timestamps). observedAt is both observed_at and last_synced,
// which Python fills from the wall clock at construction.
func buildRow(family, table string, input entityInput, observedAt time.Time) (Row, error) {
	id, err := canonicalID(input.orgID, atlassianProvider, input.instance, family, input.externalID)
	if err != nil {
		return Row{}, err
	}
	// Fields in declaration order; the derived ones (source_revision ..
	// ordering_contract, id) are not fields here.
	fields := []field{
		{"org_id", input.orgID}, {"provider", atlassianProvider},
		{"provider_instance_id", input.instance}, {"source_entity_type", input.sourceEntityType},
		{"external_id", input.externalID}, {"source_version_at", input.sourceVersionAt},
		{"source_id", nil}, {"source_url", nullableString(input.sourceURL)},
		{"source_event_at", nil}, {"source_event_id", nil},
		{"observed_at", observedAt}, {"last_synced", observedAt},
		{"raw_status", nullableString(input.rawStatus)}, {"raw_severity", nullableString(input.rawSeverity)},
		{"raw_priority", nullableString(input.rawPriority)},
		{"normalized_status", nullableString(input.normStatus)},
		{"normalized_severity", nullableString(input.normSeverity)},
		{"normalized_priority", nullableString(input.normPriority)},
		{"relationship_provenance", nil}, {"relationship_confidence", nil},
	}
	fields = append(fields, input.extra...)

	conflictFields := make([]field, 0, len(fields))
	for _, item := range fields {
		if item.name == "observed_at" || item.name == "last_synced" {
			continue
		}
		conflictFields = append(conflictFields, item)
	}
	key, err := conflictKey(family, conflictFields)
	if err != nil {
		return Row{}, err
	}
	revision, err := sourceRevision(input.sourceVersionAt, rankActiveUpdate, key)
	if err != nil {
		return Row{}, err
	}
	ingest, err := ingestRevision(observedAt, observedAt)
	if err != nil {
		return Row{}, err
	}
	return Row{
		Family: family, Table: table, ID: id, SourceRevision: revision, SourceConflict: key,
		IngestRevision: ingest, OrderingContract: orderingContract, Fields: fields,
	}, nil
}

// Batch is the canonical rows of one legacy load.
type Batch struct {
	Incidents []Row
	Alerts    []Row
	Schedules []Row
}

// Map is map_atlassian_ops_batch. observedAt stands for the wall-clock default
// Python gives observed_at and last_synced.
func Map(orgID, instance string, legacy Legacy, observedAt time.Time) (Batch, error) {
	observedAt = utc(observedAt)
	var batch Batch
	for _, item := range legacy.Incidents {
		normStatus := vocabulary(statusVocabulary, item.Status, false)
		var resolvedAt any
		if normStatus != nil && *normStatus == "resolved" {
			resolvedAt = item.LastSynced
		}
		row, err := buildRow(familyIncident, tableIncidents, entityInput{
			orgID: orgID, instance: instance, sourceEntityType: "atlassian_ops_incident",
			externalID: item.ID, sourceVersionAt: item.LastSynced, sourceURL: item.URL,
			rawStatus: stringPointer(item.Status), rawSeverity: stringPointer(item.Severity),
			normStatus: normStatus, normSeverity: vocabulary(severityVocabulary, item.Severity, true),
			extra: []field{
				{"service_id", nil}, {"service_external_id", nil}, {"escalation_policy_id", nil},
				{"title", item.Summary}, {"description", nullableString(item.Description)},
				{"started_at", item.CreatedAt}, {"resolved_at", resolvedAt},
				{"is_deleted", false}, {"deleted_at", nil},
			},
		}, observedAt)
		if err != nil {
			return Batch{}, err
		}
		batch.Incidents = append(batch.Incidents, row)
	}
	for _, item := range legacy.Alerts {
		row, err := buildRow(familyAlert, tableAlerts, entityInput{
			orgID: orgID, instance: instance, sourceEntityType: "atlassian_ops_alert",
			externalID: item.ID, sourceVersionAt: item.LastSynced,
			rawStatus: stringPointer(item.Status), rawPriority: stringPointer(item.Priority),
			normStatus:   vocabulary(statusVocabulary, item.Status, false),
			normPriority: vocabulary(priorityVocabulary, item.Priority, false),
			extra: []field{
				{"service_id", nil}, {"incident_id", nil}, {"title", ""}, {"description", nil},
				{"triggered_at", item.CreatedAt}, {"acknowledged_at", nullableTime(item.AcknowledgedAt)},
				{"resolved_at", nullableTime(item.ClosedAt)}, {"is_deleted", false}, {"deleted_at", nil},
			},
		}, observedAt)
		if err != nil {
			return Batch{}, err
		}
		batch.Alerts = append(batch.Alerts, row)
	}
	for _, item := range legacy.Schedules {
		row, err := buildRow(familySchedule, tableSchedules, entityInput{
			orgID: orgID, instance: instance, sourceEntityType: "atlassian_ops_schedule",
			externalID: item.ID, sourceVersionAt: item.LastSynced,
			extra: []field{
				{"name", item.Name}, {"description", nil}, {"timezone", nullableString(item.Timezone)},
				{"is_deleted", false}, {"deleted_at", nil},
			},
		}, observedAt)
		if err != nil {
			return Batch{}, err
		}
		batch.Schedules = append(batch.Schedules, row)
	}
	return batch, nil
}
