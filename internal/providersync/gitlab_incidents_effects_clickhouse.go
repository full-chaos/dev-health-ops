package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	gitLabOperationalBaseColumns    = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at,source_revision,source_conflict_key,ingest_revision,ordering_contract,id,source_id,source_url,source_event_at,source_event_id,observed_at,last_synced,raw_status,raw_severity,raw_priority,normalized_status,normalized_severity,normalized_priority,relationship_provenance,relationship_confidence"
	gitLabOperationalServiceColumns = gitLabOperationalBaseColumns + ",name,description,service_type,owning_team_id,escalation_policy_id,is_deleted,deleted_at"
	gitLabServiceMappingColumns     = gitLabOperationalBaseColumns + ",service_id,repo_id,repo_full_name,repo_provider,mapping_kind,rule_id,valid_from,valid_to,is_active"
)

type GitLabIncidentsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitLabIncidentsClickHouseEffects) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return err
	}
	switch effect.Destination {
	case "operational_services":
		rows, err := decodeEffectRows[gitLabOperationalServiceRow](effect)
		if err != nil {
			return err
		}
		if err := validateGitLabOperationalServices(claim, rows); err != nil {
			return err
		}
		return sink.writeServices(ctx, rows)
	case "operational_service_repository_mappings":
		rows, err := decodeEffectRows[gitLabServiceRepositoryMappingRow](effect)
		if err != nil {
			return err
		}
		if err := validateGitLabServiceMappings(claim, rows); err != nil {
			return err
		}
		return sink.writeMappings(ctx, rows)
	case "operational_incidents":
		rows, err := decodeEffectRows[jiraIncidentRow](effect)
		if err != nil {
			return err
		}
		if err := validateGitLabIncidentRows(claim, rows); err != nil {
			return err
		}
		return sink.writeIncidents(ctx, rows)
	default:
		return ErrInvalidConfiguration
	}
}

func (sink GitLabIncidentsClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return EffectConflict, err
	}
	switch effect.Destination {
	case "operational_services":
		expected, err := decodeEffectRows[gitLabOperationalServiceRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		if err := validateGitLabOperationalServices(claim, expected); err != nil {
			return EffectConflict, err
		}
		return sink.inspectServices(ctx, expected)
	case "operational_service_repository_mappings":
		expected, err := decodeEffectRows[gitLabServiceRepositoryMappingRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		if err := validateGitLabServiceMappings(claim, expected); err != nil {
			return EffectConflict, err
		}
		return sink.inspectMappings(ctx, expected)
	case "operational_incidents":
		expected, err := decodeEffectRows[jiraIncidentRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		if err := validateGitLabIncidentRows(claim, expected); err != nil {
			return EffectConflict, err
		}
		return sink.inspectIncidents(ctx, expected)
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

func (sink GitLabIncidentsClickHouseEffects) validateRequest(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if ctx == nil || sink.Conn == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "gitlab" || claim.Dataset != "incidents" {
		return ErrInvalidConfiguration
	}
	switch effect.Destination {
	case "operational_services", "operational_service_repository_mappings", "operational_incidents":
	default:
		return ErrInvalidConfiguration
	}
	return sink.Lease.Assert(ctx)
}

func (sink GitLabIncidentsClickHouseEffects) writeServices(
	ctx context.Context, rows []gitLabOperationalServiceRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, "INSERT INTO operational_services ("+gitLabOperationalServiceColumns+")")
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(gitLabOperationalServiceValues(row)...); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitLabIncidentsClickHouseEffects) writeMappings(
	ctx context.Context, rows []gitLabServiceRepositoryMappingRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, "INSERT INTO operational_service_repository_mappings ("+gitLabServiceMappingColumns+")")
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(gitLabServiceMappingValues(row)...); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitLabIncidentsClickHouseEffects) writeIncidents(
	ctx context.Context, rows []jiraIncidentRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, "INSERT INTO operational_incidents ("+jiraIncidentColumns+")")
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

func validateGitLabOperationalServices(claim Claim, rows []gitLabOperationalServiceRow) error {
	if err := validateUniqueGitLabOperationalIDs(rows, func(row gitLabOperationalServiceRow) string { return row.ID }); err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != claim.OrgID || row.Provider != "gitlab" || row.ProviderInstanceID == "" ||
			row.SourceEntityType != "repository" || row.ExternalID == "" || row.Name == "" ||
			row.ID == "" || row.SourceRevision == nil || row.IngestRevision == nil ||
			row.SourceConflictKey == "" || row.OrderingContract != 2 {
			return providerfoundation.ErrInvalidScope
		}
		canonical := row
		canonical.ID, canonical.SourceConflictKey = "", ""
		canonical.SourceRevision, canonical.IngestRevision = nil, nil
		canonical.OrderingContract = 0
		if err := fillGitLabOperationalServiceOrdering(&canonical); err != nil ||
			canonical.ID != row.ID || canonical.SourceConflictKey != row.SourceConflictKey ||
			canonical.SourceRevision.Cmp(row.SourceRevision) != 0 ||
			canonical.IngestRevision.Cmp(row.IngestRevision) != 0 {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func validateGitLabServiceMappings(claim Claim, rows []gitLabServiceRepositoryMappingRow) error {
	if err := validateUniqueGitLabOperationalIDs(rows, func(row gitLabServiceRepositoryMappingRow) string { return row.ID }); err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != claim.OrgID || row.Provider != "gitlab" || row.ProviderInstanceID == "" ||
			row.SourceEntityType != "repository_mapping" || row.ExternalID == "" ||
			row.ServiceID == "" || row.RepoID == nil || row.RepoFullName == nil ||
			row.ID == "" || row.SourceRevision == nil || row.IngestRevision == nil ||
			row.SourceConflictKey == "" || row.OrderingContract != 2 {
			return providerfoundation.ErrInvalidScope
		}
		canonical := row
		canonical.ID, canonical.SourceConflictKey = "", ""
		canonical.SourceRevision, canonical.IngestRevision = nil, nil
		canonical.OrderingContract = 0
		if err := fillGitLabServiceMappingOrdering(&canonical); err != nil ||
			canonical.ID != row.ID || canonical.SourceConflictKey != row.SourceConflictKey ||
			canonical.SourceRevision.Cmp(row.SourceRevision) != 0 ||
			canonical.IngestRevision.Cmp(row.IngestRevision) != 0 {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func validateGitLabIncidentRows(claim Claim, rows []jiraIncidentRow) error {
	if err := validateUniqueGitLabOperationalIDs(rows, func(row jiraIncidentRow) string { return row.ID }); err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != claim.OrgID || row.Provider != "gitlab" || row.ProviderInstanceID == "" ||
			!strings.EqualFold(row.SourceEntityType, "incident") || row.ExternalID == "" ||
			row.ID == "" || row.SourceRevision == nil || row.IngestRevision == nil ||
			row.SourceConflictKey == "" || row.OrderingContract != 2 || row.StartedAt == nil {
			return providerfoundation.ErrInvalidScope
		}
		canonical := row
		canonical.ID, canonical.SourceConflictKey = "", ""
		canonical.SourceRevision, canonical.IngestRevision = nil, nil
		canonical.OrderingContract = 0
		if err := fillGitLabIncidentOrdering(&canonical); err != nil ||
			canonical.ID != row.ID || canonical.SourceConflictKey != row.SourceConflictKey ||
			canonical.SourceRevision.Cmp(row.SourceRevision) != 0 ||
			canonical.IngestRevision.Cmp(row.IngestRevision) != 0 {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func validateUniqueGitLabOperationalIDs[T any](rows []T, id func(T) string) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		key := id(row)
		if _, duplicate := seen[key]; duplicate {
			return ErrInvalidConfiguration
		}
		seen[key] = struct{}{}
	}
	return nil
}

func gitLabOperationalBaseValues(
	orgID, provider, instance, entityType, externalID string,
	sourceVersion time.Time, sourceRevision *big.Int, conflict string,
	ingestRevision *big.Int, ordering uint8, id string, sourceID any,
	sourceURL *string, sourceEventAt *time.Time, sourceEventID *string,
	observedAt, lastSynced time.Time, rawStatus, rawSeverity, rawPriority,
	normalizedStatus, normalizedSeverity, normalizedPriority,
	relationshipProvenance *string, relationshipConfidence *float64,
) []any {
	return []any{
		orgID, provider, instance, entityType, externalID, sourceVersion,
		sourceRevision, conflict, ingestRevision, ordering, id, sourceID,
		sourceURL, sourceEventAt, sourceEventID, observedAt, lastSynced,
		rawStatus, rawSeverity, rawPriority, normalizedStatus, normalizedSeverity,
		normalizedPriority, relationshipProvenance, relationshipConfidence,
	}
}

func gitLabOperationalServiceValues(row gitLabOperationalServiceRow) []any {
	values := gitLabOperationalBaseValues(
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, row.SourceRevision, row.SourceConflictKey,
		row.IngestRevision, row.OrderingContract, row.ID, row.SourceID, row.SourceURL,
		row.SourceEventAt, row.SourceEventID, row.ObservedAt, row.LastSynced,
		row.RawStatus, row.RawSeverity, row.RawPriority, row.NormalizedStatus,
		row.NormalizedSeverity, row.NormalizedPriority, row.RelationshipProvenance,
		row.RelationshipConfidence,
	)
	return append(values, row.Name, row.Description, row.ServiceType, row.OwningTeamID,
		row.EscalationPolicyID, row.IsDeleted, row.DeletedAt)
}

func gitLabServiceMappingValues(row gitLabServiceRepositoryMappingRow) []any {
	values := gitLabOperationalBaseValues(
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, row.SourceRevision, row.SourceConflictKey,
		row.IngestRevision, row.OrderingContract, row.ID, row.SourceID, row.SourceURL,
		row.SourceEventAt, row.SourceEventID, row.ObservedAt, row.LastSynced,
		row.RawStatus, row.RawSeverity, row.RawPriority, row.NormalizedStatus,
		row.NormalizedSeverity, row.NormalizedPriority, row.RelationshipProvenance,
		row.RelationshipConfidence,
	)
	return append(values, row.ServiceID, row.RepoID, row.RepoFullName, row.RepoProvider,
		row.MappingKind, row.RuleID, row.ValidFrom, row.ValidTo, row.IsActive)
}

func (sink GitLabIncidentsClickHouseEffects) inspectServices(
	ctx context.Context, expected []gitLabOperationalServiceRow,
) (EffectInspection, error) {
	return inspectGitLabOperationalRows(ctx, expected, func(ctx context.Context, row gitLabOperationalServiceRow) (gitLabOperationalServiceRow, bool, error) {
		rows, err := sink.Conn.Query(ctx, "SELECT "+gitLabOperationalServiceColumns+" FROM operational_services FINAL WHERE org_id = ? AND id = ? LIMIT 1", row.OrgID, row.ID)
		if err != nil {
			return gitLabOperationalServiceRow{}, false, err
		}
		defer rows.Close()
		var actual gitLabOperationalServiceRow
		found := false
		for rows.Next() {
			var sourceRevision, ingestRevision big.Int
			if err := rows.Scan(gitLabOperationalServiceScanValues(&actual, &sourceRevision, &ingestRevision)...); err != nil {
				return gitLabOperationalServiceRow{}, false, err
			}
			actual.SourceRevision, actual.IngestRevision = new(big.Int).Set(&sourceRevision), new(big.Int).Set(&ingestRevision)
			found = true
		}
		if found && actual.SourceRevision.Cmp(row.SourceRevision) < 0 {
			found = false
		}
		return actual, found, rows.Err()
	})
}

func (sink GitLabIncidentsClickHouseEffects) inspectMappings(
	ctx context.Context, expected []gitLabServiceRepositoryMappingRow,
) (EffectInspection, error) {
	return inspectGitLabOperationalRows(ctx, expected, func(ctx context.Context, row gitLabServiceRepositoryMappingRow) (gitLabServiceRepositoryMappingRow, bool, error) {
		rows, err := sink.Conn.Query(ctx, "SELECT "+gitLabServiceMappingColumns+" FROM operational_service_repository_mappings FINAL WHERE org_id = ? AND id = ? LIMIT 1", row.OrgID, row.ID)
		if err != nil {
			return gitLabServiceRepositoryMappingRow{}, false, err
		}
		defer rows.Close()
		var actual gitLabServiceRepositoryMappingRow
		found := false
		for rows.Next() {
			var sourceRevision, ingestRevision big.Int
			if err := rows.Scan(gitLabServiceMappingScanValues(&actual, &sourceRevision, &ingestRevision)...); err != nil {
				return gitLabServiceRepositoryMappingRow{}, false, err
			}
			actual.SourceRevision, actual.IngestRevision = new(big.Int).Set(&sourceRevision), new(big.Int).Set(&ingestRevision)
			found = true
		}
		if found && actual.SourceRevision.Cmp(row.SourceRevision) < 0 {
			found = false
		}
		return actual, found, rows.Err()
	})
}

func (sink GitLabIncidentsClickHouseEffects) inspectIncidents(
	ctx context.Context, expected []jiraIncidentRow,
) (EffectInspection, error) {
	return inspectGitLabOperationalRows(ctx, expected, func(ctx context.Context, row jiraIncidentRow) (jiraIncidentRow, bool, error) {
		rows, err := sink.Conn.Query(ctx, "SELECT "+jiraIncidentColumns+" FROM operational_incidents FINAL WHERE org_id = ? AND id = ? LIMIT 1", row.OrgID, row.ID)
		if err != nil {
			return jiraIncidentRow{}, false, err
		}
		defer rows.Close()
		var actual jiraIncidentRow
		found := false
		for rows.Next() {
			var sourceRevision, ingestRevision big.Int
			if err := rows.Scan(jiraIncidentScanValues(&actual, &sourceRevision, &ingestRevision)...); err != nil {
				return jiraIncidentRow{}, false, err
			}
			actual.SourceRevision, actual.IngestRevision = new(big.Int).Set(&sourceRevision), new(big.Int).Set(&ingestRevision)
			found = true
		}
		if found && actual.SourceRevision.Cmp(row.SourceRevision) < 0 {
			found = false
		}
		return actual, found, rows.Err()
	})
}

func inspectGitLabOperationalRows[T any](
	ctx context.Context,
	expected []T,
	load func(context.Context, T) (T, bool, error),
) (EffectInspection, error) {
	if len(expected) == 0 {
		return EffectAbsent, nil
	}
	exact, absent := 0, 0
	for _, row := range expected {
		actual, found, err := load(ctx, row)
		if err != nil {
			return EffectConflict, err
		}
		if !found {
			absent++
			continue
		}
		expectedJSON, expectedErr := json.Marshal(row)
		actualJSON, actualErr := json.Marshal(actual)
		if expectedErr != nil || actualErr != nil || !bytes.Equal(expectedJSON, actualJSON) {
			return EffectConflict, nil
		}
		exact++
	}
	if exact == len(expected) {
		return EffectExact, nil
	}
	if absent == len(expected) {
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

func gitLabOperationalServiceScanValues(row *gitLabOperationalServiceRow, sourceRevision, ingestRevision *big.Int) []any {
	values := gitLabOperationalBaseScanValues(
		&row.OrgID, &row.Provider, &row.ProviderInstanceID, &row.SourceEntityType,
		&row.ExternalID, &row.SourceVersionAt, sourceRevision, &row.SourceConflictKey,
		ingestRevision, &row.OrderingContract, &row.ID, &row.SourceID, &row.SourceURL,
		&row.SourceEventAt, &row.SourceEventID, &row.ObservedAt, &row.LastSynced,
		&row.RawStatus, &row.RawSeverity, &row.RawPriority, &row.NormalizedStatus,
		&row.NormalizedSeverity, &row.NormalizedPriority, &row.RelationshipProvenance,
		&row.RelationshipConfidence,
	)
	return append(values, &row.Name, &row.Description, &row.ServiceType, &row.OwningTeamID,
		&row.EscalationPolicyID, &row.IsDeleted, &row.DeletedAt)
}

func gitLabServiceMappingScanValues(row *gitLabServiceRepositoryMappingRow, sourceRevision, ingestRevision *big.Int) []any {
	values := gitLabOperationalBaseScanValues(
		&row.OrgID, &row.Provider, &row.ProviderInstanceID, &row.SourceEntityType,
		&row.ExternalID, &row.SourceVersionAt, sourceRevision, &row.SourceConflictKey,
		ingestRevision, &row.OrderingContract, &row.ID, &row.SourceID, &row.SourceURL,
		&row.SourceEventAt, &row.SourceEventID, &row.ObservedAt, &row.LastSynced,
		&row.RawStatus, &row.RawSeverity, &row.RawPriority, &row.NormalizedStatus,
		&row.NormalizedSeverity, &row.NormalizedPriority, &row.RelationshipProvenance,
		&row.RelationshipConfidence,
	)
	return append(values, &row.ServiceID, &row.RepoID, &row.RepoFullName, &row.RepoProvider,
		&row.MappingKind, &row.RuleID, &row.ValidFrom, &row.ValidTo, &row.IsActive)
}

func gitLabOperationalBaseScanValues(values ...any) []any { return values }

var _ EffectSink = GitLabIncidentsClickHouseEffects{}
var _ EffectReadback = GitLabIncidentsClickHouseEffects{}
