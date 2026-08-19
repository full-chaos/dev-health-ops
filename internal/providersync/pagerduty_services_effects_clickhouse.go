package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

// Migration 067 changes the operational tables from the legacy
// source_version_at contract to the v2 source-revision contract. The Go writer
// must use the same bridge setting as Python while both schemas are supported.
const (
	pagerDutyServicesLegacyBaseColumns    = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at,id,source_id,source_url,source_event_at,source_event_id,observed_at,last_synced,raw_status,raw_severity,raw_priority,normalized_status,normalized_severity,normalized_priority,relationship_provenance,relationship_confidence"
	pagerDutyServicesLegacyServiceColumns = pagerDutyServicesLegacyBaseColumns + ",name,description,service_type,owning_team_id,escalation_policy_id,is_deleted,deleted_at"
	pagerDutyServicesLegacyMappingColumns = pagerDutyServicesLegacyBaseColumns + ",service_id,repo_id,repo_full_name,repo_provider,mapping_kind,rule_id,valid_from,valid_to,is_active"
)

type pagerDutyServicesStorageContract uint8

const (
	pagerDutyServicesLegacyContract  pagerDutyServicesStorageContract = 1
	pagerDutyServicesCurrentContract pagerDutyServicesStorageContract = 2
)

func configuredPagerDutyServicesStorageContract() (pagerDutyServicesStorageContract, error) {
	raw, present := os.LookupEnv("OPERATIONAL_ORDERING_CONTRACT")
	if !present || raw == "1" {
		return pagerDutyServicesLegacyContract, nil
	}
	if raw == "2" {
		return pagerDutyServicesCurrentContract, nil
	}
	return 0, ErrInvalidConfiguration
}

func (contract pagerDutyServicesStorageContract) serviceColumns() string {
	if contract == pagerDutyServicesLegacyContract {
		return pagerDutyServicesLegacyServiceColumns
	}
	return gitLabOperationalServiceColumns
}

func (contract pagerDutyServicesStorageContract) mappingColumns() string {
	if contract == pagerDutyServicesLegacyContract {
		return pagerDutyServicesLegacyMappingColumns
	}
	return gitLabServiceMappingColumns
}

func (contract pagerDutyServicesStorageContract) loadActiveServicesQuery() string {
	columns := contract.serviceColumns()
	if contract == pagerDutyServicesLegacyContract {
		return "SELECT " + columns + " FROM (SELECT " + columns +
			" FROM operational_services FINAL WHERE org_id = ?) " +
			"WHERE provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND is_deleted = 0"
	}
	return "SELECT " + columns + " FROM (SELECT " + columns +
		" FROM operational_services WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? " +
		"ORDER BY org_id, id, source_revision DESC, source_conflict_key DESC, ingest_revision DESC LIMIT 1 BY org_id, id) " +
		"WHERE is_deleted = 0"
}

func (contract pagerDutyServicesStorageContract) loadActiveMappingsQuery() string {
	columns := contract.mappingColumns()
	if contract == pagerDutyServicesLegacyContract {
		return "SELECT " + columns + " FROM (SELECT " + columns +
			" FROM operational_service_repository_mappings FINAL WHERE org_id = ?) " +
			"WHERE provider = ? AND provider_instance_id = ? AND is_active = 1"
	}
	return "SELECT " + columns + " FROM (SELECT " + columns +
		" FROM operational_service_repository_mappings WHERE org_id = ? AND provider = ? AND provider_instance_id = ? " +
		"ORDER BY org_id, id, source_revision DESC, source_conflict_key DESC, ingest_revision DESC LIMIT 1 BY org_id, id) " +
		"WHERE is_active = 1"
}

func (contract pagerDutyServicesStorageContract) loadServiceQuery() string {
	columns := contract.serviceColumns()
	if contract == pagerDutyServicesLegacyContract {
		return "SELECT " + columns +
			" FROM operational_services FINAL WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND id = ? LIMIT 1"
	}
	return "SELECT " + columns +
		" FROM operational_services WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND source_entity_type = ? AND id = ? " +
		"ORDER BY source_revision DESC, source_conflict_key DESC, ingest_revision DESC LIMIT 1"
}

func (contract pagerDutyServicesStorageContract) loadMappingQuery() string {
	columns := contract.mappingColumns()
	if contract == pagerDutyServicesLegacyContract {
		return "SELECT " + columns +
			" FROM operational_service_repository_mappings FINAL WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND id = ? LIMIT 1"
	}
	return "SELECT " + columns +
		" FROM operational_service_repository_mappings WHERE org_id = ? AND provider = ? AND provider_instance_id = ? AND id = ? " +
		"ORDER BY source_revision DESC, source_conflict_key DESC, ingest_revision DESC LIMIT 1"
}

// PagerDutyServicesClickHouseEffects owns only the two destinations produced
// by the services provider path. The repository mapping destination remains a
// separate typed effect so its source/provenance contract cannot be silently
// absorbed into OperationalService rows.
type PagerDutyServicesClickHouseEffects struct {
	Conn               driver.Conn
	Lease              providerfoundation.LeaseGuard
	ProviderInstanceID string
	Now                func() time.Time
}

func (sink PagerDutyServicesClickHouseEffects) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return err
	}
	switch effect.Destination {
	case "operational_services":
		rows, err := decodeEffectRows[pagerDutyServiceRow](effect)
		if err != nil {
			return err
		}
		if err := validatePagerDutyServiceRows(claim, rows); err != nil {
			return err
		}
		return sink.writeServicesSnapshot(ctx, claim, rows)
	case "operational_service_repository_mappings":
		rows, err := decodeEffectRows[pagerDutyServiceRepositoryMappingRow](effect)
		if err != nil {
			return err
		}
		rows, err = sink.resolvePagerDutyServiceMappings(ctx, claim, rows)
		if err != nil {
			return err
		}
		if err := validatePagerDutyServiceMappingRows(claim, rows); err != nil {
			return err
		}
		return sink.writeMappingsSnapshot(ctx, claim, rows)
	default:
		return ErrInvalidConfiguration
	}
}

func (sink PagerDutyServicesClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return EffectConflict, err
	}
	switch effect.Destination {
	case "operational_services":
		expected, err := decodeEffectRows[pagerDutyServiceRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		if err := validatePagerDutyServiceRows(claim, expected); err != nil {
			return EffectConflict, err
		}
		return sink.inspectServices(ctx, claim, expected)
	case "operational_service_repository_mappings":
		expected, err := decodeEffectRows[pagerDutyServiceRepositoryMappingRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		expected, err = sink.resolvePagerDutyServiceMappings(ctx, claim, expected)
		if err != nil {
			return EffectConflict, err
		}
		if err := validatePagerDutyServiceMappingRows(claim, expected); err != nil {
			return EffectConflict, err
		}
		return sink.inspectMappings(ctx, claim, expected)
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

func (sink PagerDutyServicesClickHouseEffects) validateRequest(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if ctx == nil || sink.Conn == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "pagerduty" || claim.Dataset != "services" {
		return ErrInvalidConfiguration
	}
	switch effect.Destination {
	case "operational_services", "operational_service_repository_mappings":
	default:
		return ErrInvalidConfiguration
	}
	if _, err := configuredPagerDutyServicesStorageContract(); err != nil {
		return err
	}
	return sink.Lease.Assert(ctx)
}

func (sink PagerDutyServicesClickHouseEffects) providerInstance(
	instances ...string,
) (string, error) {
	instance := strings.ToLower(strings.TrimSpace(sink.ProviderInstanceID))
	for _, value := range instances {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			return "", providerfoundation.ErrInvalidScope
		}
		if instance == "" {
			instance = value
		}
		if instance != value {
			return "", providerfoundation.ErrInvalidScope
		}
	}
	if instance == "" {
		return "", ErrInvalidConfiguration
	}
	return instance, nil
}

func validatePagerDutyServiceRows(claim Claim, rows []pagerDutyServiceRow) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, duplicate := seen[row.ID]; duplicate {
			return ErrInvalidConfiguration
		}
		seen[row.ID] = struct{}{}
		if row.OrgID != claim.OrgID || row.Provider != "pagerduty" ||
			row.ProviderInstanceID == "" || row.ProviderInstanceID != strings.ToLower(row.ProviderInstanceID) ||
			row.SourceEntityType != "service" || row.ExternalID == "" || row.Name == "" ||
			row.ServiceType == nil || *row.ServiceType != "technical" || row.OwningTeamID != nil ||
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
		if err := fillPagerDutyServiceOrdering(&canonical); err != nil ||
			canonical.ID != row.ID || canonical.SourceConflictKey != row.SourceConflictKey ||
			canonical.SourceRevision.Cmp(row.SourceRevision) != 0 ||
			canonical.IngestRevision.Cmp(row.IngestRevision) != 0 {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func validatePagerDutyServiceMappingRows(
	claim Claim, rows []pagerDutyServiceRepositoryMappingRow,
) error {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, duplicate := seen[row.ID]; duplicate {
			return ErrInvalidConfiguration
		}
		seen[row.ID] = struct{}{}
		if !pagerDutyMappingSourceValid(row.SourceEntityType) ||
			row.OrgID != claim.OrgID || row.Provider != "pagerduty" ||
			row.ProviderInstanceID == "" || row.ProviderInstanceID != strings.ToLower(row.ProviderInstanceID) ||
			row.ExternalID == "" || row.ServiceID == "" || row.ID == "" ||
			row.SourceRevision == nil || row.IngestRevision == nil || row.SourceConflictKey == "" ||
			row.OrderingContract != 2 || row.SourceVersionAt.IsZero() || row.ObservedAt.IsZero() ||
			row.LastSynced.IsZero() || row.RelationshipProvenance == nil ||
			*row.RelationshipProvenance != row.SourceEntityType || row.RelationshipConfidence == nil ||
			*row.RelationshipConfidence < 0 || *row.RelationshipConfidence > 1 ||
			row.RepoProvider == nil || *row.RepoProvider == "" || row.RepoFullName == nil ||
			*row.RepoFullName == "" || row.MappingKind == nil || *row.MappingKind == "" ||
			row.RuleID == nil || *row.RuleID == "" || row.ValidFrom == nil ||
			row.ValidTo != nil && row.IsActive || (!row.IsActive && row.ValidTo == nil) {
			return providerfoundation.ErrInvalidScope
		}
		canonical := row
		canonical.ID, canonical.SourceConflictKey = "", ""
		canonical.SourceRevision, canonical.IngestRevision = nil, nil
		canonical.OrderingContract = 0
		if err := fillPagerDutyServiceMappingOrdering(&canonical); err != nil ||
			canonical.ID != row.ID || canonical.SourceConflictKey != row.SourceConflictKey ||
			canonical.SourceRevision.Cmp(row.SourceRevision) != 0 ||
			canonical.IngestRevision.Cmp(row.IngestRevision) != 0 {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func pagerDutyMappingSourceValid(value string) bool {
	switch pagerDutyServiceMappingSource(value) {
	case pagerDutyMappingAdmin, pagerDutyMappingMetadata, pagerDutyMappingCompass, pagerDutyMappingHeuristic:
		return true
	default:
		return false
	}
}

func (sink PagerDutyServicesClickHouseEffects) writeServicesSnapshot(
	ctx context.Context, claim Claim, rows []pagerDutyServiceRow,
) error {
	providerInstance, err := sink.providerInstance(pagerDutyServiceInstances(rows)...)
	if err != nil {
		return err
	}
	active, err := sink.loadActiveServices(ctx, claim, providerInstance)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		seen[row.ID] = struct{}{}
	}
	observedAt := sink.snapshotObservedAt(rows, claim)
	allRows := append([]pagerDutyServiceRow(nil), rows...)
	for _, existing := range active {
		if _, present := seen[existing.ID]; present {
			continue
		}
		tombstone, tombstoneErr := pagerDutyServiceTombstone(existing, observedAt)
		if tombstoneErr != nil {
			return tombstoneErr
		}
		allRows = append(allRows, tombstone)
	}
	if len(allRows) == 0 {
		return nil
	}
	contract, err := configuredPagerDutyServicesStorageContract()
	if err != nil {
		return err
	}
	batch, err := sink.Conn.PrepareBatch(ctx, "INSERT INTO operational_services ("+contract.serviceColumns()+")")
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range allRows {
		if err := batch.Append(pagerDutyServiceValuesForContract(row, contract)...); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink PagerDutyServicesClickHouseEffects) writeMappingsSnapshot(
	ctx context.Context, claim Claim, rows []pagerDutyServiceRepositoryMappingRow,
) error {
	instances := make([]string, 0, len(rows))
	for _, row := range rows {
		instances = append(instances, row.ProviderInstanceID)
	}
	providerInstance, err := sink.providerInstance(instances...)
	if err != nil {
		return err
	}
	active, err := sink.loadActiveMappings(ctx, claim, providerInstance)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		seen[row.ID] = struct{}{}
	}
	observedAt := sink.mappingSnapshotObservedAt(rows, claim)
	allRows := append([]pagerDutyServiceRepositoryMappingRow(nil), rows...)
	for _, existing := range active {
		if _, present := seen[existing.ID]; present {
			continue
		}
		tombstone, tombstoneErr := pagerDutyServiceMappingTombstone(existing, observedAt)
		if tombstoneErr != nil {
			return tombstoneErr
		}
		allRows = append(allRows, tombstone)
	}
	if len(allRows) == 0 {
		return nil
	}
	contract, err := configuredPagerDutyServicesStorageContract()
	if err != nil {
		return err
	}
	batch, err := sink.Conn.PrepareBatch(ctx, "INSERT INTO operational_service_repository_mappings ("+contract.mappingColumns()+")")
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range allRows {
		if err := batch.Append(pagerDutyServiceMappingValuesForContract(row, contract)...); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

// resolvePagerDutyServiceMappings mirrors Python's
// resolve_repository_mappings: repository identity is looked up only in the
// org-scoped ClickHouse catalog, while unmatched evidence remains unresolved
// with its provider/full-name pair intact. The effect payload stays a typed
// evidence row; this sink-side enrichment is deterministic and is repeated on
// readback/recovery before comparing the durable row.
func (sink PagerDutyServicesClickHouseEffects) resolvePagerDutyServiceMappings(
	ctx context.Context, claim Claim, rows []pagerDutyServiceRepositoryMappingRow,
) ([]pagerDutyServiceRepositoryMappingRow, error) {
	if len(rows) == 0 {
		return rows, nil
	}
	needsCatalog := false
	for _, row := range rows {
		if row.RepoID == nil && row.RepoProvider != nil && row.RepoFullName != nil {
			needsCatalog = true
			break
		}
	}
	if !needsCatalog {
		return rows, nil
	}
	catalogRows, err := sink.Conn.Query(
		ctx, "SELECT id, provider, repo FROM repos FINAL WHERE org_id = ?", claim.OrgID,
	)
	if err != nil {
		return nil, err
	}
	defer catalogRows.Close()
	catalog := make(map[string]uuid.UUID)
	for catalogRows.Next() {
		var repoID uuid.UUID
		var provider, fullName string
		if err := catalogRows.Scan(&repoID, &provider, &fullName); err != nil {
			return nil, err
		}
		catalog[provider+"\x00"+fullName] = repoID
	}
	if err := catalogRows.Err(); err != nil {
		return nil, err
	}
	resolved := make([]pagerDutyServiceRepositoryMappingRow, len(rows))
	copy(resolved, rows)
	for index, row := range resolved {
		if row.RepoID != nil || row.RepoProvider == nil || row.RepoFullName == nil {
			continue
		}
		repoID, found := catalog[*row.RepoProvider+"\x00"+*row.RepoFullName]
		if !found {
			continue
		}
		row.RepoID = &repoID
		row.ID, row.SourceConflictKey = "", ""
		row.SourceRevision, row.IngestRevision = nil, nil
		row.OrderingContract = 0
		if err := fillPagerDutyServiceMappingOrdering(&row); err != nil {
			return nil, err
		}
		resolved[index] = row
	}
	return resolved, nil
}

func pagerDutyServiceInstances(rows []pagerDutyServiceRow) []string {
	instances := make([]string, 0, len(rows))
	for _, row := range rows {
		instances = append(instances, row.ProviderInstanceID)
	}
	return instances
}

func (sink PagerDutyServicesClickHouseEffects) snapshotObservedAt(rows []pagerDutyServiceRow, claim Claim) time.Time {
	for _, row := range rows {
		if !row.ObservedAt.IsZero() {
			return row.ObservedAt.UTC().Truncate(time.Microsecond)
		}
	}
	return sink.snapshotNow(claim)
}

func (sink PagerDutyServicesClickHouseEffects) mappingSnapshotObservedAt(rows []pagerDutyServiceRepositoryMappingRow, claim Claim) time.Time {
	for _, row := range rows {
		if !row.ObservedAt.IsZero() {
			return row.ObservedAt.UTC().Truncate(time.Microsecond)
		}
	}
	return sink.snapshotNow(claim)
}

func (sink PagerDutyServicesClickHouseEffects) snapshotNow(claim Claim) time.Time {
	if sink.Now != nil {
		return sink.Now().UTC().Truncate(time.Microsecond)
	}
	if claim.BeforeAt != nil && !claim.BeforeAt.IsZero() {
		return claim.BeforeAt.UTC().Truncate(time.Microsecond)
	}
	return time.Now().UTC().Truncate(time.Microsecond)
}

func pagerDutyServiceValues(row pagerDutyServiceRow) []any {
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

func pagerDutyServiceValuesForContract(
	row pagerDutyServiceRow, contract pagerDutyServicesStorageContract,
) []any {
	return pagerDutyServicesValuesForContract(pagerDutyServiceValues(row), contract)
}

func pagerDutyServiceMappingValues(row pagerDutyServiceRepositoryMappingRow) []any {
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

func pagerDutyServiceMappingValuesForContract(
	row pagerDutyServiceRepositoryMappingRow, contract pagerDutyServicesStorageContract,
) []any {
	return pagerDutyServicesValuesForContract(pagerDutyServiceMappingValues(row), contract)
}

func pagerDutyServicesValuesForContract(
	values []any, contract pagerDutyServicesStorageContract,
) []any {
	if contract != pagerDutyServicesLegacyContract {
		return values
	}
	// The v2 ordering fields occupy the four positions immediately after
	// source_version_at. Contract 1 stores all other canonical fields.
	legacy := make([]any, 0, len(values)-4)
	legacy = append(legacy, values[:6]...)
	return append(legacy, values[10:]...)
}

func pagerDutyServiceScanValues(row *pagerDutyServiceRow, sourceRevision, ingestRevision *big.Int) []any {
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

func pagerDutyServiceScanValuesForContract(
	row *pagerDutyServiceRow, sourceRevision, ingestRevision *big.Int,
	contract pagerDutyServicesStorageContract,
) []any {
	return pagerDutyServicesValuesForContract(
		pagerDutyServiceScanValues(row, sourceRevision, ingestRevision), contract,
	)
}

func pagerDutyServiceMappingScanValues(row *pagerDutyServiceRepositoryMappingRow, sourceRevision, ingestRevision *big.Int) []any {
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

func pagerDutyServiceMappingScanValuesForContract(
	row *pagerDutyServiceRepositoryMappingRow, sourceRevision, ingestRevision *big.Int,
	contract pagerDutyServicesStorageContract,
) []any {
	return pagerDutyServicesValuesForContract(
		pagerDutyServiceMappingScanValues(row, sourceRevision, ingestRevision), contract,
	)
}

func pagerDutyHydrateServiceOrdering(
	row *pagerDutyServiceRow, sourceRevision, ingestRevision *big.Int,
	contract pagerDutyServicesStorageContract,
) error {
	if contract == pagerDutyServicesCurrentContract {
		row.SourceRevision = new(big.Int).Set(sourceRevision)
		row.IngestRevision = new(big.Int).Set(ingestRevision)
		return nil
	}
	storedID := row.ID
	if err := fillPagerDutyServiceOrdering(row); err != nil {
		return err
	}
	if row.ID != storedID {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func pagerDutyHydrateServiceMappingOrdering(
	row *pagerDutyServiceRepositoryMappingRow, sourceRevision, ingestRevision *big.Int,
	contract pagerDutyServicesStorageContract,
) error {
	if contract == pagerDutyServicesCurrentContract {
		row.SourceRevision = new(big.Int).Set(sourceRevision)
		row.IngestRevision = new(big.Int).Set(ingestRevision)
		return nil
	}
	storedID := row.ID
	if err := fillPagerDutyServiceMappingOrdering(row); err != nil {
		return err
	}
	if row.ID != storedID {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func (sink PagerDutyServicesClickHouseEffects) loadActiveServices(
	ctx context.Context, claim Claim, providerInstance string,
) ([]pagerDutyServiceRow, error) {
	contract, err := configuredPagerDutyServicesStorageContract()
	if err != nil {
		return nil, err
	}
	rows, err := sink.Conn.Query(ctx, contract.loadActiveServicesQuery(),
		claim.OrgID, claim.Provider, providerInstance, "service")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]pagerDutyServiceRow, 0)
	for rows.Next() {
		var row pagerDutyServiceRow
		var sourceRevision, ingestRevision big.Int
		if err := rows.Scan(pagerDutyServiceScanValuesForContract(&row, &sourceRevision, &ingestRevision, contract)...); err != nil {
			return nil, err
		}
		if err := pagerDutyHydrateServiceOrdering(&row, &sourceRevision, &ingestRevision, contract); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (sink PagerDutyServicesClickHouseEffects) loadActiveMappings(
	ctx context.Context, claim Claim, providerInstance string,
) ([]pagerDutyServiceRepositoryMappingRow, error) {
	contract, err := configuredPagerDutyServicesStorageContract()
	if err != nil {
		return nil, err
	}
	rows, err := sink.Conn.Query(ctx, contract.loadActiveMappingsQuery(),
		claim.OrgID, claim.Provider, providerInstance)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]pagerDutyServiceRepositoryMappingRow, 0)
	for rows.Next() {
		var row pagerDutyServiceRepositoryMappingRow
		var sourceRevision, ingestRevision big.Int
		if err := rows.Scan(pagerDutyServiceMappingScanValuesForContract(&row, &sourceRevision, &ingestRevision, contract)...); err != nil {
			return nil, err
		}
		if err := pagerDutyHydrateServiceMappingOrdering(&row, &sourceRevision, &ingestRevision, contract); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (sink PagerDutyServicesClickHouseEffects) inspectServices(
	ctx context.Context, claim Claim, expected []pagerDutyServiceRow,
) (EffectInspection, error) {
	providerInstance, err := sink.providerInstance(pagerDutyServiceInstances(expected)...)
	if err != nil {
		return EffectConflict, err
	}
	active, err := sink.loadActiveServices(ctx, claim, providerInstance)
	if err != nil {
		return EffectConflict, err
	}
	seen := make(map[string]struct{}, len(expected))
	exact, absent := 0, 0
	for _, row := range expected {
		seen[row.ID] = struct{}{}
		actual, found, loadErr := sink.loadService(ctx, claim, providerInstance, row.ID)
		if loadErr != nil {
			return EffectConflict, loadErr
		}
		switch comparePagerDutyServiceVersion(row, actual, found) {
		case EffectExact:
			exact++
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
	if exact == len(expected) && absent == 0 {
		return EffectExact, nil
	}
	if absent == len(expected) {
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

func (sink PagerDutyServicesClickHouseEffects) inspectMappings(
	ctx context.Context, claim Claim, expected []pagerDutyServiceRepositoryMappingRow,
) (EffectInspection, error) {
	instances := make([]string, 0, len(expected))
	for _, row := range expected {
		instances = append(instances, row.ProviderInstanceID)
	}
	providerInstance, err := sink.providerInstance(instances...)
	if err != nil {
		return EffectConflict, err
	}
	active, err := sink.loadActiveMappings(ctx, claim, providerInstance)
	if err != nil {
		return EffectConflict, err
	}
	seen := make(map[string]struct{}, len(expected))
	exact, absent := 0, 0
	for _, row := range expected {
		seen[row.ID] = struct{}{}
		actual, found, loadErr := sink.loadMapping(ctx, claim, providerInstance, row.ID)
		if loadErr != nil {
			return EffectConflict, loadErr
		}
		switch comparePagerDutyServiceMappingVersion(row, actual, found) {
		case EffectExact:
			exact++
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
	if exact == len(expected) && absent == 0 {
		return EffectExact, nil
	}
	if absent == len(expected) {
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

func (sink PagerDutyServicesClickHouseEffects) loadService(
	ctx context.Context, claim Claim, providerInstance, id string,
) (pagerDutyServiceRow, bool, error) {
	contract, err := configuredPagerDutyServicesStorageContract()
	if err != nil {
		return pagerDutyServiceRow{}, false, err
	}
	rows, err := sink.Conn.Query(ctx, contract.loadServiceQuery(),
		claim.OrgID, claim.Provider, providerInstance, "service", id)
	if err != nil {
		return pagerDutyServiceRow{}, false, err
	}
	defer rows.Close()
	var actual pagerDutyServiceRow
	found := false
	for rows.Next() {
		var sourceRevision, ingestRevision big.Int
		if err := rows.Scan(pagerDutyServiceScanValuesForContract(&actual, &sourceRevision, &ingestRevision, contract)...); err != nil {
			return pagerDutyServiceRow{}, false, err
		}
		if err := pagerDutyHydrateServiceOrdering(&actual, &sourceRevision, &ingestRevision, contract); err != nil {
			return pagerDutyServiceRow{}, false, err
		}
		found = true
	}
	return actual, found, rows.Err()
}

func (sink PagerDutyServicesClickHouseEffects) loadMapping(
	ctx context.Context, claim Claim, providerInstance, id string,
) (pagerDutyServiceRepositoryMappingRow, bool, error) {
	contract, err := configuredPagerDutyServicesStorageContract()
	if err != nil {
		return pagerDutyServiceRepositoryMappingRow{}, false, err
	}
	rows, err := sink.Conn.Query(ctx, contract.loadMappingQuery(),
		claim.OrgID, claim.Provider, providerInstance, id)
	if err != nil {
		return pagerDutyServiceRepositoryMappingRow{}, false, err
	}
	defer rows.Close()
	var actual pagerDutyServiceRepositoryMappingRow
	found := false
	for rows.Next() {
		var sourceRevision, ingestRevision big.Int
		if err := rows.Scan(pagerDutyServiceMappingScanValuesForContract(&actual, &sourceRevision, &ingestRevision, contract)...); err != nil {
			return pagerDutyServiceRepositoryMappingRow{}, false, err
		}
		if err := pagerDutyHydrateServiceMappingOrdering(&actual, &sourceRevision, &ingestRevision, contract); err != nil {
			return pagerDutyServiceRepositoryMappingRow{}, false, err
		}
		found = true
	}
	return actual, found, rows.Err()
}

func comparePagerDutyServiceVersion(expected, actual pagerDutyServiceRow, found bool) EffectInspection {
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

func comparePagerDutyServiceMappingVersion(expected, actual pagerDutyServiceRepositoryMappingRow, found bool) EffectInspection {
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

func pagerDutyServiceMappingTombstone(
	row pagerDutyServiceRepositoryMappingRow, observedAt time.Time,
) (pagerDutyServiceRepositoryMappingRow, error) {
	if row.ID == "" || row.SourceVersionAt.IsZero() || observedAt.IsZero() {
		return pagerDutyServiceRepositoryMappingRow{}, providerfoundation.ErrNormalizationInvalid
	}
	version := observedAt.UTC().Truncate(time.Microsecond)
	if previousNext := row.SourceVersionAt.UTC().Truncate(time.Microsecond).Add(time.Microsecond); version.Before(previousNext) {
		version = previousNext
	}
	row.SourceVersionAt = version
	observed := observedAt.UTC().Truncate(time.Microsecond)
	row.ObservedAt, row.LastSynced = observed, observed
	row.ValidTo, row.IsActive = &observed, false
	row.SourceRevision, row.SourceConflictKey, row.IngestRevision = nil, "", nil
	row.OrderingContract = 0
	if err := fillPagerDutyServiceMappingOrdering(&row); err != nil {
		return pagerDutyServiceRepositoryMappingRow{}, err
	}
	return row, nil
}

var _ EffectSink = PagerDutyServicesClickHouseEffects{}
var _ EffectReadback = PagerDutyServicesClickHouseEffects{}
