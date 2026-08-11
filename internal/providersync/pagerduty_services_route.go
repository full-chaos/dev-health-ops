package providersync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

const (
	pagerDutyServicesMaxPages = 10_000
	pagerDutyServicesMaxRows  = 100_000
	pagerDutyServicesPerPage  = 100
)

// pagerDutyServicePayload is the provider-owned subset consumed by
// PagerDutyNormalizer.service. Raw is retained only for the separate,
// evidence-preserving repository-mapping producer below; it is never emitted
// as a generic provider result or effect row.
type pagerDutyServicePayload struct {
	ID               string                     `json:"id"`
	Name             string                     `json:"name"`
	Summary          string                     `json:"summary"`
	SelfURL          string                     `json:"self"`
	HTMLURL          string                     `json:"html_url"`
	CreatedAt        string                     `json:"created_at"`
	UpdatedAt        string                     `json:"updated_at"`
	EscalationPolicy *pagerDutyServiceReference `json:"escalation_policy"`
}

type pagerDutyServiceReference struct {
	ID string `json:"id"`
}

// pagerDutyServiceRow mirrors every field on Python's OperationalService
// dataclass. Service rows and repository-mapping rows are deliberately
// separate typed effects: service_repository_mapping.py owns mapping
// provenance and this producer does not fold that evidence into service rows.
type pagerDutyServiceRow struct {
	OrgID                  string     `json:"org_id"`
	Provider               string     `json:"provider"`
	ProviderInstanceID     string     `json:"provider_instance_id"`
	SourceEntityType       string     `json:"source_entity_type"`
	ExternalID             string     `json:"external_id"`
	SourceVersionAt        time.Time  `json:"source_version_at"`
	SourceRevision         *big.Int   `json:"source_revision"`
	SourceConflictKey      string     `json:"source_conflict_key"`
	IngestRevision         *big.Int   `json:"ingest_revision"`
	OrderingContract       uint8      `json:"ordering_contract"`
	ID                     string     `json:"id"`
	SourceID               *uuid.UUID `json:"source_id"`
	SourceURL              *string    `json:"source_url"`
	SourceEventAt          *time.Time `json:"source_event_at"`
	SourceEventID          *string    `json:"source_event_id"`
	ObservedAt             time.Time  `json:"observed_at"`
	LastSynced             time.Time  `json:"last_synced"`
	RawStatus              *string    `json:"raw_status"`
	RawSeverity            *string    `json:"raw_severity"`
	RawPriority            *string    `json:"raw_priority"`
	NormalizedStatus       *string    `json:"normalized_status"`
	NormalizedSeverity     *string    `json:"normalized_severity"`
	NormalizedPriority     *string    `json:"normalized_priority"`
	RelationshipProvenance *string    `json:"relationship_provenance"`
	RelationshipConfidence *float64   `json:"relationship_confidence"`
	Name                   string     `json:"name"`
	Description            *string    `json:"description"`
	ServiceType            *string    `json:"service_type"`
	OwningTeamID           *string    `json:"owning_team_id"`
	EscalationPolicyID     *string    `json:"escalation_policy_id"`
	IsDeleted              bool       `json:"is_deleted"`
	DeletedAt              *time.Time `json:"deleted_at"`
}

// pagerDutyServiceRepositoryMappingRow is the typed counterpart of
// ServiceRepositoryMapping. Its SourceEntityType is the mapping source enum
// value (admin_configuration, pagerduty_service_metadata,
// compass_service_catalog, or bounded_service_repository_heuristic), not the
// service source type. This preserves the Python producer's provenance
// ownership and lets the sink reconcile each source independently.
type pagerDutyServiceRepositoryMappingRow struct {
	OrgID                  string     `json:"org_id"`
	Provider               string     `json:"provider"`
	ProviderInstanceID     string     `json:"provider_instance_id"`
	SourceEntityType       string     `json:"source_entity_type"`
	ExternalID             string     `json:"external_id"`
	SourceVersionAt        time.Time  `json:"source_version_at"`
	SourceRevision         *big.Int   `json:"source_revision"`
	SourceConflictKey      string     `json:"source_conflict_key"`
	IngestRevision         *big.Int   `json:"ingest_revision"`
	OrderingContract       uint8      `json:"ordering_contract"`
	ID                     string     `json:"id"`
	SourceID               *uuid.UUID `json:"source_id"`
	SourceURL              *string    `json:"source_url"`
	SourceEventAt          *time.Time `json:"source_event_at"`
	SourceEventID          *string    `json:"source_event_id"`
	ObservedAt             time.Time  `json:"observed_at"`
	LastSynced             time.Time  `json:"last_synced"`
	RawStatus              *string    `json:"raw_status"`
	RawSeverity            *string    `json:"raw_severity"`
	RawPriority            *string    `json:"raw_priority"`
	NormalizedStatus       *string    `json:"normalized_status"`
	NormalizedSeverity     *string    `json:"normalized_severity"`
	NormalizedPriority     *string    `json:"normalized_priority"`
	RelationshipProvenance *string    `json:"relationship_provenance"`
	RelationshipConfidence *float64   `json:"relationship_confidence"`
	ServiceID              string     `json:"service_id"`
	RepoID                 *uuid.UUID `json:"repo_id"`
	RepoFullName           *string    `json:"repo_full_name"`
	RepoProvider           *string    `json:"repo_provider"`
	MappingKind            *string    `json:"mapping_kind"`
	RuleID                 *string    `json:"rule_id"`
	ValidFrom              *time.Time `json:"valid_from"`
	ValidTo                *time.Time `json:"valid_to"`
	IsActive               bool       `json:"is_active"`
}

type PagerDutyServicesRouteHandler struct {
	MaxPages int
	MaxRows  int
	PerPage  int
}

type pagerDutyServicesCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer pagerDutyServicesCountingDoer) Do(request *http.Request) (*http.Response, error) {
	(*doer.attempts)++
	return doer.delegate.Do(request)
}

func (handler PagerDutyServicesRouteHandler) limits() (int, int, int, error) {
	pages, rows, perPage := handler.MaxPages, handler.MaxRows, handler.PerPage
	if pages == 0 {
		pages = pagerDutyServicesMaxPages
	}
	if rows == 0 {
		rows = pagerDutyServicesMaxRows
	}
	if perPage == 0 {
		perPage = pagerDutyServicesPerPage
	}
	if pages < 1 || pages > pagerDutyServicesMaxPages || rows < 1 ||
		rows > pagerDutyServicesMaxRows || perPage < 1 || perPage > pagerDutyServicesPerPage {
		return 0, 0, 0, ErrInvalidConfiguration
	}
	return pages, rows, perPage, nil
}

func (handler PagerDutyServicesRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || credential.Provider != "pagerduty" ||
		claim.Provider != "pagerduty" || claim.Dataset != "services" ||
		client == nil || client.Provider != "pagerduty" || client.BaseURL == nil ||
		normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	maxPages, maxRows, perPage, err := handler.limits()
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	providerInstance, err := pagerDutyProviderInstance(credential)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Microsecond)
	requests := 0
	counted := *client
	counted.Doer = pagerDutyServicesCountingDoer{delegate: client.Doer, attempts: &requests}
	pages, err := providerfoundation.CollectPagerDutyOffsetPages(
		ctx, &counted, providerfoundation.PagerDutyOffsetOptions{
			Path: "/services", DataKey: "services", PerPage: perPage, MaxPages: maxPages,
		},
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if len(pages.Items) > maxRows || pages.CapReached {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	services := make([]pagerDutyServiceRow, 0, len(pages.Items))
	mappings := make([]pagerDutyServiceRepositoryMappingRow, 0)
	inputs := pagerDutyServiceRepositoryMappingInputsFromOptions(claim.DatasetOptions)
	for _, raw := range pages.Items {
		var payload pagerDutyServicePayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		service, err := normalizePagerDutyService(claim, providerInstance, payload, normalizedAt)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		services = append(services, service)
		var metadata map[string]any
		if err := json.Unmarshal(raw, &metadata); err != nil || metadata == nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		serviceMappings, err := pagerDutyServiceMappings(
			service, metadata, normalizedAt, inputs,
		)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		mappings = append(mappings, serviceMappings...)
	}
	mappings = selectPagerDutyPreferredMappings(mappings)
	serviceEffect, err := effectBatchFromValues(
		"operational_services", EffectReadbackRequired, services,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	mappingEffect, err := effectBatchFromValues(
		"operational_service_repository_mappings", EffectReadbackRequired, mappings,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{serviceEffect, mappingEffect},
		Result: map[string]any{
			"services_synced": len(services), "service_repository_mappings_synced": len(mappings),
		},
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests,
			Pages: pages.Pages, Records: len(services), CapReached: pages.CapReached,
		},
	}, nil
}

func normalizePagerDutyService(
	claim Claim,
	providerInstance string,
	payload pagerDutyServicePayload,
	normalizedAt time.Time,
) (pagerDutyServiceRow, error) {
	externalID := strings.TrimSpace(payload.ID)
	if externalID == "" {
		return pagerDutyServiceRow{}, providerfoundation.ErrNormalizationInvalid
	}
	sourceVersionAt, err := pagerDutyServiceSourceTime(payload, normalizedAt)
	if err != nil {
		return pagerDutyServiceRow{}, err
	}
	var sourceURL *string
	if payload.HTMLURL != "" {
		value := payload.HTMLURL
		sourceURL = &value
	} else if payload.SelfURL != "" {
		value := payload.SelfURL
		sourceURL = &value
	}
	name := payload.Name
	if name == "" {
		name = payload.Summary
	}
	if name == "" {
		name = payload.ID
	}
	serviceType := "technical"
	var escalationPolicyID *string
	if payload.EscalationPolicy != nil && strings.TrimSpace(payload.EscalationPolicy.ID) != "" {
		value := pagerDutyCanonicalOperationalID(
			claim.OrgID, "pagerduty", providerInstance,
			"operational_escalation_policy", strings.TrimSpace(payload.EscalationPolicy.ID),
		)
		escalationPolicyID = &value
	}
	row := pagerDutyServiceRow{
		OrgID: claim.OrgID, Provider: "pagerduty", ProviderInstanceID: providerInstance,
		SourceEntityType: "service", ExternalID: externalID,
		SourceVersionAt: sourceVersionAt, SourceURL: sourceURL,
		ObservedAt: normalizedAt, LastSynced: normalizedAt, Name: name,
		ServiceType: &serviceType, EscalationPolicyID: escalationPolicyID,
	}
	if err := fillPagerDutyServiceOrdering(&row); err != nil {
		return pagerDutyServiceRow{}, err
	}
	return row, nil
}

func pagerDutyServiceSourceTime(payload pagerDutyServicePayload, observedAt time.Time) (time.Time, error) {
	value := payload.UpdatedAt
	if value == "" {
		value = payload.CreatedAt
	}
	if value == "" {
		return observedAt, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, providerfoundation.ErrNormalizationInvalid
	}
	return parsed.UTC().Truncate(time.Microsecond), nil
}

func fillPagerDutyServiceOrdering(row *pagerDutyServiceRow) error {
	if row == nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	fields := gitLabOperationalBaseFields(
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, row.SourceID, row.SourceURL,
		row.SourceEventAt, row.SourceEventID, row.RawStatus, row.RawSeverity,
		row.RawPriority, row.NormalizedStatus, row.NormalizedSeverity,
		row.NormalizedPriority, row.RelationshipProvenance, row.RelationshipConfidence,
	)
	fields = append(fields,
		jiraOperationalField{"name", row.Name},
		jiraOperationalField{"description", jiraStringValue(row.Description)},
		jiraOperationalField{"service_type", jiraStringValue(row.ServiceType)},
		jiraOperationalField{"owning_team_id", jiraStringValue(row.OwningTeamID)},
		jiraOperationalField{"escalation_policy_id", jiraStringValue(row.EscalationPolicyID)},
		jiraOperationalField{"is_deleted", row.IsDeleted},
		jiraOperationalField{"deleted_at", jiraTimeValue(row.DeletedAt)},
	)
	id, conflict, sourceRevision, ingestRevision, err := deriveGitLabOperationalOrdering(
		"operational_service", row.OrgID, row.Provider, row.ProviderInstanceID,
		row.ExternalID, row.SourceVersionAt, row.ObservedAt, row.LastSynced, fields,
	)
	if err != nil {
		return err
	}
	row.ID, row.SourceConflictKey = id, conflict
	row.SourceRevision, row.IngestRevision = sourceRevision, ingestRevision
	if row.IsDeleted {
		row.SourceRevision, err = pagerDutyOperationalTombstoneRevision(sourceRevision, conflict)
		if err != nil {
			return err
		}
	}
	row.OrderingContract = 2
	return nil
}

func pagerDutyOperationalTombstoneRevision(sourceRevision *big.Int, conflict string) (*big.Int, error) {
	if sourceRevision == nil || conflict == "" {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	conflictBytes, err := hex.DecodeString(conflict)
	if err != nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	revisionDigest := sha256.Sum256(append([]byte("operational-source-revision-v1"), conflictBytes...))
	timestamp := new(big.Int).Rsh(new(big.Int).Set(sourceRevision), 64)
	rank := new(big.Int).Lsh(big.NewInt(2), 56)
	revision := new(big.Int).Lsh(timestamp, 64)
	revision.Or(revision, rank)
	revision.Or(revision, new(big.Int).SetBytes(revisionDigest[:7]))
	return revision, nil
}

func pagerDutyServiceTombstone(row pagerDutyServiceRow, observedAt time.Time) (pagerDutyServiceRow, error) {
	if row.ID == "" || row.SourceVersionAt.IsZero() || observedAt.IsZero() {
		return pagerDutyServiceRow{}, providerfoundation.ErrNormalizationInvalid
	}
	version := observedAt.UTC().Truncate(time.Microsecond)
	if previousNext := row.SourceVersionAt.UTC().Truncate(time.Microsecond).Add(time.Microsecond); version.Before(previousNext) {
		version = previousNext
	}
	row.SourceVersionAt, row.ObservedAt, row.LastSynced = version, version, version
	row.IsDeleted = true
	row.DeletedAt = &version
	row.SourceRevision, row.SourceConflictKey, row.IngestRevision = nil, "", nil
	row.OrderingContract = 0
	if err := fillPagerDutyServiceOrdering(&row); err != nil {
		return pagerDutyServiceRow{}, err
	}
	return row, nil
}

func pagerDutyCanonicalOperationalID(orgID, provider, instance, family, externalID string) string {
	quoted := []string{
		strconv.QuoteToASCII(orgID), strconv.QuoteToASCII(provider),
		strconv.QuoteToASCII(instance), strconv.QuoteToASCII(family),
		strconv.QuoteToASCII(externalID),
	}
	digest := sha256.Sum256([]byte("[" + strings.Join(quoted, ",") + "]"))
	return hex.EncodeToString(digest[:])
}

type pagerDutyServiceMappingSource string

const (
	pagerDutyMappingAdmin     pagerDutyServiceMappingSource = "admin_configuration"
	pagerDutyMappingMetadata  pagerDutyServiceMappingSource = "pagerduty_service_metadata"
	pagerDutyMappingCompass   pagerDutyServiceMappingSource = "compass_service_catalog"
	pagerDutyMappingHeuristic pagerDutyServiceMappingSource = "bounded_service_repository_heuristic"
)

func (source pagerDutyServiceMappingSource) confidence() float64 {
	switch source {
	case pagerDutyMappingAdmin:
		return 1
	case pagerDutyMappingMetadata:
		return 0.95
	case pagerDutyMappingCompass:
		return 0.9
	default:
		return 0.4
	}
}

func (source pagerDutyServiceMappingSource) mappingKind() string {
	if source == pagerDutyMappingHeuristic {
		return string(source)
	}
	return string(source) + "_exact"
}

func (source pagerDutyServiceMappingSource) ruleID() string {
	switch source {
	case pagerDutyMappingAdmin:
		return "service_repository_mapping.admin.v1"
	case pagerDutyMappingMetadata:
		return "pagerduty.service_metadata.repository_url_or_key.v1"
	case pagerDutyMappingCompass:
		return "compass.service_repository_relationship.v1"
	default:
		return "pagerduty.service_repository.bounded_name_match.v1"
	}
}

type pagerDutyServiceRepositoryReference struct {
	Provider string
	FullName string
}

type pagerDutyServiceMappingInputs struct {
	Admin   map[string][]pagerDutyServiceRepositoryReference
	Compass map[string][]pagerDutyServiceRepositoryReference
}

func pagerDutyServiceRepositoryMappingInputsFromOptions(options map[string]any) pagerDutyServiceMappingInputs {
	inputs := pagerDutyServiceMappingInputs{
		Admin:   map[string][]pagerDutyServiceRepositoryReference{},
		Compass: map[string][]pagerDutyServiceRepositoryReference{},
	}
	config, ok := options["service_repository_mappings"].(map[string]any)
	if !ok {
		return inputs
	}
	inputs.Admin = pagerDutyParseServiceRepositoryReferences(config["admin"])
	inputs.Compass = pagerDutyParseServiceRepositoryReferences(config["compass"])
	return inputs
}

func pagerDutyParseServiceRepositoryReferences(value any) map[string][]pagerDutyServiceRepositoryReference {
	result := map[string][]pagerDutyServiceRepositoryReference{}
	services, ok := value.(map[string]any)
	if !ok {
		return result
	}
	for serviceID, rawReferences := range services {
		entries, ok := rawReferences.([]any)
		if !ok {
			continue
		}
		for _, rawEntry := range entries {
			entry, ok := rawEntry.(map[string]any)
			if !ok {
				continue
			}
			provider, providerOK := entry["provider"].(string)
			fullName, fullNameOK := entry["full_name"].(string)
			if providerOK && fullNameOK && provider != "" && fullName != "" {
				result[serviceID] = append(result[serviceID], pagerDutyServiceRepositoryReference{Provider: provider, FullName: fullName})
			}
		}
	}
	return result
}

var (
	pagerDutyGitHubRepositoryURL = regexp.MustCompile(`https?://(?:www\.)?github\.com/([^/\s]+)/([^/#?\s]+)`)
	pagerDutyGitLabRepositoryURL = regexp.MustCompile(`https?://(?:www\.)?gitlab\.com/([^\s]+)/?$`)
)

func pagerDutyServiceMappings(
	service pagerDutyServiceRow,
	metadata map[string]any,
	observedAt time.Time,
	inputs pagerDutyServiceMappingInputs,
) ([]pagerDutyServiceRepositoryMappingRow, error) {
	references := pagerDutyMetadataRepositoryReferences(metadata)
	values := make([]pagerDutyServiceRepositoryMappingRow, 0, len(references))
	for _, reference := range references {
		row, err := pagerDutyServiceMappingFromReference(service, reference, pagerDutyMappingMetadata, observedAt, "")
		if err != nil {
			return nil, err
		}
		values = append(values, row)
	}
	for _, reference := range inputs.Admin[service.ExternalID] {
		row, err := pagerDutyServiceMappingFromReference(service, reference, pagerDutyMappingAdmin, observedAt, "")
		if err != nil {
			return nil, err
		}
		values = append(values, row)
	}
	for _, reference := range inputs.Compass[service.ExternalID] {
		row, err := pagerDutyServiceMappingFromReference(service, reference, pagerDutyMappingCompass, observedAt, "")
		if err != nil {
			return nil, err
		}
		values = append(values, row)
	}
	return values, nil
}

func pagerDutyMetadataRepositoryReferences(metadata map[string]any) []pagerDutyServiceRepositoryReference {
	seen := map[pagerDutyServiceRepositoryReference]struct{}{}
	var walk func(any, string)
	walk = func(value any, key string) {
		switch typed := value.(type) {
		case string:
			if match := pagerDutyGitHubRepositoryURL.FindStringSubmatch(typed); len(match) == 3 {
				seen[pagerDutyServiceRepositoryReference{Provider: "github", FullName: strings.TrimSuffix(match[1]+"/"+match[2], ".git")}] = struct{}{}
			}
			if match := pagerDutyGitLabRepositoryURL.FindStringSubmatch(typed); len(match) == 2 {
				seen[pagerDutyServiceRepositoryReference{Provider: "gitlab", FullName: strings.TrimSuffix(match[1], ".git")}] = struct{}{}
			}
			if (key == "repo" || key == "repository" || key == "repository_slug" || key == "repo_slug") && strings.Count(typed, "/") == 1 {
				seen[pagerDutyServiceRepositoryReference{Provider: "github", FullName: strings.TrimSuffix(typed, ".git")}] = struct{}{}
			}
		case map[string]any:
			for nestedKey, nestedValue := range typed {
				walk(nestedValue, strings.ToLower(nestedKey))
			}
		case []any:
			for _, nestedValue := range typed {
				walk(nestedValue, key)
			}
		}
	}
	walk(metadata, "")
	values := make([]pagerDutyServiceRepositoryReference, 0, len(seen))
	for reference := range seen {
		values = append(values, reference)
	}
	slicesSortPagerDutyReferences(values)
	return values
}

func slicesSortPagerDutyReferences(values []pagerDutyServiceRepositoryReference) {
	for index := 1; index < len(values); index++ {
		current := values[index]
		position := index
		for position > 0 && (values[position-1].Provider > current.Provider ||
			(values[position-1].Provider == current.Provider && values[position-1].FullName > current.FullName)) {
			values[position] = values[position-1]
			position--
		}
		values[position] = current
	}
}

func pagerDutyServiceMappingFromReference(
	service pagerDutyServiceRow,
	reference pagerDutyServiceRepositoryReference,
	source pagerDutyServiceMappingSource,
	observedAt time.Time,
	ruleIDOverride string,
) (pagerDutyServiceRepositoryMappingRow, error) {
	ruleID := source.ruleID()
	if ruleIDOverride != "" {
		ruleID = ruleIDOverride
	}
	fullName, provider := reference.FullName, reference.Provider
	mappingKind, provenance, confidence := source.mappingKind(), string(source), source.confidence()
	sourceEventID := "pagerduty_sync"
	row := pagerDutyServiceRepositoryMappingRow{
		OrgID: service.OrgID, Provider: service.Provider, ProviderInstanceID: service.ProviderInstanceID,
		SourceEntityType: string(source),
		ExternalID:       service.ExternalID + ":" + provider + ":" + fullName + ":" + ruleID,
		SourceVersionAt:  observedAt, SourceURL: service.SourceURL, SourceEventID: &sourceEventID,
		ObservedAt: observedAt, LastSynced: observedAt, RelationshipProvenance: &provenance,
		RelationshipConfidence: &confidence, ServiceID: service.ID,
		RepoFullName: &fullName, RepoProvider: &provider, MappingKind: &mappingKind,
		RuleID: &ruleID, ValidFrom: &observedAt, IsActive: true,
	}
	if err := fillPagerDutyServiceMappingOrdering(&row); err != nil {
		return pagerDutyServiceRepositoryMappingRow{}, err
	}
	return row, nil
}

func selectPagerDutyPreferredMappings(values []pagerDutyServiceRepositoryMappingRow) []pagerDutyServiceRepositoryMappingRow {
	preferred := map[string]pagerDutyServiceRepositoryMappingRow{}
	for _, value := range values {
		key := value.ServiceID + "\x00" + pointerString(value.RepoProvider) + "\x00" + pointerString(value.RepoFullName)
		current, exists := preferred[key]
		if !exists || (value.RelationshipConfidence != nil && current.RelationshipConfidence != nil && *value.RelationshipConfidence > *current.RelationshipConfidence) {
			preferred[key] = value
		}
	}
	result := make([]pagerDutyServiceRepositoryMappingRow, 0, len(preferred))
	for _, value := range preferred {
		result = append(result, value)
	}
	for index := 1; index < len(result); index++ {
		current := result[index]
		position := index
		for position > 0 && pagerDutyMappingSortKey(result[position-1]) > pagerDutyMappingSortKey(current) {
			result[position] = result[position-1]
			position--
		}
		result[position] = current
	}
	return result
}

func pagerDutyMappingSortKey(row pagerDutyServiceRepositoryMappingRow) string {
	return row.ServiceID + "\x00" + pointerString(row.RepoProvider) + "\x00" + pointerString(row.RepoFullName)
}

func pointerString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func fillPagerDutyServiceMappingOrdering(row *pagerDutyServiceRepositoryMappingRow) error {
	if row == nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	fields := gitLabOperationalBaseFields(
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, row.SourceID, row.SourceURL,
		row.SourceEventAt, row.SourceEventID, row.RawStatus, row.RawSeverity,
		row.RawPriority, row.NormalizedStatus, row.NormalizedSeverity,
		row.NormalizedPriority, row.RelationshipProvenance, row.RelationshipConfidence,
	)
	var repoID any
	if row.RepoID != nil {
		repoID = *row.RepoID
	}
	fields = append(fields,
		jiraOperationalField{"service_id", row.ServiceID}, jiraOperationalField{"repo_id", repoID},
		jiraOperationalField{"repo_full_name", jiraStringValue(row.RepoFullName)},
		jiraOperationalField{"repo_provider", jiraStringValue(row.RepoProvider)},
		jiraOperationalField{"mapping_kind", jiraStringValue(row.MappingKind)},
		jiraOperationalField{"rule_id", jiraStringValue(row.RuleID)},
		jiraOperationalField{"valid_from", jiraTimeValue(row.ValidFrom)},
		jiraOperationalField{"valid_to", jiraTimeValue(row.ValidTo)},
		jiraOperationalField{"is_active", row.IsActive},
	)
	id, conflict, sourceRevision, ingestRevision, err := deriveGitLabOperationalOrdering(
		"operational_service_repository_mapping", row.OrgID, row.Provider,
		row.ProviderInstanceID, row.ExternalID, row.SourceVersionAt,
		row.ObservedAt, row.LastSynced, fields,
	)
	if err != nil {
		return err
	}
	row.ID, row.SourceConflictKey = id, conflict
	row.SourceRevision, row.IngestRevision = sourceRevision, ingestRevision
	if !row.IsActive {
		row.SourceRevision, err = pagerDutyOperationalTombstoneRevision(sourceRevision, conflict)
		if err != nil {
			return err
		}
	}
	row.OrderingContract = 2
	return nil
}

var _ CompleteRouteHandler = PagerDutyServicesRouteHandler{}
