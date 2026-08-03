package providersync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

const (
	gitLabIncidentsMaxIssues = 1_000
	gitLabIncidentsMaxPages  = 10_000
	gitLabIncidentsPerPage   = 100
)

type gitLabIncidentPayload struct {
	ID          any     `json:"id"`
	IID         any     `json:"iid"`
	IssueType   string  `json:"issue_type"`
	State       *string `json:"state"`
	Title       *string `json:"title"`
	Description *string `json:"description"`
	CreatedAt   string  `json:"created_at"`
	UpdatedAt   string  `json:"updated_at"`
	ClosedAt    string  `json:"closed_at"`
	WebURL      string  `json:"web_url"`
	URL         string  `json:"url"`
	Severity    *string `json:"severity"`
}

type gitLabOperationalServiceRow struct {
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

type gitLabServiceRepositoryMappingRow struct {
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

type GitLabIncidentsRouteHandler struct {
	MaxIssues int
	MaxPages  int
	PerPage   int
}

func (handler GitLabIncidentsRouteHandler) limits() (int, int, int, error) {
	issues, pages, perPage := handler.MaxIssues, handler.MaxPages, handler.PerPage
	if issues == 0 {
		issues = gitLabIncidentsMaxIssues
	}
	if pages == 0 {
		pages = gitLabIncidentsMaxPages
	}
	if perPage == 0 {
		perPage = gitLabIncidentsPerPage
	}
	if issues < 1 || issues > gitLabIncidentsMaxIssues || pages < 1 ||
		pages > gitLabIncidentsMaxPages || perPage < 1 || perPage > gitLabIncidentsPerPage {
		return 0, 0, 0, ErrInvalidConfiguration
	}
	if perPage > issues {
		perPage = issues
	}
	return issues, pages, perPage, nil
}

func (handler GitLabIncidentsRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "incidents" || client == nil || client.Provider != "gitlab" ||
		client.BaseURL == nil || normalizedAt.IsZero() || claim.SinceAt == nil ||
		claim.BeforeAt == nil || !claim.SinceAt.Before(*claim.BeforeAt) {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	maxIssues, maxPages, perPage, err := handler.limits()
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	projectID, err := gitLabProjectID(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	requests := 0
	counted := *client
	counted.Doer = gitLabIncidentCountingDoer{delegate: client.Doer, attempts: &requests}
	root := providerRelativePath(client, "api", "v4", "projects", projectID)
	var project repositoryPayload
	if err := fetchObject(ctx, &counted, root, &project); err != nil {
		return CompleteRouteBatch{}, err
	}
	parsedProjectID, err := project.ID.Int64()
	if err != nil || parsedProjectID < 1 || strconv.FormatInt(parsedProjectID, 10) != projectID {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	repoFullName := gitLabProjectFullName(project)
	repoIDText, err := repositoryIdentity(repoFullName)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	repoID, err := uuid.Parse(repoIDText)
	if err != nil {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	providerInstance, ok := normalizedProviderInstance("gitlab", client.BaseURL)
	if !ok {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}

	query := url.Values{
		"issue_type": {"incident"}, "state": {"all"},
		"order_by": {"updated_at"}, "sort": {"desc"},
		"updated_after":  {claim.SinceAt.UTC().Format(time.RFC3339Nano)},
		"updated_before": {claim.BeforeAt.UTC().Format(time.RFC3339Nano)},
	}
	items, pages, err := collectGitLabIncidentPages(
		ctx, &counted, root+"/issues", query, maxIssues, maxPages, perPage,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Microsecond)
	services := make([]gitLabOperationalServiceRow, 0, 1)
	mappings := make([]gitLabServiceRepositoryMappingRow, 0, 1)
	incidents := make([]jiraIncidentRow, 0, len(items))
	for _, raw := range items {
		payload, decodeErr := decodeGitLabIncident(raw)
		if decodeErr != nil {
			return CompleteRouteBatch{}, decodeErr
		}
		row, normalizeErr := normalizeGitLabIncident(
			claim, providerInstance, repoFullName, payload, normalizedAt,
		)
		if normalizeErr != nil {
			return CompleteRouteBatch{}, normalizeErr
		}
		if len(services) == 0 {
			service, mapping, buildErr := buildGitLabIncidentServiceRows(
				claim, providerInstance, repoFullName, repoID, row.SourceVersionAt,
				normalizedAt,
			)
			if buildErr != nil {
				return CompleteRouteBatch{}, buildErr
			}
			services = append(services, service)
			mappings = append(mappings, mapping)
		}
		serviceID := services[0].ID
		row.ServiceID = &serviceID
		row.ServiceExternalID = &repoFullName
		if err := fillGitLabIncidentOrdering(&row); err != nil {
			return CompleteRouteBatch{}, err
		}
		incidents = append(incidents, row)
	}
	effects, err := gitLabIncidentEffects(services, mappings, incidents)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	watermark := claim.BeforeAt.UTC()
	return CompleteRouteBatch{
		Effects: effects,
		Result: map[string]any{
			"incidents_synced": len(incidents), "repo": repoFullName,
			"project_id": parsedProjectID,
		},
		Watermark: &watermark,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: requests, Pages: pages + 1, Records: len(incidents),
		},
	}, nil
}

type gitLabIncidentCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer gitLabIncidentCountingDoer) Do(request *http.Request) (*http.Response, error) {
	(*doer.attempts)++
	return doer.delegate.Do(request)
}

func collectGitLabIncidentPages(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	path string,
	baseQuery url.Values,
	maxIssues int,
	maxPages int,
	perPage int,
) ([]json.RawMessage, int, error) {
	items := make([]json.RawMessage, 0, maxIssues)
	seen := make(map[string]struct{}, maxIssues)
	for page := 1; page <= maxPages; page++ {
		query := url.Values{}
		for key, values := range baseQuery {
			query[key] = append([]string(nil), values...)
		}
		query.Set("page", strconv.Itoa(page))
		query.Set("per_page", strconv.Itoa(perPage))
		response, err := client.Do(ctx, http.MethodGet, path+"?"+query.Encode(), nil)
		if err != nil {
			return nil, 0, err
		}
		pageItems, err := decodeGitLabIncidentPage(response)
		if err != nil {
			return nil, 0, err
		}
		if len(pageItems) == 0 {
			return items, page, nil
		}
		for _, raw := range pageItems {
			var identity struct {
				ID        any    `json:"id"`
				IssueType string `json:"issue_type"`
			}
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()
			if decoder.Decode(&identity) != nil {
				return nil, 0, providerfoundation.ErrNormalizationInvalid
			}
			if !strings.EqualFold(identity.IssueType, "incident") {
				continue
			}
			id := stringValue(identity.ID)
			if id == "" {
				return nil, 0, providerfoundation.ErrNormalizationInvalid
			}
			if _, duplicate := seen[id]; duplicate {
				continue
			}
			seen[id] = struct{}{}
			items = append(items, raw)
			if len(items) == maxIssues {
				// A full page cannot prove the newest-first window was exhausted.
				// Refuse to advance the watermark instead of reproducing Python's
				// silent 1,000-row truncation.
				if len(pageItems) >= perPage {
					return nil, 0, ErrPaginationCapExceeded
				}
				return items, page, nil
			}
		}
		if len(pageItems) < perPage {
			return items, page, nil
		}
	}
	return nil, 0, ErrPaginationCapExceeded
}

func decodeGitLabIncidentPage(response *http.Response) ([]json.RawMessage, error) {
	if response == nil || response.Body == nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	defer response.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(response.Body, (32<<20)+1))
	decoder.UseNumber()
	var items []json.RawMessage
	if err := decoder.Decode(&items); err != nil || items == nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	return items, nil
}

func decodeGitLabIncident(raw json.RawMessage) (gitLabIncidentPayload, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var payload gitLabIncidentPayload
	if decoder.Decode(&payload) != nil || stringValue(payload.ID) == "" ||
		!strings.EqualFold(payload.IssueType, "incident") {
		return gitLabIncidentPayload{}, providerfoundation.ErrNormalizationInvalid
	}
	return payload, nil
}

func normalizeGitLabIncident(
	claim Claim,
	providerInstance string,
	repoFullName string,
	payload gitLabIncidentPayload,
	normalizedAt time.Time,
) (jiraIncidentRow, error) {
	createdAt, err := time.Parse(time.RFC3339Nano, payload.CreatedAt)
	if err != nil {
		return jiraIncidentRow{}, providerfoundation.ErrNormalizationInvalid
	}
	createdAt = createdAt.UTC().Truncate(time.Microsecond)
	sourceVersionAt := createdAt
	if payload.UpdatedAt != "" {
		parsed, parseErr := time.Parse(time.RFC3339Nano, payload.UpdatedAt)
		if parseErr != nil {
			return jiraIncidentRow{}, providerfoundation.ErrNormalizationInvalid
		}
		sourceVersionAt = parsed.UTC().Truncate(time.Microsecond)
	}
	var resolvedAt *time.Time
	if payload.ClosedAt != "" {
		if parsed, parseErr := time.Parse(time.RFC3339Nano, payload.ClosedAt); parseErr == nil {
			value := parsed.UTC().Truncate(time.Microsecond)
			resolvedAt = &value
		}
	}
	var sourceEventID *string
	if iid := stringValue(payload.IID); iid != "" {
		sourceEventID = &iid
	}
	var sourceURL *string
	if value := firstText(payload.WebURL, payload.URL); value != "" {
		sourceURL = &value
	}
	title := ""
	if payload.Title != nil {
		title = *payload.Title
	}
	row := jiraIncidentRow{
		OrgID: claim.OrgID, Provider: "gitlab", ProviderInstanceID: providerInstance,
		SourceEntityType: payload.IssueType, ExternalID: stringValue(payload.ID),
		SourceVersionAt: sourceVersionAt, SourceURL: sourceURL,
		SourceEventID: sourceEventID, ObservedAt: normalizedAt, LastSynced: normalizedAt,
		RawStatus: payload.State, RawSeverity: payload.Severity,
		NormalizedStatus:   normalizedGitLabOperationalStatus(payload.State),
		NormalizedSeverity: normalizedGitLabOperationalSeverity(payload.Severity),
		Title:              title, Description: payload.Description, StartedAt: &createdAt,
		ResolvedAt: resolvedAt,
	}
	if row.ExternalID == "" || repoFullName == "" {
		return jiraIncidentRow{}, providerfoundation.ErrNormalizationInvalid
	}
	return row, nil
}

func normalizedGitLabOperationalStatus(raw *string) *string {
	if raw == nil {
		return nil
	}
	value, ok := map[string]string{
		"active": "active", "acknowledged": "acknowledged", "closed": "resolved",
		"open": "open", "opened": "open", "resolved": "resolved", "suppressed": "suppressed",
	}[strings.ToLower(strings.TrimSpace(*raw))]
	if !ok {
		return nil
	}
	return &value
}

func normalizedGitLabOperationalSeverity(raw *string) *string {
	if raw == nil {
		return nil
	}
	value, ok := map[string]string{
		"critical": "critical", "high": "high", "medium": "medium", "low": "low", "info": "info",
		"sev1": "critical", "sev2": "high", "sev3": "medium", "sev4": "low",
	}[strings.ReplaceAll(strings.ToLower(strings.TrimSpace(*raw)), "-", "")]
	if !ok {
		return nil
	}
	return &value
}

func buildGitLabIncidentServiceRows(
	claim Claim,
	providerInstance string,
	repoFullName string,
	repoID uuid.UUID,
	sourceVersionAt time.Time,
	normalizedAt time.Time,
) (gitLabOperationalServiceRow, gitLabServiceRepositoryMappingRow, error) {
	serviceType := "repository"
	service := gitLabOperationalServiceRow{
		OrgID: claim.OrgID, Provider: "gitlab", ProviderInstanceID: providerInstance,
		SourceEntityType: "repository", ExternalID: repoFullName,
		SourceVersionAt: sourceVersionAt, ObservedAt: normalizedAt, LastSynced: normalizedAt,
		Name: repoFullName, ServiceType: &serviceType,
	}
	if err := fillGitLabOperationalServiceOrdering(&service); err != nil {
		return gitLabOperationalServiceRow{}, gitLabServiceRepositoryMappingRow{}, err
	}
	provenance, confidence := "native_repository_context", 1.0
	repoProvider, mappingKind := "gitlab", "repository_derived"
	mapping := gitLabServiceRepositoryMappingRow{
		OrgID: claim.OrgID, Provider: "gitlab", ProviderInstanceID: providerInstance,
		SourceEntityType: "repository_mapping", ExternalID: repoFullName + ":" + repoID.String(),
		SourceVersionAt: sourceVersionAt, ObservedAt: normalizedAt, LastSynced: normalizedAt,
		RelationshipProvenance: &provenance, RelationshipConfidence: &confidence,
		ServiceID: service.ID, RepoID: &repoID, RepoFullName: &repoFullName,
		RepoProvider: &repoProvider, MappingKind: &mappingKind, IsActive: true,
	}
	if err := fillGitLabServiceMappingOrdering(&mapping); err != nil {
		return gitLabOperationalServiceRow{}, gitLabServiceRepositoryMappingRow{}, err
	}
	return service, mapping, nil
}

func gitLabIncidentEffects(
	services []gitLabOperationalServiceRow,
	mappings []gitLabServiceRepositoryMappingRow,
	incidents []jiraIncidentRow,
) ([]EffectBatch, error) {
	serviceEffect, err := effectBatchFromValues("operational_services", EffectReadbackRequired, services)
	if err != nil {
		return nil, err
	}
	mappingEffect, err := effectBatchFromValues("operational_service_repository_mappings", EffectReadbackRequired, mappings)
	if err != nil {
		return nil, err
	}
	incidentEffect, err := effectBatchFromValues("operational_incidents", EffectReadbackRequired, incidents)
	if err != nil {
		return nil, err
	}
	return []EffectBatch{serviceEffect, mappingEffect, incidentEffect}, nil
}

func fillGitLabOperationalServiceOrdering(row *gitLabOperationalServiceRow) error {
	fields := gitLabOperationalBaseFields(
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, row.SourceID, row.SourceURL,
		row.SourceEventAt, row.SourceEventID, row.RawStatus, row.RawSeverity,
		row.RawPriority, row.NormalizedStatus, row.NormalizedSeverity,
		row.NormalizedPriority, row.RelationshipProvenance, row.RelationshipConfidence,
	)
	fields = append(fields,
		jiraOperationalField{"name", row.Name}, jiraOperationalField{"description", jiraStringValue(row.Description)},
		jiraOperationalField{"service_type", jiraStringValue(row.ServiceType)}, jiraOperationalField{"owning_team_id", jiraStringValue(row.OwningTeamID)},
		jiraOperationalField{"escalation_policy_id", jiraStringValue(row.EscalationPolicyID)}, jiraOperationalField{"is_deleted", row.IsDeleted},
		jiraOperationalField{"deleted_at", jiraTimeValue(row.DeletedAt)},
	)
	id, conflict, sourceRevision, ingestRevision, err := deriveGitLabOperationalOrdering(
		"operational_service", row.OrgID, row.Provider, row.ProviderInstanceID,
		row.ExternalID, row.SourceVersionAt, row.ObservedAt, row.LastSynced, fields,
	)
	if err != nil {
		return err
	}
	row.ID, row.SourceConflictKey, row.SourceRevision, row.IngestRevision = id, conflict, sourceRevision, ingestRevision
	row.OrderingContract = 2
	return nil
}

func fillGitLabServiceMappingOrdering(row *gitLabServiceRepositoryMappingRow) error {
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
		jiraOperationalField{"repo_full_name", jiraStringValue(row.RepoFullName)}, jiraOperationalField{"repo_provider", jiraStringValue(row.RepoProvider)},
		jiraOperationalField{"mapping_kind", jiraStringValue(row.MappingKind)}, jiraOperationalField{"rule_id", jiraStringValue(row.RuleID)},
		jiraOperationalField{"valid_from", jiraTimeValue(row.ValidFrom)}, jiraOperationalField{"valid_to", jiraTimeValue(row.ValidTo)},
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
	row.ID, row.SourceConflictKey, row.SourceRevision, row.IngestRevision = id, conflict, sourceRevision, ingestRevision
	row.OrderingContract = 2
	return nil
}

func fillGitLabIncidentOrdering(row *jiraIncidentRow) error {
	if row == nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	fields := gitLabOperationalBaseFields(
		row.OrgID, row.Provider, row.ProviderInstanceID, row.SourceEntityType,
		row.ExternalID, row.SourceVersionAt, nil, row.SourceURL,
		row.SourceEventAt, row.SourceEventID, row.RawStatus, row.RawSeverity,
		row.RawPriority, row.NormalizedStatus, row.NormalizedSeverity,
		row.NormalizedPriority, row.RelationshipProvenance, row.RelationshipConfidence,
	)
	fields = append(fields,
		jiraOperationalField{"service_id", jiraStringValue(row.ServiceID)},
		jiraOperationalField{"service_external_id", jiraStringValue(row.ServiceExternalID)},
		jiraOperationalField{"escalation_policy_id", jiraStringValue(row.EscalationPolicyID)},
		jiraOperationalField{"title", row.Title},
		jiraOperationalField{"description", jiraStringValue(row.Description)},
		jiraOperationalField{"started_at", jiraTimeValue(row.StartedAt)},
		jiraOperationalField{"resolved_at", jiraTimeValue(row.ResolvedAt)},
		jiraOperationalField{"is_deleted", row.IsDeleted},
		jiraOperationalField{"deleted_at", jiraTimeValue(row.DeletedAt)},
	)
	id, conflict, sourceRevision, ingestRevision, err := deriveGitLabOperationalOrdering(
		"operational_incident", row.OrgID, row.Provider, row.ProviderInstanceID,
		row.ExternalID, row.SourceVersionAt, row.ObservedAt, row.LastSynced, fields,
	)
	if err != nil {
		return err
	}
	row.ID, row.SourceConflictKey, row.SourceRevision, row.IngestRevision =
		id, conflict, sourceRevision, ingestRevision
	row.OrderingContract = 2
	return nil
}

func gitLabOperationalBaseFields(
	orgID, provider, providerInstance, sourceEntityType, externalID string,
	sourceVersionAt time.Time,
	sourceID *uuid.UUID,
	sourceURL *string,
	sourceEventAt *time.Time,
	sourceEventID *string,
	rawStatus, rawSeverity, rawPriority *string,
	normalizedStatus, normalizedSeverity, normalizedPriority *string,
	relationshipProvenance *string,
	relationshipConfidence *float64,
) []jiraOperationalField {
	var sourceIDValue any
	if sourceID != nil {
		sourceIDValue = *sourceID
	}
	var confidence any
	if relationshipConfidence != nil {
		confidence = *relationshipConfidence
	}
	return []jiraOperationalField{
		{"org_id", orgID}, {"provider", provider}, {"provider_instance_id", providerInstance},
		{"source_entity_type", sourceEntityType}, {"external_id", externalID},
		{"source_version_at", sourceVersionAt}, {"source_id", sourceIDValue},
		{"source_url", jiraStringValue(sourceURL)}, {"source_event_at", jiraTimeValue(sourceEventAt)},
		{"source_event_id", jiraStringValue(sourceEventID)}, {"raw_status", jiraStringValue(rawStatus)},
		{"raw_severity", jiraStringValue(rawSeverity)}, {"raw_priority", jiraStringValue(rawPriority)},
		{"normalized_status", jiraStringValue(normalizedStatus)}, {"normalized_severity", jiraStringValue(normalizedSeverity)},
		{"normalized_priority", jiraStringValue(normalizedPriority)}, {"relationship_provenance", jiraStringValue(relationshipProvenance)},
		{"relationship_confidence", confidence},
	}
}

func deriveGitLabOperationalOrdering(
	family, orgID, provider, providerInstance, externalID string,
	sourceVersionAt, observedAt, lastSynced time.Time,
	fields []jiraOperationalField,
) (string, string, *big.Int, *big.Int, error) {
	parts := []string{orgID, provider, providerInstance, family, externalID}
	for _, part := range parts {
		if part == "" {
			return "", "", nil, nil, providerfoundation.ErrNormalizationInvalid
		}
	}
	quoted := make([]string, len(parts))
	for index, part := range parts {
		quoted[index] = strconv.QuoteToASCII(part)
	}
	idDigest := sha256Bytes([]byte("[" + strings.Join(quoted, ",") + "]"))
	conflict, err := encodeJiraOperationalConflict(family, fields)
	if err != nil {
		return "", "", nil, nil, providerfoundation.ErrNormalizationInvalid
	}
	conflictBytes, err := hex.DecodeString(conflict)
	if err != nil {
		return "", "", nil, nil, providerfoundation.ErrNormalizationInvalid
	}
	revisionDigest := sha256Bytes(append([]byte("operational-source-revision-v1"), conflictBytes...))
	sourceMicros, err := jiraOperationalMicros(sourceVersionAt)
	if err != nil {
		return "", "", nil, nil, err
	}
	sourceRevision := new(big.Int).Lsh(new(big.Int).SetUint64(sourceMicros), 64)
	sourceRevision.Or(sourceRevision, new(big.Int).Lsh(big.NewInt(1), 56))
	sourceRevision.Or(sourceRevision, new(big.Int).SetBytes(revisionDigest[:7]))
	lastSyncedMicros, err := jiraOperationalMicros(lastSynced)
	if err != nil {
		return "", "", nil, nil, err
	}
	observedMicros, err := jiraOperationalMicros(observedAt)
	if err != nil {
		return "", "", nil, nil, err
	}
	ingestRevision := new(big.Int).Lsh(new(big.Int).SetUint64(lastSyncedMicros), 64)
	ingestRevision.Or(ingestRevision, new(big.Int).SetUint64(observedMicros))
	return hex.EncodeToString(idDigest), conflict, sourceRevision, ingestRevision, nil
}

func sha256Bytes(value []byte) []byte {
	digest := sha256.Sum256(value)
	return digest[:]
}

var _ CompleteRouteHandler = GitLabIncidentsRouteHandler{}
