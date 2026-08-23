package providersync

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

const (
	pagerDutyBusinessServicesMaxPages = 10_000
	pagerDutyBusinessServicesMaxRows  = 100_000
	pagerDutyBusinessServicesPerPage  = 100
)

// pagerDutyBusinessServicePayload is the provider-owned subset consumed by
// Python's PagerDutyNormalizer.business_service. Unknown PagerDuty fields are
// intentionally ignored because they are not part of the persisted contract.
type pagerDutyBusinessServicePayload struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Summary     string  `json:"summary"`
	SelfURL     string  `json:"self"`
	HTMLURL     string  `json:"html_url"`
	Description *string `json:"description"`
	CreatedAt   string  `json:"created_at"`
	UpdatedAt   string  `json:"updated_at"`
}

// pagerDutyBusinessServiceRow mirrors every field on Python's canonical
// OperationalService dataclass. Business services and technical services
// share the ClickHouse table, so source_entity_type and service_type are both
// part of the row's typed identity and ordering input.
type pagerDutyBusinessServiceRow struct {
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

// PagerDutyBusinessServicesRouteHandler owns source collection and canonical
// normalization only. Route registration, switches, and worker wiring stay
// with the integration lane.
type PagerDutyBusinessServicesRouteHandler struct {
	MaxPages int
	MaxRows  int
	PerPage  int
}

type pagerDutyBusinessServicesCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer pagerDutyBusinessServicesCountingDoer) Do(request *http.Request) (*http.Response, error) {
	(*doer.attempts)++
	return doer.delegate.Do(request)
}

func (handler PagerDutyBusinessServicesRouteHandler) limits() (int, int, int, error) {
	pages, rows, perPage := handler.MaxPages, handler.MaxRows, handler.PerPage
	if pages == 0 {
		pages = pagerDutyBusinessServicesMaxPages
	}
	if rows == 0 {
		rows = pagerDutyBusinessServicesMaxRows
	}
	if perPage == 0 {
		perPage = pagerDutyBusinessServicesPerPage
	}
	if pages < 1 || pages > pagerDutyBusinessServicesMaxPages || rows < 1 ||
		rows > pagerDutyBusinessServicesMaxRows || perPage < 1 || perPage > pagerDutyBusinessServicesPerPage {
		return 0, 0, 0, ErrInvalidConfiguration
	}
	return pages, rows, perPage, nil
}

func (handler PagerDutyBusinessServicesRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || credential.Provider != "pagerduty" ||
		claim.Provider != "pagerduty" || claim.Dataset != "business-services" ||
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
	counted.Doer = pagerDutyBusinessServicesCountingDoer{
		delegate: client.Doer, attempts: &requests,
	}
	pages, err := providerfoundation.CollectPagerDutyOffsetPages(
		ctx, &counted, providerfoundation.PagerDutyOffsetOptions{
			Path: "/business_services", DataKey: "business_services",
			PerPage: perPage, MaxPages: maxPages,
		},
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if len(pages.Items) > maxRows || pages.PageBudgetExhausted {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	rows := make([]pagerDutyBusinessServiceRow, 0, len(pages.Items))
	for _, raw := range pages.Items {
		var payload pagerDutyBusinessServicePayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		row, err := normalizePagerDutyBusinessService(
			claim, providerInstance, payload, normalizedAt,
		)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		rows = append(rows, row)
	}
	effect, err := effectBatchFromValues(
		"operational_services", EffectReadbackRequired, rows,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result:  map[string]any{"business_services_synced": len(rows)},
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests,
			Pages: pages.Pages, Records: len(rows), CapReached: pages.PageBudgetExhausted,
		},
	}, nil
}

func normalizePagerDutyBusinessService(
	claim Claim,
	providerInstance string,
	payload pagerDutyBusinessServicePayload,
	normalizedAt time.Time,
) (pagerDutyBusinessServiceRow, error) {
	externalID := strings.TrimSpace(payload.ID)
	if externalID == "" {
		return pagerDutyBusinessServiceRow{}, providerfoundation.ErrNormalizationInvalid
	}
	sourceVersionAt, err := pagerDutyBusinessServiceSourceTime(payload, normalizedAt)
	if err != nil {
		return pagerDutyBusinessServiceRow{}, err
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
	serviceType := "business"
	row := pagerDutyBusinessServiceRow{
		OrgID: claim.OrgID, Provider: "pagerduty", ProviderInstanceID: providerInstance,
		SourceEntityType: "business_service", ExternalID: externalID,
		SourceVersionAt: sourceVersionAt, SourceURL: sourceURL,
		ObservedAt: normalizedAt, LastSynced: normalizedAt, Name: name,
		Description: payload.Description, ServiceType: &serviceType,
	}
	if err := fillPagerDutyBusinessServiceOrdering(&row); err != nil {
		return pagerDutyBusinessServiceRow{}, err
	}
	return row, nil
}

func pagerDutyBusinessServiceSourceTime(
	payload pagerDutyBusinessServicePayload, observedAt time.Time,
) (time.Time, error) {
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

func fillPagerDutyBusinessServiceOrdering(row *pagerDutyBusinessServiceRow) error {
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
	row.OrderingContract = 2
	return nil
}

func pagerDutyBusinessServiceTombstone(
	row pagerDutyBusinessServiceRow, observedAt time.Time,
) (pagerDutyBusinessServiceRow, error) {
	if row.ID == "" || row.SourceVersionAt.IsZero() || observedAt.IsZero() {
		return pagerDutyBusinessServiceRow{}, providerfoundation.ErrNormalizationInvalid
	}
	version := observedAt.UTC().Truncate(time.Microsecond)
	previousNext := row.SourceVersionAt.UTC().Truncate(time.Microsecond).Add(time.Microsecond)
	if version.Before(previousNext) {
		version = previousNext
	}
	row.SourceVersionAt = version
	row.ObservedAt = version
	row.LastSynced = version
	row.IsDeleted = true
	row.DeletedAt = &version
	row.SourceRevision = nil
	row.SourceConflictKey = ""
	row.IngestRevision = nil
	row.OrderingContract = 0
	if err := fillPagerDutyBusinessServiceOrdering(&row); err != nil {
		return pagerDutyBusinessServiceRow{}, err
	}
	return row, nil
}

var _ CompleteRouteHandler = PagerDutyBusinessServicesRouteHandler{}
