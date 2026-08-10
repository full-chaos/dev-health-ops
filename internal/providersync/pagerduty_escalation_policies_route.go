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
	pagerDutyEscalationPoliciesMaxPages = 10_000
	pagerDutyEscalationPoliciesMaxRows  = 100_000
	pagerDutyEscalationPoliciesPerPage  = 100
)

// pagerDutyEscalationPolicyPayload is the provider-owned subset needed by the
// canonical Python EscalationPolicy normalizer. Unknown PagerDuty fields are
// intentionally ignored, as they are not part of the persisted contract.
type pagerDutyEscalationPolicyPayload struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Summary   string `json:"summary"`
	SelfURL   string `json:"self"`
	HTMLURL   string `json:"html_url"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// pagerDutyEscalationPolicyRow mirrors every field on Python's canonical
// operational.EscalationPolicy dataclass, including its persisted ordering
// fields. The generic oracle compares this complete row against the live
// Python normalizer rather than a hand-picked subset.
type pagerDutyEscalationPolicyRow struct {
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
	IsDeleted              bool       `json:"is_deleted"`
	DeletedAt              *time.Time `json:"deleted_at"`
}

// PagerDutyEscalationPoliciesRouteHandler owns only source collection and
// canonical normalization. Route registration and worker wiring remain with
// the integration lane so this provider slice cannot activate itself.
type PagerDutyEscalationPoliciesRouteHandler struct {
	MaxPages int
	MaxRows  int
	PerPage  int
}

type pagerDutyEscalationPoliciesCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer pagerDutyEscalationPoliciesCountingDoer) Do(request *http.Request) (*http.Response, error) {
	(*doer.attempts)++
	return doer.delegate.Do(request)
}

func (handler PagerDutyEscalationPoliciesRouteHandler) limits() (int, int, int, error) {
	pages, rows, perPage := handler.MaxPages, handler.MaxRows, handler.PerPage
	if pages == 0 {
		pages = pagerDutyEscalationPoliciesMaxPages
	}
	if rows == 0 {
		rows = pagerDutyEscalationPoliciesMaxRows
	}
	if perPage == 0 {
		perPage = pagerDutyEscalationPoliciesPerPage
	}
	if pages < 1 || pages > pagerDutyEscalationPoliciesMaxPages || rows < 1 ||
		rows > pagerDutyEscalationPoliciesMaxRows || perPage < 1 || perPage > pagerDutyEscalationPoliciesPerPage {
		return 0, 0, 0, ErrInvalidConfiguration
	}
	return pages, rows, perPage, nil
}

func (handler PagerDutyEscalationPoliciesRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || credential.Provider != "pagerduty" || claim.Provider != "pagerduty" ||
		claim.Dataset != "escalation-policies" || client == nil || client.Provider != "pagerduty" ||
		client.BaseURL == nil || normalizedAt.IsZero() {
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
	counted.Doer = pagerDutyEscalationPoliciesCountingDoer{delegate: client.Doer, attempts: &requests}
	pages, err := providerfoundation.CollectPagerDutyOffsetPages(
		ctx, &counted, providerfoundation.PagerDutyOffsetOptions{
			Path: "/escalation_policies", DataKey: "escalation_policies",
			PerPage: perPage, MaxPages: maxPages,
		},
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if len(pages.Items) > maxRows || pages.CapReached {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	rows := make([]pagerDutyEscalationPolicyRow, 0, len(pages.Items))
	for _, raw := range pages.Items {
		var payload pagerDutyEscalationPolicyPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		row, err := normalizePagerDutyEscalationPolicy(
			claim, providerInstance, payload, normalizedAt,
		)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		rows = append(rows, row)
	}
	effect, err := effectBatchFromValues(
		"operational_escalation_policies", EffectReadbackRequired, rows,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result:  map[string]any{"escalation_policies_synced": len(rows)},
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests,
			Pages: pages.Pages, Records: len(rows), CapReached: pages.CapReached,
		},
	}, nil
}

func pagerDutyProviderInstance(credential providerfoundation.Credential) (string, error) {
	instance := strings.TrimSpace(credential.Config["subdomain"])
	if instance == "" {
		if value, ok := credential.Secret("subdomain"); ok && value.Configured() {
			instance = strings.TrimSpace(value.Reveal())
		}
	}
	if instance == "" {
		return "", providerfoundation.ErrNormalizationInvalid
	}
	return strings.ToLower(instance), nil
}

func normalizePagerDutyEscalationPolicy(
	claim Claim,
	providerInstance string,
	payload pagerDutyEscalationPolicyPayload,
	normalizedAt time.Time,
) (pagerDutyEscalationPolicyRow, error) {
	rawID := payload.ID
	externalID := strings.TrimSpace(rawID)
	if externalID == "" {
		return pagerDutyEscalationPolicyRow{}, providerfoundation.ErrNormalizationInvalid
	}
	sourceVersionAt, err := pagerDutySourceTime(payload, normalizedAt)
	if err != nil {
		return pagerDutyEscalationPolicyRow{}, err
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
		name = rawID
	}
	row := pagerDutyEscalationPolicyRow{
		OrgID: claim.OrgID, Provider: "pagerduty", ProviderInstanceID: providerInstance,
		SourceEntityType: "escalation_policy", ExternalID: externalID,
		SourceVersionAt: sourceVersionAt, SourceURL: sourceURL,
		ObservedAt: normalizedAt, LastSynced: normalizedAt, Name: name,
	}
	if err := fillPagerDutyEscalationPolicyOrdering(&row); err != nil {
		return pagerDutyEscalationPolicyRow{}, err
	}
	return row, nil
}

func pagerDutySourceTime(
	payload pagerDutyEscalationPolicyPayload, observedAt time.Time,
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

func fillPagerDutyEscalationPolicyOrdering(row *pagerDutyEscalationPolicyRow) error {
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
		jiraOperationalField{"is_deleted", row.IsDeleted},
		jiraOperationalField{"deleted_at", jiraTimeValue(row.DeletedAt)},
	)
	id, conflict, sourceRevision, ingestRevision, err := deriveGitLabOperationalOrdering(
		"operational_escalation_policy", row.OrgID, row.Provider,
		row.ProviderInstanceID, row.ExternalID, row.SourceVersionAt,
		row.ObservedAt, row.LastSynced, fields,
	)
	if err != nil {
		return err
	}
	row.ID, row.SourceConflictKey, row.SourceRevision, row.IngestRevision =
		id, conflict, sourceRevision, ingestRevision
	row.OrderingContract = 2
	return nil
}

var _ CompleteRouteHandler = PagerDutyEscalationPoliciesRouteHandler{}
