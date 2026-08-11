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
	pagerDutyUsersMaxPages = 10_000
	pagerDutyUsersMaxRows  = 100_000
	pagerDutyUsersPerPage  = 100
)

// pagerDutyUserPayload is the provider-owned subset consumed by Python's
// PagerDutyNormalizer.user. Unknown PagerDuty fields are deliberately ignored;
// they are not part of the persisted OperationalUser contract.
type pagerDutyUserPayload struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	Summary   string  `json:"summary"`
	SelfURL   string  `json:"self"`
	HTMLURL   string  `json:"html_url"`
	Email     *string `json:"email"`
	CreatedAt string  `json:"created_at"`
	UpdatedAt string  `json:"updated_at"`
}

// pagerDutyUserRow mirrors every field on Python's canonical
// OperationalUser dataclass, including persisted ordering fields. The live
// Python oracle compares this complete row, not a hand-picked projection.
type pagerDutyUserRow struct {
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
	DisplayName            string     `json:"display_name"`
	Email                  *string    `json:"email"`
	IsDeleted              bool       `json:"is_deleted"`
	DeletedAt              *time.Time `json:"deleted_at"`
}

// PagerDutyUsersRouteHandler owns source collection and canonical
// normalization. Executor registration, route switches, and matrix/config
// wiring belong to the integration lane and intentionally do not live here.
type PagerDutyUsersRouteHandler struct {
	MaxPages int
	MaxRows  int
	PerPage  int
}

type pagerDutyUsersCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer pagerDutyUsersCountingDoer) Do(request *http.Request) (*http.Response, error) {
	(*doer.attempts)++
	return doer.delegate.Do(request)
}

func (handler PagerDutyUsersRouteHandler) limits() (int, int, int, error) {
	pages, rows, perPage := handler.MaxPages, handler.MaxRows, handler.PerPage
	if pages == 0 {
		pages = pagerDutyUsersMaxPages
	}
	if rows == 0 {
		rows = pagerDutyUsersMaxRows
	}
	if perPage == 0 {
		perPage = pagerDutyUsersPerPage
	}
	if pages < 1 || pages > pagerDutyUsersMaxPages || rows < 1 ||
		rows > pagerDutyUsersMaxRows || perPage < 1 || perPage > pagerDutyUsersPerPage {
		return 0, 0, 0, ErrInvalidConfiguration
	}
	return pages, rows, perPage, nil
}

func (handler PagerDutyUsersRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || credential.Provider != "pagerduty" ||
		claim.Provider != "pagerduty" || claim.Dataset != "users" || client == nil ||
		client.Provider != "pagerduty" || client.BaseURL == nil || normalizedAt.IsZero() {
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
	counted.Doer = pagerDutyUsersCountingDoer{delegate: client.Doer, attempts: &requests}
	pages, err := providerfoundation.CollectPagerDutyOffsetPages(
		ctx, &counted, providerfoundation.PagerDutyOffsetOptions{
			Path: "/users", DataKey: "users", PerPage: perPage, MaxPages: maxPages,
		},
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if len(pages.Items) > maxRows || pages.CapReached {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	rows := make([]pagerDutyUserRow, 0, len(pages.Items))
	for _, raw := range pages.Items {
		var payload pagerDutyUserPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		row, err := normalizePagerDutyUser(claim, providerInstance, payload, normalizedAt)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		rows = append(rows, row)
	}
	effect, err := effectBatchFromValues("operational_users", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result:  map[string]any{"users_synced": len(rows)},
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests,
			Pages: pages.Pages, Records: len(rows), CapReached: pages.CapReached,
		},
	}, nil
}

func normalizePagerDutyUser(
	claim Claim,
	providerInstance string,
	payload pagerDutyUserPayload,
	normalizedAt time.Time,
) (pagerDutyUserRow, error) {
	externalID := strings.TrimSpace(payload.ID)
	if externalID == "" {
		return pagerDutyUserRow{}, providerfoundation.ErrNormalizationInvalid
	}
	sourceVersionAt, err := pagerDutyUserSourceTime(payload, normalizedAt)
	if err != nil {
		return pagerDutyUserRow{}, err
	}
	var sourceURL *string
	if payload.HTMLURL != "" {
		value := payload.HTMLURL
		sourceURL = &value
	} else if payload.SelfURL != "" {
		value := payload.SelfURL
		sourceURL = &value
	}
	displayName := payload.Name
	if displayName == "" {
		displayName = payload.Summary
	}
	if displayName == "" {
		displayName = payload.ID
	}
	row := pagerDutyUserRow{
		OrgID: claim.OrgID, Provider: "pagerduty", ProviderInstanceID: providerInstance,
		SourceEntityType: "user", ExternalID: externalID,
		SourceVersionAt: sourceVersionAt, SourceURL: sourceURL,
		ObservedAt: normalizedAt, LastSynced: normalizedAt,
		DisplayName: displayName, Email: payload.Email,
	}
	if err := fillPagerDutyUserOrdering(&row); err != nil {
		return pagerDutyUserRow{}, err
	}
	return row, nil
}

func pagerDutyUserSourceTime(
	payload pagerDutyUserPayload, observedAt time.Time,
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

// pagerDutyUserTombstone mirrors Python's _reference_tombstone. A complete
// source snapshot may omit a previously active user; the tombstone must win
// the ReplacingMergeTree version race even when the snapshot timestamp equals
// the prior row's source version.
func pagerDutyUserTombstone(
	row pagerDutyUserRow, observedAt time.Time,
) (pagerDutyUserRow, error) {
	if row.ID == "" || row.SourceVersionAt.IsZero() || observedAt.IsZero() {
		return pagerDutyUserRow{}, providerfoundation.ErrNormalizationInvalid
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
	if err := fillPagerDutyUserOrdering(&row); err != nil {
		return pagerDutyUserRow{}, err
	}
	return row, nil
}

func fillPagerDutyUserOrdering(row *pagerDutyUserRow) error {
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
		jiraOperationalField{"display_name", row.DisplayName},
		jiraOperationalField{"email", jiraStringValue(row.Email)},
		jiraOperationalField{"is_deleted", row.IsDeleted},
		jiraOperationalField{"deleted_at", jiraTimeValue(row.DeletedAt)},
	)
	id, conflict, sourceRevision, ingestRevision, err := deriveGitLabOperationalOrdering(
		"operational_user", row.OrgID, row.Provider, row.ProviderInstanceID,
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

var _ CompleteRouteHandler = PagerDutyUsersRouteHandler{}
