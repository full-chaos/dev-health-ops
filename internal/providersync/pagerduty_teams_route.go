package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

const (
	pagerDutyTeamsMaxPages = 10_000
	pagerDutyTeamsMaxRows  = 100_000
	pagerDutyTeamsPerPage  = 100
)

// pagerDutyTeamPayload is the provider-owned subset consumed by Python's
// PagerDutyNormalizer.team. Unknown PagerDuty fields are deliberately
// ignored; they are not part of the persisted OperationalTeam contract.
type pagerDutyTeamPayload struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Summary     string  `json:"summary"`
	Description *string `json:"description"`
	SelfURL     string  `json:"self"`
	HTMLURL     string  `json:"html_url"`
	CreatedAt   string  `json:"created_at"`
	UpdatedAt   string  `json:"updated_at"`
}

// pagerDutyTeamRow mirrors every field on Python's canonical
// operational.OperationalTeam dataclass, including the persisted ordering
// fields. The live Python oracle compares this complete row rather than a
// hand-picked projection.
type pagerDutyTeamRow struct {
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

// PagerDutyTeamsRouteHandler owns source collection and canonical
// normalization. Executor registration, route switches, and matrix/config
// wiring belong to the integration lane and intentionally do not live here.
type PagerDutyTeamsRouteHandler struct {
	MaxPages int
	MaxRows  int
	PerPage  int
}

type pagerDutyTeamsCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer pagerDutyTeamsCountingDoer) Do(request *http.Request) (*http.Response, error) {
	(*doer.attempts)++
	return doer.delegate.Do(request)
}

func (handler PagerDutyTeamsRouteHandler) limits() (int, int, int, error) {
	pages, rows, perPage := handler.MaxPages, handler.MaxRows, handler.PerPage
	if pages == 0 {
		pages = pagerDutyTeamsMaxPages
	}
	if rows == 0 {
		rows = pagerDutyTeamsMaxRows
	}
	if perPage == 0 {
		perPage = pagerDutyTeamsPerPage
	}
	if pages < 1 || pages > pagerDutyTeamsMaxPages || rows < 1 ||
		rows > pagerDutyTeamsMaxRows || perPage < 1 || perPage > pagerDutyTeamsPerPage {
		return 0, 0, 0, ErrInvalidConfiguration
	}
	return pages, rows, perPage, nil
}

func (handler PagerDutyTeamsRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || credential.Provider != "pagerduty" ||
		claim.Provider != "pagerduty" || claim.Dataset != "teams" || client == nil ||
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
	counted.Doer = pagerDutyTeamsCountingDoer{delegate: client.Doer, attempts: &requests}
	pages, err := providerfoundation.CollectPagerDutyOffsetPages(
		ctx, &counted, providerfoundation.PagerDutyOffsetOptions{
			Path: "/teams", DataKey: "teams", PerPage: perPage, MaxPages: maxPages,
		},
	)
	if err != nil {
		var providerErr *providerfoundation.ProviderError
		if errors.As(err, &providerErr) && providerErr.StatusCode == http.StatusPaymentRequired {
			return CompleteRouteBatch{}, fmt.Errorf("pagerduty teams: %w", ErrProviderDatasetUnavailable)
		}
		return CompleteRouteBatch{}, err
	}
	if len(pages.Items) > maxRows || pages.CapReached {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	rows := make([]pagerDutyTeamRow, 0, len(pages.Items))
	for _, raw := range pages.Items {
		var payload pagerDutyTeamPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		row, err := normalizePagerDutyTeam(claim, providerInstance, payload, normalizedAt)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		rows = append(rows, row)
	}
	effect, err := effectBatchFromValues("operational_teams", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests,
			Pages: pages.Pages, Records: len(rows), CapReached: pages.CapReached,
		},
	}, nil
}

func normalizePagerDutyTeam(
	claim Claim,
	providerInstance string,
	payload pagerDutyTeamPayload,
	normalizedAt time.Time,
) (pagerDutyTeamRow, error) {
	externalID := strings.TrimSpace(payload.ID)
	if externalID == "" {
		return pagerDutyTeamRow{}, providerfoundation.ErrNormalizationInvalid
	}
	sourceVersionAt, err := pagerDutyTeamSourceTime(payload, normalizedAt)
	if err != nil {
		return pagerDutyTeamRow{}, err
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
	row := pagerDutyTeamRow{
		OrgID: claim.OrgID, Provider: "pagerduty", ProviderInstanceID: providerInstance,
		SourceEntityType: "team", ExternalID: externalID, SourceVersionAt: sourceVersionAt,
		SourceURL: sourceURL, ObservedAt: normalizedAt, LastSynced: normalizedAt,
		Name: name, Description: payload.Description,
	}
	if err := fillPagerDutyTeamOrdering(&row); err != nil {
		return pagerDutyTeamRow{}, err
	}
	return row, nil
}

func pagerDutyTeamSourceTime(
	payload pagerDutyTeamPayload, observedAt time.Time,
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

// pagerDutyTeamTombstone mirrors Python's _reference_tombstone. A complete
// source snapshot may omit a previously active team; the tombstone must win
// the ReplacingMergeTree version race even when the snapshot timestamp equals
// the prior row's source version.
func pagerDutyTeamTombstone(
	row pagerDutyTeamRow, observedAt time.Time,
) (pagerDutyTeamRow, error) {
	if row.ID == "" || row.SourceVersionAt.IsZero() || observedAt.IsZero() {
		return pagerDutyTeamRow{}, providerfoundation.ErrNormalizationInvalid
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
	if err := fillPagerDutyTeamOrdering(&row); err != nil {
		return pagerDutyTeamRow{}, err
	}
	return row, nil
}

func fillPagerDutyTeamOrdering(row *pagerDutyTeamRow) error {
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
		"operational_team", row.OrgID, row.Provider, row.ProviderInstanceID,
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

var _ CompleteRouteHandler = PagerDutyTeamsRouteHandler{}
