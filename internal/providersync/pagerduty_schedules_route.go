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
	pagerDutySchedulesMaxPages = 10_000
	pagerDutySchedulesMaxRows  = 100_000
	pagerDutySchedulesPerPage  = 100
)

// pagerDutySchedulePayload is the provider-owned subset consumed by Python's
// PagerDutyNormalizer.schedule. The schedules unit deliberately does not own
// on-call assignment payloads; those are a separate source unit and table.
type pagerDutySchedulePayload struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	Summary   string  `json:"summary"`
	SelfURL   string  `json:"self"`
	HTMLURL   string  `json:"html_url"`
	CreatedAt string  `json:"created_at"`
	UpdatedAt string  `json:"updated_at"`
	TimeZone  *string `json:"time_zone"`
}

// pagerDutyScheduleRow mirrors every field on Python's canonical
// OnCallSchedule dataclass, including the persisted ordering fields.
type pagerDutyScheduleRow struct {
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
	Timezone               *string    `json:"timezone"`
	IsDeleted              bool       `json:"is_deleted"`
	DeletedAt              *time.Time `json:"deleted_at"`
}

// PagerDutySchedulesRouteHandler owns source collection and canonical
// normalization only. Registry, capability, and worker wiring stay outside
// this provider slice.
type PagerDutySchedulesRouteHandler struct {
	MaxPages int
	MaxRows  int
	PerPage  int
}

type pagerDutySchedulesCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer pagerDutySchedulesCountingDoer) Do(request *http.Request) (*http.Response, error) {
	(*doer.attempts)++
	return doer.delegate.Do(request)
}

func (handler PagerDutySchedulesRouteHandler) limits() (int, int, int, error) {
	pages, rows, perPage := handler.MaxPages, handler.MaxRows, handler.PerPage
	if pages == 0 {
		pages = pagerDutySchedulesMaxPages
	}
	if rows == 0 {
		rows = pagerDutySchedulesMaxRows
	}
	if perPage == 0 {
		perPage = pagerDutySchedulesPerPage
	}
	if pages < 1 || pages > pagerDutySchedulesMaxPages || rows < 1 ||
		rows > pagerDutySchedulesMaxRows || perPage < 1 || perPage > pagerDutySchedulesPerPage {
		return 0, 0, 0, ErrInvalidConfiguration
	}
	return pages, rows, perPage, nil
}

func (handler PagerDutySchedulesRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || credential.Provider != "pagerduty" ||
		claim.Provider != "pagerduty" || claim.Dataset != "schedules" || client == nil ||
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
	counted.Doer = pagerDutySchedulesCountingDoer{delegate: client.Doer, attempts: &requests}
	pages, err := providerfoundation.CollectPagerDutyOffsetPages(
		ctx, &counted, providerfoundation.PagerDutyOffsetOptions{
			Path: "/schedules", DataKey: "schedules", PerPage: perPage, MaxPages: maxPages,
		},
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if len(pages.Items) > maxRows || pages.PageBudgetExhausted {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	rows := make([]pagerDutyScheduleRow, 0, len(pages.Items))
	for _, raw := range pages.Items {
		var payload pagerDutySchedulePayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		row, err := normalizePagerDutySchedule(claim, providerInstance, payload, normalizedAt)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		rows = append(rows, row)
	}
	effect, err := effectBatchFromValues(
		"operational_on_call_schedules", EffectReadbackRequired, rows,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result:  map[string]any{"schedules_synced": len(rows)},
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests,
			Pages: pages.Pages, Records: len(rows), CapReached: pages.PageBudgetExhausted,
		},
	}, nil
}

func normalizePagerDutySchedule(
	claim Claim,
	providerInstance string,
	payload pagerDutySchedulePayload,
	normalizedAt time.Time,
) (pagerDutyScheduleRow, error) {
	externalID := strings.TrimSpace(payload.ID)
	if externalID == "" {
		return pagerDutyScheduleRow{}, providerfoundation.ErrNormalizationInvalid
	}
	sourceVersionAt, err := pagerDutyScheduleSourceTime(payload, normalizedAt)
	if err != nil {
		return pagerDutyScheduleRow{}, err
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
	row := pagerDutyScheduleRow{
		OrgID: claim.OrgID, Provider: "pagerduty", ProviderInstanceID: providerInstance,
		SourceEntityType: "schedule", ExternalID: externalID,
		SourceVersionAt: sourceVersionAt, SourceURL: sourceURL,
		ObservedAt: normalizedAt, LastSynced: normalizedAt, Name: name,
		Timezone: payload.TimeZone,
	}
	if err := fillPagerDutyScheduleOrdering(&row); err != nil {
		return pagerDutyScheduleRow{}, err
	}
	return row, nil
}

func pagerDutyScheduleSourceTime(
	payload pagerDutySchedulePayload, observedAt time.Time,
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

func fillPagerDutyScheduleOrdering(row *pagerDutyScheduleRow) error {
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
		jiraOperationalField{"timezone", jiraStringValue(row.Timezone)},
		jiraOperationalField{"is_deleted", row.IsDeleted},
		jiraOperationalField{"deleted_at", jiraTimeValue(row.DeletedAt)},
	)
	id, conflict, sourceRevision, ingestRevision, err := deriveGitLabOperationalOrdering(
		"operational_on_call_schedule", row.OrgID, row.Provider, row.ProviderInstanceID,
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

func pagerDutyScheduleTombstone(
	row pagerDutyScheduleRow, observedAt time.Time,
) (pagerDutyScheduleRow, error) {
	if row.ID == "" || row.SourceVersionAt.IsZero() || observedAt.IsZero() {
		return pagerDutyScheduleRow{}, providerfoundation.ErrNormalizationInvalid
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
	if err := fillPagerDutyScheduleOrdering(&row); err != nil {
		return pagerDutyScheduleRow{}, err
	}
	return row, nil
}

var _ CompleteRouteHandler = PagerDutySchedulesRouteHandler{}
