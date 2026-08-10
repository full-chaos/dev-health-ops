package providersync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
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
	pagerDutyIncidentFamilyMaxPages      = 10_000
	pagerDutyIncidentFamilyMaxRows       = 100_000
	pagerDutyIncidentFamilyPerPage       = 100
	pagerDutyIncidentFamilyEnrichmentCap = 100
	pagerDutyIncidentFamilyMaxChildPages = 10_000
)

// PagerDuty's incident family has one parent stream and three independently
// persisted child streams. The family handler owns the shared parent cursor,
// cap, and completion rules while keeping one typed effect per Python sink
// boundary. It intentionally does not register or activate a worker route.
type PagerDutyIncidentFamilyRouteHandler struct {
	MaxPages      int
	MaxRows       int
	PerPage       int
	EnrichmentCap int
}

type pagerDutyIncidentReferencePayload struct {
	ID      string  `json:"id"`
	Summary *string `json:"summary"`
}

type pagerDutyIncidentPayload struct {
	ID                 string                             `json:"id"`
	Type               *string                            `json:"type"`
	Summary            *string                            `json:"summary"`
	SelfURL            *string                            `json:"self"`
	HTMLURL            *string                            `json:"html_url"`
	CreatedAt          *string                            `json:"created_at"`
	UpdatedAt          *string                            `json:"updated_at"`
	IncidentNumber     *int                               `json:"incident_number"`
	Title              *string                            `json:"title"`
	Status             *string                            `json:"status"`
	Urgency            *string                            `json:"urgency"`
	ResolvedAt         *string                            `json:"resolved_at"`
	LastStatusChangeAt *string                            `json:"last_status_change_at"`
	Service            *pagerDutyIncidentReferencePayload `json:"service"`
	Priority           *pagerDutyIncidentReferencePayload `json:"priority"`
}

type pagerDutyAlertPayload struct {
	ID        string  `json:"id"`
	Type      *string `json:"type"`
	Summary   *string `json:"summary"`
	SelfURL   *string `json:"self"`
	HTMLURL   *string `json:"html_url"`
	CreatedAt *string `json:"created_at"`
	UpdatedAt *string `json:"updated_at"`
	Status    *string `json:"status"`
	Severity  *string `json:"severity"`
}

type pagerDutyLogEntryPayload struct {
	ID        string  `json:"id"`
	Type      *string `json:"type"`
	Summary   *string `json:"summary"`
	Message   *string `json:"message"`
	SelfURL   *string `json:"self"`
	HTMLURL   *string `json:"html_url"`
	CreatedAt *string `json:"created_at"`
	UpdatedAt *string `json:"updated_at"`
}

type pagerDutyNotePayload struct {
	ID        string                             `json:"id"`
	Type      *string                            `json:"type"`
	SelfURL   *string                            `json:"self"`
	HTMLURL   *string                            `json:"html_url"`
	CreatedAt *string                            `json:"created_at"`
	UpdatedAt *string                            `json:"updated_at"`
	Content   *string                            `json:"content"`
	User      *pagerDutyIncidentReferencePayload `json:"user"`
}

// Each row mirrors the complete Python canonical dataclass field set. The
// ordering fields are included in the durable effect payload even though the
// migrated ClickHouse tables intentionally store only their production
// projection.
type pagerDutyIncidentRow struct {
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
	ServiceID              *string    `json:"service_id"`
	ServiceExternalID      *string    `json:"service_external_id"`
	EscalationPolicyID     *string    `json:"escalation_policy_id"`
	Title                  string     `json:"title"`
	Description            *string    `json:"description"`
	StartedAt              *time.Time `json:"started_at"`
	ResolvedAt             *time.Time `json:"resolved_at"`
	IsDeleted              bool       `json:"is_deleted"`
	DeletedAt              *time.Time `json:"deleted_at"`
}

type pagerDutyAlertRow struct {
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
	ServiceID              *string    `json:"service_id"`
	IncidentID             *string    `json:"incident_id"`
	Title                  string     `json:"title"`
	Description            *string    `json:"description"`
	TriggeredAt            *time.Time `json:"triggered_at"`
	AcknowledgedAt         *time.Time `json:"acknowledged_at"`
	ResolvedAt             *time.Time `json:"resolved_at"`
	IsDeleted              bool       `json:"is_deleted"`
	DeletedAt              *time.Time `json:"deleted_at"`
}

type pagerDutyLogEntryRow struct {
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
	IncidentID             string     `json:"incident_id"`
	EventType              string     `json:"event_type"`
	Body                   *string    `json:"body"`
	ActorType              *string    `json:"actor_type"`
	ActorID                *string    `json:"actor_id"`
	OccurredAt             *time.Time `json:"occurred_at"`
}

type pagerDutyNoteRow struct {
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
	IncidentID             string     `json:"incident_id"`
	Body                   string     `json:"body"`
	AuthorUserID           *string    `json:"author_user_id"`
	CreatedAt              *time.Time `json:"created_at"`
}

type pagerDutyIncidentFamilyCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer pagerDutyIncidentFamilyCountingDoer) Do(request *http.Request) (*http.Response, error) {
	(*doer.attempts)++
	return doer.delegate.Do(request)
}

type pagerDutyIncidentFamilyEndpoint struct {
	path      string
	decode    func([]byte) ([]json.RawMessage, bool, error)
	paginated bool
}

type pagerDutyIncidentsEnvelope struct {
	Incidents []json.RawMessage `json:"incidents"`
	More      *bool             `json:"more"`
}

type pagerDutyAlertsEnvelope struct {
	Alerts []json.RawMessage `json:"alerts"`
	More   *bool             `json:"more"`
}

type pagerDutyLogEntriesEnvelope struct {
	LogEntries []json.RawMessage `json:"log_entries"`
	More       *bool             `json:"more"`
}

type pagerDutyNotesEnvelope struct {
	Notes []json.RawMessage `json:"notes"`
}

func decodePagerDutyIncidents(body []byte) ([]json.RawMessage, bool, error) {
	var envelope pagerDutyIncidentsEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Incidents == nil || envelope.More == nil {
		return nil, false, providerfoundation.ErrPaginationInvalid
	}
	return envelope.Incidents, *envelope.More, nil
}

func decodePagerDutyAlerts(body []byte) ([]json.RawMessage, bool, error) {
	var envelope pagerDutyAlertsEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Alerts == nil || envelope.More == nil {
		return nil, false, providerfoundation.ErrPaginationInvalid
	}
	return envelope.Alerts, *envelope.More, nil
}

func decodePagerDutyLogEntries(body []byte) ([]json.RawMessage, bool, error) {
	var envelope pagerDutyLogEntriesEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.LogEntries == nil || envelope.More == nil {
		return nil, false, providerfoundation.ErrPaginationInvalid
	}
	return envelope.LogEntries, *envelope.More, nil
}

func decodePagerDutyNotes(body []byte) ([]json.RawMessage, bool, error) {
	var envelope pagerDutyNotesEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Notes == nil {
		return nil, false, providerfoundation.ErrPaginationInvalid
	}
	return envelope.Notes, false, nil
}

func pagerDutyIncidentChildEndpoint(client *providerfoundation.HTTPClient, incidentID, dataset string) (pagerDutyIncidentFamilyEndpoint, error) {
	if client == nil || strings.TrimSpace(incidentID) == "" {
		return pagerDutyIncidentFamilyEndpoint{}, providerfoundation.ErrNormalizationInvalid
	}
	base := providerRelativePath(client, "incidents", incidentID)
	switch dataset {
	case "incident-alerts":
		return pagerDutyIncidentFamilyEndpoint{path: base + "/alerts", decode: decodePagerDutyAlerts, paginated: true}, nil
	case "incident-log-entries":
		return pagerDutyIncidentFamilyEndpoint{path: base + "/log_entries", decode: decodePagerDutyLogEntries, paginated: true}, nil
	case "incident-notes":
		return pagerDutyIncidentFamilyEndpoint{path: base + "/notes", decode: decodePagerDutyNotes, paginated: false}, nil
	default:
		return pagerDutyIncidentFamilyEndpoint{}, ErrInvalidConfiguration
	}
}

type pagerDutyIncidentPageCollection struct {
	Items      []json.RawMessage
	Pages      int
	CapReached bool
}

func pagerDutyIncidentFamilyLimits(handler PagerDutyIncidentFamilyRouteHandler) (int, int, int, int, error) {
	maxPages, maxRows, perPage, cap := handler.MaxPages, handler.MaxRows, handler.PerPage, handler.EnrichmentCap
	if maxPages == 0 {
		maxPages = pagerDutyIncidentFamilyMaxPages
	}
	if maxRows == 0 {
		maxRows = pagerDutyIncidentFamilyMaxRows
	}
	if perPage == 0 {
		perPage = pagerDutyIncidentFamilyPerPage
	}
	if cap == 0 {
		cap = pagerDutyIncidentFamilyEnrichmentCap
	}
	if maxPages < 1 || maxPages > pagerDutyIncidentFamilyMaxPages || maxRows < 1 ||
		maxRows > pagerDutyIncidentFamilyMaxRows || perPage < 1 || perPage > pagerDutyIncidentFamilyPerPage ||
		cap < 0 || cap > pagerDutyIncidentFamilyMaxRows {
		return 0, 0, 0, 0, ErrInvalidConfiguration
	}
	return maxPages, maxRows, perPage, cap, nil
}

func (handler PagerDutyIncidentFamilyRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || credential.Provider != "pagerduty" ||
		claim.Provider != "pagerduty" || client == nil || client.Provider != "pagerduty" ||
		client.BaseURL == nil || normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	if claim.Dataset != "incidents" && claim.Dataset != "incident-alerts" &&
		claim.Dataset != "incident-log-entries" && claim.Dataset != "incident-notes" {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	maxPages, maxRows, perPage, cap, err := pagerDutyIncidentFamilyLimits(handler)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	providerInstance, err := pagerDutyProviderInstance(credential)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Microsecond)
	if claim.Dataset != "incidents" {
		if value, present := claim.DatasetOptions["enabled"]; present {
			enabled, ok := value.(bool)
			if !ok {
				return CompleteRouteBatch{}, ErrInvalidConfiguration
			}
			if !enabled {
				return handler.emptyPagerDutyIncidentFamilyBatch(claim, 0, 0)
			}
		}
	}
	requests := 0
	counted := *client
	counted.Doer = pagerDutyIncidentFamilyCountingDoer{delegate: client.Doer, attempts: &requests}
	parentEndpoint := pagerDutyIncidentFamilyEndpoint{
		path: "/incidents", decode: decodePagerDutyIncidents, paginated: true,
	}
	query := url.Values{}
	if claim.SinceAt != nil {
		query.Set("since", claim.SinceAt.UTC().Format(time.RFC3339Nano))
	}
	if claim.BeforeAt != nil {
		query.Set("until", claim.BeforeAt.UTC().Format(time.RFC3339Nano))
	}
	parents, err := collectPagerDutyIncidentFamilyPages(
		ctx, &counted, parentEndpoint, query, perPage, maxPages, maxRows,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if parents.CapReached {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	if claim.Dataset == "incidents" {
		return handler.collectPagerDutyIncidents(claim, providerInstance, normalizedAt, parents, requests)
	}
	if value, present := claim.DatasetOptions["enrichment_cap"]; present {
		cap, err = pagerDutyIncidentFamilyMaxRowsOption(value, cap)
		if err != nil || cap > pagerDutyIncidentFamilyMaxRows {
			return CompleteRouteBatch{}, ErrInvalidConfiguration
		}
	}
	return handler.collectPagerDutyEnrichment(
		ctx, claim, providerInstance, &counted, normalizedAt, parents, &requests,
		perPage, maxPages, maxRows, cap,
	)
}

func collectPagerDutyIncidentFamilyPages(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	endpoint pagerDutyIncidentFamilyEndpoint,
	baseQuery url.Values,
	perPage, maxPages, maxRows int,
) (pagerDutyIncidentPageCollection, error) {
	if ctx == nil || client == nil || endpoint.decode == nil || strings.TrimSpace(endpoint.path) == "" ||
		perPage < 1 || maxPages < 1 || maxRows < 1 {
		return pagerDutyIncidentPageCollection{}, providerfoundation.ErrPaginationInvalid
	}
	result := pagerDutyIncidentPageCollection{Items: make([]json.RawMessage, 0)}
	offset := 0
	for {
		if result.Pages >= maxPages {
			result.CapReached = true
			return result, nil
		}
		items, more, err := requestPagerDutyIncidentFamilyPage(
			ctx, client, endpoint, baseQuery, offset, perPage,
		)
		if err != nil {
			return result, err
		}
		result.Pages++
		if len(items) > maxRows-len(result.Items) {
			return result, ErrPaginationCapExceeded
		}
		result.Items = append(result.Items, items...)
		if !more {
			return result, nil
		}
		if len(result.Items) == maxRows {
			return result, ErrPaginationCapExceeded
		}
		if len(items) == 0 {
			return result, providerfoundation.ErrPaginationInvalid
		}
		offset += len(items)
	}
}

func requestPagerDutyIncidentFamilyPage(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	endpoint pagerDutyIncidentFamilyEndpoint,
	baseQuery url.Values,
	offset, perPage int,
) ([]json.RawMessage, bool, error) {
	query := make(url.Values, len(baseQuery)+2)
	for key, values := range baseQuery {
		query[key] = append([]string(nil), values...)
	}
	if endpoint.paginated {
		query.Set("limit", strconv.Itoa(perPage))
		query.Set("offset", strconv.Itoa(offset))
	}
	target := endpoint.path
	if encoded := query.Encode(); encoded != "" {
		target += "?" + encoded
	}
	response, err := client.Do(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, false, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, false, providerfoundation.ErrPaginationInvalid
	}
	return endpoint.decode(body)
}

func (handler PagerDutyIncidentFamilyRouteHandler) collectPagerDutyIncidents(
	claim Claim,
	providerInstance string,
	normalizedAt time.Time,
	parents pagerDutyIncidentPageCollection,
	requests int,
) (CompleteRouteBatch, error) {
	rows := make([]pagerDutyIncidentRow, 0, len(parents.Items))
	var watermark *time.Time
	for _, raw := range parents.Items {
		payload, err := decodePagerDutyIncidentPayload(raw)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		createdAt, err := pagerDutyOptionalTime(payload.CreatedAt)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		if pagerDutyIncidentOutsideWindow(createdAt, claim) {
			continue
		}
		row, err := normalizePagerDutyIncident(claim, providerInstance, payload, normalizedAt)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		rows = append(rows, row)
		watermark = pagerDutyMaxTime(watermark, createdAt)
	}
	effect, err := effectBatchFromValues("operational_incidents", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects:   []EffectBatch{effect},
		Result:    map[string]any{"incidents_synced": len(rows)},
		Watermark: watermark,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests,
			Pages: parents.Pages, Records: len(rows), CapReached: parents.CapReached,
		},
	}, nil
}

func (handler PagerDutyIncidentFamilyRouteHandler) emptyPagerDutyIncidentFamilyBatch(
	claim Claim, requests, pages int,
) (CompleteRouteBatch, error) {
	destination, resultKey := pagerDutyIncidentFamilyDestination(claim.Dataset)
	var (
		effect EffectBatch
		err    error
	)
	switch claim.Dataset {
	case "incident-alerts":
		effect, err = effectBatchFromValues(destination, EffectReadbackRequired, []pagerDutyAlertRow{})
	case "incident-log-entries":
		effect, err = effectBatchFromValues(destination, EffectReadbackRequired, []pagerDutyLogEntryRow{})
	case "incident-notes":
		effect, err = effectBatchFromValues(destination, EffectReadbackRequired, []pagerDutyNoteRow{})
	default:
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result:  map[string]any{resultKey: 0},
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: requests, Pages: pages,
		},
	}, nil
}

func (handler PagerDutyIncidentFamilyRouteHandler) collectPagerDutyEnrichment(
	ctx context.Context,
	claim Claim,
	providerInstance string,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
	parents pagerDutyIncidentPageCollection,
	requests *int,
	perPage, maxPages, maxRows, cap int,
) (CompleteRouteBatch, error) {
	destination, resultKey := pagerDutyIncidentFamilyDestination(claim.Dataset)
	if destination == "" {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	alertRows := make([]pagerDutyAlertRow, 0)
	logEntryRows := make([]pagerDutyLogEntryRow, 0)
	noteRows := make([]pagerDutyNoteRow, 0)
	watermark := pagerDutyTimePointer(claim.SinceAt)
	var earliestUndrained *time.Time
	childPages := 0
	childRecords := 0
	for _, raw := range parents.Items {
		incident, err := decodePagerDutyIncidentPayload(raw)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		createdAt, err := pagerDutyOptionalTime(incident.CreatedAt)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		if pagerDutyIncidentOutsideWindow(createdAt, claim) {
			continue
		}
		incidentRow, err := normalizePagerDutyIncident(claim, providerInstance, incident, normalizedAt)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		watermark = pagerDutyMaxTime(watermark, createdAt)
		if cap == 0 {
			continue
		}
		endpoint, err := pagerDutyIncidentChildEndpoint(client, incident.ID, claim.Dataset)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		childCount := 0
		undrained := false
		offset := 0
		for {
			if childPages >= maxPages {
				return CompleteRouteBatch{}, ErrPaginationCapExceeded
			}
			items, more, requestErr := requestPagerDutyIncidentFamilyPage(
				ctx, client, endpoint, nil, offset, perPage,
			)
			if requestErr != nil {
				return CompleteRouteBatch{}, requestErr
			}
			childPages++
			remaining := cap - childCount
			take := len(items)
			if take > remaining {
				take = remaining
			}
			if take > 0 {
				if childRecords > maxRows-take {
					return CompleteRouteBatch{}, ErrPaginationCapExceeded
				}
				for _, childRaw := range items[:take] {
					switch claim.Dataset {
					case "incident-alerts":
						payload, decodeErr := decodePagerDutyAlertPayload(childRaw)
						if decodeErr != nil {
							return CompleteRouteBatch{}, decodeErr
						}
						row, normalizeErr := normalizePagerDutyAlert(claim, providerInstance, payload, incidentRow.ID, normalizedAt)
						if normalizeErr != nil {
							return CompleteRouteBatch{}, normalizeErr
						}
						alertRows = append(alertRows, row)
					case "incident-log-entries":
						payload, decodeErr := decodePagerDutyLogEntryPayload(childRaw)
						if decodeErr != nil {
							return CompleteRouteBatch{}, decodeErr
						}
						row, normalizeErr := normalizePagerDutyLogEntry(claim, providerInstance, payload, incidentRow.ID, normalizedAt)
						if normalizeErr != nil {
							return CompleteRouteBatch{}, normalizeErr
						}
						logEntryRows = append(logEntryRows, row)
					case "incident-notes":
						payload, decodeErr := decodePagerDutyNotePayload(childRaw)
						if decodeErr != nil {
							return CompleteRouteBatch{}, decodeErr
						}
						row, normalizeErr := normalizePagerDutyNote(claim, providerInstance, payload, incidentRow.ID, normalizedAt)
						if normalizeErr != nil {
							return CompleteRouteBatch{}, normalizeErr
						}
						noteRows = append(noteRows, row)
					}
				}
				childCount += take
				childRecords += take
			}
			if childCount == cap {
				undrained = more || len(items) > remaining
				break
			}
			if !more {
				break
			}
			if len(items) == 0 {
				return CompleteRouteBatch{}, providerfoundation.ErrPaginationInvalid
			}
			offset += len(items)
		}
		if undrained {
			earliestUndrained = pagerDutyMinTime(earliestUndrained, createdAt)
		}
	}
	if earliestUndrained != nil && (watermark == nil || earliestUndrained.Before(*watermark)) {
		watermark = earliestUndrained
	}
	var effect EffectBatch
	var err error
	switch claim.Dataset {
	case "incident-alerts":
		effect, err = effectBatchFromValues(destination, EffectReadbackRequired, alertRows)
	case "incident-log-entries":
		effect, err = effectBatchFromValues(destination, EffectReadbackRequired, logEntryRows)
	case "incident-notes":
		effect, err = effectBatchFromValues(destination, EffectReadbackRequired, noteRows)
	default:
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects:   []EffectBatch{effect},
		Result:    map[string]any{resultKey: childRecords},
		Watermark: watermark,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: valueOrZero(requests),
			Pages: parents.Pages + childPages, Records: childRecords,
		},
	}, nil
}

func valueOrZero(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

func pagerDutyIncidentFamilyDestination(dataset string) (string, string) {
	switch dataset {
	case "incidents":
		return "operational_incidents", "incidents_synced"
	case "incident-alerts":
		return "operational_alerts", "alerts_synced"
	case "incident-log-entries":
		return "operational_incident_timeline_events", "log_entries_synced"
	case "incident-notes":
		return "operational_incident_notes", "notes_synced"
	default:
		return "", ""
	}
}

func pagerDutyIncidentOutsideWindow(createdAt *time.Time, claim Claim) bool {
	if createdAt == nil {
		return false
	}
	if claim.SinceAt != nil && createdAt.Before(claim.SinceAt.UTC()) {
		return true
	}
	return claim.BeforeAt != nil && createdAt.After(claim.BeforeAt.UTC())
}

func pagerDutyMaxTime(current, candidate *time.Time) *time.Time {
	if candidate == nil {
		return current
	}
	if current == nil || candidate.After(*current) {
		copy := candidate.UTC().Truncate(time.Microsecond)
		return &copy
	}
	return current
}

func pagerDutyMinTime(current, candidate *time.Time) *time.Time {
	if candidate == nil {
		return current
	}
	if current == nil || candidate.Before(*current) {
		copy := candidate.UTC().Truncate(time.Microsecond)
		return &copy
	}
	return current
}

func pagerDutyTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC().Truncate(time.Microsecond)
	return &copy
}

func decodePagerDutyIncidentPayload(raw json.RawMessage) (pagerDutyIncidentPayload, error) {
	var payload pagerDutyIncidentPayload
	if err := json.Unmarshal(raw, &payload); err != nil || strings.TrimSpace(payload.ID) == "" {
		return pagerDutyIncidentPayload{}, providerfoundation.ErrNormalizationInvalid
	}
	return payload, nil
}

func decodePagerDutyAlertPayload(raw json.RawMessage) (pagerDutyAlertPayload, error) {
	var payload pagerDutyAlertPayload
	if err := json.Unmarshal(raw, &payload); err != nil || strings.TrimSpace(payload.ID) == "" {
		return pagerDutyAlertPayload{}, providerfoundation.ErrNormalizationInvalid
	}
	return payload, nil
}

func decodePagerDutyLogEntryPayload(raw json.RawMessage) (pagerDutyLogEntryPayload, error) {
	var payload pagerDutyLogEntryPayload
	if err := json.Unmarshal(raw, &payload); err != nil || strings.TrimSpace(payload.ID) == "" {
		return pagerDutyLogEntryPayload{}, providerfoundation.ErrNormalizationInvalid
	}
	return payload, nil
}

func decodePagerDutyNotePayload(raw json.RawMessage) (pagerDutyNotePayload, error) {
	var payload pagerDutyNotePayload
	if err := json.Unmarshal(raw, &payload); err != nil || strings.TrimSpace(payload.ID) == "" {
		return pagerDutyNotePayload{}, providerfoundation.ErrNormalizationInvalid
	}
	return payload, nil
}

func pagerDutyOptionalTime(value *string) (*time.Time, error) {
	if value == nil || *value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, *value)
	if err != nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	result := parsed.UTC().Truncate(time.Microsecond)
	return &result, nil
}

func pagerDutyIncidentSourceTime(updatedAt, createdAt *string, observedAt time.Time) (time.Time, error) {
	value := updatedAt
	if value == nil || *value == "" {
		value = createdAt
	}
	if parsed, err := pagerDutyOptionalTime(value); err != nil {
		return time.Time{}, err
	} else if parsed != nil {
		return *parsed, nil
	}
	return observedAt.UTC().Truncate(time.Microsecond), nil
}

func pagerDutyStringOrFallback(values ...*string) string {
	for _, value := range values {
		if value != nil && *value != "" {
			return *value
		}
	}
	return ""
}

func pagerDutyStringOrID(values ...*string) string {
	return pagerDutyStringOrFallback(values...)
}

func pagerDutyURL(htmlURL, selfURL *string) *string {
	if htmlURL != nil && *htmlURL != "" {
		value := *htmlURL
		return &value
	}
	if selfURL != nil {
		value := *selfURL
		return &value
	}
	return nil
}

func pagerDutyReferenceValue(reference *pagerDutyIncidentReferencePayload) *string {
	if reference == nil {
		return nil
	}
	if reference.Summary != nil && *reference.Summary != "" {
		value := *reference.Summary
		return &value
	}
	value := reference.ID
	return &value
}

func pagerDutyNormalizeStatus(raw *string) *string {
	if raw == nil {
		return nil
	}
	var value string
	switch *raw {
	case "triggered":
		value = "open"
	case "acknowledged":
		value = "acknowledged"
	case "resolved":
		value = "resolved"
	default:
		return nil
	}
	return &value
}

func pagerDutyNormalizeIncidentSeverity(raw *string) *string {
	if raw == nil {
		return nil
	}
	if *raw != "high" && *raw != "low" {
		return nil
	}
	value := *raw
	return &value
}

func pagerDutyNormalizeAlertSeverity(raw *string) *string {
	values := map[string]string{"critical": "critical", "error": "high", "warning": "medium", "info": "info"}
	if raw == nil {
		return nil
	}
	value, ok := values[*raw]
	if !ok {
		return nil
	}
	return &value
}

func pagerDutyNormalizeIncidentPriority(reference *pagerDutyIncidentReferencePayload) *string {
	value := pagerDutyReferenceValue(reference)
	if value == nil {
		return nil
	}
	var normalized string
	switch *value {
	case "P1":
		normalized = "high"
	case "P2":
		normalized = "medium"
	case "P3", "P4":
		normalized = "low"
	default:
		return nil
	}
	return &normalized
}

func pagerDutyCanonicalReferenceID(orgID, providerInstance, family, externalID string) *string {
	if strings.TrimSpace(externalID) == "" {
		return nil
	}
	parts := []string{orgID, "pagerduty", providerInstance, family, strings.TrimSpace(externalID)}
	for _, part := range parts {
		if part == "" {
			return nil
		}
	}
	quoted := make([]string, len(parts))
	for index, part := range parts {
		quoted[index] = strconv.QuoteToASCII(part)
	}
	digest := sha256.Sum256([]byte("[" + strings.Join(quoted, ",") + "]"))
	value := hex.EncodeToString(digest[:])
	return &value
}

func normalizePagerDutyIncident(
	claim Claim,
	providerInstance string,
	payload pagerDutyIncidentPayload,
	normalizedAt time.Time,
) (pagerDutyIncidentRow, error) {
	externalID := strings.TrimSpace(payload.ID)
	if externalID == "" {
		return pagerDutyIncidentRow{}, providerfoundation.ErrNormalizationInvalid
	}
	sourceVersionAt, err := pagerDutyIncidentSourceTime(payload.UpdatedAt, payload.CreatedAt, normalizedAt)
	if err != nil {
		return pagerDutyIncidentRow{}, err
	}
	createdAt, err := pagerDutyOptionalTime(payload.CreatedAt)
	if err != nil {
		return pagerDutyIncidentRow{}, err
	}
	resolvedAt, err := pagerDutyOptionalTime(payload.ResolvedAt)
	if err != nil {
		return pagerDutyIncidentRow{}, err
	}
	lastStatusChangeAt, err := pagerDutyOptionalTime(payload.LastStatusChangeAt)
	if err != nil {
		return pagerDutyIncidentRow{}, err
	}
	if payload.Status == nil || *payload.Status != "resolved" {
		resolvedAt = nil
	} else if resolvedAt == nil {
		resolvedAt = lastStatusChangeAt
	}
	row := pagerDutyIncidentRow{
		OrgID: claim.OrgID, Provider: "pagerduty", ProviderInstanceID: providerInstance,
		SourceEntityType: "incident", ExternalID: externalID, SourceVersionAt: sourceVersionAt,
		SourceURL: pagerDutyURL(payload.HTMLURL, payload.SelfURL), ObservedAt: normalizedAt,
		LastSynced: normalizedAt, RawStatus: payload.Status, RawSeverity: payload.Urgency,
		RawPriority:        pagerDutyReferenceValue(payload.Priority),
		NormalizedStatus:   pagerDutyNormalizeStatus(payload.Status),
		NormalizedSeverity: pagerDutyNormalizeIncidentSeverity(payload.Urgency),
		NormalizedPriority: pagerDutyNormalizeIncidentPriority(payload.Priority),
		Title:              pagerDutyStringOrFallback(payload.Title, payload.Summary, &payload.ID),
		SourceEventAt:      createdAt, StartedAt: createdAt, ResolvedAt: resolvedAt,
	}
	if payload.IncidentNumber != nil && *payload.IncidentNumber != 0 {
		sourceEventID := strconv.Itoa(*payload.IncidentNumber)
		row.SourceEventID = &sourceEventID
	}
	if payload.Service != nil {
		serviceExternalID := payload.Service.ID
		row.ServiceExternalID = &serviceExternalID
		row.ServiceID = pagerDutyCanonicalReferenceID(claim.OrgID, providerInstance, "operational_service", serviceExternalID)
	}
	if err := fillPagerDutyIncidentOrdering(&row); err != nil {
		return pagerDutyIncidentRow{}, err
	}
	return row, nil
}

func normalizePagerDutyAlert(
	claim Claim,
	providerInstance string,
	payload pagerDutyAlertPayload,
	incidentID string,
	normalizedAt time.Time,
) (pagerDutyAlertRow, error) {
	externalID := strings.TrimSpace(payload.ID)
	if externalID == "" || incidentID == "" {
		return pagerDutyAlertRow{}, providerfoundation.ErrNormalizationInvalid
	}
	sourceVersionAt, err := pagerDutyIncidentSourceTime(payload.UpdatedAt, payload.CreatedAt, normalizedAt)
	if err != nil {
		return pagerDutyAlertRow{}, err
	}
	triggeredAt, err := pagerDutyOptionalTime(payload.CreatedAt)
	if err != nil {
		return pagerDutyAlertRow{}, err
	}
	resolvedAt := (*time.Time)(nil)
	if payload.Status != nil && *payload.Status == "resolved" {
		resolvedAt, err = pagerDutyOptionalTime(payload.UpdatedAt)
		if err != nil {
			return pagerDutyAlertRow{}, err
		}
		if resolvedAt == nil {
			resolvedAt, err = pagerDutyOptionalTime(payload.CreatedAt)
			if err != nil {
				return pagerDutyAlertRow{}, err
			}
			if resolvedAt == nil {
				copy := sourceVersionAt
				resolvedAt = &copy
			}
		}
	}
	row := pagerDutyAlertRow{
		OrgID: claim.OrgID, Provider: "pagerduty", ProviderInstanceID: providerInstance,
		SourceEntityType: "alert", ExternalID: externalID, SourceVersionAt: sourceVersionAt,
		SourceURL: pagerDutyURL(payload.HTMLURL, payload.SelfURL), ObservedAt: normalizedAt,
		LastSynced: normalizedAt, RawStatus: payload.Status, RawSeverity: payload.Severity,
		NormalizedStatus:   pagerDutyNormalizeStatus(payload.Status),
		NormalizedSeverity: pagerDutyNormalizeAlertSeverity(payload.Severity),
		IncidentID:         &incidentID, Title: pagerDutyStringOrFallback(payload.Summary, &payload.ID),
		TriggeredAt: triggeredAt, ResolvedAt: resolvedAt,
	}
	if err := fillPagerDutyAlertOrdering(&row); err != nil {
		return pagerDutyAlertRow{}, err
	}
	return row, nil
}

func normalizePagerDutyLogEntry(
	claim Claim,
	providerInstance string,
	payload pagerDutyLogEntryPayload,
	incidentID string,
	normalizedAt time.Time,
) (pagerDutyLogEntryRow, error) {
	externalID := strings.TrimSpace(payload.ID)
	if externalID == "" || incidentID == "" {
		return pagerDutyLogEntryRow{}, providerfoundation.ErrNormalizationInvalid
	}
	sourceVersionAt, err := pagerDutyIncidentSourceTime(payload.UpdatedAt, payload.CreatedAt, normalizedAt)
	if err != nil {
		return pagerDutyLogEntryRow{}, err
	}
	occurredAt, err := pagerDutyOptionalTime(payload.CreatedAt)
	if err != nil {
		return pagerDutyLogEntryRow{}, err
	}
	eventType := "pagerduty_log_entry"
	if payload.Type != nil && *payload.Type != "" {
		eventType = *payload.Type
	}
	row := pagerDutyLogEntryRow{
		OrgID: claim.OrgID, Provider: "pagerduty", ProviderInstanceID: providerInstance,
		SourceEntityType: "log_entry", ExternalID: externalID, SourceVersionAt: sourceVersionAt,
		SourceURL: pagerDutyURL(payload.HTMLURL, payload.SelfURL), ObservedAt: normalizedAt,
		LastSynced: normalizedAt, IncidentID: incidentID, EventType: eventType,
		Body:      pagerDutyFirstPresent(payload.Summary, payload.Message),
		ActorType: pagerDutyStringPointer("pagerduty"), OccurredAt: occurredAt,
	}
	if err := fillPagerDutyLogEntryOrdering(&row); err != nil {
		return pagerDutyLogEntryRow{}, err
	}
	return row, nil
}

func normalizePagerDutyNote(
	claim Claim,
	providerInstance string,
	payload pagerDutyNotePayload,
	incidentID string,
	normalizedAt time.Time,
) (pagerDutyNoteRow, error) {
	externalID := strings.TrimSpace(payload.ID)
	if externalID == "" || incidentID == "" {
		return pagerDutyNoteRow{}, providerfoundation.ErrNormalizationInvalid
	}
	sourceVersionAt, err := pagerDutyIncidentSourceTime(payload.UpdatedAt, payload.CreatedAt, normalizedAt)
	if err != nil {
		return pagerDutyNoteRow{}, err
	}
	createdAt, err := pagerDutyOptionalTime(payload.CreatedAt)
	if err != nil {
		return pagerDutyNoteRow{}, err
	}
	row := pagerDutyNoteRow{
		OrgID: claim.OrgID, Provider: "pagerduty", ProviderInstanceID: providerInstance,
		SourceEntityType: "note", ExternalID: externalID, SourceVersionAt: sourceVersionAt,
		SourceURL: pagerDutyURL(payload.HTMLURL, payload.SelfURL), ObservedAt: normalizedAt,
		LastSynced: normalizedAt, IncidentID: incidentID, Body: pagerDutyStringValue(payload.Content),
		CreatedAt: createdAt,
	}
	if payload.User != nil {
		row.AuthorUserID = pagerDutyCanonicalReferenceID(claim.OrgID, providerInstance, "operational_user", payload.User.ID)
	}
	if err := fillPagerDutyNoteOrdering(&row); err != nil {
		return pagerDutyNoteRow{}, err
	}
	return row, nil
}

func pagerDutyStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func pagerDutyStringPointer(value string) *string {
	return &value
}

func pagerDutyFirstPresent(values ...*string) *string {
	for _, value := range values {
		if value != nil && *value != "" {
			copy := *value
			return &copy
		}
	}
	for _, value := range values {
		if value != nil {
			copy := *value
			return &copy
		}
	}
	return nil
}

func pagerDutyIncidentFamilyMaxRowsOption(value any, fallback int) (int, error) {
	if value == nil {
		return fallback, nil
	}
	var parsed int64
	switch typed := value.(type) {
	case int:
		parsed = int64(typed)
	case int64:
		parsed = typed
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || math.Trunc(typed) != typed || typed < 0 || typed > float64(math.MaxInt) {
			return 0, ErrInvalidConfiguration
		}
		parsed = int64(typed)
	case json.Number:
		value, err := strconv.ParseInt(string(typed), 10, 64)
		if err != nil {
			return 0, ErrInvalidConfiguration
		}
		parsed = value
	default:
		return 0, ErrInvalidConfiguration
	}
	if parsed < 0 || uint64(parsed) > uint64(math.MaxInt) {
		return 0, ErrInvalidConfiguration
	}
	return int(parsed), nil
}

var _ CompleteRouteHandler = PagerDutyIncidentFamilyRouteHandler{}
