package providersync

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

const (
	pagerDutyOnCallsMaxPages = 10_000
	pagerDutyOnCallsMaxRows  = 100_000
	pagerDutyOnCallsPerPage  = 100
)

// pagerDutyOnCallReference is the only nested provider data consumed by the
// canonical on-call normalizer. Keeping the references typed prevents a
// provider response from smuggling arbitrary values into the effect row.
type pagerDutyOnCallReference struct {
	ID string `json:"id"`
}

type pagerDutyOnCallPayload struct {
	ID               string                    `json:"id"`
	Type             string                    `json:"type"`
	Start            string                    `json:"start"`
	End              string                    `json:"end"`
	EscalationLevel  *int32                    `json:"escalation_level"`
	User             *pagerDutyOnCallReference `json:"user"`
	Schedule         *pagerDutyOnCallReference `json:"schedule"`
	EscalationPolicy *pagerDutyOnCallReference `json:"escalation_policy"`
	SelfURL          string                    `json:"self"`
	HTMLURL          string                    `json:"html_url"`
	CreatedAt        string                    `json:"created_at"`
	UpdatedAt        string                    `json:"updated_at"`
}

// pagerDutyOnCallRow mirrors every field on Python's OnCallAssignment
// dataclass, including ordering values. The assignment table itself does not
// persist the derived ordering columns; effects reconstruct and verify them
// on readback before declaring a write exact.
type pagerDutyOnCallRow struct {
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
	ScheduleID             *string    `json:"schedule_id"`
	UserID                 *string    `json:"user_id"`
	EscalationPolicyID     *string    `json:"escalation_policy_id"`
	EscalationLevel        *int32     `json:"escalation_level"`
	StartsAt               *time.Time `json:"starts_at"`
	EndsAt                 *time.Time `json:"ends_at"`
}

// PagerDutyOnCallsRouteHandler owns only source collection and canonical
// normalization. Registration, matrix/config, and worker wiring stay in the
// integration lane.
type PagerDutyOnCallsRouteHandler struct {
	MaxPages int
	MaxRows  int
	PerPage  int
}

type pagerDutyOnCallsCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer pagerDutyOnCallsCountingDoer) Do(request *http.Request) (*http.Response, error) {
	(*doer.attempts)++
	return doer.delegate.Do(request)
}

func (handler PagerDutyOnCallsRouteHandler) limits() (int, int, int, error) {
	pages, rows, perPage := handler.MaxPages, handler.MaxRows, handler.PerPage
	if pages == 0 {
		pages = pagerDutyOnCallsMaxPages
	}
	if rows == 0 {
		rows = pagerDutyOnCallsMaxRows
	}
	if perPage == 0 {
		perPage = pagerDutyOnCallsPerPage
	}
	if pages < 1 || pages > pagerDutyOnCallsMaxPages || rows < 1 ||
		rows > pagerDutyOnCallsMaxRows || perPage < 1 || perPage > pagerDutyOnCallsPerPage {
		return 0, 0, 0, ErrInvalidConfiguration
	}
	return pages, rows, perPage, nil
}

func (handler PagerDutyOnCallsRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || credential.Provider != "pagerduty" ||
		claim.Provider != "pagerduty" || claim.Dataset != "on-calls" || client == nil ||
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
	counted.Doer = pagerDutyOnCallsCountingDoer{delegate: client.Doer, attempts: &requests}
	pages, err := providerfoundation.CollectPagerDutyOffsetPages(
		ctx, &counted, providerfoundation.PagerDutyOffsetOptions{
			Path: "/oncalls", DataKey: "oncalls", PerPage: perPage, MaxPages: maxPages,
		},
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if len(pages.Items) > maxRows || pages.CapReached {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	rows := make([]pagerDutyOnCallRow, 0, len(pages.Items))
	for _, raw := range pages.Items {
		var payload pagerDutyOnCallPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		row, err := normalizePagerDutyOnCall(
			claim, providerInstance, payload, normalizedAt,
		)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		rows = append(rows, row)
	}
	effect, err := effectBatchFromValues(
		"operational_on_call_assignments", EffectReadbackRequired, rows,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result:  map[string]any{"on_calls_synced": len(rows)},
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests,
			Pages: pages.Pages, Records: len(rows), CapReached: pages.CapReached,
		},
	}, nil
}

func normalizePagerDutyOnCall(
	claim Claim,
	providerInstance string,
	payload pagerDutyOnCallPayload,
	normalizedAt time.Time,
) (pagerDutyOnCallRow, error) {
	externalID, err := pagerDutyOnCallExternalID(payload)
	if err != nil {
		return pagerDutyOnCallRow{}, err
	}
	sourceVersionAt, err := pagerDutyOnCallSourceTime(payload, normalizedAt)
	if err != nil {
		return pagerDutyOnCallRow{}, err
	}
	start, err := pagerDutyOnCallTime(payload.Start)
	if err != nil {
		return pagerDutyOnCallRow{}, err
	}
	end, err := pagerDutyOnCallTime(payload.End)
	if err != nil {
		return pagerDutyOnCallRow{}, err
	}
	sourceURL := pagerDutyOnCallURL(payload)
	row := pagerDutyOnCallRow{
		OrgID: claim.OrgID, Provider: "pagerduty", ProviderInstanceID: providerInstance,
		SourceEntityType: "oncall", ExternalID: externalID,
		SourceVersionAt: sourceVersionAt, SourceURL: sourceURL,
		ObservedAt: normalizedAt, LastSynced: normalizedAt,
		ScheduleID: pagerDutyOnCallReferenceID(
			claim.OrgID, providerInstance, payload.Schedule, "operational_on_call_schedule",
		),
		UserID: pagerDutyOnCallReferenceID(
			claim.OrgID, providerInstance, payload.User, "operational_user",
		),
		EscalationPolicyID: pagerDutyOnCallReferenceID(
			claim.OrgID, providerInstance, payload.EscalationPolicy, "operational_escalation_policy",
		),
		EscalationLevel: payload.EscalationLevel,
		StartsAt:        start, EndsAt: end,
	}
	if (payload.User != nil && strings.TrimSpace(payload.User.ID) == "") ||
		(payload.Schedule != nil && strings.TrimSpace(payload.Schedule.ID) == "") ||
		(payload.EscalationPolicy != nil && strings.TrimSpace(payload.EscalationPolicy.ID) == "") {
		return pagerDutyOnCallRow{}, providerfoundation.ErrNormalizationInvalid
	}
	if err := fillPagerDutyOnCallOrdering(&row); err != nil {
		return pagerDutyOnCallRow{}, err
	}
	return row, nil
}

func pagerDutyOnCallExternalID(payload pagerDutyOnCallPayload) (string, error) {
	if externalID := strings.TrimSpace(payload.ID); externalID != "" {
		return externalID, nil
	}
	if payload.EscalationPolicy == nil || payload.User == nil ||
		strings.TrimSpace(payload.EscalationPolicy.ID) == "" ||
		strings.TrimSpace(payload.User.ID) == "" || payload.EscalationLevel == nil {
		return "", providerfoundation.ErrNormalizationInvalid
	}
	scheduleID := "<permanent>"
	if payload.Schedule != nil && payload.Schedule.ID != "" {
		scheduleID = payload.Schedule.ID
	}
	start := "<permanent>"
	if payload.Start != "" {
		parsed, err := pagerDutyOnCallTime(payload.Start)
		if err != nil {
			return "", err
		}
		start = pagerDutyPythonISOTime(*parsed)
	}
	end := "<permanent>"
	if payload.End != "" {
		parsed, err := pagerDutyOnCallTime(payload.End)
		if err != nil {
			return "", err
		}
		end = pagerDutyPythonISOTime(*parsed)
	}
	return strings.Join([]string{
		payload.EscalationPolicy.ID, scheduleID, payload.User.ID,
		strconv.FormatInt(int64(*payload.EscalationLevel), 10), start, end,
	}, "|"), nil
}

func pagerDutyOnCallSourceTime(
	payload pagerDutyOnCallPayload, observedAt time.Time,
) (time.Time, error) {
	value := payload.UpdatedAt
	if value == "" {
		value = payload.CreatedAt
	}
	if value == "" {
		return observedAt, nil
	}
	return pagerDutyOnCallParsedTime(value)
}

func pagerDutyOnCallTime(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := pagerDutyOnCallParsedTime(value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func pagerDutyOnCallParsedTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, providerfoundation.ErrNormalizationInvalid
	}
	return parsed.UTC().Truncate(time.Microsecond), nil
}

func pagerDutyPythonISOTime(value time.Time) string {
	value = value.UTC().Truncate(time.Microsecond)
	if value.Nanosecond() == 0 {
		return value.Format("2006-01-02T15:04:05-07:00")
	}
	return value.Format("2006-01-02T15:04:05.000000-07:00")
}

func pagerDutyOnCallURL(payload pagerDutyOnCallPayload) *string {
	value := payload.HTMLURL
	if value == "" {
		value = payload.SelfURL
	}
	if value == "" {
		return nil
	}
	return &value
}

func pagerDutyOnCallReferenceID(
	orgID, providerInstance string,
	reference *pagerDutyOnCallReference, family string,
) *string {
	if reference == nil {
		return nil
	}
	id := canonicalPagerDutyOperationalID(
		orgID, "pagerduty", providerInstance, family, strings.TrimSpace(reference.ID),
	)
	return &id
}

func canonicalPagerDutyOperationalID(
	orgID, provider, providerInstance, family, externalID string,
) string {
	parts := []string{orgID, provider, providerInstance, family, externalID}
	quoted := make([]string, len(parts))
	for index, part := range parts {
		quoted[index] = strconv.QuoteToASCII(part)
	}
	digest := sha256Bytes([]byte("[" + strings.Join(quoted, ",") + "]"))
	return hex.EncodeToString(digest)
}

func fillPagerDutyOnCallOrdering(row *pagerDutyOnCallRow) error {
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
	var escalationLevel any
	if row.EscalationLevel != nil {
		escalationLevel = *row.EscalationLevel
	}
	fields = append(fields,
		jiraOperationalField{"schedule_id", jiraStringValue(row.ScheduleID)},
		jiraOperationalField{"user_id", jiraStringValue(row.UserID)},
		jiraOperationalField{"escalation_policy_id", jiraStringValue(row.EscalationPolicyID)},
		jiraOperationalField{"escalation_level", escalationLevel},
		jiraOperationalField{"starts_at", jiraTimeValue(row.StartsAt)},
		jiraOperationalField{"ends_at", jiraTimeValue(row.EndsAt)},
	)
	id, conflict, sourceRevision, ingestRevision, err := deriveGitLabOperationalOrdering(
		"operational_on_call_assignment", row.OrgID, row.Provider,
		row.ProviderInstanceID, row.ExternalID, row.SourceVersionAt,
		row.ObservedAt, row.LastSynced, fields,
	)
	if err != nil {
		return err
	}
	row.ID, row.SourceConflictKey = id, conflict
	row.SourceRevision, row.IngestRevision = sourceRevision, ingestRevision
	row.OrderingContract = 2
	return nil
}

var _ CompleteRouteHandler = PagerDutyOnCallsRouteHandler{}
