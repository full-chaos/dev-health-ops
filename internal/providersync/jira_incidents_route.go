package providersync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	jiraIncidentAPIOrigin = "https://api.atlassian.com"
	jiraIncidentMaxPages  = 1_000
	jiraIncidentMaxRows   = 100_000
	jiraIncidentPerPage   = 100
)

type jiraIncidentPayload struct {
	ID     string `json:"id"`
	Key    string `json:"key"`
	Fields struct {
		Summary        string  `json:"summary"`
		Created        string  `json:"created"`
		Updated        string  `json:"updated"`
		ResolutionDate *string `json:"resolutiondate"`
		Status         struct {
			Name           string `json:"name"`
			StatusCategory struct {
				Key string `json:"key"`
			} `json:"statusCategory"`
		} `json:"status"`
		Priority *struct {
			Name string `json:"name"`
		} `json:"priority"`
	} `json:"fields"`
}

// jiraIncidentRow mirrors every dataclass field on Python's
// OperationalIncident. The generic oracle reflects the live dataclass and
// compares the entire row, including the UInt128 ordering values.
type jiraIncidentRow struct {
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
	SourceID               *string    `json:"source_id"`
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

type JiraIncidentRouteHandler struct {
	Entitlement IncidentEntitlement
	MaxPages    int
	MaxRows     int
	PerPage     int
}

func (handler JiraIncidentRouteHandler) limits() (int, int, int, error) {
	pages, rows, perPage := handler.MaxPages, handler.MaxRows, handler.PerPage
	if pages == 0 {
		pages = jiraIncidentMaxPages
	}
	if rows == 0 {
		rows = jiraIncidentMaxRows
	}
	if perPage == 0 {
		perPage = jiraIncidentPerPage
	}
	if pages < 1 || pages > jiraIncidentMaxPages || rows < 1 ||
		rows > jiraIncidentMaxRows || perPage < 1 || perPage > jiraIncidentPerPage {
		return 0, 0, 0, ErrInvalidConfiguration
	}
	return pages, rows, perPage, nil
}

func (handler JiraIncidentRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "jira" ||
		claim.Dataset != "incidents" || client == nil || client.Provider != "jira" ||
		client.BaseURL == nil || normalizedAt.IsZero() || claim.SinceAt == nil ||
		claim.BeforeAt == nil || !claim.SinceAt.Before(*claim.BeforeAt) ||
		handler.Entitlement == nil {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	if err := requireIncidentEntitlement(
		ctx, handler.Entitlement, client.Metrics, claim, IncidentEntitlementSeamCollect,
	); err != nil {
		return CompleteRouteBatch{}, err
	}
	maxPages, maxRows, perPage, err := handler.limits()
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	baseURL, err := trustedJiraCloudOrigin(client.BaseURL.String())
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	projectKey := strings.TrimSpace(claim.SourceExternalID)
	if projectKey == "" || strings.ContainsAny(projectKey, ",()\"\r\n") {
		return CompleteRouteBatch{}, providerfoundation.ErrInvalidScope
	}

	var tenant struct {
		CloudID string `json:"cloudId"`
	}
	if err := jiraIncidentFetchObject(ctx, client, http.MethodGet, "/_edge/tenant_info", nil, &tenant); err != nil {
		return CompleteRouteBatch{}, err
	}
	cloudID := strings.TrimSpace(tenant.CloudID)
	if cloudID == "" {
		return CompleteRouteBatch{}, providerfoundation.ErrInvalidScope
	}
	if configured := strings.TrimSpace(credential.Config["cloud_id"]); configured != "" && configured != cloudID {
		return CompleteRouteBatch{}, providerfoundation.ErrInvalidScope
	}

	servicePages, serviceRequests, err := verifyJiraServiceProject(
		ctx, client, projectKey, maxPages, maxRows, perPage,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	issues, searchPages, err := collectJiraIncidentIssues(
		ctx, client, projectKey, *claim.SinceAt, *claim.BeforeAt,
		maxPages, maxRows, perPage,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	admissionClient, err := jiraIncidentAdmissionClient(client, claim)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	rows := make([]jiraIncidentRow, 0, len(issues))
	admissionRequests := 0
	for _, issue := range issues {
		admitted, err := admitJiraIncident(ctx, admissionClient, cloudID, issue.ID)
		admissionRequests++
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		if !admitted {
			continue
		}
		row, err := normalizeJiraIncident(claim, cloudID, baseURL, issue, normalizedAt)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		rows = append(rows, row)
	}
	effect, err := effectBatchFromValues("operational_incidents", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	watermark := claim.BeforeAt.UTC()
	return CompleteRouteBatch{
		Effects:   []EffectBatch{effect},
		Result:    map[string]any{"incidents_synced": len(rows), "project_key": projectKey},
		Watermark: &watermark,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: 1 + serviceRequests + searchPages + admissionRequests,
			Pages:    servicePages + searchPages, Records: len(rows),
		},
	}, nil
}

func trustedJiraCloudOrigin(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" ||
		!strings.HasSuffix(strings.ToLower(parsed.Hostname()), ".atlassian.net") ||
		parsed.User != nil || parsed.Port() != "" ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", providerfoundation.ErrInvalidScope
	}
	return "https://" + strings.ToLower(parsed.Hostname()), nil
}

func verifyJiraServiceProject(
	ctx context.Context, client *providerfoundation.HTTPClient, projectKey string,
	maxPages, maxRows, perPage int,
) (int, int, error) {
	start, pages, rowsSeen := 0, 0, 0
	found := false
	for {
		if pages >= maxPages {
			return pages, pages, ErrPaginationCapExceeded
		}
		var page struct {
			Values     json.RawMessage `json:"values"`
			IsLastPage *bool           `json:"isLastPage"`
		}
		path := "/rest/servicedeskapi/servicedesk?limit=" + strconv.Itoa(perPage) + "&start=" + strconv.Itoa(start)
		if err := jiraIncidentFetchObject(ctx, client, http.MethodGet, path, nil, &page); err != nil {
			return pages, pages + 1, err
		}
		pages++
		if page.IsLastPage == nil {
			return pages, pages, providerfoundation.ErrNormalizationInvalid
		}
		var values []json.RawMessage
		if len(page.Values) == 0 || bytes.Equal(bytes.TrimSpace(page.Values), []byte("null")) ||
			json.Unmarshal(page.Values, &values) != nil {
			return pages, pages, providerfoundation.ErrNormalizationInvalid
		}
		rowsSeen += len(values)
		if rowsSeen > maxRows {
			return pages, pages, ErrPaginationCapExceeded
		}
		for _, raw := range values {
			var desk struct {
				ProjectKey string `json:"projectKey"`
			}
			if json.Unmarshal(raw, &desk) == nil && desk.ProjectKey == projectKey {
				found = true
			}
		}
		if *page.IsLastPage {
			if !found {
				return pages, pages, providerfoundation.ErrInvalidScope
			}
			return pages, pages, nil
		}
		if len(values) == 0 {
			return pages, pages, ErrPaginationCapExceeded
		}
		next := start + len(values)
		if next <= start {
			return pages, pages, ErrPaginationCapExceeded
		}
		start = next
	}
}

func collectJiraIncidentIssues(
	ctx context.Context, client *providerfoundation.HTTPClient, projectKey string,
	since, before time.Time, maxPages, maxRows, perPage int,
) ([]jiraIncidentPayload, int, error) {
	issues := make([]jiraIncidentPayload, 0)
	token := ""
	seen := map[string]struct{}{}
	for pages := 0; ; pages++ {
		if pages >= maxPages {
			return nil, pages, ErrPaginationCapExceeded
		}
		body := map[string]any{
			"jql": fmt.Sprintf(
				`project in (%s) AND "Ticket category" = Incidents AND updated >= "%s" AND updated < "%s" ORDER BY updated ASC, key ASC`,
				projectKey, since.Format(time.RFC3339Nano), before.Format(time.RFC3339Nano),
			),
			"maxResults": perPage,
			"fields":     []string{"id", "key", "summary", "created", "updated", "resolutiondate", "status", "priority"},
		}
		if token != "" {
			body["nextPageToken"] = token
		}
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, pages, ErrInvalidConfiguration
		}
		var page struct {
			Issues        json.RawMessage `json:"issues"`
			IsLast        *bool           `json:"isLast"`
			NextPageToken any             `json:"nextPageToken"`
		}
		if err := jiraIncidentFetchObject(ctx, client, http.MethodPost, "/rest/api/3/search/jql", bytes.NewReader(encoded), &page); err != nil {
			return nil, pages + 1, err
		}
		var pageIssues []json.RawMessage
		if page.IsLast == nil || len(page.Issues) == 0 ||
			bytes.Equal(bytes.TrimSpace(page.Issues), []byte("null")) ||
			json.Unmarshal(page.Issues, &pageIssues) != nil {
			return nil, pages + 1, providerfoundation.ErrNormalizationInvalid
		}
		if len(issues)+len(pageIssues) > maxRows {
			return nil, pages + 1, ErrPaginationCapExceeded
		}
		for _, raw := range pageIssues {
			var issue jiraIncidentPayload
			if err := decodeJiraIncidentJSON(raw, &issue); err != nil {
				return nil, pages + 1, err
			}
			issues = append(issues, issue)
		}
		if *page.IsLast {
			return issues, pages + 1, nil
		}
		next, ok := page.NextPageToken.(string)
		if !ok || next == "" {
			return nil, pages + 1, ErrPaginationCapExceeded
		}
		if _, duplicate := seen[next]; duplicate {
			return nil, pages + 1, ErrPaginationCapExceeded
		}
		seen[next] = struct{}{}
		token = next
	}
}

func jiraIncidentAdmissionClient(
	client *providerfoundation.HTTPClient, claim Claim,
) (*providerfoundation.HTTPClient, error) {
	admission, err := providerfoundation.NewHTTPClient(
		"jira", jiraIncidentAPIOrigin, client.Doer, client.Auth, client.Retry, client.Lease,
	)
	if err != nil {
		return nil, err
	}
	admission.Budget, admission.Gate, admission.Metrics = client.Budget, client.Gate, client.Metrics
	admission.BudgetKey = client.BudgetKey
	admission.BudgetKey.Provider = "jira"
	admission.BudgetKey.OrgID = claim.OrgID
	admission.BudgetKey.Host = "api.atlassian.com"
	return admission, nil
}

func admitJiraIncident(
	ctx context.Context, client *providerfoundation.HTTPClient, cloudID, issueID string,
) (bool, error) {
	if issueID == "" {
		return false, providerfoundation.ErrNormalizationInvalid
	}
	for _, r := range issueID {
		if r < '0' || r > '9' {
			return false, providerfoundation.ErrNormalizationInvalid
		}
	}
	path := "/jsm/incidents/cloudId/" + url.PathEscape(cloudID) + "/v1/incident/" + url.PathEscape(issueID)
	var responseObject map[string]json.RawMessage
	err := jiraIncidentFetchObject(ctx, client, http.MethodGet, path, nil, &responseObject)
	if err == nil {
		return responseObject != nil, nil
	}
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) && providerErr.Class == providerfoundation.ErrorNotFound {
		return false, nil
	}
	return false, err
}

func jiraIncidentFetchObject(
	ctx context.Context, client *providerfoundation.HTTPClient, method, path string,
	body io.Reader, target any,
) error {
	response, err := client.Do(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return providerfoundation.ErrNormalizationInvalid
	}
	limited, err := io.ReadAll(io.LimitReader(response.Body, nativeMaxObjectBytes+1))
	if err != nil || len(limited) > nativeMaxObjectBytes || decodeJiraIncidentJSON(limited, target) != nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	return nil
}

func decodeJiraIncidentJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return providerfoundation.ErrNormalizationInvalid
	}
	return nil
}

func normalizeJiraIncident(
	claim Claim, cloudID, baseURL string, issue jiraIncidentPayload, normalizedAt time.Time,
) (jiraIncidentRow, error) {
	cloudID = strings.ToLower(strings.TrimSpace(cloudID))
	issue.ID, issue.Key = strings.TrimSpace(issue.ID), strings.TrimSpace(issue.Key)
	if claim.Validate() != nil || claim.Provider != "jira" || claim.Dataset != "incidents" ||
		cloudID == "" || issue.ID == "" || issue.Key == "" || issue.Fields.Summary == "" ||
		issue.Fields.Status.Name == "" || issue.Fields.Status.StatusCategory.Key == "" || normalizedAt.IsZero() {
		return jiraIncidentRow{}, providerfoundation.ErrNormalizationInvalid
	}
	trustedBase, err := trustedJiraCloudOrigin(baseURL)
	if err != nil {
		return jiraIncidentRow{}, err
	}
	created, err := parseJiraIncidentTime(issue.Fields.Created)
	if err != nil {
		return jiraIncidentRow{}, err
	}
	updated, err := parseJiraIncidentTime(issue.Fields.Updated)
	if err != nil {
		return jiraIncidentRow{}, err
	}
	var resolution *time.Time
	if issue.Fields.ResolutionDate != nil {
		parsed, err := parseJiraIncidentTime(*issue.Fields.ResolutionDate)
		if err != nil {
			return jiraIncidentRow{}, err
		}
		resolution = &parsed
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Microsecond)
	statusCategory := strings.ToLower(issue.Fields.Status.StatusCategory.Key)
	normalizedStatus := map[string]string{
		"new": "open", "open": "open", "indeterminate": "active", "done": "resolved",
	}[statusCategory]
	if normalizedStatus == "" {
		normalizedStatus = "active"
	}
	rawStatus := issue.Fields.Status.Name
	var rawPriority *string
	if issue.Fields.Priority != nil {
		value := issue.Fields.Priority.Name
		rawPriority = &value
	}
	sourceURL := trustedBase + "/browse/" + issue.Key
	sourceEventID := issue.Key
	row := jiraIncidentRow{
		OrgID: claim.OrgID, Provider: "jira", ProviderInstanceID: cloudID,
		SourceEntityType: "jsm_incident", ExternalID: issue.ID,
		SourceVersionAt: updated, SourceURL: &sourceURL, SourceEventAt: &created,
		SourceEventID: &sourceEventID, ObservedAt: normalizedAt, LastSynced: normalizedAt,
		RawStatus: &rawStatus, RawPriority: rawPriority, NormalizedStatus: &normalizedStatus,
		Title: issue.Fields.Summary, StartedAt: &created, IsDeleted: false,
	}
	if statusCategory == "done" {
		row.ResolvedAt = resolution
	}
	if err := fillJiraIncidentOrdering(&row); err != nil {
		return jiraIncidentRow{}, err
	}
	return row, nil
}

func parseJiraIncidentTime(raw string) (time.Time, error) {
	// Shares normalizeJiraOffset with the work-items path: Jira Cloud returns
	// "+0000" offsets that strict RFC3339 parsing rejects, which failed every
	// real incidents unit with ErrNormalizationInvalid -- a category that is
	// not deterministically terminal, so it burned all 5 attempts and
	// terminalized the whole dataset (CHAOS-3869).
	normalized := normalizeJiraOffset(strings.TrimSpace(raw))
	for _, layout := range jiraTimestampLayouts {
		if parsed, err := time.Parse(layout, normalized); err == nil {
			return parsed.UTC().Truncate(time.Microsecond), nil
		}
	}
	return time.Time{}, providerfoundation.ErrNormalizationInvalid
}

type jiraOperationalField struct {
	name  string
	value any
}

func fillJiraIncidentOrdering(row *jiraIncidentRow) error {
	if row == nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	parts := []string{row.OrgID, row.Provider, row.ProviderInstanceID, "operational_incident", row.ExternalID}
	for _, part := range parts {
		if part == "" {
			return providerfoundation.ErrNormalizationInvalid
		}
	}
	quoted := make([]string, len(parts))
	for index, part := range parts {
		quoted[index] = strconv.QuoteToASCII(part)
	}
	idDigest := sha256.Sum256([]byte("[" + strings.Join(quoted, ",") + "]"))
	row.ID = hex.EncodeToString(idDigest[:])
	fields := []jiraOperationalField{
		{"org_id", row.OrgID}, {"provider", row.Provider},
		{"provider_instance_id", row.ProviderInstanceID}, {"source_entity_type", row.SourceEntityType},
		{"external_id", row.ExternalID}, {"source_version_at", row.SourceVersionAt},
		{"source_id", nil}, {"source_url", jiraStringValue(row.SourceURL)},
		{"source_event_at", jiraTimeValue(row.SourceEventAt)}, {"source_event_id", jiraStringValue(row.SourceEventID)},
		{"raw_status", jiraStringValue(row.RawStatus)}, {"raw_severity", nil},
		{"raw_priority", jiraStringValue(row.RawPriority)}, {"normalized_status", jiraStringValue(row.NormalizedStatus)},
		{"normalized_severity", nil}, {"normalized_priority", nil},
		{"relationship_provenance", nil}, {"relationship_confidence", nil},
		{"service_id", nil}, {"service_external_id", nil}, {"escalation_policy_id", nil},
		{"title", row.Title}, {"description", nil}, {"started_at", jiraTimeValue(row.StartedAt)},
		{"resolved_at", jiraTimeValue(row.ResolvedAt)}, {"is_deleted", row.IsDeleted}, {"deleted_at", nil},
	}
	conflict, err := encodeJiraOperationalConflict("operational_incident", fields)
	if err != nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	row.SourceConflictKey = conflict
	conflictBytes, _ := hex.DecodeString(conflict)
	revisionDigest := sha256.Sum256(append([]byte("operational-source-revision-v1"), conflictBytes...))
	sourceMicros, err := jiraOperationalMicros(row.SourceVersionAt)
	if err != nil {
		return err
	}
	row.SourceRevision = new(big.Int).Lsh(new(big.Int).SetUint64(sourceMicros), 64)
	row.SourceRevision.Or(row.SourceRevision, new(big.Int).Lsh(big.NewInt(1), 56))
	row.SourceRevision.Or(row.SourceRevision, new(big.Int).SetBytes(revisionDigest[:7]))
	lastSyncedMicros, err := jiraOperationalMicros(row.LastSynced)
	if err != nil {
		return err
	}
	observedMicros, err := jiraOperationalMicros(row.ObservedAt)
	if err != nil {
		return err
	}
	row.IngestRevision = new(big.Int).Lsh(new(big.Int).SetUint64(lastSyncedMicros), 64)
	row.IngestRevision.Or(row.IngestRevision, new(big.Int).SetUint64(observedMicros))
	row.OrderingContract = 2
	return nil
}

func encodeJiraOperationalConflict(family string, fields []jiraOperationalField) (string, error) {
	var encoded bytes.Buffer
	encoded.WriteString("operational-conflict-v1")
	if err := encodeJiraOperationalField(&encoded, "entity_family", family); err != nil {
		return "", err
	}
	for _, field := range fields {
		if err := encodeJiraOperationalField(&encoded, field.name, field.value); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(encoded.Bytes()), nil
}

func encodeJiraOperationalField(out *bytes.Buffer, name string, value any) error {
	valueType, marker, encoded, err := encodeJiraOperationalValue(value)
	if err != nil || name == "" {
		return providerfoundation.ErrNormalizationInvalid
	}
	writeJiraLength(out, []byte(name), 4)
	writeJiraLength(out, []byte(valueType), 2)
	out.WriteByte(marker)
	writeJiraLength(out, encoded, 8)
	return nil
}

func encodeJiraOperationalValue(value any) (string, byte, []byte, error) {
	switch typed := value.(type) {
	case nil:
		return "null", 0, nil, nil
	case bool:
		if typed {
			return "bool", 1, []byte{1}, nil
		}
		return "bool", 1, []byte{0}, nil
	case string:
		return "string", 1, []byte(typed), nil
	case time.Time:
		if _, err := jiraOperationalMicros(typed); err != nil {
			return "", 0, nil, err
		}
		return "datetime", 1, []byte(typed.UTC().Format("2006-01-02T15:04:05.000000Z")), nil
	case uuid.UUID:
		return "uuid", 1, []byte(strings.ToLower(typed.String())), nil
	case int:
		return "integer", 1, []byte(strconv.FormatInt(int64(typed), 10)), nil
	case int8:
		return "integer", 1, []byte(strconv.FormatInt(int64(typed), 10)), nil
	case int16:
		return "integer", 1, []byte(strconv.FormatInt(int64(typed), 10)), nil
	case int32:
		return "integer", 1, []byte(strconv.FormatInt(int64(typed), 10)), nil
	case int64:
		return "integer", 1, []byte(strconv.FormatInt(typed, 10)), nil
	case uint:
		return "integer", 1, []byte(strconv.FormatUint(uint64(typed), 10)), nil
	case uint8:
		return "integer", 1, []byte(strconv.FormatUint(uint64(typed), 10)), nil
	case uint16:
		return "integer", 1, []byte(strconv.FormatUint(uint64(typed), 10)), nil
	case uint32:
		return "integer", 1, []byte(strconv.FormatUint(uint64(typed), 10)), nil
	case uint64:
		return "integer", 1, []byte(strconv.FormatUint(typed, 10)), nil
	case float64:
		encoded := make([]byte, 8)
		binary.BigEndian.PutUint64(encoded, math.Float64bits(typed))
		return "float64", 1, encoded, nil
	default:
		return "", 0, nil, providerfoundation.ErrNormalizationInvalid
	}
}

func writeJiraLength(out *bytes.Buffer, value []byte, width int) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(len(value)))
	out.Write(encoded[8-width:])
	out.Write(value)
}

func jiraOperationalMicros(value time.Time) (uint64, error) {
	_, offset := value.Zone()
	maximum := time.Date(2299, 12, 31, 23, 59, 59, 999999000, time.UTC)
	if value.IsZero() || offset != 0 || value.Before(time.Unix(0, 0).UTC()) || value.After(maximum) {
		return 0, providerfoundation.ErrNormalizationInvalid
	}
	return uint64(value.UTC().UnixMicro()), nil
}

func jiraStringValue(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func jiraTimeValue(value *time.Time) any {
	if value == nil {
		return nil
	}
	return *value
}

var _ CompleteRouteHandler = JiraIncidentRouteHandler{}
