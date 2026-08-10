package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

const (
	// The Python helper passes the processor batch size through to the code
	// client. The code client itself requests exactly one 100-record page for
	// each security endpoint; that frozen single-page boundary is intentional.
	defaultGitLabSecurityMaxAlerts = 1_000
	gitLabSecurityPerPage          = 100
)

// gitLabSecurityAlertRow is the sink-ready form of SecurityAlertData. The
// provider's code client owns source mapping; the route adds tenant/repo
// identity and the retry-stable last_synced value at this boundary.
type gitLabSecurityAlertRow struct {
	OrgID       string     `json:"org_id"`
	RepoID      string     `json:"repo_id"`
	AlertID     string     `json:"alert_id"`
	Source      string     `json:"source"`
	Severity    *string    `json:"severity"`
	State       *string    `json:"state"`
	PackageName *string    `json:"package_name"`
	CVEID       *string    `json:"cve_id"`
	URL         *string    `json:"url"`
	Title       *string    `json:"title"`
	Description *string    `json:"description"`
	CreatedAt   time.Time  `json:"created_at"`
	FixedAt     *time.Time `json:"fixed_at"`
	DismissedAt *time.Time `json:"dismissed_at"`
	LastSynced  time.Time  `json:"last_synced"`
}

// GitLabSecurityRouteHandler mirrors
// processors.gitlab._fetch_gitlab_security_alerts_sync. It deliberately has
// no switch/registry wiring in this slice; the parent lane owns activation.
type GitLabSecurityRouteHandler struct{ MaxAlerts int }

type gitLabSecurityResponseObserver struct {
	attempts    *int
	lastStatus  int
	lastHeaders http.Header
}

type gitLabSecurityCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	observe  *gitLabSecurityResponseObserver
}

func (doer gitLabSecurityCountingDoer) Do(request *http.Request) (*http.Response, error) {
	(*doer.observe.attempts)++
	response, err := doer.delegate.Do(request)
	if response != nil {
		doer.observe.lastStatus = response.StatusCode
		doer.observe.lastHeaders = response.Header.Clone()
	}
	return response, err
}

func (handler GitLabSecurityRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "security" || client == nil || client.Provider != "gitlab" ||
		client.BaseURL == nil || normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	maxAlerts := handler.MaxAlerts
	if maxAlerts == 0 {
		maxAlerts = defaultGitLabSecurityMaxAlerts
	}
	if maxAlerts < 1 {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	projectID, err := gitLabProjectID(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}

	requests := 0
	observer := &gitLabSecurityResponseObserver{attempts: &requests}
	counted := *client
	counted.Doer = gitLabSecurityCountingDoer{delegate: client.Doer, observe: observer}
	root := providerRelativePath(&counted, "api", "v4", "projects", projectID)
	var project repositoryPayload
	if err := fetchObject(ctx, &counted, root, &project); err != nil {
		// Project resolution is not optional in the Python client: without its
		// returned numeric id and full name no security row can be scoped.
		return CompleteRouteBatch{}, err
	}
	parsedProjectID, err := project.ID.Int64()
	if err != nil || parsedProjectID < 1 || strconv.FormatInt(parsedProjectID, 10) != projectID {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	fullName := gitLabProjectFullName(project)
	repoID, err := repositoryIdentity(fullName)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)

	findings, findingPages, err := fetchGitLabSecurityObjects(
		ctx, &counted, root+"/vulnerability_findings", observer,
	)
	findingsOptional, fatal := gitLabSecurityEndpointDecision(ctx, err, observer)
	if fatal != nil {
		return CompleteRouteBatch{}, fatal
	}
	if err != nil {
		if !findingsOptional {
			return gitLabSecurityBatch(claim, fullName, parsedProjectID, normalizedAt, nil, requests, findingPages)
		}
		// The Python processor catches any optional security fetch exception and
		// continues with the alerts already accumulated by the helper. A failed
		// endpoint therefore contributes no rows but does not block the other
		// optional endpoint; control-plane cancellation/lease/rate-limit errors
		// were returned above.
		findings, findingPages = nil, 0
	}

	dependencies, dependencyPages, err := fetchGitLabSecurityObjects(
		ctx, &counted, root+"/dependencies", observer,
	)
	dependenciesOptional, fatal := gitLabSecurityEndpointDecision(ctx, err, observer)
	if fatal != nil {
		return CompleteRouteBatch{}, fatal
	}
	if err != nil {
		if !dependenciesOptional {
			return gitLabSecurityBatch(claim, fullName, parsedProjectID, normalizedAt, nil, requests, findingPages+dependencyPages)
		}
		dependencies, dependencyPages = nil, 0
	}

	// GitLabCodeClient.get_security_alerts slices the combined source list,
	// before processors.gitlab applies its since filter. Preserve that order.
	combined := make([]gitLabSecurityObject, 0, len(findings)+len(dependencies))
	combined = append(combined, findings...)
	combined = append(combined, dependencies...)
	if len(combined) > maxAlerts {
		combined = combined[:maxAlerts]
	}
	rows := make([]gitLabSecurityAlertRow, 0, len(combined))
	for _, object := range combined {
		row, include, normalizeErr := normalizeGitLabSecurityAlert(
			claim, repoID, object.Source, object.Payload, normalizedAt,
		)
		if normalizeErr != nil {
			return CompleteRouteBatch{}, normalizeErr
		}
		if !include {
			// The Python processor discards mapped alerts with a missing or
			// malformed created_at before constructing the persisted model.
			continue
		}
		if securityAlertOutsideWindow(row.CreatedAt, claim) {
			continue
		}
		rows = append(rows, row)
	}

	return gitLabSecurityBatch(claim, fullName, parsedProjectID, normalizedAt, rows, requests, findingPages+dependencyPages)
}

type gitLabSecurityObject struct {
	Source  string
	Payload map[string]any
}

func fetchGitLabSecurityObjects(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	path string,
	observer *gitLabSecurityResponseObserver,
) ([]gitLabSecurityObject, int, error) {
	// GitLabCodeClient calls the security endpoints directly with only
	// per_page; unlike the provider's generic paginator it does not add a
	// page=1 parameter. Preserve that request shape as well as its deliberate
	// one-page/no-X-Next-Page traversal boundary.
	query := url.Values{"per_page": {strconv.Itoa(gitLabSecurityPerPage)}}
	response, err := client.Do(ctx, http.MethodGet, path+"?"+query.Encode(), nil)
	if err != nil {
		return nil, 0, err
	}
	if response == nil || response.Body == nil {
		return nil, 0, providerfoundation.ErrNormalizationInvalid
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return nil, 0, err
	}
	var rawItems []json.RawMessage
	if err := json.Unmarshal(body, &rawItems); err != nil {
		return nil, 0, providerfoundation.ErrNormalizationInvalid
	}
	objects := make([]gitLabSecurityObject, 0, len(rawItems))
	source := "gitlab_vulnerability"
	if strings.HasSuffix(path, "/dependencies") {
		source = "gitlab_dependency"
	}
	for _, raw := range rawItems {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var payload map[string]any
		if err := decoder.Decode(&payload); err != nil || payload == nil {
			return nil, 1, providerfoundation.ErrNormalizationInvalid
		}
		if source == "gitlab_dependency" {
			for _, rawVulnerability := range gitLabSecurityList(payload["vulnerabilities"]) {
				vulnerability, ok := rawVulnerability.(map[string]any)
				if !ok {
					return nil, 1, providerfoundation.ErrNormalizationInvalid
				}
				// _map_dependency_alert receives both the dependency package
				// and its vulnerability. Flatten only at the Go normalization
				// boundary; the raw request shape remains unchanged.
				flattened := cloneGitLabSecurityMap(vulnerability)
				flattened["package_name"] = payload["name"]
				objects = append(objects, gitLabSecurityObject{Source: source, Payload: flattened})
			}
			continue
		}
		objects = append(objects, gitLabSecurityObject{Source: source, Payload: payload})
	}
	return objects, 1, nil
}

func gitLabSecurityEndpointDecision(
	ctx context.Context,
	err error,
	observer *gitLabSecurityResponseObserver,
) (optional bool, fatal error) {
	if err == nil {
		return true, nil
	}
	if ctx != nil && ctx.Err() != nil {
		return false, ctx.Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, providerfoundation.ErrLeaseLost) || errors.Is(err, ErrLeaseLost) {
		return false, err
	}
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) {
		if providerErr.Class == providerfoundation.ErrorRateLimited ||
			(providerErr.StatusCode == http.StatusForbidden && gitLabSecurityRateLimited(observer.lastHeaders)) {
			return false, err
		}
		if providerErr.StatusCode == http.StatusForbidden || providerErr.StatusCode == http.StatusNotFound {
			return true, nil
		}
	}
	// A non-optional exception aborts get_security_alerts before its second
	// endpoint, and the processor catches the whole call as an empty result.
	// Return false so the route emits the same empty successful batch without
	// manufacturing partial rows from the endpoint fetched first.
	return false, nil
}

func gitLabSecurityBatch(
	claim Claim,
	fullName string,
	projectID int64,
	normalizedAt time.Time,
	rows []gitLabSecurityAlertRow,
	requests int,
	pages int,
) (CompleteRouteBatch, error) {
	effect, err := effectBatchFromValues("security_alerts", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects:   []EffectBatch{effect},
		Result:    map[string]any{"security_alerts_synced": len(rows), "repo": fullName, "project_id": projectID},
		Watermark: claim.BeforeAt,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: requests, Pages: pages, Records: len(rows),
		},
	}, nil
}

func gitLabSecurityRateLimited(headers http.Header) bool {
	if headers == nil {
		return false
	}
	return strings.TrimSpace(gitLabSecurityHeader(headers, "Retry-After")) != "" ||
		strings.TrimSpace(gitLabSecurityHeader(headers, "RateLimit-Remaining")) == "0"
}

func gitLabSecurityHeader(headers http.Header, name string) string {
	for key, values := range headers {
		if strings.EqualFold(key, name) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func normalizeGitLabSecurityAlert(
	claim Claim,
	repoID string,
	source string,
	payload map[string]any,
	normalizedAt time.Time,
) (gitLabSecurityAlertRow, bool, error) {
	row := gitLabSecurityAlertRow{
		OrgID: claim.OrgID, RepoID: repoID, Source: source, LastSynced: normalizedAt,
	}
	switch source {
	case "gitlab_vulnerability":
		id := strings.TrimSpace(stringValue(payload["id"]))
		if id == "" {
			return gitLabSecurityAlertRow{}, false, providerfoundation.ErrNormalizationInvalid
		}
		row.AlertID = "gitlab_vuln:" + id
		row.Severity = gitLabSecurityOptionalString(payload["severity"])
		row.State = gitLabSecurityOptionalString(payload["state"])
		row.Title = gitLabSecurityOptionalString(payload["name"])
		if links, ok := payload["links"].(map[string]any); ok {
			row.URL = gitLabSecurityOptionalString(links["url"])
		}
		for _, identifier := range gitLabSecurityList(payload["identifiers"]) {
			item, ok := identifier.(map[string]any)
			if ok && stringValue(item["type"]) == "cve" {
				row.CVEID = gitLabSecurityOptionalString(item["name"])
				break
			}
		}
		createdAt := parseGitLabSecurityTime(payload["created_at"])
		if createdAt == nil {
			return row, false, nil
		}
		row.CreatedAt = createdAt.UTC().Truncate(time.Millisecond)
	case "gitlab_dependency":
		id := strings.TrimSpace(stringValue(payload["id"]))
		if id == "" {
			return gitLabSecurityAlertRow{}, false, providerfoundation.ErrNormalizationInvalid
		}
		row.AlertID = "gitlab_dep:" + id
		row.Severity = gitLabSecurityOptionalString(payload["severity"])
		row.PackageName = gitLabSecurityOptionalString(payload["package_name"])
		row.URL = gitLabSecurityOptionalString(payload["url"])
		row.Title = gitLabSecurityOptionalString(payload["name"])
		// The dependency API has no vulnerability timestamp. Python uses
		// datetime.now(timezone.utc); normalizedAt is the route's stable,
		// executor-supplied equivalent so retries converge.
		row.CreatedAt = normalizedAt
	default:
		return gitLabSecurityAlertRow{}, false, providerfoundation.ErrNormalizationInvalid
	}
	if err := row.validate(claim); err != nil {
		return gitLabSecurityAlertRow{}, false, err
	}
	return row, true, nil
}

func gitLabSecurityList(value any) []any {
	items, _ := value.([]any)
	return items
}

func cloneGitLabSecurityMap(input map[string]any) map[string]any {
	cloned := make(map[string]any, len(input)+1)
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

func parseGitLabSecurityTime(value any) *time.Time {
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.Replace(text, "Z", "+00:00", 1))
	if err != nil {
		return nil
	}
	parsed = parsed.UTC()
	return &parsed
}

func gitLabSecurityOptionalString(value any) *string {
	text := strings.TrimSpace(stringValue(value))
	if text == "" {
		return nil
	}
	return &text
}

func (row gitLabSecurityAlertRow) validate(claim Claim) error {
	if row.OrgID == "" || row.OrgID != claim.OrgID || row.AlertID == "" ||
		(row.Source != "gitlab_vulnerability" && row.Source != "gitlab_dependency") ||
		row.CreatedAt.IsZero() || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	if _, err := uuid.Parse(row.RepoID); err != nil {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

var _ CompleteRouteHandler = GitLabSecurityRouteHandler{}
