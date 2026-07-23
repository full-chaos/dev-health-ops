package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	nativeMaxPages        = 100
	nativePerPage         = 100
	nativeMaxCommentsPage = 5
	nativeMaxHistoryPage  = 3
	nativeMaxObjectBytes  = 2 << 20
)

type FetchEvidence struct {
	Provider   string
	Dataset    string
	Requests   int
	Pages      int
	Records    int
	CapReached bool
}

type FetchResult struct {
	Envelopes []providerfoundation.NormalizedEnvelope
	Evidence  FetchEvidence
	Watermark *time.Time
}

type DatasetHandler interface {
	Fetch(context.Context, Claim, *providerfoundation.HTTPClient) (FetchResult, error)
}

// NativeRESTHandler owns the GitHub/GitLab reference and work-item REST
// surfaces. Other datasets remain on their named Python compatibility
// adapters until their semantic sinks have independent parity evidence.
type NativeRESTHandler struct{ Now func() time.Time }

func (handler NativeRESTHandler) now() time.Time {
	if handler.Now != nil {
		return handler.Now().UTC()
	}
	return time.Now().UTC()
}

func (handler NativeRESTHandler) Fetch(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
) (FetchResult, error) {
	if ctx == nil || client == nil || claim.Validate() != nil || client.Provider != claim.Provider {
		return FetchResult{}, ErrInvalidConfiguration
	}
	if !isNativeReferenceWorkItemDataset(claim.Dataset) {
		return FetchResult{}, ErrCompatibilityRequired
	}
	switch claim.Provider {
	case "github":
		return handler.fetchGitHub(ctx, claim, client)
	case "gitlab":
		return handler.fetchGitLab(ctx, claim, client)
	default:
		return FetchResult{}, ErrInvalidConfiguration
	}
}

func isNativeReferenceWorkItemDataset(dataset string) bool {
	switch dataset {
	case "repo-metadata", "work-items", "work-item-labels", "work-item-projects",
		"work-item-history", "work-item-comments":
		return true
	default:
		return false
	}
}

func (handler NativeRESTHandler) fetchGitHub(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
) (FetchResult, error) {
	owner, repository, err := splitGitHubRepository(claim.SourceExternalID)
	if err != nil {
		return FetchResult{}, err
	}
	root := "/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(repository)
	switch claim.Dataset {
	case "repo-metadata":
		var payload repositoryPayload
		if err := fetchObject(ctx, client, root, &payload); err != nil {
			return FetchResult{}, err
		}
		envelope, err := handler.normalizeRepository(claim, "github:repo:"+claim.SourceExternalID, payload)
		return singleFetchResult(claim, envelope, err)
	case "work-items":
		items, evidence, err := collectGitHubWorkItems(ctx, claim, client, root)
		if err != nil {
			return FetchResult{}, err
		}
		envelopes, err := handler.normalizeGitHubWorkItems(claim, items)
		return resultWithEvidence(claim, envelopes, evidence, err)
	case "work-item-labels":
		page, err := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
			Path: root + "/labels", Query: url.Values{"per_page": {"100"}},
			MaxPages: nativeMaxPages,
		})
		envelopes, normalizeErr := handler.normalizeNamedRecords(claim, "work_item_label", "label", page.Items)
		return pageFetchResult(claim, envelopes, page, err, normalizeErr)
	case "work-item-projects":
		page, err := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
			Path: root + "/milestones", Query: url.Values{"state": {"all"}, "per_page": {"100"}},
			MaxPages: nativeMaxPages,
		})
		envelopes, normalizeErr := handler.normalizeProjects(claim, page.Items)
		return pageFetchResult(claim, envelopes, page, err, normalizeErr)
	case "work-item-history", "work-item-comments":
		return handler.fetchGitHubChildren(ctx, claim, client, root)
	default:
		return FetchResult{}, ErrCompatibilityRequired
	}
}

func (handler NativeRESTHandler) fetchGitLab(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
) (FetchResult, error) {
	projectID, err := gitLabProjectID(claim.SourceExternalID)
	if err != nil {
		return FetchResult{}, err
	}
	root := "/api/v4/projects/" + projectID
	switch claim.Dataset {
	case "repo-metadata":
		var payload repositoryPayload
		if err := fetchObject(ctx, client, root, &payload); err != nil {
			return FetchResult{}, err
		}
		sourceID := "gitlab:project:" + projectID
		envelope, err := handler.normalizeRepository(claim, sourceID, payload)
		return singleFetchResult(claim, envelope, err)
	case "work-items":
		items, evidence, err := collectGitLabWorkItems(ctx, claim, client, root)
		if err != nil {
			return FetchResult{}, err
		}
		envelopes, err := handler.normalizeGitLabWorkItems(claim, items)
		return resultWithEvidence(claim, envelopes, evidence, err)
	case "work-item-labels":
		page, err := providerfoundation.CollectGitLabPageParamPages(ctx, client, providerfoundation.GitLabPageOptions{
			Path: root + "/labels", PerPage: nativePerPage, MaxPages: nativeMaxPages,
		})
		envelopes, normalizeErr := handler.normalizeNamedRecords(claim, "work_item_label", "label", page.Items)
		return pageFetchResult(claim, envelopes, page, err, normalizeErr)
	case "work-item-projects":
		page, err := providerfoundation.CollectGitLabPageParamPages(ctx, client, providerfoundation.GitLabPageOptions{
			Path: root + "/milestones", Query: url.Values{"state": {"all"}},
			PerPage: nativePerPage, MaxPages: nativeMaxPages,
		})
		envelopes, normalizeErr := handler.normalizeProjects(claim, page.Items)
		return pageFetchResult(claim, envelopes, page, err, normalizeErr)
	case "work-item-history", "work-item-comments":
		return handler.fetchGitLabChildren(ctx, claim, client, root)
	default:
		return FetchResult{}, ErrCompatibilityRequired
	}
}

type repositoryPayload struct {
	ID            json.Number `json:"id"`
	Name          string      `json:"name"`
	FullName      string      `json:"full_name"`
	PathWithNS    string      `json:"path_with_namespace"`
	HTMLURL       string      `json:"html_url"`
	WebURL        string      `json:"web_url"`
	DefaultBranch string      `json:"default_branch"`
	Archived      bool        `json:"archived"`
	UpdatedAt     string      `json:"updated_at"`
	LastActivity  string      `json:"last_activity_at"`
}

func (handler NativeRESTHandler) normalizeRepository(
	claim Claim,
	sourceID string,
	payload repositoryPayload,
) (providerfoundation.NormalizedEnvelope, error) {
	name := payload.FullName
	if name == "" {
		name = payload.PathWithNS
	}
	if name == "" {
		name = claim.SourceName
	}
	observedAt := firstTime(payload.UpdatedAt, payload.LastActivity)
	if observedAt.IsZero() {
		observedAt = handler.now()
	}
	attributes := map[string]string{
		"name":           name,
		"default_branch": payload.DefaultBranch,
	}
	if payload.HTMLURL != "" {
		attributes["url"] = payload.HTMLURL
	} else if payload.WebURL != "" {
		attributes["url"] = payload.WebURL
	}
	return providerfoundation.NormalizeSourceRecord(normalizationContext(claim), providerfoundation.SourceRecord{
		Provider: claim.Provider, OrgID: claim.OrgID, EntityType: "repository",
		SourceID: sourceID, ObservedAt: observedAt, Attributes: attributes,
	})
}

type workItemPayload struct {
	IID         int             `json:"iid"`
	Number      int             `json:"number"`
	Title       string          `json:"title"`
	State       string          `json:"state"`
	CreatedAt   string          `json:"created_at"`
	UpdatedAt   string          `json:"updated_at"`
	ClosedAt    string          `json:"closed_at"`
	MergedAt    string          `json:"merged_at"`
	Draft       bool            `json:"draft"`
	Labels      json.RawMessage `json:"labels"`
	PullRequest json.RawMessage `json:"pull_request"`
}

func collectGitHubIssues(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	root string,
) ([]json.RawMessage, FetchEvidence, error) {
	query := url.Values{"state": {"all"}, "per_page": {"100"}}
	if claim.SinceAt != nil {
		query.Set("since", claim.SinceAt.UTC().Format(time.RFC3339))
	}
	page, err := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
		Path: root + "/issues", Query: query, MaxPages: nativeMaxPages,
	})
	evidence := FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Requests: page.Pages, Pages: page.Pages, CapReached: page.CapReached}
	if err != nil {
		return nil, evidence, err
	}
	items := make([]json.RawMessage, 0, len(page.Items))
	for _, raw := range page.Items {
		var item workItemPayload
		if json.Unmarshal(raw, &item) != nil {
			return nil, evidence, providerfoundation.ErrNormalizationInvalid
		}
		// GitHub's /issues endpoint includes pull requests. The production
		// GitHubWorkClient always removes them here and fetches /pulls only when
		// the frozen sync_prs processor flag is true.
		if len(item.PullRequest) != 0 && string(item.PullRequest) != "null" {
			continue
		}
		items = append(items, raw)
	}
	return filterWorkItemWindow(items, claim), evidence, nil
}

func collectGitHubWorkItems(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	root string,
) ([]json.RawMessage, FetchEvidence, error) {
	items, evidence, err := collectGitHubIssues(ctx, claim, client, root)
	if err != nil || !claim.ProcessorFlags["sync_prs"] {
		return items, evidence, err
	}
	query := url.Values{
		"state":     {"all"},
		"sort":      {"updated"},
		"direction": {"desc"},
		"per_page":  {"100"},
	}
	page, err := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
		Path: root + "/pulls", Query: query, MaxPages: nativeMaxPages,
	})
	evidence.Requests += page.Pages
	evidence.Pages += page.Pages
	evidence.CapReached = evidence.CapReached || page.CapReached
	if err != nil {
		return nil, evidence, err
	}
	items = append(items, filterWorkItemWindow(page.Items, claim)...)
	return items, evidence, nil
}

func collectGitLabWorkItems(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	root string,
) ([]json.RawMessage, FetchEvidence, error) {
	query := url.Values{"state": {"all"}}
	if claim.SinceAt != nil {
		query.Set("updated_after", claim.SinceAt.UTC().Format(time.RFC3339))
	}
	if claim.BeforeAt != nil {
		query.Set("updated_before", claim.BeforeAt.UTC().Format(time.RFC3339))
	}
	var items []json.RawMessage
	evidence := FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset}
	for _, path := range []string{root + "/issues", root + "/merge_requests"} {
		page, err := providerfoundation.CollectGitLabPageParamPages(ctx, client, providerfoundation.GitLabPageOptions{
			Path: path, Query: query, PerPage: nativePerPage, MaxPages: nativeMaxPages,
		})
		evidence.Requests += page.Pages
		evidence.Pages += page.Pages
		evidence.CapReached = evidence.CapReached || page.CapReached
		if err != nil {
			return nil, evidence, err
		}
		items = append(items, page.Items...)
	}
	return filterWorkItemWindow(items, claim), evidence, nil
}

func (handler NativeRESTHandler) normalizeGitHubWorkItems(
	claim Claim,
	items []json.RawMessage,
) ([]providerfoundation.NormalizedEnvelope, error) {
	envelopes := make([]providerfoundation.NormalizedEnvelope, 0, len(items))
	for _, raw := range items {
		var item workItemPayload
		if json.Unmarshal(raw, &item) != nil || item.Number < 1 || item.Title == "" {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		status, itemType := issueStatusAndType(item.State, item.Draft, labels(raw))
		statusRaw := item.State
		workItemPrefix := "gh:"
		if rawFieldPresent(raw, "merged_at") {
			workItemPrefix = "ghpr:"
			itemType = "pr"
			switch {
			case item.MergedAt != "" || item.State == "merged":
				status, statusRaw = "done", "merged"
			case item.State == "closed":
				status = "canceled"
			case item.Draft:
				status = "todo"
			default:
				status = "in_progress"
			}
		}
		projectID := claim.SourceExternalID
		envelope, err := providerfoundation.NormalizeWorkItem(normalizationContext(claim), providerfoundation.WorkItemRecord{
			Provider: claim.Provider, OrgID: claim.OrgID,
			WorkItemID: workItemPrefix + claim.SourceExternalID + "#" + strconv.Itoa(item.Number),
			Title:      item.Title, Type: itemType, Status: status, StatusRaw: &statusRaw,
			ProjectID: &projectID, UpdatedAt: firstTime(item.UpdatedAt, item.CreatedAt),
		})
		if err != nil {
			return nil, err
		}
		envelopes = append(envelopes, envelope)
	}
	return envelopes, nil
}

func (handler NativeRESTHandler) normalizeGitLabWorkItems(
	claim Claim,
	items []json.RawMessage,
) ([]providerfoundation.NormalizedEnvelope, error) {
	envelopes := make([]providerfoundation.NormalizedEnvelope, 0, len(items))
	for _, raw := range items {
		var item workItemPayload
		if json.Unmarshal(raw, &item) != nil || item.IID < 1 || item.Title == "" {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		isMR := rawFieldPresent(raw, "merged_at")
		status, itemType := issueStatusAndType(item.State, item.Draft, labels(raw))
		separator := "#"
		if isMR {
			separator = "!"
			itemType = "merge_request"
			switch {
			case item.MergedAt != "" || item.State == "merged":
				status = "done"
			case item.State == "closed":
				status = "canceled"
			default:
				status = "in_progress"
			}
		}
		statusRaw := item.State
		projectID := claim.SourceName
		if projectID == "" {
			projectID = claim.SourceExternalID
		}
		envelope, err := providerfoundation.NormalizeWorkItem(normalizationContext(claim), providerfoundation.WorkItemRecord{
			Provider: claim.Provider, OrgID: claim.OrgID,
			WorkItemID: "gitlab:" + projectID + separator + strconv.Itoa(item.IID),
			Title:      item.Title, Type: itemType, Status: status, StatusRaw: &statusRaw,
			ProjectID: &projectID, UpdatedAt: firstTime(item.UpdatedAt, item.CreatedAt),
		})
		if err != nil {
			return nil, err
		}
		envelopes = append(envelopes, envelope)
	}
	return envelopes, nil
}

func (handler NativeRESTHandler) normalizeNamedRecords(
	claim Claim,
	entityType string,
	identityPrefix string,
	items []json.RawMessage,
) ([]providerfoundation.NormalizedEnvelope, error) {
	envelopes := make([]providerfoundation.NormalizedEnvelope, 0, len(items))
	for _, raw := range items {
		var item struct {
			ID        json.Number `json:"id"`
			Name      string      `json:"name"`
			Color     string      `json:"color"`
			UpdatedAt string      `json:"updated_at"`
		}
		if json.Unmarshal(raw, &item) != nil || item.Name == "" {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		observedAt := firstTime(item.UpdatedAt)
		if observedAt.IsZero() {
			observedAt = handler.now()
		}
		sourceID := strings.Join([]string{claim.Provider, claim.SourceExternalID, identityPrefix, item.Name}, ":")
		envelope, err := providerfoundation.NormalizeSourceRecord(normalizationContext(claim), providerfoundation.SourceRecord{
			Provider: claim.Provider, OrgID: claim.OrgID, EntityType: entityType,
			SourceID: sourceID, ObservedAt: observedAt,
			Attributes: map[string]string{"name": item.Name, "color": item.Color},
		})
		if err != nil {
			return nil, err
		}
		envelopes = append(envelopes, envelope)
	}
	return envelopes, nil
}

func (handler NativeRESTHandler) normalizeProjects(
	claim Claim,
	items []json.RawMessage,
) ([]providerfoundation.NormalizedEnvelope, error) {
	envelopes := make([]providerfoundation.NormalizedEnvelope, 0, len(items))
	for _, raw := range items {
		var item struct {
			ID        json.Number `json:"id"`
			Number    int         `json:"number"`
			IID       int         `json:"iid"`
			Title     string      `json:"title"`
			State     string      `json:"state"`
			UpdatedAt string      `json:"updated_at"`
			DueOn     string      `json:"due_on"`
			DueDate   string      `json:"due_date"`
		}
		if json.Unmarshal(raw, &item) != nil || item.Title == "" {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		id := item.ID.String()
		if id == "" {
			id = strconv.Itoa(maxInt(item.Number, item.IID))
		}
		if id == "" || id == "0" {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		observedAt := firstTime(item.UpdatedAt)
		if observedAt.IsZero() {
			observedAt = handler.now()
		}
		due := item.DueOn
		if due == "" {
			due = item.DueDate
		}
		envelope, err := providerfoundation.NormalizeSourceRecord(normalizationContext(claim), providerfoundation.SourceRecord{
			Provider: claim.Provider, OrgID: claim.OrgID, EntityType: "work_item_project",
			SourceID:   strings.Join([]string{claim.Provider, claim.SourceExternalID, "project", id}, ":"),
			ObservedAt: observedAt,
			Attributes: map[string]string{"title": item.Title, "state": item.State, "due": due},
		})
		if err != nil {
			return nil, err
		}
		envelopes = append(envelopes, envelope)
	}
	return envelopes, nil
}

func (handler NativeRESTHandler) fetchGitHubChildren(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	root string,
) (FetchResult, error) {
	parents, evidence, err := collectGitHubWorkItems(ctx, claim, client, root)
	if err != nil {
		return FetchResult{}, err
	}
	var envelopes []providerfoundation.NormalizedEnvelope
	for _, parent := range parents {
		var item workItemPayload
		if json.Unmarshal(parent, &item) != nil || item.Number < 1 {
			return FetchResult{}, providerfoundation.ErrNormalizationInvalid
		}
		suffix, entityType, maxPages := "/events", "work_item_transition", nativeMaxHistoryPage
		if claim.Dataset == "work-item-comments" {
			suffix, entityType, maxPages = "/comments", "work_item_comment", nativeMaxCommentsPage
		}
		page, pageErr := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
			Path:  root + "/issues/" + strconv.Itoa(item.Number) + suffix,
			Query: url.Values{"per_page": {"100"}}, MaxPages: maxPages,
		})
		evidence.Requests += page.Pages
		evidence.Pages += page.Pages
		evidence.CapReached = evidence.CapReached || page.CapReached
		if pageErr != nil {
			return FetchResult{}, pageErr
		}
		child, normalizeErr := handler.normalizeChildren(claim, entityType, strconv.Itoa(item.Number), page.Items)
		if normalizeErr != nil {
			return FetchResult{}, normalizeErr
		}
		envelopes = append(envelopes, child...)
	}
	return resultWithEvidence(claim, envelopes, evidence, nil)
}

func (handler NativeRESTHandler) fetchGitLabChildren(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	root string,
) (FetchResult, error) {
	parents, evidence, err := collectGitLabWorkItems(ctx, claim, client, root)
	if err != nil {
		return FetchResult{}, err
	}
	var envelopes []providerfoundation.NormalizedEnvelope
	for _, parent := range parents {
		var item workItemPayload
		if json.Unmarshal(parent, &item) != nil || item.IID < 1 {
			return FetchResult{}, providerfoundation.ErrNormalizationInvalid
		}
		resource := "issues"
		if rawFieldPresent(parent, "merged_at") {
			resource = "merge_requests"
		}
		var paths []string
		entityType, maxPages := "work_item_transition", nativeMaxHistoryPage
		if claim.Dataset == "work-item-comments" {
			entityType, maxPages = "work_item_comment", nativeMaxCommentsPage
			paths = []string{root + "/" + resource + "/" + strconv.Itoa(item.IID) + "/notes"}
		} else {
			base := root + "/" + resource + "/" + strconv.Itoa(item.IID)
			paths = []string{base + "/resource_state_events", base + "/resource_label_events"}
		}
		for _, path := range paths {
			page, pageErr := providerfoundation.CollectGitLabPageParamPages(ctx, client, providerfoundation.GitLabPageOptions{
				Path: path, PerPage: nativePerPage, MaxPages: maxPages,
			})
			evidence.Requests += page.Pages
			evidence.Pages += page.Pages
			evidence.CapReached = evidence.CapReached || page.CapReached
			if pageErr != nil {
				return FetchResult{}, pageErr
			}
			child, normalizeErr := handler.normalizeChildren(claim, entityType, strconv.Itoa(item.IID), page.Items)
			if normalizeErr != nil {
				return FetchResult{}, normalizeErr
			}
			envelopes = append(envelopes, child...)
		}
	}
	return resultWithEvidence(claim, envelopes, evidence, nil)
}

func (handler NativeRESTHandler) normalizeChildren(
	claim Claim,
	entityType string,
	parentID string,
	items []json.RawMessage,
) ([]providerfoundation.NormalizedEnvelope, error) {
	envelopes := make([]providerfoundation.NormalizedEnvelope, 0, len(items))
	for index, raw := range items {
		var item struct {
			ID        json.Number     `json:"id"`
			Event     string          `json:"event"`
			State     string          `json:"state"`
			Action    string          `json:"action"`
			Body      string          `json:"body"`
			System    bool            `json:"system"`
			CreatedAt string          `json:"created_at"`
			UpdatedAt string          `json:"updated_at"`
			Label     json.RawMessage `json:"label"`
		}
		if json.Unmarshal(raw, &item) != nil {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		if entityType == "work_item_comment" && item.System {
			continue
		}
		observedAt := firstTime(item.UpdatedAt, item.CreatedAt)
		if observedAt.IsZero() {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		id := item.ID.String()
		if id == "" {
			id = strconv.Itoa(index) + ":" + observedAt.Format(time.RFC3339Nano)
		}
		attributes := map[string]string{
			"work_item_id": parentID,
			"event":        firstText(item.Event, item.State, item.Action),
		}
		if entityType == "work_item_comment" {
			attributes["body_length"] = strconv.Itoa(len(item.Body))
		}
		envelope, err := providerfoundation.NormalizeSourceRecord(normalizationContext(claim), providerfoundation.SourceRecord{
			Provider: claim.Provider, OrgID: claim.OrgID, EntityType: entityType,
			SourceID:   strings.Join([]string{claim.Provider, claim.SourceExternalID, parentID, entityType, id}, ":"),
			ObservedAt: observedAt, Attributes: attributes,
		})
		if err != nil {
			return nil, err
		}
		envelopes = append(envelopes, envelope)
	}
	return envelopes, nil
}

func fetchObject(ctx context.Context, client *providerfoundation.HTTPClient, path string, target any) error {
	response, err := client.Do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(response.Body, nativeMaxObjectBytes+1))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return providerfoundation.ErrNormalizationInvalid
	}
	return nil
}

func filterWorkItemWindow(items []json.RawMessage, claim Claim) []json.RawMessage {
	filtered := make([]json.RawMessage, 0, len(items))
	for _, raw := range items {
		var item workItemPayload
		if json.Unmarshal(raw, &item) != nil {
			continue
		}
		updatedAt := firstTime(item.UpdatedAt, item.CreatedAt)
		if updatedAt.IsZero() ||
			(claim.SinceAt != nil && updatedAt.Before(claim.SinceAt.UTC())) ||
			(claim.BeforeAt != nil && updatedAt.After(claim.BeforeAt.UTC())) {
			continue
		}
		filtered = append(filtered, raw)
	}
	return filtered
}

func normalizationContext(claim Claim) providerfoundation.NormalizationContext {
	return providerfoundation.NormalizationContext{
		IntegrationID: claim.IntegrationID,
		Provenance: providerfoundation.Provenance{
			Source: claim.Provider + "_rest", Confidence: "1.0",
			EvidenceID: claim.Provider + ":" + claim.Dataset + ":" + claim.ID,
		},
	}
}

func issueStatusAndType(state string, draft bool, labels []string) (string, string) {
	status := "todo"
	if state == "closed" {
		status = "done"
	} else if draft {
		status = "todo"
	} else {
		for _, label := range labels {
			switch strings.ToLower(strings.TrimSpace(label)) {
			case "blocked":
				status = "blocked"
			case "in review", "review":
				status = "in_review"
			case "in progress", "in-progress", "doing":
				status = "in_progress"
			case "backlog":
				status = "backlog"
			}
		}
	}
	itemType := "issue"
	for _, label := range labels {
		switch strings.ToLower(strings.TrimSpace(label)) {
		case "bug", "type: bug":
			itemType = "bug"
		case "story", "feature", "type: story":
			itemType = "story"
		case "chore", "type: chore":
			itemType = "chore"
		case "incident", "type: incident":
			itemType = "incident"
		}
	}
	return status, itemType
}

func labels(raw json.RawMessage) []string {
	var direct struct {
		Labels []json.RawMessage `json:"labels"`
	}
	if json.Unmarshal(raw, &direct) != nil {
		return nil
	}
	values := make([]string, 0, len(direct.Labels))
	for _, rawLabel := range direct.Labels {
		var name string
		if json.Unmarshal(rawLabel, &name) == nil && name != "" {
			values = append(values, name)
			continue
		}
		var object struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(rawLabel, &object) == nil && object.Name != "" {
			values = append(values, object.Name)
		}
	}
	return values
}

func rawFieldPresent(raw json.RawMessage, field string) bool {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		return false
	}
	_, ok := object[field]
	return ok
}

func splitGitHubRepository(value string) (string, string, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", ErrInvalidConfiguration
	}
	return parts[0], parts[1], nil
}

func gitLabProjectID(value string) (string, error) {
	id, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || id < 1 {
		return "", ErrInvalidConfiguration
	}
	return strconv.FormatInt(id, 10), nil
}

func firstTime(values ...string) time.Time {
	for _, value := range values {
		if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return parsed.UTC()
		}
	}
	return time.Time{}
}

func firstText(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func singleFetchResult(
	claim Claim,
	envelope providerfoundation.NormalizedEnvelope,
	err error,
) (FetchResult, error) {
	return resultWithEvidence(claim, []providerfoundation.NormalizedEnvelope{envelope}, FetchEvidence{
		Provider: claim.Provider, Dataset: claim.Dataset, Requests: 1, Pages: 1,
	}, err)
}

func pageFetchResult(
	claim Claim,
	envelopes []providerfoundation.NormalizedEnvelope,
	page providerfoundation.PageCollection,
	requestErr error,
	normalizeErr error,
) (FetchResult, error) {
	if requestErr != nil {
		return FetchResult{}, requestErr
	}
	return resultWithEvidence(claim, envelopes, FetchEvidence{
		Provider: claim.Provider, Dataset: claim.Dataset, Requests: page.Pages,
		Pages: page.Pages, CapReached: page.CapReached,
	}, normalizeErr)
}

func resultWithEvidence(
	claim Claim,
	envelopes []providerfoundation.NormalizedEnvelope,
	evidence FetchEvidence,
	err error,
) (FetchResult, error) {
	if err != nil {
		return FetchResult{}, err
	}
	evidence.Records = len(envelopes)
	var watermark *time.Time
	for _, envelope := range envelopes {
		if watermark == nil || envelope.ObservedAt.After(*watermark) {
			value := envelope.ObservedAt
			watermark = &value
		}
	}
	return FetchResult{Envelopes: envelopes, Evidence: evidence, Watermark: watermark}, nil
}

var _ DatasetHandler = NativeRESTHandler{}
