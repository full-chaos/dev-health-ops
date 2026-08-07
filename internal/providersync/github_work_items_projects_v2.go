package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const gitHubProjectsV2IntegrationConfigKey = "github_projects_v2"

// GitHubProjectV2Target is durable, non-secret integration configuration.
//
// RATIFIED (D18, cutover Decision Log): targets are durable integration-scoped
// configuration read from the claim, and collection uses the already
// claim-resolved Credential/HTTPClient. Environment target/token fallback is
// not part of the Go route.
//
// Two deliberate divergences from the active Python producer follow from that,
// and are divergences of record rather than porting defects:
//
//   - Python reads targets from process-global GITHUB_PROJECTS_V2
//     (metrics/work_items.py:433) and swallows every malformed entry with a
//     bare `except Exception: continue`. Go reads durable config and fails
//     closed, because a silent-skip grammar over an operator-visible config
//     column would hide a typo as "no projects configured".
//   - Python builds a SECOND client from process-global GITHUB_TOKEN
//     (metrics/work_items.py:403-409), ignoring both the resolved integration
//     credential and its base URL. On GitHub Enterprise Server that client
//     reaches github.com while this one honors the claim's base URL.
//
// Ratification is not activation: this collector still owns no registration,
// readiness, watermark, or alias.
type GitHubProjectV2Target struct {
	OrgLogin      string `json:"org_login"`
	ProjectNumber int    `json:"project_number"`
}

// GitHubProjectV2Usage is the credential-free actual request accounting for
// Projects v2. Python classifies unprefixed GraphQL work-item traffic under
// work_item_prs/graphql_cost; keeping that vocabulary lets the eventual D16
// composer join actuals to the provider budget without exposing query or
// target details.
type GitHubProjectV2Usage struct {
	Transport    string
	RouteFamily  string
	Dimension    string
	RequestCount int
}

// GitHubProjectV2FetchResult is a semantic, unregistered fetch result. It is
// intentionally not CompleteRouteBatch and owns no effects or watermark.
type GitHubProjectV2FetchResult struct {
	Rows     githubWorkItemRows
	Evidence FetchEvidence
	Usage    GitHubProjectV2Usage
	Targets  int
}

// GitHubProjectV2Fetcher preserves Python's per-source fanout: callers may
// invoke it once for each existing work-items claim, and each invocation
// fetches the claim's complete durable target list.
//
// That fanout is an AMPLIFICATION, not a mirror, and D18 accepts it only
// temporarily. Python calls parse_github_projects_v2_env() once per JOB —
// job_work_items.py sits it at the same indentation as
// `for discovered_repo in discovered_repos`, i.e. OUTSIDE the repo loop — and
// merges the result once at the end. The Go unit boundary is per-source, so
// every claim refetches the same org-wide projects. Collapsing that to
// one fetch per integration moves the D16 unit boundary and is therefore a
// separately tracked follow-up with its own oracle strategy; it must not be
// smuggled in here, and it must not gate the all-or-nothing alias activation.
type GitHubProjectV2Fetcher struct{}

func githubProjectV2Targets(claim Claim) ([]GitHubProjectV2Target, error) {
	value, configured := claim.IntegrationConfig[gitHubProjectsV2IntegrationConfigKey]
	if !configured || value == nil {
		return []GitHubProjectV2Target{}, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, ErrInvalidConfiguration
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var targets []GitHubProjectV2Target
	if err := decoder.Decode(&targets); err != nil || targets == nil {
		return nil, ErrInvalidConfiguration
	}
	for index := range targets {
		targets[index].OrgLogin = strings.TrimSpace(targets[index].OrgLogin)
		if targets[index].OrgLogin == "" || targets[index].ProjectNumber < 1 {
			return nil, ErrInvalidConfiguration
		}
	}
	return targets, nil
}

func (GitHubProjectV2Fetcher) Fetch(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
	resolveIdentity githubIdentityResolver,
) (GitHubProjectV2FetchResult, error) {
	// There is deliberately no `credential.ID == ""` clause. claim.Validate()
	// has already returned nil by the time this is evaluated, and Unit.Validate
	// refuses an empty CredentialID (lease.go), so an empty credential.ID is
	// necessarily unequal to the claim's and the equality clause below already
	// decides it. A clause that cannot fail on its own is not defence in depth;
	// it is an unkillable mutation that reads as coverage forever — measured as
	// exactly that before removal.
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		!isWorkItemFamilyDataset(claim.Dataset) || credential.Provider != "github" ||
		credential.ID != claim.CredentialID || client == nil ||
		client.Provider != "github" || client.BaseURL == nil || client.Lease == nil ||
		normalizedAt.IsZero() {
		return GitHubProjectV2FetchResult{}, ErrInvalidConfiguration
	}
	targets, err := githubProjectV2Targets(claim)
	if err != nil {
		return GitHubProjectV2FetchResult{}, err
	}
	result := GitHubProjectV2FetchResult{
		Rows: githubWorkItemRows{
			WorkItems: []githubWorkItemRow{}, StatusTransitions: []githubWorkItemTransitionRow{},
			Dependencies: []githubWorkItemDependencyRow{}, ReopenEvents: []githubWorkItemReopenRow{},
			Interactions: []githubWorkItemInteractionRow{}, Sprints: []githubSprintRow{},
			AIAttributions: []githubAIAttributionRow{},
		},
		Evidence: FetchEvidence{Provider: "github", Dataset: "projects-v2"},
		Usage: GitHubProjectV2Usage{
			Transport: "graphql", RouteFamily: "work_item_prs", Dimension: BudgetGraphQLCost,
		},
		Targets: len(targets),
	}
	if len(targets) == 0 {
		return result, nil
	}
	counted := *client
	counted.Doer = gitHubProjectV2CountingDoer{
		delegate: client.Doer, requests: &result.Evidence.Requests,
		graphqlRequests: &result.Usage.RequestCount, graphqlPath: gitHubGraphQLPath(client),
	}
	// Python's dict preserves first insertion position while later values win.
	// Preserve that exact behavior across targets, including duplicate targets.
	workItemIndex := map[string]int{}
	for _, target := range targets {
		items, err := fetchGitHubProjectV2Target(ctx, &counted, target, &result.Evidence)
		if err != nil {
			return finishGitHubProjectV2Fetch(result), err
		}
		projectScopeID := fmt.Sprintf("ghprojv2:%s#%d", target.OrgLogin, target.ProjectNumber)
		for _, item := range items {
			row, transitions, emitted, err := normalizeGitHubProjectV2Item(
				claim, item, projectScopeID, resolveIdentity, normalizedAt,
			)
			if err != nil {
				return finishGitHubProjectV2Fetch(result), err
			}
			if !emitted {
				continue
			}
			if index, exists := workItemIndex[row.WorkItemID]; exists {
				result.Rows.WorkItems[index] = row
			} else {
				workItemIndex[row.WorkItemID] = len(result.Rows.WorkItems)
				result.Rows.WorkItems = append(result.Rows.WorkItems, row)
			}
			result.Rows.StatusTransitions = append(result.Rows.StatusTransitions, transitions...)
		}
	}
	return finishGitHubProjectV2Fetch(result), nil
}

func finishGitHubProjectV2Fetch(result GitHubProjectV2FetchResult) GitHubProjectV2FetchResult {
	result.Evidence.Records = len(result.Rows.WorkItems) + len(result.Rows.StatusTransitions)
	return result
}

type gitHubProjectV2CountingDoer struct {
	delegate        providerfoundation.HTTPDoer
	requests        *int
	graphqlRequests *int
	graphqlPath     string
}

func (doer gitHubProjectV2CountingDoer) Do(request *http.Request) (*http.Response, error) {
	*doer.requests++
	if request.URL.EscapedPath() == doer.graphqlPath {
		*doer.graphqlRequests++
	}
	return doer.delegate.Do(request)
}

func fetchGitHubProjectV2Target(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	target GitHubProjectV2Target,
	evidence *FetchEvidence,
) ([]gitHubProjectV2ItemPayload, error) {
	items := []gitHubProjectV2ItemPayload{}
	outerCursor := ""
	seenOuter := map[string]struct{}{}
	for {
		variables := map[string]any{
			"login": target.OrgLogin, "number": target.ProjectNumber,
			"first": 50, "after": nil,
		}
		if outerCursor != "" {
			variables["after"] = outerCursor
		}
		var envelope gitHubProjectV2ItemsEnvelope
		if err := fetchGitHubProjectV2GraphQL(ctx, client, gitHubProjectsV2ItemsQuery, variables, &envelope, evidence); err != nil {
			return nil, err
		}
		if envelope.Data.Organization == nil || envelope.Data.Organization.ProjectV2 == nil {
			return items, nil
		}
		connection := envelope.Data.Organization.ProjectV2.Items
		for index := range connection.Nodes {
			item := connection.Nodes[index]
			if item.Changes.PageInfo.HasNextPage {
				cursor := strings.TrimSpace(item.Changes.PageInfo.EndCursor)
				if cursor == "" || strings.TrimSpace(item.ID) == "" {
					return nil, providerfoundation.ErrPaginationInvalid
				}
				seenChanges := map[string]struct{}{}
				for {
					if _, repeated := seenChanges[cursor]; repeated {
						return nil, providerfoundation.ErrPaginationInvalid
					}
					seenChanges[cursor] = struct{}{}
					var continuation gitHubProjectV2ChangesEnvelope
					if err := fetchGitHubProjectV2GraphQL(ctx, client, gitHubProjectsV2ChangesQuery,
						map[string]any{"itemId": item.ID, "after": cursor}, &continuation, evidence); err != nil {
						return nil, err
					}
					if continuation.Data.Node == nil {
						return nil, providerfoundation.ErrPaginationInvalid
					}
					more := continuation.Data.Node.Changes
					item.Changes.Nodes = append(item.Changes.Nodes, more.Nodes...)
					if !more.PageInfo.HasNextPage {
						break
					}
					next := strings.TrimSpace(more.PageInfo.EndCursor)
					if next == "" || next == cursor {
						return nil, providerfoundation.ErrPaginationInvalid
					}
					cursor = next
				}
			}
			items = append(items, item)
		}
		if !connection.PageInfo.HasNextPage {
			return items, nil
		}
		next := strings.TrimSpace(connection.PageInfo.EndCursor)
		if next == "" || next == outerCursor {
			return nil, providerfoundation.ErrPaginationInvalid
		}
		if _, repeated := seenOuter[next]; repeated {
			return nil, providerfoundation.ErrPaginationInvalid
		}
		seenOuter[next] = struct{}{}
		outerCursor = next
	}
}

func fetchGitHubProjectV2GraphQL(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	query string,
	variables map[string]any,
	destination any,
	evidence *FetchEvidence,
) error {
	body, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	response, err := client.Do(ctx, http.MethodPost, gitHubGraphQLPath(client), bytes.NewReader(body))
	if err != nil {
		return err
	}
	evidence.Pages++
	defer response.Body.Close()
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return providerfoundation.ErrPaginationInvalid
	}
	var errorsHolder interface{ graphQLErrors() []json.RawMessage }
	if typed, ok := destination.(interface{ graphQLErrors() []json.RawMessage }); ok {
		errorsHolder = typed
	}
	if errorsHolder != nil && len(errorsHolder.graphQLErrors()) > 0 {
		return providerfoundation.ErrGraphQLResponse
	}
	return nil
}

type gitHubProjectV2PageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

type gitHubProjectV2Connection[T any] struct {
	Nodes    []T                     `json:"nodes"`
	PageInfo gitHubProjectV2PageInfo `json:"pageInfo"`
}

type gitHubProjectV2ItemsEnvelope struct {
	Data struct {
		Organization *struct {
			ProjectV2 *struct {
				Items gitHubProjectV2Connection[gitHubProjectV2ItemPayload] `json:"items"`
			} `json:"projectV2"`
		} `json:"organization"`
	} `json:"data"`
	Errors []json.RawMessage `json:"errors"`
}

func (envelope *gitHubProjectV2ItemsEnvelope) graphQLErrors() []json.RawMessage {
	return envelope.Errors
}

type gitHubProjectV2ChangesEnvelope struct {
	Data struct {
		Node *struct {
			Changes gitHubProjectV2Connection[gitHubProjectV2ChangePayload] `json:"changes"`
		} `json:"node"`
	} `json:"data"`
	Errors []json.RawMessage `json:"errors"`
}

func (envelope *gitHubProjectV2ChangesEnvelope) graphQLErrors() []json.RawMessage {
	return envelope.Errors
}

type gitHubProjectV2ItemPayload struct {
	ID          string                                                      `json:"id"`
	CreatedAt   *string                                                     `json:"createdAt"`
	UpdatedAt   *string                                                     `json:"updatedAt"`
	Content     gitHubProjectV2ContentPayload                               `json:"content"`
	FieldValues gitHubProjectV2Connection[gitHubProjectV2FieldValuePayload] `json:"fieldValues"`
	Changes     gitHubProjectV2Connection[gitHubProjectV2ChangePayload]     `json:"changes"`
}

type gitHubProjectV2ContentPayload struct {
	Typename   string  `json:"__typename"`
	ID         string  `json:"id"`
	Number     int     `json:"number"`
	Title      string  `json:"title"`
	Body       *string `json:"body"`
	URL        *string `json:"url"`
	State      *string `json:"state"`
	CreatedAt  *string `json:"createdAt"`
	UpdatedAt  *string `json:"updatedAt"`
	ClosedAt   *string `json:"closedAt"`
	Repository struct {
		NameWithOwner string `json:"nameWithOwner"`
	} `json:"repository"`
	Labels    gitHubProjectV2Connection[githubWorkItemLabelPayload] `json:"labels"`
	Assignees gitHubProjectV2Connection[githubWorkItemUserPayload]  `json:"assignees"`
	Author    *githubWorkItemUserPayload                            `json:"author"`
}

type gitHubProjectV2FieldValuePayload struct {
	Typename string      `json:"__typename"`
	Name     *string     `json:"name"`
	Title    *string     `json:"title"`
	ID       *string     `json:"id"`
	Number   json.Number `json:"number"`
	Field    struct {
		Name string `json:"name"`
	} `json:"field"`
}

type gitHubProjectV2ChangePayload struct {
	Field struct {
		Name string `json:"name"`
	} `json:"field"`
	PreviousValue *struct {
		Name *string `json:"name"`
	} `json:"previousValue"`
	NewValue *struct {
		Name *string `json:"name"`
	} `json:"newValue"`
	CreatedAt *string `json:"createdAt"`
	Actor     *struct {
		Login *string `json:"login"`
	} `json:"actor"`
}

func normalizeGitHubProjectV2Item(
	claim Claim,
	item gitHubProjectV2ItemPayload,
	projectScopeID string,
	resolveIdentity githubIdentityResolver,
	normalizedAt time.Time,
) (githubWorkItemRow, []githubWorkItemTransitionRow, bool, error) {
	if claim.Validate() != nil || claim.Provider != "github" || !isWorkItemFamilyDataset(claim.Dataset) ||
		strings.TrimSpace(projectScopeID) == "" || normalizedAt.IsZero() {
		return githubWorkItemRow{}, nil, false, ErrInvalidConfiguration
	}
	if item.Content.Typename == "PullRequest" || (item.Content.Typename != "Issue" && item.Content.Typename != "DraftIssue") {
		return githubWorkItemRow{}, []githubWorkItemTransitionRow{}, false, nil
	}
	statusRaw, sprintName, sprintID := "", "", ""
	var storyPoints *float64
	for _, value := range item.FieldValues.Nodes {
		fieldName := normalizeWorkItemLabel(value.Field.Name)
		switch value.Typename {
		case "ProjectV2ItemFieldSingleSelectValue":
			if fieldName == "status" && value.Name != nil {
				statusRaw = *value.Name
			}
		case "ProjectV2ItemFieldIterationValue":
			if strings.Contains(fieldName, "iteration") || strings.Contains(fieldName, "sprint") {
				if value.Title != nil {
					sprintName = *value.Title
				}
				if value.ID != nil {
					sprintID = *value.ID
				}
			}
		case "ProjectV2ItemFieldNumberValue":
			if fieldName == "estimate" || fieldName == "points" || fieldName == "story points" || fieldName == "size" {
				if parsed, err := strconv.ParseFloat(string(value.Number), 64); err == nil {
					storyPoints = &parsed
				}
			}
		}
	}
	content := item.Content
	workItemID := "ghproj:" + item.ID
	if content.Typename == "Issue" && strings.TrimSpace(content.Repository.NameWithOwner) != "" && content.Number > 0 {
		workItemID = "gh:" + content.Repository.NameWithOwner + "#" + strconv.Itoa(content.Number)
	}
	transitions := make([]githubWorkItemTransitionRow, 0, len(item.Changes.Nodes))
	for _, change := range item.Changes.Nodes {
		fieldName := normalizeWorkItemLabel(change.Field.Name)
		if fieldName != "status" && fieldName != "phase" {
			continue
		}
		occurredAt := parseGitHubWorkItemTime(change.CreatedAt)
		if occurredAt == nil || change.NewValue == nil || change.NewValue.Name == nil || strings.TrimSpace(*change.NewValue.Name) == "" {
			continue
		}
		fromRaw, toRaw := "", strings.TrimSpace(*change.NewValue.Name)
		if change.PreviousValue != nil && change.PreviousValue.Name != nil {
			fromRaw = strings.TrimSpace(*change.PreviousValue.Name)
		}
		var actor *string
		if change.Actor != nil && change.Actor.Login != nil {
			identity := resolveGitHubWorkItemIdentity(githubWorkItemUserPayload{Login: change.Actor.Login}, resolveIdentity)
			if identity != "" && identity != "unknown" {
				actor = &identity
			}
		}
		transition := githubWorkItemTransitionRow{
			WorkItemID: workItemID, Provider: "github", OccurredAt: occurredAt.UTC(),
			FromStatusRaw: nullableString(fromRaw), ToStatusRaw: nullableString(toRaw),
			FromStatus: githubProjectV2Status(fromRaw, nil, ""),
			ToStatus:   githubProjectV2Status(toRaw, nil, ""), Actor: actor, OrgID: claim.OrgID,
			LastSynced: normalizedAt.UTC(),
		}
		if err := transition.validate(claim); err != nil {
			return githubWorkItemRow{}, nil, false, err
		}
		transitions = append(transitions, transition)
	}
	createdAt := parseGitHubWorkItemTime(content.CreatedAt)
	if createdAt == nil {
		createdAt = parseGitHubWorkItemTime(item.CreatedAt)
	}
	if createdAt == nil {
		fallback := normalizedAt.UTC()
		createdAt = &fallback
	}
	updatedAt := parseGitHubWorkItemTime(content.UpdatedAt)
	if updatedAt == nil {
		updatedAt = parseGitHubWorkItemTime(item.UpdatedAt)
	}
	if updatedAt == nil {
		copy := *createdAt
		updatedAt = &copy
	}
	closedAt := parseGitHubWorkItemTime(content.ClosedAt)
	labels := make([]string, 0, len(content.Labels.Nodes))
	for _, label := range content.Labels.Nodes {
		if label.Name != "" {
			labels = append(labels, label.Name)
		}
	}
	assignees := make([]string, 0, len(content.Assignees.Nodes))
	for _, user := range content.Assignees.Nodes {
		identity := resolveGitHubWorkItemIdentity(user, resolveIdentity)
		if identity != "" && identity != "unknown" {
			assignees = append(assignees, identity)
		}
	}
	var reporter *string
	if content.Author != nil {
		identity := resolveGitHubWorkItemIdentity(*content.Author, resolveIdentity)
		if identity != "" && identity != "unknown" {
			reporter = &identity
		}
	}
	state := ""
	if content.State != nil {
		state = *content.State
	}
	status := githubProjectV2Status(statusRaw, labels, state)
	row := githubWorkItemRow{
		WorkItemID: workItemID, Provider: "github", Title: content.Title, Type: "issue",
		Status: status, StatusRaw: nullableString(statusRaw), Description: content.Body,
		RepoID: nil, ProjectID: stringPointer(projectScopeID), Assignees: assignees,
		Reporter: reporter, CreatedAt: createdAt.UTC(), UpdatedAt: updatedAt.UTC(),
		ClosedAt: closedAt, Labels: labels, StoryPoints: storyPoints, URL: content.URL,
		OrgID: claim.OrgID, LastSynced: normalizedAt.UTC(),
	}
	if statusRaw == "" {
		row.StatusRaw = nullableString(state)
	}
	if sprintID != "" {
		row.SprintID = &sprintID
	}
	if sprintName != "" {
		row.SprintName = &sprintName
	}
	if closedAt != nil {
		completed := closedAt.UTC()
		row.CompletedAt = &completed
	}
	if content.Typename == "Issue" {
		row.Type = githubIssueType(labels)
	}
	if row.Description != nil && *row.Description == "" {
		row.Description = nil
	}
	if err := validateGitHubProjectV2Row(claim, row); err != nil {
		return githubWorkItemRow{}, nil, false, err
	}
	return row, transitions, true, nil
}

func githubProjectV2Status(statusRaw string, labels []string, state string) string {
	if statusRaw != "" {
		switch normalizeWorkItemLabel(statusRaw) {
		case "backlog", "icebox", "triage":
			return "backlog"
		case "todo", "to do", "ready", "ready for dev":
			return "todo"
		case "in progress", "doing", "wip":
			return "in_progress"
		case "in review", "review", "code review", "qa":
			return "in_review"
		case "blocked", "on hold":
			return "blocked"
		case "done", "closed", "resolved":
			return "done"
		case "canceled", "cancelled", "won't do":
			return "canceled"
		}
	}
	return githubIssueStatus(state, labels)
}

func validateGitHubProjectV2Row(claim Claim, row githubWorkItemRow) error {
	if row.Provider != "github" || row.OrgID == "" || row.OrgID != claim.OrgID ||
		row.WorkItemID == "" || row.RepoID != nil || row.ProjectID == nil ||
		!strings.HasPrefix(*row.ProjectID, "ghprojv2:") || row.Type == "" || row.Status == "" ||
		row.CreatedAt.IsZero() || row.UpdatedAt.IsZero() || row.Assignees == nil || row.Labels == nil {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func mergeGitHubProjectV2Rows(repository, projects githubWorkItemRows) githubWorkItemRows {
	merged := repository
	merged.WorkItems = append([]githubWorkItemRow{}, repository.WorkItems...)
	index := make(map[string]int, len(merged.WorkItems)+len(projects.WorkItems))
	for position, row := range merged.WorkItems {
		index[row.WorkItemID] = position
	}
	for _, row := range projects.WorkItems {
		if position, exists := index[row.WorkItemID]; exists {
			merged.WorkItems[position] = row
		} else {
			index[row.WorkItemID] = len(merged.WorkItems)
			merged.WorkItems = append(merged.WorkItems, row)
		}
	}
	merged.StatusTransitions = append(append([]githubWorkItemTransitionRow{}, repository.StatusTransitions...), projects.StatusTransitions...)
	return merged
}

// These literals intentionally preserve Python's documented leaf
// truncations (labels 50, assignees 10, fieldValues 20). The outer items and
// nested changes connections are fully paginated by the fetcher.
const gitHubProjectsV2ItemsQuery = `
query($login: String!, $number: Int!, $after: String, $first: Int!) {
  organization(login: $login) {
    projectV2(number: $number) {
      items(first: $first, after: $after) {
        nodes {
          id createdAt updatedAt
          content {
            __typename
            ... on Issue { id number title url state createdAt updatedAt closedAt repository { nameWithOwner } labels(first: 50) { nodes { name } } assignees(first: 10) { nodes { login email name } } author { login email name } }
            ... on PullRequest { id number title url state createdAt updatedAt closedAt mergedAt repository { nameWithOwner } labels(first: 50) { nodes { name } } assignees(first: 10) { nodes { login email name } } author { login email name } }
            ... on DraftIssue { id title createdAt updatedAt }
          }
          fieldValues(first: 20) { nodes {
            __typename
            ... on ProjectV2ItemFieldSingleSelectValue { name field { ... on ProjectV2SingleSelectField { name } } }
            ... on ProjectV2ItemFieldTextValue { text field { ... on ProjectV2FieldCommon { name } } }
            ... on ProjectV2ItemFieldIterationValue { title id field { ... on ProjectV2FieldCommon { name } } }
            ... on ProjectV2ItemFieldNumberValue { number field { ... on ProjectV2FieldCommon { name } } }
          } }
          changes(first: 100, orderBy: {field: CREATED_AT, direction: ASC}) { nodes { field { ... on ProjectV2FieldCommon { name } } previousValue { ... on ProjectV2ItemFieldSingleSelectValue { name } } newValue { ... on ProjectV2ItemFieldSingleSelectValue { name } } createdAt actor { login } } pageInfo { hasNextPage endCursor } }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`

const gitHubProjectsV2ChangesQuery = `
query($itemId: ID!, $after: String) {
  node(id: $itemId) {
    ... on ProjectV2Item {
      changes(first: 100, after: $after, orderBy: {field: CREATED_AT, direction: ASC}) {
        nodes { field { ... on ProjectV2FieldCommon { name } } previousValue { ... on ProjectV2ItemFieldSingleSelectValue { name } } newValue { ... on ProjectV2ItemFieldSingleSelectValue { name } } createdAt actor { login } }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`
