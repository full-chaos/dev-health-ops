package providerfoundation

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
)

const maxProviderPageBody = 32 << 20
const maximumProviderPages = 10_000
const maximumGitLabPerPage = 100
const maximumJiraResults = 100
const launchDarklyFlagPageSize = 50
const launchDarklyAuditPageSize = 20
const maximumLaunchDarklyAuditItems = 1_000

var ErrPaginationInvalid = errors.New("provider pagination response is invalid")
var ErrGraphQLResponse = errors.New("provider GraphQL response contains errors")
var ErrGraphQLComplexity = errors.New("provider GraphQL query exceeds complexity limit")

// PageCollection reports bounded pagination evidence. Pages counts logical
// page responses; physical retry attempts remain available through Metrics.
type PageCollection struct {
	Items      []json.RawMessage
	Pages      int
	CapReached bool
}

type GitHubPageOptions struct {
	Path     string
	Query    url.Values
	DataKey  string
	MaxPages int
}

// CollectGitHubLinkPages mirrors the Python InstrumentedRESTCore contract:
// caller parameters are sent only on the first request and each absolute
// rel="next" URL is subsequently followed as-is. HTTPClient still rejects a
// next URL whose host differs from the configured GitHub/GHE host.
func CollectGitHubLinkPages(
	ctx context.Context,
	client *HTTPClient,
	options GitHubPageOptions,
) (PageCollection, error) {
	if ctx == nil || client == nil || strings.TrimSpace(options.Path) == "" ||
		options.MaxPages < 1 || options.MaxPages > maximumProviderPages {
		return PageCollection{}, ErrPaginationInvalid
	}
	next, err := pageURL(options.Path, options.Query)
	if err != nil {
		return PageCollection{}, err
	}
	result := PageCollection{}
	for next != "" {
		if result.Pages >= options.MaxPages {
			result.CapReached = true
			return result, nil
		}
		response, err := client.Do(ctx, http.MethodGet, next, nil)
		if err != nil {
			return PageCollection{}, err
		}
		items, decodeErr := decodePage(response, options.DataKey)
		if decodeErr != nil {
			return PageCollection{}, decodeErr
		}
		result.Pages++
		result.Items = append(result.Items, items...)
		next = githubNextLink(response.Header.Get("Link"))
	}
	return result, nil
}

type GitLabPageOptions struct {
	Path     string
	Query    url.Values
	PerPage  int
	MaxPages int
}

// CollectGitLabPageParamPages mirrors Python's page/per_page paginator. A
// non-empty X-Next-Page is authoritative; an absent or empty header falls back
// to the full-page item-count heuristic. Malformed next-page values stop
// safely, matching the existing Python implementation.
func CollectGitLabPageParamPages(
	ctx context.Context,
	client *HTTPClient,
	options GitLabPageOptions,
) (PageCollection, error) {
	if ctx == nil || client == nil || strings.TrimSpace(options.Path) == "" ||
		options.PerPage < 1 || options.PerPage > maximumGitLabPerPage ||
		options.MaxPages < 1 || options.MaxPages > maximumProviderPages {
		return PageCollection{}, ErrPaginationInvalid
	}
	page := 1
	result := PageCollection{}
	for page > 0 {
		if result.Pages >= options.MaxPages {
			result.CapReached = true
			return result, nil
		}
		query := cloneValues(options.Query)
		query.Set("page", strconv.Itoa(page))
		if query.Get("per_page") == "" {
			query.Set("per_page", strconv.Itoa(options.PerPage))
		}
		target, err := pageURL(options.Path, query)
		if err != nil {
			return PageCollection{}, err
		}
		response, err := client.Do(ctx, http.MethodGet, target, nil)
		if err != nil {
			return PageCollection{}, err
		}
		items, decodeErr := decodePage(response, "")
		if decodeErr != nil {
			return PageCollection{}, decodeErr
		}
		result.Pages++
		if len(items) == 0 {
			return result, nil
		}
		result.Items = append(result.Items, items...)
		nextHeader := strings.TrimSpace(response.Header.Get("X-Next-Page"))
		if nextHeader != "" {
			nextPage, parseErr := strconv.Atoi(nextHeader)
			if parseErr != nil || nextPage < 1 {
				return result, nil
			}
			page = nextPage
			continue
		}
		if len(items) < options.PerPage {
			return result, nil
		}
		page++
	}
	return result, nil
}

type LinearPageOptions struct {
	Query          string
	Variables      map[string]any
	ConnectionPath []string
	PerPage        int
	MaxPages       int
}

// CollectLinearGraphQLPages mirrors Linear's first/after connection contract.
// GraphQL errors returned with HTTP 200 are permanent and never retried.
func CollectLinearGraphQLPages(
	ctx context.Context,
	client *HTTPClient,
	options LinearPageOptions,
) (PageCollection, error) {
	if ctx == nil || client == nil || strings.TrimSpace(options.Query) == "" ||
		len(options.ConnectionPath) < 1 || options.PerPage < 1 ||
		options.PerPage > maximumGitLabPerPage || options.MaxPages < 1 ||
		options.MaxPages > maximumProviderPages {
		return PageCollection{}, ErrPaginationInvalid
	}
	result := PageCollection{}
	cursor := ""
	for {
		if result.Pages >= options.MaxPages {
			result.CapReached = true
			return result, nil
		}
		variables := cloneAnyMap(options.Variables)
		variables["first"] = options.PerPage
		if cursor == "" {
			variables["after"] = nil
		} else {
			variables["after"] = cursor
		}
		body, err := json.Marshal(map[string]any{
			"query":     options.Query,
			"variables": variables,
		})
		if err != nil {
			return PageCollection{}, ErrPaginationInvalid
		}
		response, err := client.Do(ctx, http.MethodPost, "/graphql", bytes.NewReader(body))
		if err != nil {
			return PageCollection{}, err
		}
		payload, err := decodeJSONObject(response)
		if err != nil {
			return PageCollection{}, err
		}
		if rawErrors, ok := payload["errors"]; ok && string(rawErrors) != "null" &&
			string(rawErrors) != "[]" {
			if graphqlComplexityError(rawErrors) {
				return PageCollection{}, ErrGraphQLComplexity
			}
			return PageCollection{}, ErrGraphQLResponse
		}
		connection, err := nestedJSONObject(payload, append([]string{"data"}, options.ConnectionPath...)...)
		if err != nil {
			return PageCollection{}, err
		}
		var nodes []json.RawMessage
		if rawNodes, ok := connection["nodes"]; ok {
			if err := json.Unmarshal(rawNodes, &nodes); err != nil {
				return PageCollection{}, ErrPaginationInvalid
			}
		}
		result.Pages++
		result.Items = append(result.Items, nodes...)
		var pageInfo struct {
			HasNextPage bool   `json:"hasNextPage"`
			EndCursor   string `json:"endCursor"`
		}
		if rawPageInfo, ok := connection["pageInfo"]; ok {
			if err := json.Unmarshal(rawPageInfo, &pageInfo); err != nil {
				return PageCollection{}, ErrPaginationInvalid
			}
		}
		if !pageInfo.HasNextPage {
			return result, nil
		}
		if pageInfo.EndCursor == "" || pageInfo.EndCursor == cursor {
			return PageCollection{}, ErrPaginationInvalid
		}
		cursor = pageInfo.EndCursor
	}
}

type JiraPageOptions struct {
	Path       string
	Query      url.Values
	DataKey    string
	MaxResults int
	MaxPages   int
}

// CollectJiraTokenOffsetPages preserves Jira's mixed enhanced-search contract:
// nextPageToken is authoritative when present; otherwise startAt advances by
// the number of returned rows. isLast=true terminates either mode.
func CollectJiraTokenOffsetPages(
	ctx context.Context,
	client *HTTPClient,
	options JiraPageOptions,
) (PageCollection, error) {
	if ctx == nil || client == nil || strings.TrimSpace(options.Path) == "" ||
		strings.TrimSpace(options.DataKey) == "" || options.MaxResults < 1 ||
		options.MaxResults > maximumJiraResults || options.MaxPages < 1 ||
		options.MaxPages > maximumProviderPages {
		return PageCollection{}, ErrPaginationInvalid
	}
	result := PageCollection{}
	startAt := 0
	token := ""
	for {
		if result.Pages >= options.MaxPages {
			result.CapReached = true
			return result, nil
		}
		query := cloneValues(options.Query)
		query.Set("maxResults", strconv.Itoa(options.MaxResults))
		if token == "" {
			query.Set("startAt", strconv.Itoa(startAt))
			query.Del("nextPageToken")
		} else {
			query.Set("nextPageToken", token)
			query.Del("startAt")
		}
		target, err := pageURL(options.Path, query)
		if err != nil {
			return PageCollection{}, err
		}
		response, err := client.Do(ctx, http.MethodGet, target, nil)
		if err != nil {
			return PageCollection{}, err
		}
		payload, err := decodeJSONObject(response)
		if err != nil {
			return PageCollection{}, err
		}
		var items []json.RawMessage
		if rawItems, ok := payload[options.DataKey]; ok {
			if err := json.Unmarshal(rawItems, &items); err != nil {
				return PageCollection{}, ErrPaginationInvalid
			}
		}
		result.Pages++
		if len(items) == 0 {
			return result, nil
		}
		result.Items = append(result.Items, items...)
		var isLast bool
		if rawIsLast, ok := payload["isLast"]; ok && string(rawIsLast) != "null" {
			if err := json.Unmarshal(rawIsLast, &isLast); err != nil {
				return PageCollection{}, ErrPaginationInvalid
			}
		}
		if isLast {
			return result, nil
		}
		next := ""
		if rawNext, ok := payload["nextPageToken"]; ok && string(rawNext) != "null" {
			if err := json.Unmarshal(rawNext, &next); err != nil {
				return PageCollection{}, ErrPaginationInvalid
			}
		}
		if next != "" {
			if next == token {
				return PageCollection{}, ErrPaginationInvalid
			}
			token = next
			continue
		}
		token = ""
		startAt += len(items)
	}
}

type LaunchDarklyOffsetOptions struct {
	Path     string
	Query    url.Values
	MaxPages int
}

func CollectLaunchDarklyOffsetPages(
	ctx context.Context,
	client *HTTPClient,
	options LaunchDarklyOffsetOptions,
) (PageCollection, error) {
	if ctx == nil || client == nil || strings.TrimSpace(options.Path) == "" ||
		options.MaxPages < 1 || options.MaxPages > maximumProviderPages {
		return PageCollection{}, ErrPaginationInvalid
	}
	result := PageCollection{}
	offset := 0
	for {
		if result.Pages >= options.MaxPages {
			result.CapReached = true
			return result, nil
		}
		query := cloneValues(options.Query)
		query.Set("limit", strconv.Itoa(launchDarklyFlagPageSize))
		query.Set("offset", strconv.Itoa(offset))
		target, err := pageURL(options.Path, query)
		if err != nil {
			return PageCollection{}, err
		}
		response, err := client.Do(ctx, http.MethodGet, target, nil)
		if err != nil {
			return PageCollection{}, err
		}
		payload, err := decodeJSONObject(response)
		if err != nil {
			return PageCollection{}, err
		}
		var items []json.RawMessage
		if rawItems, ok := payload["items"]; ok {
			if err := json.Unmarshal(rawItems, &items); err != nil {
				return PageCollection{}, ErrPaginationInvalid
			}
		}
		result.Pages++
		result.Items = append(result.Items, items...)
		total := len(result.Items)
		if rawTotal, ok := payload["totalCount"]; ok {
			if err := json.Unmarshal(rawTotal, &total); err != nil || total < 0 {
				return PageCollection{}, ErrPaginationInvalid
			}
		}
		if len(result.Items) >= total || len(items) < launchDarklyFlagPageSize {
			return result, nil
		}
		offset += launchDarklyFlagPageSize
	}
}

type LaunchDarklyAuditOptions struct {
	Since    *time.Time
	MaxItems int
}

func CollectLaunchDarklyAuditPages(
	ctx context.Context,
	client *HTTPClient,
	options LaunchDarklyAuditOptions,
) (PageCollection, error) {
	if ctx == nil || client == nil || options.MaxItems < 0 ||
		options.MaxItems > maximumLaunchDarklyAuditItems {
		return PageCollection{}, ErrPaginationInvalid
	}
	if options.MaxItems == 0 {
		return PageCollection{}, nil
	}
	query := url.Values{"limit": {strconv.Itoa(launchDarklyAuditPageSize)}}
	if options.Since != nil {
		query.Set("after", strconv.FormatInt(options.Since.UTC().UnixMilli(), 10))
	}
	next, err := pageURL("/api/v2/auditlog", query)
	if err != nil {
		return PageCollection{}, err
	}
	maxPages := options.MaxItems/launchDarklyAuditPageSize + 2
	result := PageCollection{}
	seen := map[string]bool{}
	for result.Pages < maxPages {
		if seen[next] {
			return PageCollection{}, ErrPaginationInvalid
		}
		seen[next] = true
		response, err := client.Do(ctx, http.MethodGet, next, nil)
		if err != nil {
			return PageCollection{}, err
		}
		payload, err := decodeJSONObject(response)
		if err != nil {
			return PageCollection{}, err
		}
		var items []json.RawMessage
		if rawItems, ok := payload["items"]; ok {
			if err := json.Unmarshal(rawItems, &items); err != nil {
				return PageCollection{}, ErrPaginationInvalid
			}
		}
		result.Pages++
		if len(items) == 0 {
			return result, nil
		}
		remaining := options.MaxItems - len(result.Items)
		if len(items) > remaining {
			items = items[:remaining]
		}
		result.Items = append(result.Items, items...)
		if len(result.Items) >= options.MaxItems {
			return result, nil
		}
		href, err := launchDarklyNextHref(payload)
		if err != nil {
			return PageCollection{}, err
		}
		if href == "" {
			return result, nil
		}
		next = href
	}
	result.CapReached = true
	return result, nil
}

func pageURL(path string, query url.Values) (string, error) {
	parsed, err := url.Parse(path)
	if err != nil {
		return "", ErrPaginationInvalid
	}
	values := parsed.Query()
	for key, entries := range query {
		values.Del(key)
		for _, entry := range entries {
			values.Add(key, entry)
		}
	}
	parsed.RawQuery = values.Encode()
	return parsed.String(), nil
}

func cloneValues(values url.Values) url.Values {
	cloned := make(url.Values, len(values)+2)
	for key, entries := range values {
		cloned[key] = append([]string(nil), entries...)
	}
	return cloned
}

func decodePage(response *http.Response, dataKey string) ([]json.RawMessage, error) {
	if response == nil || response.Body == nil {
		return nil, ErrPaginationInvalid
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, maxProviderPageBody+1)
	body, err := io.ReadAll(limited)
	if err != nil || len(body) > maxProviderPageBody {
		return nil, ErrPaginationInvalid
	}
	if dataKey == "" {
		var items []json.RawMessage
		if err := json.Unmarshal(body, &items); err != nil {
			return nil, ErrPaginationInvalid
		}
		return items, nil
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, ErrPaginationInvalid
	}
	raw, ok := envelope[dataKey]
	if !ok {
		return []json.RawMessage{}, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, ErrPaginationInvalid
	}
	return items, nil
}

func decodeJSONObject(response *http.Response) (map[string]json.RawMessage, error) {
	if response == nil || response.Body == nil {
		return nil, ErrPaginationInvalid
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxProviderPageBody+1))
	if err != nil || len(body) > maxProviderPageBody {
		return nil, ErrPaginationInvalid
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, ErrPaginationInvalid
	}
	return payload, nil
}

func nestedJSONObject(
	root map[string]json.RawMessage,
	path ...string,
) (map[string]json.RawMessage, error) {
	current := root
	for _, part := range path {
		raw, ok := current[part]
		if !ok || string(raw) == "null" {
			return nil, ErrPaginationInvalid
		}
		var next map[string]json.RawMessage
		if err := json.Unmarshal(raw, &next); err != nil {
			return nil, ErrPaginationInvalid
		}
		current = next
	}
	return current, nil
}

func cloneAnyMap(input map[string]any) map[string]any {
	cloned := make(map[string]any, len(input)+2)
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

func graphqlComplexityError(raw json.RawMessage) bool {
	var errors []struct {
		Message    string `json:"message"`
		Extensions struct {
			Code string `json:"code"`
		} `json:"extensions"`
	}
	if json.Unmarshal(raw, &errors) != nil {
		return false
	}
	for _, item := range errors {
		message := strings.ToLower(item.Message)
		code := strings.ToUpper(item.Extensions.Code)
		if strings.Contains(message, "complexity") ||
			strings.Contains(message, "too complex") ||
			strings.Contains(code, "COMPLEXITY") {
			return true
		}
	}
	return false
}

func launchDarklyNextHref(payload map[string]json.RawMessage) (string, error) {
	links, ok := payload["_links"]
	if !ok || string(links) == "null" {
		return "", nil
	}
	var values map[string]struct {
		Href string `json:"href"`
	}
	if err := json.Unmarshal(links, &values); err != nil {
		return "", ErrPaginationInvalid
	}
	return strings.TrimSpace(values["next"].Href), nil
}

func githubNextLink(header string) string {
	for _, part := range splitLinkHeader(header) {
		open, close := strings.IndexByte(part, '<'), strings.IndexByte(part, '>')
		if open < 0 || close <= open {
			continue
		}
		for _, attribute := range strings.Split(part[close+1:], ";") {
			key, value, found := strings.Cut(strings.TrimSpace(attribute), "=")
			if found && strings.EqualFold(key, "rel") && strings.Trim(value, `"`) == "next" {
				return strings.TrimSpace(part[open+1 : close])
			}
		}
	}
	return ""
}

func splitLinkHeader(header string) []string {
	var parts []string
	start, angle := 0, 0
	for index, char := range header {
		switch char {
		case '<':
			angle++
		case '>':
			if angle > 0 {
				angle--
			}
		case ',':
			if angle == 0 {
				parts = append(parts, strings.TrimSpace(header[start:index]))
				start = index + 1
			}
		}
	}
	if tail := strings.TrimSpace(header[start:]); tail != "" {
		parts = append(parts, tail)
	}
	return parts
}
