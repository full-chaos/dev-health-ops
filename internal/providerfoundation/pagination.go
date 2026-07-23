package providerfoundation

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const maxProviderPageBody = 32 << 20

var ErrPaginationInvalid = errors.New("provider pagination response is invalid")

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
	if ctx == nil || client == nil || strings.TrimSpace(options.Path) == "" || options.MaxPages < 1 {
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
		options.PerPage < 1 || options.MaxPages < 1 {
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
