package providerfoundation

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const maximumPagerDutyPerPage = 100

// PagerDutyOffsetOptions mirrors PagerDuty REST v2's bounded offset contract.
// The provider returns an envelope containing the named collection and a
// required boolean `more` marker; an advertised next page with no rows is an
// invalid/non-progressing response and must fail closed.
type PagerDutyOffsetOptions struct {
	Path     string
	DataKey  string
	PerPage  int
	MaxPages int
}

func CollectPagerDutyOffsetPages(
	ctx context.Context,
	client *HTTPClient,
	options PagerDutyOffsetOptions,
) (PageCollection, error) {
	if ctx == nil || client == nil || strings.TrimSpace(options.Path) == "" ||
		strings.TrimSpace(options.DataKey) == "" || options.PerPage < 1 ||
		options.PerPage > maximumPagerDutyPerPage || options.MaxPages < 1 ||
		options.MaxPages > maximumProviderPages {
		return PageCollection{}, ErrPaginationInvalid
	}
	result := PageCollection{}
	offset := 0
	for {
		if result.Pages >= options.MaxPages {
			result.CapReached = true
			return result, nil
		}
		query := url.Values{
			"limit":  {strconv.Itoa(options.PerPage)},
			"offset": {strconv.Itoa(offset)},
		}
		target, err := pageURL(options.Path, query)
		if err != nil {
			return PageCollection{}, err
		}
		response, err := client.Do(ctx, http.MethodGet, target, nil)
		if err != nil {
			return result, err
		}
		payload, err := decodeJSONObject(response)
		if err != nil {
			return result, err
		}
		rawItems, ok := payload[options.DataKey]
		if !ok || string(rawItems) == "null" {
			return result, ErrPaginationInvalid
		}
		var items []json.RawMessage
		if err := json.Unmarshal(rawItems, &items); err != nil {
			return result, ErrPaginationInvalid
		}
		rawMore, ok := payload["more"]
		if !ok || string(rawMore) == "null" {
			return result, ErrPaginationInvalid
		}
		var more bool
		if err := json.Unmarshal(rawMore, &more); err != nil {
			return result, ErrPaginationInvalid
		}
		result.Pages++
		result.Items = append(result.Items, items...)
		if !more {
			return result, nil
		}
		if len(items) == 0 {
			return result, ErrPaginationInvalid
		}
		offset += len(items)
	}
}
