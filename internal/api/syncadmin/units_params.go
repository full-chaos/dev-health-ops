package syncadmin

import (
	"math/big"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// Paged units read. Any of offset, status, dataset_key, source_id, since or
// before switches the route from the whole-run read (limit slices the
// output, as the Python route does) to a page of the filtered units.
const (
	unitsPageDefaultLimit = 100
	unitsPageMaxLimit     = 1000
	unitsStatusMaxValues  = 12
	unitsValueMaxLength   = 128
	unitsPageMaxOffset    = 1_000_000
)

// unitsStatusAllowlist is every unit status the filter accepts; any other
// value is a 422, so the filter never carries free text.
var unitsStatusAllowlist = map[string]bool{
	"planned": true, "pending": true, "dispatching": true, "dispatched": true, "running": true, "retrying": true,
	"success": true, "failed": true, "cancelled": true, "canceled": true, "skipped": true,
}

var unitsPagedParams = []string{"offset", "status", "dataset_key", "source_id", "since", "before", "error_category"}

// unitsErrorCategoryAllowlist is every error_category the filter accepts.
var unitsErrorCategoryAllowlist = map[string]bool{"provider_budget_contention": true}

type unitsQuery struct {
	paged  bool
	limit  *big.Int
	offset int64
	filter unitFilter
}

func unitsQueryError(name, kind, msg string, input string) pybody.Error {
	return pybody.Error{Type: kind, Loc: []pyjson.Value{"query", name}, Msg: msg, Input: input}
}

func parseUnitsQuery(values url.Values, errs *pybody.Errors) unitsQuery {
	var query unitsQuery
	for _, name := range unitsPagedParams {
		if _, present := values[name]; present {
			query.paged = true
		}
	}
	rawLimit := pybody.LastQuery(values, "limit")
	if !query.paged {
		if rawLimit != nil {
			if value, ok := errs.QueryInt("limit", rawLimit, 0, nil, nil); ok {
				query.limit = value
			}
		}
		return query
	}
	zero, maxLimit := int64(0), int64(unitsPageMaxLimit)
	if value, ok := errs.QueryInt("limit", rawLimit, unitsPageDefaultLimit, &zero, &maxLimit); ok {
		query.limit = value
	}
	maxOffset := int64(unitsPageMaxOffset)
	if value, ok := errs.QueryInt("offset", pybody.LastQuery(values, "offset"), 0, &zero, &maxOffset); ok {
		query.offset = value.Int64()
	}
	for _, raw := range values["status"] {
		for _, status := range strings.Split(raw, ",") {
			status = strings.TrimSpace(status)
			if !unitsStatusAllowlist[status] || len(query.filter.Statuses) >= unitsStatusMaxValues {
				*errs = append(*errs, unitsQueryError("status", "value_error", "status must be 1 to 12 comma-separated unit statuses", raw))
				break
			}
			query.filter.Statuses = append(query.filter.Statuses, status)
		}
	}
	if raw := pybody.LastQuery(values, "dataset_key"); raw != nil {
		if *raw == "" || len(*raw) > unitsValueMaxLength {
			*errs = append(*errs, unitsQueryError("dataset_key", "string_too_long", "dataset_key must be 1 to 128 characters", *raw))
		} else {
			query.filter.DatasetKey = raw
		}
	}
	if raw := pybody.LastQuery(values, "source_id"); raw != nil {
		if parsed, err := uuid.Parse(*raw); err != nil {
			*errs = append(*errs, unitsQueryError("source_id", "uuid_parsing", "Input should be a valid UUID", *raw))
		} else {
			query.filter.SourceID = &parsed
		}
	}
	if raw := pybody.LastQuery(values, "error_category"); raw != nil {
		if !unitsErrorCategoryAllowlist[*raw] {
			*errs = append(*errs, unitsQueryError("error_category", "value_error", "error_category must be provider_budget_contention", *raw))
		} else {
			query.filter.ErrorCategory = raw
		}
	}
	query.filter.Since = parseUnitsInstant(values, "since", errs)
	query.filter.Before = parseUnitsInstant(values, "before", errs)
	return query
}

func parseUnitsInstant(values url.Values, name string, errs *pybody.Errors) *time.Time {
	raw := pybody.LastQuery(values, name)
	if raw == nil {
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02"} {
		if parsed, err := time.Parse(layout, *raw); err == nil {
			utc := parsed.UTC()
			return &utc
		}
	}
	*errs = append(*errs, unitsQueryError(name, "datetime_parsing", "Input should be a valid datetime (RFC 3339 or YYYY-MM-DD)", *raw))
	return nil
}
