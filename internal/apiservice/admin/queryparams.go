package admin

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// queryInt is a `limit`/`offset`-shaped query param: absent means
// defaultValue, present-and-unparsable is a 422 "int_parsing" pydantic
// error (FastAPI's own shape for a malformed query int).
func queryInt(values url.Values, name string, defaultValue int) (int, *pybody.Error) {
	raw := values.Get(name)
	if raw == "" {
		return defaultValue, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, &pybody.Error{Type: "int_parsing", Loc: []pyjson.Value{"query", name},
			Msg: "Input should be a valid integer, unable to parse string as an integer", Input: raw}
	}
	return n, nil
}

// queryBool is an `active_only`-shaped query param (FastAPI's bool
// coercion accepts true/false/1/0/yes/no/on/off, case-insensitive).
func queryBool(values url.Values, name string, defaultValue bool) (bool, *pybody.Error) {
	raw := values.Get(name)
	if raw == "" {
		return defaultValue, nil
	}
	switch raw {
	case "true", "True", "TRUE", "1", "yes", "Yes", "YES", "on", "On", "ON":
		return true, nil
	case "false", "False", "FALSE", "0", "no", "No", "NO", "off", "Off", "OFF":
		return false, nil
	default:
		return false, &pybody.Error{Type: "bool_parsing", Loc: []pyjson.Value{"query", name},
			Msg: "Input should be a valid boolean, unable to interpret input", Input: raw}
	}
}

// querySearch is `q`-shaped: an optional string with pydantic's
// min_length=1/max_length=200, "" (absent) meaning "no search" like the
// Python route's own `q.strip() if q and q.strip() else None`.
func querySearch(values url.Values, name string) (*string, *pybody.Error) {
	if _, present := values[name]; !present {
		return nil, nil
	}
	raw := values.Get(name)
	if len(raw) < 1 || len(raw) > 200 {
		return nil, &pybody.Error{Type: "string_too_short", Loc: []pyjson.Value{"query", name},
			Msg: "String should have at least 1 character", Input: raw}
	}
	trimmed := raw
	for len(trimmed) > 0 && (trimmed[0] == ' ' || trimmed[len(trimmed)-1] == ' ') {
		if trimmed[0] == ' ' {
			trimmed = trimmed[1:]
			continue
		}
		trimmed = trimmed[:len(trimmed)-1]
	}
	if trimmed == "" {
		return nil, nil
	}
	return &trimmed, nil
}

// writeQueryError answers a malformed-query-param request the way FastAPI
// does: 422 {"detail": [...]}.
func writeQueryError(w http.ResponseWriter, failure *pybody.Error) {
	policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail([]pybody.Error{*failure}), nil)
}
