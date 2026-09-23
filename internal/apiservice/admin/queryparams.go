package admin

import (
	"math"
	"net/http"
	"net/url"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// queryLastValue returns a repeated query param's LAST value -- verified
// live: Starlette/FastAPI resolve `?limit=0&limit=1`, `?active_only=false&
// active_only=true` and `?role=owner&role=member` all to the LAST one, for
// every scalar query type. A live round found Go picking the FIRST
// duplicate instead, here and on the users `q` search route.
func queryLastValue(values url.Values, name string) (value string, present bool) {
	raw, present := values[name]
	if !present || len(raw) == 0 {
		return "", present
	}
	return raw[len(raw)-1], true
}

// queryInt is a `limit`/`offset`-shaped query param: ABSENT means
// defaultValue; PRESENT (including present-and-empty, "limit=") and
// unparsable is a 422 "int_parsing" pydantic error (FastAPI's own shape for
// a malformed query int) -- verified live: `limit=` 422s, it does not fall
// back to the default the way a genuinely absent key does. Gating on
// presence, not on the raw string being non-empty, is the fix a live round
// found missing here.
//
// Parsing uses pythonparity.ParseInt (surrounding whitespace, a leading
// sign, and single underscores between digits all accepted, matching a
// live round's `?limit=%201%20` repro) rather than strconv.Atoi. This is
// not a perfect match for FastAPI's own lax-int query coercion in two
// narrow, unreported corners: pydantic-core additionally truncates an
// exact `N.0`-shaped string (e.g. "10.0" -> 10, verified live), which
// ParseInt rejects; and pydantic-core rejects non-ASCII decimal digits
// (e.g. "１２３"), which ParseInt -- matching real int()'s wider Unicode
// digit-folding -- accepts. Neither was part of the reported divergence.
func queryInt(values url.Values, name string, defaultValue int) (int, *pybody.Error) {
	value, present := queryLastValue(values, name)
	if !present {
		return defaultValue, nil
	}
	parsed, err := pythonparity.ParseInt(value)
	if err != nil || !parsed.IsInt64() || parsed.Int64() < math.MinInt32 || parsed.Int64() > math.MaxInt32 {
		return 0, &pybody.Error{Type: "int_parsing", Loc: []pyjson.Value{"query", name},
			Msg: "Input should be a valid integer, unable to parse string as an integer", Input: value}
	}
	return int(parsed.Int64()), nil
}

// queryBool is an `active_only`/`dry_run`-shaped query param. Absent means
// defaultValue; present coerces via pybody.PydanticBool, the same
// case-insensitive true/false/t/f/yes/no/y/n/on/off/1/0 vocabulary FastAPI's
// own bool query coercion uses (verified live to be identical to pydantic's
// model-field bool coercion) -- a live round found the previous
// exact-case-only switch rejecting spellings FastAPI accepts (e.g. "YeS").
func queryBool(values url.Values, name string, defaultValue bool) (bool, *pybody.Error) {
	value, present := queryLastValue(values, name)
	if !present {
		return defaultValue, nil
	}
	if parsed, ok := pybody.PydanticBool(value); ok {
		return parsed, nil
	}
	return false, &pybody.Error{Type: "bool_parsing", Loc: []pyjson.Value{"query", name},
		Msg: "Input should be a valid boolean, unable to interpret input", Input: value}
}

// querySearch is `q`-shaped: an optional string with pydantic's
// min_length=1/max_length=200, "" (absent) meaning "no search" like the
// Python route's own `q.strip() if q and q.strip() else None`. A live
// round found this route picking the FIRST of a repeated `q`, the same
// class as queryInt/queryBool above (queryLastValue).
func querySearch(values url.Values, name string) (*string, *pybody.Error) {
	raw, present := queryLastValue(values, name)
	if !present {
		return nil, nil
	}
	if len(raw) < 1 || len(raw) > 200 {
		return nil, &pybody.Error{Type: "string_too_short", Loc: []pyjson.Value{"query", name},
			Msg: "String should have at least 1 character", Input: raw}
	}
	trimmed := pythonparity.Strip(raw)
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
