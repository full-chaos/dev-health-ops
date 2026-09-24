package httpapi

import "net/url"

// QueryLast is how FastAPI/Starlette resolve a repeated SCALAR query
// parameter: the LAST occurrence, "" when the name is absent (a present,
// empty value, e.g. "?limit=", is also "" and must still be validated, never
// treated as absent -- check Has/the raw slice separately when that
// distinction matters). Go's own url.Values.Get returns the FIRST
// occurrence; this is the one place that mismatch is closed, so every route
// reading a scalar query param goes through here (or QueryLastPtr below)
// instead of re-deriving the same slice-index logic per package.
func QueryLast(values url.Values, name string) string {
	raw, present := values[name]
	if !present || len(raw) == 0 {
		return ""
	}
	return raw[len(raw)-1]
}

// QueryLastPtr is QueryLast for a caller that must distinguish "absent"
// (nil) from "present" (a valid pointer, possibly to "").
func QueryLastPtr(values url.Values, name string) *string {
	raw, present := values[name]
	if !present || len(raw) == 0 {
		return nil
	}
	last := raw[len(raw)-1]
	return &last
}
