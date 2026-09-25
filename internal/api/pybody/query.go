package pybody

import (
	"net/url"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// LastQueryValue is how FastAPI reads a scalar query parameter: the LAST of
// repeated values, nil when the name is absent. A present, empty value
// ("?limit=") is "" and is validated, never treated as absent. This is a
// thin *string-returning wrapper -- the one shared implementation is
// httpapi.QueryLastPtr; every other package reading a query param goes
// through httpapi directly (CHAOS-6585: three independent copies of this
// exact logic existed before the consolidation).
func LastQueryValue(values url.Values, name string) *string {
	return httpapi.QueryLastPtr(values, name)
}

// RequiredQueryString validates one required `str` query parameter with
// FastAPI's Query(min_length=) (code points; 0 = no bound). raw is nil when
// the parameter is absent: a "missing" error whose input is None.
func (e *Errors) RequiredQueryString(name string, raw *string, minLength int) (string, bool) {
	loc := []pyjson.Value{"query", name}
	if raw == nil {
		*e = append(*e, Error{Type: "missing", Loc: loc, Msg: "Field required", Input: nil})
		return "", false
	}
	return e.validateStringValue(*raw, loc, minLength, 0)
}
