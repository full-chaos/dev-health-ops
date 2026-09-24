package pybody

import (
	"net/url"

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
