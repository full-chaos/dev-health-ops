package apiservice

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// originProtectedPaths is _middleware.py's _CORS_PROTECTED_PATHS.
var originProtectedPaths = map[string]bool{"/api/v1/auth/register": true}

// OriginValidation is api/middleware/csrf.py's OriginValidationMiddleware
// with the Python api's parameters: a state-changing request to a protected
// path must carry an Origin, or failing that a Referer, whose scheme and
// host are one of the CORS origins. It sits where Python registers it:
// inside the org-scope and impersonation middlewares, outside the security
// headers and CORS, so its 403 carries neither.
type OriginValidation struct {
	allowed map[string]bool
}

// NewOriginValidation normalizes allowed as the middleware's __init__ does;
// an entry that does not normalize (a bare "*" included) is dropped, so "*"
// never allows every origin here.
func NewOriginValidation(allowed []string) *OriginValidation {
	v := &OriginValidation{allowed: map[string]bool{}}
	for _, origin := range allowed {
		if normalized, ok, _ := normalizeOrigin(origin, true); ok {
			v.allowed[normalized] = true
		}
	}
	return v
}

// normalizeOrigin is _normalize_origin: "scheme://netloc", both lowered,
// when urlparse(value.strip()) has both; present is false for an absent
// header. err is urlparse's ValueError, which the middleware does not
// catch.
func normalizeOrigin(value string, present bool) (string, bool, error) {
	if !present || value == "" {
		return "", false, nil
	}
	split, err := pythonparity.SplitURL(pythonparity.Strip(value))
	if err != nil {
		return "", false, err
	}
	if split.Scheme == "" || split.Netloc == "" {
		return "", false, nil
	}
	return pythonparity.Lower(split.Scheme) + "://" + pythonparity.Lower(split.Netloc), true, nil
}

// lastHeader is the middleware's header dict: the LAST value of a repeated
// header, decoded as latin-1.
func lastHeader(r *http.Request, name string) (string, bool) {
	values := r.Header.Values(name)
	if len(values) == 0 {
		return "", false
	}
	return policy.Latin1(values[len(values)-1]), true
}

// Wrap applies the check in front of next.
func (v *OriginValidation) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		default:
			next.ServeHTTP(w, r)
			return
		}
		if !originProtectedPaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		for _, header := range []string{"Origin", "Referer"} {
			value, present := lastHeader(r, header)
			origin, ok, err := normalizeOrigin(value, present)
			if err != nil {
				policy.WriteInternal(w)
				return
			}
			if ok && v.allowed[origin] {
				next.ServeHTTP(w, r)
				return
			}
		}
		policy.WriteDetail(w, http.StatusForbidden, "Request origin validation failed", nil)
	})
}
