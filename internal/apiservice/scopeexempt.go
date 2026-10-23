package apiservice

import (
	"context"
	"net/http"
	"regexp"
	"strings"
)

// signatureOnlyRoutes are the api-listener routes whose only credential is a
// provider signature checked by the handler, as "METHOD path" mapped to the
// credential class their endpoint-profile row must name. They skip the
// org-scope and impersonation middlewares so the signature is the first
// check. A path ending in "/{binding_id}" matches that prefix plus exactly
// one canonical UUID segment.
var signatureOnlyRoutes = map[string]string{
	"POST /api/v1/billing/webhooks/stripe":         "stripe_webhook_signature",
	"POST /api/v1/webhooks/github":                 "github_webhook_signature",
	"POST /api/v1/webhooks/gitlab":                 "gitlab_webhook_signature",
	"POST /api/v1/webhooks/jira":                   "jira_webhook_signature",
	"POST /api/v1/webhooks/pagerduty/{binding_id}": "pagerduty_webhook_signature",
}

const bindingSuffix = "{binding_id}"

var canonicalUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func signatureOnlyMatch(method, escapedPath string) bool {
	key := method + " " + escapedPath
	if _, ok := signatureOnlyRoutes[key]; ok {
		return true
	}
	for route := range signatureOnlyRoutes {
		prefix, templated := strings.CutSuffix(route, bindingSuffix)
		if !templated {
			continue
		}
		if segment, ok := strings.CutPrefix(key, prefix); ok && canonicalUUID.MatchString(segment) {
			return true
		}
	}
	return false
}

type signatureOnlyKey struct{}

// MarkSignatureOnly records, on the raw request target, that the request is
// exactly one of signatureOnlyRoutes. It must run before DecodedPathRouting,
// which drops the encoded form of the path.
func MarkSignatureOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if signatureOnlyMatch(r.Method, r.URL.EscapedPath()) {
			r = r.WithContext(context.WithValue(r.Context(), signatureOnlyKey{}, true))
		}
		next.ServeHTTP(w, r)
	})
}

// skipScope runs mw for every request except a marked signature-only one.
func skipScope(mw func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	if mw == nil {
		return nil
	}
	return func(next http.Handler) http.Handler {
		guarded := mw(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if marked, _ := r.Context().Value(signatureOnlyKey{}).(bool); marked {
				next.ServeHTTP(w, r)
				return
			}
			guarded.ServeHTTP(w, r)
		})
	}
}
