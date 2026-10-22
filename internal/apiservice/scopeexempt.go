package apiservice

import (
	"context"
	"net/http"
)

// signatureOnlyRoutes are the api-listener routes whose only credential is a
// provider signature checked by the handler: "METHOD path". They skip the
// org-scope and impersonation middlewares so the signature is the first check.
var signatureOnlyRoutes = map[string]bool{
	"POST /api/v1/billing/webhooks/stripe": true,
}

type signatureOnlyKey struct{}

// MarkSignatureOnly records, on the raw request target, that the request is
// exactly one of signatureOnlyRoutes. It must run before DecodedPathRouting,
// which drops the encoded form of the path.
func MarkSignatureOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if signatureOnlyRoutes[r.Method+" "+r.URL.EscapedPath()] {
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
