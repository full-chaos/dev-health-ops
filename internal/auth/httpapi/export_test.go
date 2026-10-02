package httpapi

import "net/http"

// ForwardedScheme is ForwardedTrust.scheme for the external test package of the
// frozen forwarded-scheme oracle, which cannot import the program helper from
// inside package httpapi.
func ForwardedScheme(trust ForwardedTrust, request *http.Request) string {
	return trust.scheme(request)
}
