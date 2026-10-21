package httpapi

import "net/http"

// NoStore answers every request with Cache-Control: no-store, set before next
// runs so a refusal or an error answer carries it too.
func NoStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}
