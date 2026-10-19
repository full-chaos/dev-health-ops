package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// An anonymous caller must get 401 before the document pipeline reads or
// parses any part of the body: a 400 or 413 for an unauthenticated request is
// an oracle on body validity and opens the parser to anonymous input.
func TestAnonymousDocumentRoutesRefuseBeforeReadingTheBody(t *testing.T) {
	getenv := getenvFunc(func(string) string { return "" })
	verifier, priv := iaVerifier(t)
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probe": true})
	mux.Register("probe", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	byDigest := map[string]string{digestHex(iaDocument): "probe"}
	allow := func(context.Context, string) bool { return true }
	routes := map[string]http.Handler{
		"/query":               newDocumentDispatchHandler(getenv, mux, byDigest, verifier, nil, nil, "", nil),
		"/query/proof":         newDocumentDispatchHandler(getenv, mux, byDigest, verifier, nil, nil, digest.KindQuery, nil),
		"/query/proof-write":   newDocumentDispatchHandler(getenv, mux, byDigest, verifier, nil, nil, digest.KindMutation, allow),
		"/query/run-operation": newDocumentDispatchHandler(getenv, mux, byDigest, verifier, nil, nil, "", nil),
	}
	bodies := map[string]string{
		"empty":       "",
		"malformed":   "{not json",
		"well_formed": `{"query":` + jsonQuoteString(iaDocument) + `}`,
		"oversized":   strings.Repeat("x", defaultGraphQLMaxQueryBytes+1),
		"deep_int":    `{"query":"","v":` + strings.Repeat("9", 5000) + `}`,
	}
	for path, handler := range routes {
		for name, body := range bodies {
			t.Run(path+"/anonymous/"+name, func(t *testing.T) {
				request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)
				if recorder.Code != http.StatusUnauthorized {
					t.Fatalf("anonymous %s body = %d, want 401", name, recorder.Code)
				}
			})
		}
	}

	// The same bad bodies from an authenticated caller keep their refusals.
	envelope := iaEnvelope(t, priv, principal.Claims{OrgID: "org-1", Role: "admin"})
	authed := map[string]struct {
		body string
		want int
	}{
		"malformed": {"{not json", http.StatusBadRequest},
		"oversized": {strings.Repeat("x", defaultGraphQLMaxQueryBytes+1), http.StatusRequestEntityTooLarge},
		"deep_int":  {`{"query":"","v":` + strings.Repeat("9", 5000) + `}`, http.StatusInternalServerError},
	}
	for path, handler := range routes {
		for name, c := range authed {
			t.Run(path+"/authenticated/"+name, func(t *testing.T) {
				request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(c.body))
				request.Header.Set("Authorization", "Bearer "+envelope)
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)
				if recorder.Code != c.want {
					t.Fatalf("authenticated %s body = %d, want %d", name, recorder.Code, c.want)
				}
			})
		}
	}
}
