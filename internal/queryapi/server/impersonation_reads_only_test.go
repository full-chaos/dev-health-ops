package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// An applied impersonation session is reads only (D6714): a live superuser that holds admin.impersonate, beside a
// live go-api row, is served as the target on a query and refused (403, no resolver) on a mutation, whatever the
// switch. With the switch on the legacy refusal answers first, with the same 403.

// TestImpersonatedWriteDoorRefusesTheMutation: the write door refuses the impersonating holder's mutation; the same
// holder with no live row is served as itself.
func TestImpersonatedWriteDoorRefusesTheMutation(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	for switchName, on := range revokedSwitches {
		for signing, token := range legacyTokens(t, minter) {
			t.Run(switchName+" "+signing, func(t *testing.T) {
				door, seen := revokedWriteDoor(t, minter, legacyStore(legacySession(), nil), on)
				if got := postProofWrite(door, token); got.Code != http.StatusForbidden || strings.TrimSpace(got.Body.String()) != legacyRefusalBody || len(*seen) != 0 {
					t.Fatalf("live row: %d %q, ran %d; want 403 %s and no resolver", got.Code, got.Body.String(), len(*seen), legacyRefusalBody)
				}
				none, seenNone := revokedWriteDoor(t, minter, legacyStore(nil, nil), on)
				if got := postProofWrite(none, token); got.Code != http.StatusOK || len(*seenNone) != 1 || (*seenNone)[0].ImpersonationActive {
					t.Fatalf("no row: %d %q claims=%+v, want 200 served as itself", got.Code, got.Body.String(), *seenNone)
				}
			})
		}
	}
}

// TestImpersonatedMutationIsRefusedOnThePipeline: the same holder's registered mutation is refused on /query (public
// and internal), /query/run-operation and POST /graphql; its query there is served as the target (switch unset).
func TestImpersonatedMutationIsRefusedOnThePipeline(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	routes := []struct{ listener, path string }{{"query", "/query"}, {"internal", "/query"}, {"internal", "/query/run-operation"}}
	for signing, token := range legacyTokens(t, minter) {
		for _, route := range routes {
			t.Run(signing+" "+route.listener+" "+route.path, func(t *testing.T) {
				write := legacyStack(t, minter, legacyStore(legacySession(), nil), false)
				if got := legacyQueryDocument(write, route.listener, route.path, token, legacyMutationDocument); got.Status != http.StatusForbidden || got.Body != legacyRefusalBody || len(*write.seen) != 0 {
					t.Fatalf("mutation: got %d %s, ran %d; want 403 %s and no resolver", got.Status, got.Body, len(*write.seen), legacyRefusalBody)
				}
				read := legacyStack(t, minter, legacyStore(legacySession(), nil), false)
				if got := legacyQueryDocument(read, route.listener, route.path, token, iaDocument); got.Status != http.StatusOK || len(*read.seen) != 1 || !(*read.seen)[0].ImpersonationActive || (*read.seen)[0].OrgID != irTargetOrg.String() {
					t.Fatalf("query: got %d %s claims=%+v, want 200 served as the target", got.Status, got.Body, *read.seen)
				}
			})
		}
		t.Run(signing+" POST /graphql", func(t *testing.T) {
			write := legacyStack(t, minter, legacyStore(legacySession(), nil), false)
			if got := legacyGraphQLDocument(write, http.MethodPost, token, legacyMutationDocument); got.Status != http.StatusForbidden || got.Body != legacyRefusalBody || len(*write.seen) != 0 {
				t.Fatalf("mutation: got %d %s, ran %d; want 403 %s and no resolver", got.Status, got.Body, len(*write.seen), legacyRefusalBody)
			}
			read := legacyStack(t, minter, legacyStore(legacySession(), nil), false)
			if got := legacyGraphQLDocument(read, http.MethodPost, token, iaDocument); got.Status != http.StatusOK || len(*read.seen) != 1 || !(*read.seen)[0].ImpersonationActive {
				t.Fatalf("query: got %d %s claims=%+v, want 200 served as the target", got.Status, got.Body, *read.seen)
			}
		})
	}
}

// legacyQueryDocument is legacyQuery with the document named.
func legacyQueryDocument(s *behaviourStack, listener, path, bearer, document string) legacyAnswer {
	body, _ := json.Marshal(map[string]string{"query": document})
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+bearer)
	recorder := httptest.NewRecorder()
	s.handlers[listener].ServeHTTP(recorder, request)
	header := recorder.Header().Clone()
	header.Del("X-Request-ID")
	return legacyAnswer{recorder.Code, strings.TrimSpace(recorder.Body.String()), header}
}
