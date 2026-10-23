package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens/withissuer"
)

// The MCP caller-class listener takes its identity from the internal identity headers only and refuses any
// Authorization header: every kind of user token, with otherwise valid MCP headers, is refused 401 with the
// authorization reason before any ClickHouse call (counted), and the answer does not echo the token.
func TestMCPRefusesUserTokensAsAuthorization(t *testing.T) {
	tokens := withissuer.All(t)
	l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
	for _, tok := range tokens {
		header := validMCPHeaders()
		header.Set("Authorization", "Bearer "+tok.Value)
		rec := mcpDo(l.mcp, http.MethodPost, header, mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
		assertMCPRefused(t, rec, l.ch, http.StatusUnauthorized, mcpReasonAuthorization)
		usertokens.NoLeak(t, "the MCP refusal body ("+tok.Kind+")", rec.Body.String(), tokens)
	}
}

// /buildinfo accepts the internal identity headers or the effective-principal envelope, never the user's
// edge access token: every kind of user token as the bearer is refused 401 (it is no envelope), and the
// control shows a genuine envelope is accepted by the same handler.
func TestBuildInfoRefusesUserTokens(t *testing.T) {
	tokens := withissuer.All(t)
	verifier, priv := iaVerifier(t)
	handler := newBuildInfoHandler(verifier)
	for _, tok := range tokens {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/buildinfo", nil)
		request.Header.Set("Authorization", "Bearer "+tok.Value)
		handler(recorder, request)
		if recorder.Code != http.StatusUnauthorized {
			t.Errorf("%s: /buildinfo answered %d, want 401", tok.Kind, recorder.Code)
		}
		usertokens.NoLeak(t, "the /buildinfo refusal body ("+tok.Kind+")", recorder.Body.String(), tokens)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/buildinfo", nil)
	request.Header.Set("Authorization", "Bearer "+iaEnvelope(t, priv, principal.Claims{OrgID: "org-1", Role: "member"}))
	handler(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("control: a genuine envelope was refused by /buildinfo: %d %s", recorder.Code, recorder.Body.String())
	}
}
