package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// bootstrapBearer is a value of the Compose workload bootstrap credential's
// shape (fcwlb_ and 43 base64url characters), built at run time. That
// credential is accepted only at auth-service's workload-token exchange.
func bootstrapBearer() string { return "fcwlb_" + strings.Repeat("Bq", 21) + "Z" }

// G-36 on query-api: the workload bootstrap credential as a bearer is no
// credential at either authenticated surface: the MCP listener refuses it
// 401 before any ClickHouse call, and /buildinfo refuses it 401 (it is no
// envelope); neither answer echoes it.
func TestABootstrapCredentialOpensNoQueryAPISurface(t *testing.T) {
	value := bootstrapBearer()
	if len(value) != len("fcwlb_")+43 {
		t.Fatalf("the test value has %d characters", len(value))
	}
	l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
	header := validMCPHeaders()
	header.Set("Authorization", "Bearer "+value)
	rec := mcpDo(l.mcp, http.MethodPost, header, mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
	assertMCPRefused(t, rec, l.ch, http.StatusUnauthorized, mcpReasonAuthorization)
	if strings.Contains(rec.Body.String(), value) {
		t.Fatal("the MCP refusal echoes the value")
	}

	verifier, _ := iaVerifier(t)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/buildinfo", nil)
	request.Header.Set("Authorization", "Bearer "+value)
	newBuildInfoHandler(verifier)(recorder, request)
	if recorder.Code != http.StatusUnauthorized || strings.Contains(recorder.Body.String(), value) {
		t.Fatalf("/buildinfo answered %d %q, want 401 without the value", recorder.Code, recorder.Body.String())
	}
}
