package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCreateOrgInviteAnswers503WithoutLinkSecret(t *testing.T) {
	h := &handlers{}
	request := httptest.NewRequest(http.MethodPost, "/orgs/00000000-0000-0000-0000-000000000001/invites", nil)
	request.SetPathValue("org_id", "00000000-0000-0000-0000-000000000001")
	recorder := httptest.NewRecorder()
	h.createOrgInvite(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "config_missing: LINK_TOKEN_SECRET") {
		t.Fatalf("status %d body %q, want 503 config_missing", recorder.Code, recorder.Body.String())
	}
}
