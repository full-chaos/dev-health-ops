package admin

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

const inviteGuardOrg = "00000000-0000-0000-0000-000000000001"

// inviteCaller runs createOrgInvite with no link secret as the non-superuser provActor, whose role in the org is
// role ("" = no membership row).
func inviteCaller(role string) *httptest.ResponseRecorder {
	rows := map[string][]any{}
	if role != "" {
		rows["SELECT role FROM memberships WHERE org_id = $1 AND user_id = $2"] = []any{role}
	}
	h := &handlers{store: pgStore{Pool: &provisionDB{rows: rows}}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	request := httptest.NewRequest(http.MethodPost, "/orgs/"+inviteGuardOrg+"/invites", nil)
	request.SetPathValue("org_id", inviteGuardOrg)
	request = request.WithContext(policy.WithUser(request.Context(), &policy.User{UserID: validUUID, ID: provActor}))
	recorder := httptest.NewRecorder()
	h.createOrgInvite(recorder, request)
	return recorder
}

func TestCreateOrgInviteAnswers503WithoutLinkSecret(t *testing.T) {
	for _, role := range []string{"owner", "admin"} {
		recorder := inviteCaller(role)
		if recorder.Code != http.StatusServiceUnavailable || !(strings.Contains(recorder.Body.String(), `"reason_code":"config_missing"`) && strings.Contains(recorder.Body.String(), "LINK_TOKEN_SECRET")) {
			t.Fatalf("org %s: status %d body %q, want 503 config_missing", role, recorder.Code, recorder.Body.String())
		}
	}
}

func TestCreateOrgInviteRefusesANonAdminBeforeTheLinkSecretCheck(t *testing.T) {
	for _, role := range []string{"", "member", "viewer"} {
		recorder := inviteCaller(role)
		if recorder.Code != http.StatusForbidden || !strings.Contains(recorder.Body.String(), "Admin access required for organization") || strings.Contains(recorder.Body.String(), "config_missing") {
			t.Fatalf("role %q: status %d body %q, want 403 Admin access required for organization", role, recorder.Code, recorder.Body.String())
		}
	}
}
