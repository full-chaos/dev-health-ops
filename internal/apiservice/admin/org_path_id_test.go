package admin

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestMalformedOrgIDIsANamed400OnEveryOrgRoute pins CHAOS-9014: GET
// /api/v1/admin/orgs/current is no route, it matches the {org_id} wildcard,
// and a value that is no UUID answered 500. Every org route that reads the
// wildcard now answers the named 400 that DELETE /orgs/{org_id} already gave.
func TestMalformedOrgIDIsANamed400OnEveryOrgRoute(t *testing.T) {
	h := &handlers{}
	routes := map[string]http.HandlerFunc{
		"get org":        h.getOrganization,
		"update org":     h.updateOrganization,
		"list members":   h.listMembers,
		"add member":     h.addMember,
		"update member":  h.updateMemberRole,
		"remove member":  h.removeMember,
		"transfer owner": h.transferOwnership,
		"create invite":  h.createOrgInvite,
		"delete org":     h.deleteOrganization,
	}
	for name, handler := range routes {
		for _, value := range []string{"current", "not-a-uuid", "", "1234"} {
			t.Run(name+"/"+value, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/orgs/x", nil)
				req.SetPathValue("org_id", value)
				rec := httptest.NewRecorder()
				handler(rec, req)
				if rec.Code != http.StatusBadRequest || rec.Body.String() != `{"detail":"Invalid organization id"}` {
					t.Fatalf("status = %d body = %s, want 400 Invalid organization id", rec.Code, rec.Body.String())
				}
			})
		}
	}
}

func TestMalformedUserIDIsANamed400(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetPathValue("user_id", "me")
	rec := httptest.NewRecorder()
	if _, ok := pathUserID(rec, req); ok || rec.Code != http.StatusBadRequest || rec.Body.String() != `{"detail":"Invalid user id"}` {
		t.Fatalf("ok/status/body = %d %s, want 400 Invalid user id", rec.Code, rec.Body.String())
	}
}
