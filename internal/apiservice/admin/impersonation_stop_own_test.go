package admin

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

// stopAs posts the stop route as admin, with a body that names another admin's target and open row: the stop reads
// neither.
func (c *delegatedCase) stopAs(admin, otherTarget, otherRow uuid.UUID) *httptest.ResponseRecorder {
	raw := `{"target_user_id":"` + otherTarget.String() + `","session_id":"` + otherRow.String() + `","admin_user_id":"` + otherTarget.String() + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/impersonate/stop", strings.NewReader(raw))
	body, _, _, _ := pybody.Read(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(raw)))
	user := &policy.User{UserID: admin.String(), SessionID: &c.sid}
	rec := httptest.NewRecorder()
	c.h.impersonateStop().ServeHTTP(rec, req.WithContext(policy.WithBody(policy.WithUser(req.Context(), user), body)))
	return rec
}

// The stop route ends only the caller's own open impersonation row, on go-api's own stop and through auth-service:
// the row is looked up by the caller's user id, and the body's ids are never read. A caller with no open row of their
// own ends nothing, while another admin's row stays open.
func TestStopEndsOnlyTheCallersOwnImpersonation(t *testing.T) {
	for _, viaAuthService := range []bool{false, true} {
		name := "go-api stop"
		if viaAuthService {
			name = "auth-service stop"
		}
		t.Run(name, func(t *testing.T) {
			c := newDelegatedCase(viaAuthService)
			other, ownRow, otherRow := uuid.New(), uuid.New(), uuid.New()
			c.db.openBy = map[uuid.UUID]uuid.UUID{c.admin: ownRow, other: otherRow}
			rec := c.stopAs(c.admin, other, otherRow)
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"stopped"`) {
				t.Fatalf("own stop: %d %s", rec.Code, rec.Body.String())
			}
			ended := c.db.ended
			if viaAuthService {
				ended = c.delegator.ended
			}
			if !slices.Equal(ended, []uuid.UUID{ownRow}) {
				t.Errorf("ended %v, want only the caller's row %s", ended, ownRow)
			}
			if !slices.Equal(c.db.lookups, []uuid.UUID{c.admin}) {
				t.Errorf("lookups by %v, want only the caller %s", c.db.lookups, c.admin)
			}

			none := newDelegatedCase(viaAuthService)
			none.db.openBy = map[uuid.UUID]uuid.UUID{other: otherRow}
			rec = none.stopAs(none.admin, other, otherRow)
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "No active impersonation session") {
				t.Fatalf("stop with no own row: %d %s", rec.Code, rec.Body.String())
			}
			if len(none.db.ended) != 0 || len(none.delegator.ended) != 0 {
				t.Errorf("ended %v / %v, want nothing", none.db.ended, none.delegator.ended)
			}
		})
	}
}
