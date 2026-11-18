package acr

import (
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

// The principal access read stays strict while the other go-api routes read
// their query with httpapi.Query: a pair the shared reader would keep (a ";",
// a bad escape, invalid UTF-8) is refused here, never read as a sid.
func TestPrincipalAccessStaysStrictOnPairsTheSharedReaderKeeps(t *testing.T) {
	wire := sessionid.Wire(accessSession)
	for _, query := range []string{"sid=" + wire + ";", "sid=" + wire + "&x=a;b", "sid=" + wire + "&x=%zz", "sid=" + wire + "%zz"} {
		store := &fakeAccessStore{}
		rec := do(accessServer(t, store, accessCreds(), Deps{}), accessPath(accessOrg.String(), accessUser.String(), query), "Bearer "+accessToken)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d %s, want 400", query, rec.Code, rec.Body.String())
			continue
		}
		if code, _ := decodeBody(t, rec)["code"].(string); code != InvalidQueryCode && code != InvalidSessionCode {
			t.Errorf("%s: code %q, want %s or %s", query, code, InvalidQueryCode, InvalidSessionCode)
		}
		if len(store.queries) != 0 {
			t.Errorf("%s: a refused request read the store", query)
		}
	}
}
