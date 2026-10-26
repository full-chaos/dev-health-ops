package apiservice

import (
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

func i64ptr(v int64) *int64 { return &v }

// A user access token on a widened row (GET /api/v1/auth/me, the real row
// table) is refused when its session began at another users.token_version.
// probeUser's token_version is 3.
func TestUserTokenOnAWidenedRowFollowsTheSessionTokenVersionPin(t *testing.T) {
	minter := newEdDSAMinter(t)
	store := newProbeStore()
	sessions := map[string]struct {
		sid    uuid.UUID
		pin    *int64
		status int
	}{
		"pin older than the user token_version": {uuid.MustParse("00000000-0000-4000-8000-0000000000e1"), i64ptr(2), http.StatusUnauthorized},
		"pin newer than the user token_version": {uuid.MustParse("00000000-0000-4000-8000-0000000000e2"), i64ptr(4), http.StatusUnauthorized},
		"pin equal to the user token_version":   {uuid.MustParse("00000000-0000-4000-8000-0000000000e3"), i64ptr(3), http.StatusOK},
		// This row is expected to change once sessions without a pin are refused.
		"passes while unpinned": {uuid.MustParse("00000000-0000-4000-8000-0000000000e4"), nil, http.StatusOK},
	}
	for _, tc := range sessions {
		store.sessions[tc.sid] = policy.SessionRow{PrincipalID: probeUser, LegacyTokenVersion: tc.pin}
	}
	f := newRoutedFixture(t, store, realRows(t), probeIssuer, minter.keys, nil, probeNow)
	for name, tc := range sessions {
		t.Run(name, func(t *testing.T) {
			token := minter.mint(probeUser, map[string]any{"sid": sessionid.Wire(tc.sid)})
			got := probeDo(f.handler, probeRequest{name, http.MethodGet, probeAuthenticated, bearer(token)})
			if got.Status != tc.status {
				t.Fatalf("%d %s, want %d", got.Status, got.Body, tc.status)
			}
			if tc.status == http.StatusUnauthorized && got.Body != genericRefusal {
				t.Fatalf("refusal is not the generic answer: %s", got.Body)
			}
		})
	}
}
