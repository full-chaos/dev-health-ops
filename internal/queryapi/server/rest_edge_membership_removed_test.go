package server

import (
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

func TestQueryAPIRESTEdgeTokenRefusedAfterMembershipRemoved(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	edge := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})
	for name, routed := range map[string]bool{"routed": true, "unrouted": false} {
		t.Run(name, func(t *testing.T) {
			mk := func(mutate func(*accessStore)) *restFixture {
				store := newAccessStore()
				if mutate != nil {
					mutate(store)
				}
				var setup *accessSetup
				if routed {
					setup = testAccess(m.keys, nil, accessTestNow)
				}
				return newRESTFixture(t, store, setup, true)
			}
			if got := mk(nil).do(http.MethodGet, edge); got.Status != http.StatusOK {
				t.Fatalf("member: got %+v, want 200", got)
			}
			if got := mk(func(s *accessStore) { s.members = nil }).do(http.MethodGet, edge); got.Status != http.StatusUnauthorized || got.Body != restRefusal {
				t.Fatalf("membership removed: got %+v, want 401 %s", got, restRefusal)
			}
			if got := mk(func(s *accessStore) { s.errIsMember = policy.ErrUnavailable }).do(http.MethodGet, edge); got.Status != http.StatusServiceUnavailable {
				t.Fatalf("membership store down: got %+v, want 503", got)
			}
		})
	}
}
